"""
Lambda 7: Memory Writer (Unified S3 Architecture)
==================================================
Writes conversation entries to the memory layer.

Trigger:
    - API invocation from app or Lambda 06 async call

Flow:
    - Write memory item(s) to DynamoDB
    - Optionally index into OpenSearch
    - Support delete, compress, and metadata updates

Features:
    - Session-based memory storage
    - Automatic TTL management
    - Conversation threading
    - Context summarization

Configuration:
    - Memory: 512 MB
    - Timeout: 30 seconds
    - Runtime: Python 3.12
    - Layers: None (boto3 only)
    - Trigger: API Gateway or Lambda 06 async call

Environment Variables:
    - AWS_DEFAULT_REGION: AWS region (default: ca-central-1)
    - MEMORY_TABLE: mech-avatar-memory
    - MEMORY_TTL_HOURS, MAX_CONTENT_LENGTH
    - OPENSEARCH_HOST (optional)
    - OPENSEARCH_MEMORY_INDEX, OPENSEARCH_EMBEDDING_FIELD
    - EMBEDDING_MODEL
    - ENABLE_CLOUDWATCH_METRICS
    - DLQ_URL (optional), ENABLE_DLQ (default: true)

Settings:
    - OpenSearch indexing is best effort
    - Circuit breaker wraps Bedrock and OpenSearch calls
"""

import json
import boto3
import os
import hashlib
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from decimal import Decimal
from pydantic import BaseModel, ConfigDict, ValidationError
from botocore.exceptions import BotoCoreError, ClientError

try:
    from opensearchpy import OpenSearch, RequestsHttpConnection
    from requests_aws4auth import AWS4Auth
except Exception:  # Optional dependency for vector indexing
    OpenSearch = None
    RequestsHttpConnection = None
    AWS4Auth = None

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Correlation ID for distributed tracing
_correlation_id: Optional[str] = None


def set_correlation_id(request_id: str) -> None:
    """Set correlation ID for request tracing."""
    global _correlation_id
    _correlation_id = request_id


def log_event(event_type: str, message: str, **kwargs) -> None:
    """Structured logging with correlation ID and context."""
    log_data = {
        'correlation_id': _correlation_id,
        'event': event_type,
        'message': message,
        'timestamp': datetime.utcnow().isoformat(),
        **kwargs
    }
    logger.info(json.dumps(log_data))

# ============================================================================
# CONSTANTS & CONFIGURATION
# ============================================================================

AWS_REGION = os.environ.get('AWS_DEFAULT_REGION', 'ca-central-1')
MEMORY_TABLE = os.environ.get('MEMORY_TABLE', 'mechavatar-lab-cac1-mech-user-queires')

# Memory configuration
MEMORY_TTL_HOURS = int(os.environ.get('MEMORY_TTL_HOURS', 24))
MAX_CONTENT_LENGTH = int(os.environ.get('MAX_CONTENT_LENGTH', 10000))
ENABLE_CLOUDWATCH_METRICS = os.environ.get('ENABLE_CLOUDWATCH_METRICS', 'true').lower() == 'true'
DLQ_URL = os.environ.get('DLQ_URL')
ENABLE_DLQ = os.environ.get('ENABLE_DLQ', 'true').lower() == 'true'

# OpenSearch (optional vector memory indexing) - Updated
OPENSEARCH_HOST = os.environ.get('OPENSEARCH_HOST', '')
OPENSEARCH_MEMORY_INDEX = os.environ.get('OPENSEARCH_MEMORY_INDEX', 'mechavatar-lab-cac1-memory')
OPENSEARCH_EMBEDDING_FIELD = os.environ.get('OPENSEARCH_EMBEDDING_FIELD', 'embedding')
EMBEDDING_MODEL = (
    os.environ.get('EMBEDDING_INFERENCE_PROFILE_ARN')
    or os.environ.get('EMBEDDING_MODEL', 'amazon.titan-embed-text-v2:0')
)

# ============================================================================
# AWS CLIENTS (Lazy Loading)
# ============================================================================

_clients: Dict[str, Any] = {}
_cloudwatch_client = None
_opensearch_client = None
_dlq_client = None
_circuit_breakers: Dict[str, Any] = {}


class CircuitBreaker:
    """Simple circuit breaker for downstream dependencies."""

    def __init__(self, threshold: int = 5, recovery_seconds: int = 30):
        self.threshold = threshold
        self.recovery_seconds = recovery_seconds
        self.failures = 0
        self.open_until = 0.0

    def allow(self) -> bool:
        if self.open_until == 0.0:
            return True
        return time.time() >= self.open_until

    def record_success(self) -> None:
        self.failures = 0
        self.open_until = 0.0

    def record_failure(self) -> None:
        self.failures += 1
        if self.failures >= self.threshold:
            self.open_until = time.time() + self.recovery_seconds


def _get_circuit_breaker(name: str) -> CircuitBreaker:
    if name not in _circuit_breakers:
        _circuit_breakers[name] = CircuitBreaker()
    return _circuit_breakers[name]


def get_resource(service: str):
    """Get or create AWS resource."""
    key = f"{service}_resource"
    if key not in _clients:
        _clients[key] = boto3.resource(service, region_name=AWS_REGION)
    return _clients[key]


def get_client(service: str):
    """Get or create AWS client."""
    if service not in _clients:
        _clients[service] = boto3.client(service, region_name=AWS_REGION)
    return _clients[service]


def get_cloudwatch_client():
    """Get or create CloudWatch client."""
    global _cloudwatch_client
    if _cloudwatch_client is None:
        _cloudwatch_client = boto3.client('cloudwatch', region_name=AWS_REGION)
    return _cloudwatch_client


def get_dlq_client():
    """Get or create SQS client for DLQ dispatch."""
    global _dlq_client
    if _dlq_client is None:
        _dlq_client = boto3.client('sqs', region_name=AWS_REGION)
    return _dlq_client


def send_to_dlq(payload: Dict[str, Any], error: Exception, error_type: str) -> None:
    """Send failed events to DLQ for offline inspection."""
    if not ENABLE_DLQ or not DLQ_URL:
        return
    try:
        message = {
            'error_type': error_type,
            'error_message': str(error),
            'payload': payload,
            'timestamp': datetime.utcnow().isoformat()
        }
        get_dlq_client().send_message(
            QueueUrl=DLQ_URL,
            MessageBody=json.dumps(message, default=str)
        )
    except Exception as dlq_error:
        logger.warning(f"DLQ send failed: {dlq_error}")


def call_with_circuit_breaker(name: str, func, *args, **kwargs):
    breaker = _get_circuit_breaker(name)
    if not breaker.allow():
        raise RuntimeError(f"Circuit breaker open for {name}")
    try:
        result = func(*args, **kwargs)
        breaker.record_success()
        return result
    except Exception:
        breaker.record_failure()
        raise


class MemoryWriteEvent(BaseModel):
    model_config = ConfigDict(extra='allow')
    operation: Optional[str] = 'write'
    session_id: Optional[str] = None
    role: Optional[str] = None
    content: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    ttl_hours: Optional[int] = None
    user_message: Optional[str] = None
    assistant_response: Optional[str] = None
    max_items: Optional[int] = None
    timestamp: Optional[str] = None
    metadata_updates: Optional[Dict[str, Any]] = None


def get_opensearch_client():
    """Get OpenSearch client with IAM auth if configured."""
    global _opensearch_client
    if _opensearch_client is not None:
        return _opensearch_client
    if not OPENSEARCH_HOST or OpenSearch is None or AWS4Auth is None:
        return None

    host = OPENSEARCH_HOST.replace('https://', '').replace('http://', '').rstrip('/')
    credentials = boto3.Session().get_credentials()
    auth = AWS4Auth(credentials.access_key, credentials.secret_key,
                    AWS_REGION, 'es', session_token=credentials.token)

    _opensearch_client = OpenSearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=30
    )
    return _opensearch_client


def emit_metric(name: str, value: float, unit: str = 'Count', dimensions: Dict[str, str] = None) -> None:
    """Emit CloudWatch metric for observability."""
    if not ENABLE_CLOUDWATCH_METRICS:
        return
    try:
        metric = {
            'MetricName': name,
            'Value': value,
            'Unit': unit,
            'Timestamp': datetime.utcnow()
        }
        if dimensions:
            metric['Dimensions'] = [{'Name': k, 'Value': str(v)} for k, v in dimensions.items()]
        get_cloudwatch_client().put_metric_data(
            Namespace='MECH-Avatar/MemoryWriter',
            MetricData=[metric]
        )
    except Exception as e:
        logger.warning(f"Metric emit failed: {e}")


# ============================================================================
# MEMORY OPERATIONS
# ============================================================================

def generate_embedding(text: str, max_retries: int = 3) -> List[float]:
    """Generate embedding for memory indexing."""
    client = get_client('bedrock-runtime')

    for attempt in range(max_retries):
        try:
            response = call_with_circuit_breaker(
                'bedrock',
                client.invoke_model,
                modelId=EMBEDDING_MODEL,
                contentType='application/json',
                body=json.dumps({'inputText': text})
            )
            result = json.loads(response['body'].read().decode('utf-8'))
            return result.get('embedding', [])
        except client.exceptions.ThrottlingException:
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) + 0.5
                logger.warning(f"Bedrock throttled, waiting {wait_time}s (attempt {attempt + 1})")
                time.sleep(wait_time)
            else:
                raise
        except Exception as e:
            if attempt < max_retries - 1 and 'throttl' in str(e).lower():
                wait_time = (2 ** attempt) + 0.5
                logger.warning(f"Bedrock error, retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
            else:
                raise


def index_memory_item(item_id: str, session_id: str, timestamp: str, role: str,
                      content: str, metadata: Dict = None) -> None:
    """Index memory item into OpenSearch for semantic search."""
    client = get_opensearch_client()
    if not client:
        return

    try:
        embedding = generate_embedding(content)
        if not embedding:
            return

        body = {
            'session_id': session_id,
            'timestamp': timestamp,
            'role': role,
            'content': content,
            'metadata': metadata or {},
            OPENSEARCH_EMBEDDING_FIELD: embedding,
            'indexed_at': datetime.utcnow().isoformat()
        }
        call_with_circuit_breaker(
            'opensearch',
            client.index,
            index=OPENSEARCH_MEMORY_INDEX,
            id=item_id,
            body=body
        )
    except Exception as e:
        logger.warning(f"Memory index failed: {e}")


def delete_memory_index_items(session_id: str, item_ids: List[str] = None) -> None:
    """Best-effort delete for memory items in OpenSearch."""
    client = get_opensearch_client()
    if not client:
        return

    try:
        if item_ids:
            for item_id in item_ids:
                try:
                    call_with_circuit_breaker(
                        'opensearch',
                        client.delete,
                        index=OPENSEARCH_MEMORY_INDEX,
                        id=item_id
                    )
                except Exception:
                    pass
        else:
            query = {
                "query": {
                    "term": {"session_id": session_id}
                }
            }
            call_with_circuit_breaker(
                'opensearch',
                client.delete_by_query,
                index=OPENSEARCH_MEMORY_INDEX,
                body=query,
                conflicts='proceed'
            )
    except Exception as e:
        logger.warning(f"Memory index delete failed: {e}")

def write_memory_item(session_id: str, role: str, content: str,
                      metadata: Dict = None, ttl_hours: int = None) -> Dict[str, Any]:
    """
    Write a single memory item to DynamoDB.
    
    Args:
        session_id: Unique session identifier
        role: 'user' or 'assistant'
        content: Message content
        metadata: Optional additional metadata
        ttl_hours: Custom TTL (defaults to MEMORY_TTL_HOURS)
    
    Returns:
        Created item details
    """
    try:
        table = get_resource('dynamodb').Table(MEMORY_TABLE)
        
        # Generate timestamp with microsecond precision for uniqueness
        timestamp = datetime.utcnow().isoformat() + 'Z'
        
        # Calculate TTL
        ttl = ttl_hours or MEMORY_TTL_HOURS
        expires_at = int((datetime.utcnow() + timedelta(hours=ttl)).timestamp())
        
        # Truncate content if too long
        if len(content) > MAX_CONTENT_LENGTH:
            content = content[:MAX_CONTENT_LENGTH] + '... [truncated]'
        
        # Create item
        item = {
            'session_id': session_id,
            'timestamp': timestamp,
            'role': role,
            'content': content,
            'created_at': datetime.utcnow().isoformat(),
            'expires_at': expires_at  # DynamoDB TTL field
        }
        
        # Add optional metadata
        if metadata:
            item['metadata'] = metadata
        
        # Write to DynamoDB
        table.put_item(Item=item)

        # Best-effort index for semantic search
        item_id = f"{session_id}#{timestamp}"
        index_memory_item(item_id, session_id, timestamp, role, content, metadata)
        
        logger.info(f"Wrote memory item: {session_id}/{timestamp}")
        
        return {
            'session_id': session_id,
            'timestamp': timestamp,
            'role': role,
            'expires_at': datetime.utcfromtimestamp(expires_at).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error writing memory item: {e}")
        raise


def write_conversation_pair(session_id: str, user_message: str, 
                            assistant_response: str, metadata: Dict = None) -> Dict[str, Any]:
    """
    Write a user-assistant conversation pair to memory.
    
    Ensures proper ordering of messages.
    """
    try:
        # Write user message first
        user_result = write_memory_item(
            session_id=session_id,
            role='user',
            content=user_message,
            metadata={'type': 'query', **(metadata or {})}
        )
        
        # Small delay for timestamp ordering
        time.sleep(0.001)
        
        # Write assistant response
        assistant_result = write_memory_item(
            session_id=session_id,
            role='assistant',
            content=assistant_response,
            metadata={'type': 'response', **(metadata or {})}
        )
        
        return {
            'session_id': session_id,
            'user_timestamp': user_result['timestamp'],
            'assistant_timestamp': assistant_result['timestamp'],
            'messages_written': 2
        }
        
    except Exception as e:
        logger.error(f"Error writing conversation pair: {e}")
        raise


def delete_session_memory(session_id: str) -> Dict[str, Any]:
    """
    Delete all memory items for a session.
    """
    try:
        table = get_resource('dynamodb').Table(MEMORY_TABLE)
        
        # Query all items for session
        response = table.query(
            KeyConditionExpression='session_id = :sid',
            ExpressionAttributeValues={':sid': session_id},
            ProjectionExpression='session_id, #ts',
            ExpressionAttributeNames={'#ts': 'timestamp'}
        )
        
        items = response.get('Items', [])
        deleted_count = 0
        
        # Batch delete
        with table.batch_writer() as batch:
            for item in items:
                batch.delete_item(Key={
                    'session_id': item['session_id'],
                    'timestamp': item['timestamp']
                })
                deleted_count += 1

        delete_memory_index_items(session_id)
        
        logger.info(f"Deleted {deleted_count} memory items for session {session_id}")
        
        return {
            'session_id': session_id,
            'deleted_count': deleted_count
        }
        
    except Exception as e:
        logger.error(f"Error deleting session memory: {e}")
        raise


def summarize_and_compress(session_id: str, max_items: int = 10) -> Dict[str, Any]:
    """
    Summarize older conversation items to save space.
    
    Keeps recent items, compresses older ones into summary.
    """
    try:
        table = get_resource('dynamodb').Table(MEMORY_TABLE)
        
        # Get all items
        response = table.query(
            KeyConditionExpression='session_id = :sid',
            ExpressionAttributeValues={':sid': session_id},
            ScanIndexForward=True  # Oldest first
        )
        
        items = response.get('Items', [])
        
        if len(items) <= max_items:
            return {
                'session_id': session_id,
                'action': 'no_compression_needed',
                'item_count': len(items)
            }
        
        # Items to compress (keep the last max_items)
        items_to_compress = items[:-max_items]
        items_to_keep = items[-max_items:]
        
        # Create summary of compressed items
        summary_parts = []
        for item in items_to_compress:
            role = item.get('role', 'unknown')
            content = item.get('content', '')[:200]  # Truncate
            summary_parts.append(f"[{role}]: {content}...")
        
        summary_text = "CONVERSATION HISTORY SUMMARY:\n" + "\n".join(summary_parts)
        
        # Write summary as new item
        write_memory_item(
            session_id=session_id,
            role='system',
            content=summary_text,
            metadata={'type': 'summary', 'compressed_count': len(items_to_compress)}
        )
        
        # Delete compressed items
        with table.batch_writer() as batch:
            for item in items_to_compress:
                batch.delete_item(Key={
                    'session_id': item['session_id'],
                    'timestamp': item['timestamp']
                })

        compressed_ids = [f"{item['session_id']}#{item['timestamp']}" for item in items_to_compress]
        delete_memory_index_items(session_id, compressed_ids)
        
        logger.info(f"Compressed {len(items_to_compress)} items for session {session_id}")
        
        return {
            'session_id': session_id,
            'action': 'compressed',
            'compressed_count': len(items_to_compress),
            'remaining_count': len(items_to_keep) + 1  # +1 for summary
        }
        
    except Exception as e:
        logger.error(f"Error compressing memory: {e}")
        raise


def update_item_metadata(session_id: str, timestamp: str, 
                         metadata_updates: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update metadata for an existing memory item.
    """
    try:
        table = get_resource('dynamodb').Table(MEMORY_TABLE)
        
        # Build update expression
        update_parts = []
        expr_values = {}
        
        for key, value in metadata_updates.items():
            safe_key = key.replace('-', '_')
            update_parts.append(f"metadata.{safe_key} = :{safe_key}")
            expr_values[f':{safe_key}'] = value
        
        table.update_item(
            Key={
                'session_id': session_id,
                'timestamp': timestamp
            },
            UpdateExpression='SET ' + ', '.join(update_parts),
            ExpressionAttributeValues=expr_values
        )
        
        return {
            'session_id': session_id,
            'timestamp': timestamp,
            'updated_fields': list(metadata_updates.keys())
        }
        
    except Exception as e:
        logger.error(f"Error updating item metadata: {e}")
        raise


# ============================================================================
# LAMBDA HANDLER
# ============================================================================

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for memory write operations.
    
    Operations:
    - write: Write single memory item
    - write_pair: Write user-assistant conversation pair
    - delete_session: Delete all session memory
    - compress: Compress/summarize older items
    - update_metadata: Update item metadata
    
    Input:
    {
        "operation": "write",
        "session_id": "abc123",
        "role": "user",
        "content": "What is MECH?",
        "metadata": {...}
    }
    
    Output:
    {
        "statusCode": 200,
        "session_id": "abc123",
        "timestamp": "2024-01-01T00:00:00Z"
    }
    """
    start_time = datetime.utcnow()
    request_id = context.aws_request_id if context else 'local'
    set_correlation_id(request_id)
    log_event('request_start', 'Memory Writer invoked', request_id=request_id)
    
    try:
        event = MemoryWriteEvent.model_validate(event).model_dump()
        operation = event.get('operation', 'write')
        session_id = event.get('session_id')
        
        if not session_id:
            log_event('validation_error', 'Missing required parameter: session_id')
            return {
                'statusCode': 400,
                'error': 'Missing required parameter: session_id'
            }
        
        log_event('operation_start', 'Memory write operation', operation=operation, session_id=session_id)
        
        if operation == 'write':
            role = event.get('role')
            content = event.get('content')
            metadata = event.get('metadata')
            ttl_hours = event.get('ttl_hours')
            
            if not role or not content:
                log_event('validation_error', 'Missing required parameters: role, content')
                return {
                    'statusCode': 400,
                    'error': 'Missing required parameters: role, content'
                }
            
            result = write_memory_item(session_id, role, content, metadata, ttl_hours)
            
            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            emit_metric('Invocation', 1)
            emit_metric('DurationMs', duration_ms, 'Milliseconds')
            emit_metric('OperationSuccess', 1, dimensions={'operation': operation})
            log_event('request_complete', 'Memory write complete',
                      operation=operation, session_id=session_id, duration_ms=duration_ms)
            return {
                'statusCode': 200,
                **result
            }
        
        elif operation == 'write_pair':
            user_message = event.get('user_message')
            assistant_response = event.get('assistant_response')
            metadata = event.get('metadata')
            
            if not user_message or not assistant_response:
                log_event('validation_error', 'Missing required parameters: user_message, assistant_response')
                return {
                    'statusCode': 400,
                    'error': 'Missing required parameters: user_message, assistant_response'
                }
            
            result = write_conversation_pair(session_id, user_message, 
                                            assistant_response, metadata)
            
            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            emit_metric('Invocation', 1)
            emit_metric('DurationMs', duration_ms, 'Milliseconds')
            emit_metric('OperationSuccess', 1, dimensions={'operation': operation})
            log_event('request_complete', 'Memory write complete',
                      operation=operation, session_id=session_id, duration_ms=duration_ms)
            return {
                'statusCode': 200,
                **result
            }
        
        elif operation == 'delete_session':
            result = delete_session_memory(session_id)
            
            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            emit_metric('Invocation', 1)
            emit_metric('DurationMs', duration_ms, 'Milliseconds')
            emit_metric('OperationSuccess', 1, dimensions={'operation': operation})
            log_event('request_complete', 'Memory write complete',
                      operation=operation, session_id=session_id, duration_ms=duration_ms)
            return {
                'statusCode': 200,
                **result
            }
        
        elif operation == 'compress':
            max_items = event.get('max_items', 10)
            result = summarize_and_compress(session_id, max_items)
            
            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            emit_metric('Invocation', 1)
            emit_metric('DurationMs', duration_ms, 'Milliseconds')
            emit_metric('OperationSuccess', 1, dimensions={'operation': operation})
            log_event('request_complete', 'Memory write complete',
                      operation=operation, session_id=session_id, duration_ms=duration_ms)
            return {
                'statusCode': 200,
                **result
            }
        
        elif operation == 'update_metadata':
            timestamp = event.get('timestamp')
            metadata_updates = event.get('metadata_updates', {})
            
            if not timestamp or not metadata_updates:
                log_event('validation_error', 'Missing required parameters: timestamp, metadata_updates')
                return {
                    'statusCode': 400,
                    'error': 'Missing required parameters: timestamp, metadata_updates'
                }
            
            result = update_item_metadata(session_id, timestamp, metadata_updates)
            
            duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            emit_metric('Invocation', 1)
            emit_metric('DurationMs', duration_ms, 'Milliseconds')
            emit_metric('OperationSuccess', 1, dimensions={'operation': operation})
            log_event('request_complete', 'Memory write complete',
                      operation=operation, session_id=session_id, duration_ms=duration_ms)
            return {
                'statusCode': 200,
                **result
            }
        
        else:
            log_event('validation_error', f'Unknown operation: {operation}')
            return {
                'statusCode': 400,
                'error': f'Unknown operation: {operation}'
            }
        
    except ValidationError as e:
        emit_metric('Invocation', 1)
        emit_metric('Failure', 1)
        log_event('request_error', 'Invalid input payload', errors=e.errors())
        send_to_dlq(event if isinstance(event, dict) else {'event': str(event)}, e, 'validation_error')
        return {
            'statusCode': 400,
            'error': 'Invalid input payload'
        }

    except (ClientError, BotoCoreError) as e:
        emit_metric('Invocation', 1)
        emit_metric('Failure', 1)
        log_event('request_error', f"AWS service error: {e}")
        send_to_dlq(event if isinstance(event, dict) else {'event': str(event)}, e, 'aws_error')
        return {
            'statusCode': 502,
            'error': str(e)
        }

    except Exception as e:
        emit_metric('Invocation', 1)
        emit_metric('Failure', 1)
        log_event('request_error', f"Memory write failed: {e}")
        logger.error(f"Memory write error: {e}", exc_info=True)
        send_to_dlq(event if isinstance(event, dict) else {'event': str(event)}, e, 'unexpected_error')

        return {
            'statusCode': 500,
            'error': str(e)
        }
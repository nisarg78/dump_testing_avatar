"""
Lambda 03: Embeddings Generator
================================
MECH Avatar - BMO Lab Account (939620275271)

Generates vector embeddings for document chunks using AWS Bedrock Titan V2.

S3 Structure:
    Input:  mechavatar-lab-cac1-s3-inbound/chunks/{document_id}.jsonl
    Output: mechavatar-lab-cac1-s3-inbound/embeddings/{document_id}.jsonl

Model: Amazon Titan Embeddings V2 (1024 dimensions)

Environment Variables:
    AWS_DEFAULT_REGION              = ca-central-1
    AWS_ACCOUNT_ID                  = 939620275271
    BUCKET_NAME                     = mechavatar-lab-cac1-s3-inbound
    PIPELINE_TABLE                  = mechavatar-lab-cac1-mech-processing-pipeline
    EMBEDDING_INFERENCE_PROFILE_ARN = amazon.titan-embed-text-v2:0
    EMBEDDING_DIMENSIONS            = 1024

Lambda Configuration:
    Memory: 2048 MB | Timeout: 600s | Runtime: Python 3.11
    Trigger: Step Functions after Lambda 02
"""

import json
import boto3
import os
import logging
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from pydantic import BaseModel, ConfigDict, ValidationError
from botocore.exceptions import BotoCoreError, ClientError

# Import centralized BMO configuration
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from config import (
        AWS_REGION, AWS_ACCOUNT_ID, BUCKET_NAME, PIPELINE_TABLE,
        BEDROCK_EMBEDDING_MODEL
    )
    EMBEDDING_MODEL = BEDROCK_EMBEDDING_MODEL
except ImportError:
    AWS_REGION = os.environ.get('AWS_DEFAULT_REGION', 'ca-central-1')
    AWS_ACCOUNT_ID = os.environ.get('AWS_ACCOUNT_ID', '939620275271')
    BUCKET_NAME = os.environ.get('BUCKET_NAME', 'mechavatar-lab-cac1-s3-inbound')
    PIPELINE_TABLE = os.environ.get('PIPELINE_TABLE', 'mechavatar-lab-cac1-mech-processing-pipeline')
    EMBEDDING_MODEL = os.environ.get('EMBEDDING_INFERENCE_PROFILE_ARN', 'amazon.titan-embed-text-v2:0')

EMBEDDING_DIMENSIONS = 1024

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_correlation_id: Optional[str] = None


def set_correlation_id(request_id: str) -> None:
    global _correlation_id
    _correlation_id = request_id


def log_event(event_type: str, message: str, **kwargs) -> None:
    log_data = {
        'correlation_id': _correlation_id,
        'event': event_type,
        'message': message,
        'timestamp': datetime.utcnow().isoformat(),
        **kwargs
    }
    logger.info(json.dumps(log_data))

# ============================================================================
# BATCH CONFIGURATION (env vars for tuning)
# ============================================================================

BATCH_SIZE = int(os.environ.get('BATCH_SIZE', 25))
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', 3))
RETRY_DELAY = float(os.environ.get('RETRY_DELAY', 0.5))

# Text limits
MAX_TEXT_LENGTH = 40000  # Characters per chunk for embedding


# ============================================================================
# AWS CLIENTS
# ============================================================================

_clients: Dict[str, Any] = {}
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


def get_client(service: str):
    """Get or create AWS client."""
    if service not in _clients:
        _clients[service] = boto3.client(service, region_name=AWS_REGION)
    return _clients[service]


def get_resource(service: str):
    """Get or create AWS resource."""
    key = f"{service}_resource"
    if key not in _clients:
        _clients[key] = boto3.resource(service, region_name=AWS_REGION)
    return _clients[key]


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


class EmbeddingEvent(BaseModel):
    model_config = ConfigDict(extra='allow')
    document_id: str
    chunks_s3_key: str
    chunk_count: Optional[int] = 0
    file_name: Optional[str] = None
    file_type: Optional[str] = None


def get_pipeline_status(document_id: str) -> Optional[str]:
    """Read current pipeline status for idempotency checks."""
    try:
        table = get_resource('dynamodb').Table(PIPELINE_TABLE)
        response = table.get_item(
            Key={'document_id': document_id},
            ProjectionExpression='#s',
            ExpressionAttributeNames={'#s': 'status'}
        )
        return response.get('Item', {}).get('status')
    except Exception as e:
        logger.warning(f"Failed to read pipeline status: {e}")
        return None


# ============================================================================
# EMBEDDING GENERATION
# ============================================================================

def estimate_tokens(text: str) -> int:
    """Estimate token count (rough approximation: 4 chars per token)."""
    return len(text) // 4


def generate_embedding(text: str, retries: int = 0) -> List[float]:
    """
    Generate embedding vector using Amazon Titan Embeddings V2.
    
    Returns 1024-dimensional vector.
    """
    if not text or not text.strip():
        logger.warning("Empty text provided for embedding")
        return [0.0] * EMBEDDING_DIMENSIONS
    
    # Truncate if too long
    if len(text) > MAX_TEXT_LENGTH:
        text = text[:MAX_TEXT_LENGTH]
        logger.warning(f"Text truncated to {MAX_TEXT_LENGTH} characters")
    
    try:
        client = get_client('bedrock-runtime')
        
        response = call_with_circuit_breaker(
            'bedrock',
            client.invoke_model,
            modelId=EMBEDDING_MODEL,
            contentType='application/json',
            body=json.dumps({"inputText": text})
        )
        
        result = json.loads(response['body'].read().decode('utf-8'))
        embedding = result.get('embedding', [])
        
        if not embedding:
            logger.warning("Empty embedding returned by Bedrock")
            return [0.0] * EMBEDDING_DIMENSIONS
        
        return embedding
        
    except Exception as e:
        error_str = str(e)
        
        # Check for throttling
        if 'ThrottlingException' in error_str or 'rate' in error_str.lower():
            if retries < MAX_RETRIES:
                wait_time = RETRY_DELAY * (2 ** retries)  # Exponential backoff
                logger.warning(f"Rate limited, waiting {wait_time}s (retry {retries + 1}/{MAX_RETRIES})")
                time.sleep(wait_time)
                return generate_embedding(text, retries + 1)
        
        # Other retryable errors
        if retries < MAX_RETRIES:
            logger.warning(f"Embedding retry {retries + 1}/{MAX_RETRIES}: {error_str}")
            time.sleep(RETRY_DELAY)
            return generate_embedding(text, retries + 1)
        
        logger.error(f"Failed to generate embedding after {MAX_RETRIES} retries: {e}")
        raise


def generate_embeddings_batch(chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Generate embeddings for a batch of chunks.
    
    Adds embedding vector and metadata to each chunk.
    """
    results = []
    total_tokens = 0
    errors_summary = {}
    
    for i, chunk in enumerate(chunks):
        try:
            text = chunk.get('text', '')
            chunk_id = chunk.get('chunk_id', f'chunk_{i}')
            
            # Validate chunk has text
            if not text or not text.strip():
                logger.warning(f"Chunk {chunk_id} has no text content, skipping embedding")
                results.append({
                    **chunk,
                    'embedding': None,
                    'embedding_error': 'Empty text content',
                    'embedding_generated_at': datetime.utcnow().isoformat()
                })
                continue
            
            # Generate embedding
            embedding = generate_embedding(text)
            token_estimate = estimate_tokens(text)
            total_tokens += token_estimate
            
            # Add embedding to chunk
            enhanced_chunk = {
                **chunk,
                'embedding': embedding,
                'embedding_model': EMBEDDING_MODEL,
                'embedding_dimensions': EMBEDDING_DIMENSIONS,
                'token_estimate': token_estimate,
                'embedding_generated_at': datetime.utcnow().isoformat()
            }
            
            results.append(enhanced_chunk)
            
            # Progress logging
            if (i + 1) % 10 == 0:
                logger.info(f"Generated {i + 1}/{len(chunks)} embeddings")
            
            # Rate limiting pause every batch
            if (i + 1) % BATCH_SIZE == 0:
                time.sleep(0.1)  # Brief pause to avoid throttling
                
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            chunk_id = chunk.get('chunk_id', f'chunk_{i}')
            
            # Track error types
            if error_type not in errors_summary:
                errors_summary[error_type] = 0
            errors_summary[error_type] += 1
            
            logger.error(f"Error generating embedding for chunk {chunk_id}: {error_type}: {error_msg}")
            
            # Include chunk without embedding, mark as failed
            results.append({
                **chunk,
                'embedding': None,
                'embedding_error': f"{error_type}: {error_msg}",
                'embedding_generated_at': datetime.utcnow().isoformat()
            })
    
    # Log summary
    if errors_summary:
        logger.error(f"Embedding generation errors: {errors_summary}")
    logger.info(f"Generated embeddings for {len(results)} chunks, ~{total_tokens} tokens")
    return results


# ============================================================================
# S3 OPERATIONS
# ============================================================================

def load_chunks_from_s3(s3_key: str) -> List[Dict[str, Any]]:
    """Load chunks from JSONL file in S3."""
    try:
        s3 = get_client('s3')
        response = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        
        chunks = []
        for line_num, line in enumerate(content.strip().split('\n'), 1):
            if line.strip():
                try:
                    chunk = json.loads(line)
                    chunks.append(chunk)
                except json.JSONDecodeError as e:
                    logger.warning(f"Skipping invalid JSON at line {line_num}: {e}")
                    continue
        
        logger.info(f"Loaded {len(chunks)} chunks from s3://{BUCKET_NAME}/{s3_key}")
        
        # Log first chunk sample for debugging
        if chunks:
            sample = chunks[0]
            logger.info(f"First chunk sample - ID: {sample.get('chunk_id')}, "
                       f"Text length: {len(sample.get('text', ''))}, "
                       f"Fields: {list(sample.keys())}")
        
        return chunks
        
    except Exception as e:
        logger.error(f"Error loading chunks from S3: {e}")
        raise


def save_embeddings_to_s3(chunks_with_embeddings: List[Dict[str, Any]], 
                          document_id: str) -> str:
    """Save chunks with embeddings as JSONL to S3."""
    try:
        s3 = get_client('s3')
        s3_key = f"embeddings/{document_id}.jsonl"
        
        # Convert to JSONL
        jsonl_lines = []
        for chunk in chunks_with_embeddings:
            jsonl_lines.append(json.dumps(chunk))
        
        content = '\n'.join(jsonl_lines)
        
        # Calculate stats
        successful = sum(1 for c in chunks_with_embeddings if c.get('embedding'))
        total_tokens = sum(c.get('token_estimate', 0) for c in chunks_with_embeddings)
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=content.encode('utf-8'),
            ContentType='application/jsonl',
            Metadata={
                'document_id': document_id,
                'embedding_count': str(successful),
                'total_chunks': str(len(chunks_with_embeddings)),
                'embedding_model': EMBEDDING_MODEL,
                'estimated_tokens': str(total_tokens)
            }
        )
        
        logger.info(f"Saved embeddings to s3://{BUCKET_NAME}/{s3_key}")
        return s3_key
        
    except Exception as e:
        logger.error(f"Error saving embeddings to S3: {e}")
        raise


# ============================================================================
# DYNAMODB OPERATIONS
# ============================================================================

def update_pipeline_status(document_id: str, status: str, message: str = '',
                           metadata: Dict = None):
    """Update pipeline status in DynamoDB."""
    try:
        table = get_resource('dynamodb').Table(PIPELINE_TABLE)
        
        update_expr = 'SET #s = :status, stage_message = :msg, updated_at = :ts'
        expr_names = {'#s': 'status'}
        expr_values = {
            ':status': status,
            ':msg': message,
            ':ts': datetime.utcnow().isoformat()
        }
        
        if metadata:
            for key, value in metadata.items():
                safe_key = key.replace('-', '_')
                update_expr += f', {safe_key} = :{safe_key}'
                expr_values[f':{safe_key}'] = value
        
        table.update_item(
            Key={'document_id': document_id},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_values
        )
        
        logger.info(f"Updated pipeline status: {document_id} -> {status}")
        
    except Exception as e:
        logger.error(f"Failed to update pipeline status: {e}")


# ============================================================================
# LAMBDA HANDLER
# ============================================================================

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for embeddings generation.
    
    Input from Step Functions:
    {
        "document_id": "doc-abc123",
        "chunks_s3_key": "chunks/doc-abc123.jsonl",
        "chunk_count": 42,
        "file_name": "document.pdf",
        "file_type": "pdf"
    }
    
    Output:
    {
        "statusCode": 200,
        "document_id": "doc-abc123",
        "embeddings_s3_key": "embeddings/doc-abc123.jsonl",
        "embedding_count": 42,
        "token_count": 31456,
        "metrics": {...}
    }
    """
    start_time = time.time()
    request_id = context.aws_request_id if context else 'local'
    set_correlation_id(request_id)
    log_event('request_start', 'Embeddings Generator invoked', request_id=request_id)
    
    try:
        event = EmbeddingEvent.model_validate(event).model_dump()

        # Extract parameters
        document_id = event.get('document_id')
        chunks_s3_key = event.get('chunks_s3_key')
        expected_count = event.get('chunk_count', 0)
        file_name = event.get('file_name', 'unknown')
        file_type = event.get('file_type', 'unknown')
        
        log_event('parameters_parsed', 'Embedding request parameters',
              document_id=document_id, chunk_count=expected_count, file_name=file_name, file_type=file_type)
        
        if not all([document_id, chunks_s3_key]):
            log_event('validation_error', 'Missing required parameters')
            raise ValueError("Missing required parameters: document_id, chunks_s3_key")

        current_status = get_pipeline_status(document_id)
        completed_statuses = {'embeddings_complete', 'indexed', 'partially_indexed', 'completed'}
        if current_status in completed_statuses:
            log_event('idempotent_skip', 'Embeddings already completed',
                      document_id=document_id, status=current_status)
            return {
                'statusCode': 200,
                'document_id': document_id,
                'status': current_status,
                'message': 'Embeddings already completed'
            }
        
        # Update status
        update_pipeline_status(
            document_id, 
            'generating_embeddings',
            f'Loading {expected_count} chunks'
        )
        
        # Load chunks from S3
        chunks = load_chunks_from_s3(chunks_s3_key)
        
        if not chunks:
            raise ValueError(f"No chunks found in {chunks_s3_key}")
        
        # Update status
        update_pipeline_status(
            document_id,
            'generating_embeddings',
            f'Generating embeddings for {len(chunks)} chunks'
        )
        
        # Generate embeddings
        chunks_with_embeddings = generate_embeddings_batch(chunks)
        
        # Count successful embeddings
        successful = sum(1 for c in chunks_with_embeddings if c.get('embedding'))
        failed = len(chunks_with_embeddings) - successful
        total_tokens = sum(c.get('token_estimate', 0) for c in chunks_with_embeddings)
        
        if successful == 0:
            # Collect error details for debugging
            error_types = {}
            for chunk in chunks_with_embeddings:
                error = chunk.get('embedding_error', 'Unknown error')
                if error not in error_types:
                    error_types[error] = 0
                error_types[error] += 1
            
            error_summary = '; '.join(f"{err}: {count}" for err, count in error_types.items())
            detailed_error = f"Failed to generate any embeddings for {len(chunks)} chunks. Errors: {error_summary}"
            logger.error(detailed_error)
            logger.error(f"First chunk sample - ID: {chunks[0].get('chunk_id')}, Text length: {len(chunks[0].get('text', ''))}, Text preview: {chunks[0].get('text', '')[:100]}")
            raise ValueError(detailed_error)
        
        # Save to S3
        embeddings_s3_key = save_embeddings_to_s3(chunks_with_embeddings, document_id)
        
        # Calculate metrics
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        # Update status
        update_pipeline_status(
            document_id,
            'embeddings_complete',
            f'Generated {successful}/{len(chunks)} embeddings',
            metadata={
                'embedding_count': successful,
                'embedding_failed': failed,
                'token_count': total_tokens,
                'embedding_ms': processing_time_ms
            }
        )

        log_event('request_complete', 'Embeddings generation complete',
                  document_id=document_id, duration_ms=processing_time_ms,
                  success=successful, failed=failed)
        
        return {
            'statusCode': 200,
            'document_id': document_id,
            'embeddings_s3_key': embeddings_s3_key,
            'embedding_count': successful,
            'embedding_failed': failed,
            'token_count': total_tokens,
            'file_name': file_name,
            'file_type': file_type,
            'chunk_count': len(chunks),
            'metrics': {
                'embedding_ms': processing_time_ms,
                'token_count': total_tokens,
                'chunks_processed': len(chunks),
                'embeddings_successful': successful,
                'embeddings_failed': failed
            }
        }
        
    except ValidationError as e:
        log_event('request_error', 'Invalid input payload', errors=e.errors())
        return {
            'statusCode': 400,
            'error': 'Invalid input payload',
            'error_type': 'validation'
        }

    except (ClientError, BotoCoreError) as e:
        log_event('request_error', f"AWS service error: {str(e)}", document_id=event.get('document_id'))
        return {
            'statusCode': 502,
            'error': str(e),
            'error_type': 'aws_error',
            'document_id': event.get('document_id') if isinstance(event, dict) else None
        }

    except Exception as e:
        log_event('request_error', f"Embeddings generation failed: {e}")
        logger.exception("Embeddings generation failed")
        
        if 'document_id' in event:
            update_pipeline_status(
                event['document_id'],
                'embedding_failed',
                str(e)
            )
        
        return {
            'statusCode': 500,
            'error': str(e),
            'document_id': event.get('document_id')
        }

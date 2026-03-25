"""
Lambda 09: WebSocket Manager
============================
Real-time streaming for MECH Avatar chat interface.

Trigger:
    - API Gateway WebSocket API

Routes:
    - $connect → Store connection ID in DynamoDB
    - $disconnect → Remove connection ID
    - sendMessage → Invoke Lambda 06 and stream response

Features:
    - WebSocket connection management
    - Real-time response streaming (word-by-word)
    - Connection timeout handling
    - JWT authentication on connect

Configuration:
    - Memory: 512 MB
    - Timeout: 300 seconds (5 minutes for streaming)
    - Runtime: Python 3.12
    - Layers: None (boto3 only)

Environment Variables:
    - AWS_DEFAULT_REGION: AWS region (default: ca-central-1)
    - CONNECTIONS_TABLE: DynamoDB table for connections
    - LAMBDA_06_ARN: Lambda 06 Query Processor ARN
    - WEBSOCKET_API_ENDPOINT: WebSocket API endpoint for callbacks
    - CONNECTION_TTL_HOURS: Connection TTL (default: 2)

Author: MECH Avatar Team | Version: 2.0.0 | Phase: 2
"""

import json
import boto3
import os
import logging
import time
import base64
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from decimal import Decimal

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_correlation_id: Optional[str] = None


def set_correlation_id(request_id: str) -> None:
    """Set correlation ID for tracing."""
    global _correlation_id
    _correlation_id = request_id


def log_event(event_type: str, message: str, **kwargs) -> None:
    """Structured logging."""
    log_data = {
        'correlation_id': _correlation_id,
        'event': event_type,
        'message': message,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'lambda': 'lambda_09_websocket_manager',
        **kwargs
    }
    logger.info(json.dumps(log_data))


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Centralized configuration."""
    
    AWS_REGION = os.environ.get('AWS_DEFAULT_REGION', 'ca-central-1')
    
    # DynamoDB - Updated to new naming convention
    CONNECTIONS_TABLE = os.environ.get('CONNECTIONS_TABLE', 'mechavatar-lab-cac1-user-sessions')
    
    # Lambda ARNs
    LAMBDA_06_ARN = os.environ.get('LAMBDA_06_ARN', '')
    
    # WebSocket API
    WEBSOCKET_API_ENDPOINT = os.environ.get('WEBSOCKET_API_ENDPOINT', '')
    
    # Connection settings
    CONNECTION_TTL_HOURS = int(os.environ.get('CONNECTION_TTL_HOURS', 2))
    
    # Streaming settings
    STREAM_CHUNK_SIZE = int(os.environ.get('STREAM_CHUNK_SIZE', 50))  # Characters per chunk


# ============================================================================
# 6 PERSONAS (Embedded config for WebSocket)
# ============================================================================

PERSONAS = {
    "developer": {
        "id": "developer",
        "name": "MECH Coder",
        "system_prompt": """You are MECH Coder, a MECH mainframe developer assistant specialized in technical documentation.

FOCUS AREAS:
- COBOL program analysis and code structure
- JCL job configurations and execution flow
- Assembler routines and low-level operations
- Database access patterns (DB2, IMS, VSAM)
- Copy books, procedures, and include files
- Program-to-program calls and linkage conventions

GROUNDING RULES:
- Only answer based on retrieved documentation
- If information is not in the context, say so clearly
- Always cite source documents with file names""",
        "metadata_filters": {
            "document_type": ["technical_spec", "program", "procedure", "copybook"],
            "folder": ["Mech_Documentations", "Mech Software Documentation"],
        },
        "knowledge_weights": {
            "procedures": 2.0,
            "technical_specs": 1.5,
            "programs": 2.0,
            "copybooks": 1.8,
        },
        "top_k": 15,
        "min_relevance_score": 0.6,
        "enable_self_rag": False,
    },
    "business_analyst": {
        "id": "business_analyst",
        "name": "MECH Researcher",
        "system_prompt": """You are MECH Researcher, a MECH business analyst assistant focused on requirements and specifications.

FOCUS AREAS:
- Business requirements and functional specifications
- Process flows and workflow documentation
- Data dictionaries and field definitions
- Business rules and validation logic

GROUNDING RULES:
- Only answer based on retrieved documentation
- If information is not in the context, say so clearly
- Always cite source documents""",
        "metadata_filters": {
            "document_type": ["function_spec", "business_requirement", "data_dictionary"],
            "folder": ["A-02-Function Specifications", "Mech Data Dictionary"],
        },
        "knowledge_weights": {
            "function_specs": 2.0,
            "business_docs": 1.5,
            "data_dictionaries": 1.8,
        },
        "top_k": 12,
        "min_relevance_score": 0.65,
        "enable_self_rag": False,
    },
    "qa_tester": {
        "id": "qa_tester",
        "name": "MECH Validator",
        "system_prompt": """You are MECH Validator, a MECH QA tester assistant specialized in testing documentation.

FOCUS AREAS:
- Test plans and test cases
- Validation procedures and checklists
- Error codes and handling procedures

GROUNDING RULES:
- Only answer based on retrieved documentation
- If information is not in the context, say so clearly
- Always cite source documents""",
        "metadata_filters": {
            "document_type": ["test_doc", "error_table", "validation_spec"],
            "folder": ["Mech CDIC FID Playbooks", "Maintenance Shared Documents"],
        },
        "knowledge_weights": {
            "test_docs": 2.0,
            "error_tables": 1.8,
            "validation_specs": 1.5,
        },
        "top_k": 12,
        "min_relevance_score": 0.6,
        "enable_self_rag": False,
    },
    "support_engineer": {
        "id": "support_engineer",
        "name": "MECH Helper",
        "system_prompt": """You are MECH Helper, a MECH support engineer assistant focused on operations and troubleshooting.

FOCUS AREAS:
- User guides and operating procedures
- Troubleshooting steps and runbooks
- Maintenance procedures and schedules

GROUNDING RULES:
- Only answer based on retrieved documentation
- If information is not in the context, say so clearly
- Always cite source documents""",
        "metadata_filters": {
            "document_type": ["user_guide", "runbook", "maintenance_doc"],
            "folder": ["A-09-Users Guides", "Maintenance Shared Documents"],
        },
        "knowledge_weights": {
            "user_guides": 2.0,
            "runbooks": 1.8,
            "maintenance_docs": 1.5,
        },
        "top_k": 10,
        "min_relevance_score": 0.55,
        "enable_self_rag": False,
    },
    "architect": {
        "id": "architect",
        "name": "MECH Strategist",
        "system_prompt": """You are MECH Strategist, a MECH system architect assistant focused on design and architecture.

FOCUS AREAS:
- System architecture and design patterns
- Integration interfaces and data flows
- Technical overviews and diagrams

GROUNDING RULES:
- Only answer based on retrieved documentation
- If information is not in the context, say so clearly
- Always cite source documents""",
        "metadata_filters": {
            "document_type": ["technical_overview", "design_spec", "architecture_doc"],
            "folder": ["A-08-Technical Overviews", "A-16-Detailed Design Specifications"],
        },
        "knowledge_weights": {
            "design_specs": 2.0,
            "technical_overviews": 1.8,
            "architecture_docs": 1.5,
        },
        "top_k": 15,
        "min_relevance_score": 0.7,
        "enable_self_rag": False,
    },
    "compliance_officer": {
        "id": "compliance_officer",
        "name": "MECH Auditor",
        "system_prompt": """You are MECH Auditor, a MECH compliance officer assistant focused on regulatory and security.

FOCUS AREAS:
- CDIC regulatory requirements
- Security policies and procedures
- Audit documentation and trails

GROUNDING RULES:
- Only answer based on retrieved documentation
- If information is not in the context, say so clearly
- Always cite source documents

SPECIAL: Self-RAG verification is ENABLED for this persona to ensure accuracy.""",
        "metadata_filters": {
            "document_type": ["compliance_doc", "regulatory_requirement", "security_policy"],
            "folder": ["CDIC Requirements Docmentation", "Mech Security Documentation"],
        },
        "knowledge_weights": {
            "compliance_docs": 2.5,
            "security_docs": 2.0,
            "regulatory_docs": 2.0,
            "general_docs": 0.5,
        },
        "top_k": 20,
        "min_relevance_score": 0.8,
        "enable_self_rag": True,
    },
}

DEFAULT_PERSONA = "developer"


# ============================================================================
# AWS CLIENTS (Lazy initialization)
# ============================================================================

_dynamodb = None
_lambda_client = None
_api_gateway_client = None


def get_dynamodb():
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource('dynamodb', region_name=Config.AWS_REGION)
    return _dynamodb


def get_lambda_client():
    global _lambda_client
    if _lambda_client is None:
        _lambda_client = boto3.client('lambda', region_name=Config.AWS_REGION)
    return _lambda_client


def get_api_gateway_client(endpoint: str):
    """Get API Gateway Management API client for sending messages."""
    global _api_gateway_client
    if _api_gateway_client is None:
        _api_gateway_client = boto3.client(
            'apigatewaymanagementapi',
            endpoint_url=endpoint,
            region_name=Config.AWS_REGION
        )
    return _api_gateway_client


# ============================================================================
# AUTHENTICATION
# ============================================================================

def extract_user_from_token(token: str) -> tuple:
    """
    Extract user_id and persona from JWT token.
    
    Returns:
        Tuple of (user_id, persona) or (None, None) if invalid
    """
    if not token:
        return None, None
    
    try:
        # Decode JWT payload (without verification - API Gateway does that)
        parts = token.split('.')
        if len(parts) != 3:
            return None, None
        
        payload_part = parts[1]
        # Add padding if needed
        padding = 4 - len(payload_part) % 4
        if padding != 4:
            payload_part += '=' * padding
        
        payload = json.loads(base64.urlsafe_b64decode(payload_part))
        
        user_id = payload.get('email') or payload.get('cognito:username') or payload.get('sub')
        
        # Extract persona from Cognito groups
        groups = payload.get('cognito:groups', [])
        persona = DEFAULT_PERSONA
        
        for group in groups:
            if group in PERSONAS:
                persona = group
                break
        
        custom_persona = payload.get('custom:persona')
        if custom_persona and custom_persona in PERSONAS:
            persona = custom_persona
        
        return user_id, persona
        
    except Exception as e:
        log_event('AUTH_ERROR', f'Failed to decode token: {str(e)}')
        return None, None


# ============================================================================
# CONNECTION MANAGEMENT
# ============================================================================

def store_connection(connection_id: str, user_id: str, persona: str) -> bool:
    """
    Store WebSocket connection in DynamoDB.
    
    Args:
        connection_id: WebSocket connection ID
        user_id: Authenticated user ID
        persona: User's persona
    
    Returns:
        True if stored successfully
    """
    try:
        table = get_dynamodb().Table(Config.CONNECTIONS_TABLE)
        
        ttl = int(time.time()) + (Config.CONNECTION_TTL_HOURS * 3600)
        
        table.put_item(
            Item={
                'connection_id': connection_id,
                'user_id': user_id,
                'persona': persona,
                'connected_at': datetime.now(timezone.utc).isoformat(),
                'ttl': ttl
            }
        )
        
        log_event('CONNECTION_STORED', 'Connection stored',
                 connection_id=connection_id, user_id=user_id, persona=persona)
        
        return True
        
    except Exception as e:
        log_event('CONNECTION_STORE_ERROR', f'Failed to store connection: {str(e)}')
        return False


def remove_connection(connection_id: str) -> bool:
    """
    Remove WebSocket connection from DynamoDB.
    
    Args:
        connection_id: WebSocket connection ID
    
    Returns:
        True if removed successfully
    """
    try:
        table = get_dynamodb().Table(Config.CONNECTIONS_TABLE)
        
        table.delete_item(
            Key={'connection_id': connection_id}
        )
        
        log_event('CONNECTION_REMOVED', 'Connection removed',
                 connection_id=connection_id)
        
        return True
        
    except Exception as e:
        log_event('CONNECTION_REMOVE_ERROR', f'Failed to remove connection: {str(e)}')
        return False


def get_connection(connection_id: str) -> Optional[Dict[str, Any]]:
    """
    Get connection info from DynamoDB.
    
    Args:
        connection_id: WebSocket connection ID
    
    Returns:
        Connection info dict or None
    """
    try:
        table = get_dynamodb().Table(Config.CONNECTIONS_TABLE)
        
        response = table.get_item(
            Key={'connection_id': connection_id}
        )
        
        return response.get('Item')
        
    except Exception as e:
        log_event('CONNECTION_GET_ERROR', f'Failed to get connection: {str(e)}')
        return None


# ============================================================================
# MESSAGE BROADCASTING
# ============================================================================

def send_to_connection(endpoint: str, connection_id: str, data: Dict[str, Any]) -> bool:
    """
    Send message to a WebSocket connection.
    
    Args:
        endpoint: WebSocket API endpoint
        connection_id: Target connection ID
        data: Message data to send
    
    Returns:
        True if sent successfully
    """
    try:
        client = get_api_gateway_client(endpoint)
        
        client.post_to_connection(
            ConnectionId=connection_id,
            Data=json.dumps(data).encode('utf-8')
        )
        
        return True
        
    except client.exceptions.GoneException:
        # Connection is stale, remove it
        log_event('CONNECTION_GONE', 'Connection no longer exists',
                 connection_id=connection_id)
        remove_connection(connection_id)
        return False
        
    except Exception as e:
        log_event('SEND_ERROR', f'Failed to send to connection: {str(e)}',
                 connection_id=connection_id)
        return False


def stream_response(endpoint: str, connection_id: str, text: str, 
                    message_type: str = 'chunk') -> None:
    """
    Stream a response in chunks to a WebSocket connection.
    
    Args:
        endpoint: WebSocket API endpoint
        connection_id: Target connection ID
        text: Full text to stream
        message_type: Type of message (chunk, final, error)
    """
    if message_type == 'final':
        # Send complete message
        send_to_connection(endpoint, connection_id, {
            'type': 'final',
            'content': text,
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
        return
    
    if message_type == 'error':
        send_to_connection(endpoint, connection_id, {
            'type': 'error',
            'error': text,
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
        return
    
    # Stream in chunks
    chunk_size = Config.STREAM_CHUNK_SIZE
    words = text.split(' ')
    current_chunk = []
    
    for word in words:
        current_chunk.append(word)
        
        # Send chunk when it reaches the size limit
        if len(' '.join(current_chunk)) >= chunk_size:
            send_to_connection(endpoint, connection_id, {
                'type': 'chunk',
                'content': ' '.join(current_chunk) + ' ',
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
            current_chunk = []
            time.sleep(0.05)  # Small delay for smooth streaming
    
    # Send remaining words
    if current_chunk:
        send_to_connection(endpoint, connection_id, {
            'type': 'chunk',
            'content': ' '.join(current_chunk),
            'timestamp': datetime.now(timezone.utc).isoformat()
        })


# ============================================================================
# REQUEST HANDLERS
# ============================================================================

def handle_connect(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle WebSocket $connect event.
    
    Authenticates user and stores connection.
    """
    connection_id = event['requestContext']['connectionId']
    
    # Get token from query string
    query_params = event.get('queryStringParameters') or {}
    token = query_params.get('token', '')
    
    # Authenticate
    user_id, persona = extract_user_from_token(token)
    
    if not user_id:
        log_event('CONNECT_REJECTED', 'Connection rejected - invalid token',
                 connection_id=connection_id)
        return {
            'statusCode': 401,
            'body': 'Unauthorized'
        }
    
    # Store connection
    if not store_connection(connection_id, user_id, persona):
        return {
            'statusCode': 500,
            'body': 'Failed to store connection'
        }
    
    log_event('CONNECT_SUCCESS', 'WebSocket connected',
             connection_id=connection_id, user_id=user_id, persona=persona)
    
    return {
        'statusCode': 200,
        'body': 'Connected'
    }


def handle_disconnect(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle WebSocket $disconnect event.
    
    Removes connection from DynamoDB.
    """
    connection_id = event['requestContext']['connectionId']
    
    remove_connection(connection_id)
    
    log_event('DISCONNECT', 'WebSocket disconnected',
             connection_id=connection_id)
    
    return {
        'statusCode': 200,
        'body': 'Disconnected'
    }


def handle_send_message(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle sendMessage action - process query and stream response.
    
    Expected body:
    {
        "action": "sendMessage",
        "query": "What is MYABC table?",
        "session_id": "optional-session-id"
    }
    """
    connection_id = event['requestContext']['connectionId']
    domain_name = event['requestContext']['domainName']
    stage = event['requestContext']['stage']
    
    # Build WebSocket callback endpoint
    endpoint = f"https://{domain_name}/{stage}"
    
    # Get connection info
    connection = get_connection(connection_id)
    if not connection:
        return {
            'statusCode': 401,
            'body': 'Connection not found'
        }
    
    user_id = connection.get('user_id')
    persona = connection.get('persona', DEFAULT_PERSONA)
    
    # Parse message body
    try:
        body = json.loads(event.get('body', '{}'))
    except json.JSONDecodeError:
        send_to_connection(endpoint, connection_id, {
            'type': 'error',
            'error': 'Invalid JSON'
        })
        return {'statusCode': 400, 'body': 'Invalid JSON'}
    
    query = body.get('query', '').strip()
    session_id = body.get('session_id', f"sess-{int(time.time())}")
    
    if not query:
        send_to_connection(endpoint, connection_id, {
            'type': 'error',
            'error': 'Query is required'
        })
        return {'statusCode': 400, 'body': 'Query required'}
    
    # Send acknowledgment
    send_to_connection(endpoint, connection_id, {
        'type': 'ack',
        'message': 'Processing your query...',
        'session_id': session_id,
        'persona': PERSONAS.get(persona, {}).get('name', 'Assistant')
    })
    
    log_event('QUERY_RECEIVED', 'Processing query via WebSocket',
             connection_id=connection_id, user_id=user_id, 
             query_length=len(query), persona=persona)
    
    try:
        persona_config = PERSONAS.get(persona, PERSONAS[DEFAULT_PERSONA])

        # Stage 1: Planning
        send_to_connection(endpoint, connection_id, {
            'type': 'stage',
            'stage': 'planning',
            'message': 'Analyzing your query and planning retrieval strategy...'
        })

        # Build Lambda 06 payload
        lambda_payload = {
            'query': query,
            'session_id': session_id,
            'user_id': user_id,
            'persona': persona,
            'persona_config': {
                'name': persona_config.get('name'),
                'system_prompt': persona_config.get('system_prompt'),
                'metadata_filters': persona_config.get('metadata_filters', {}),
                'knowledge_weights': persona_config.get('knowledge_weights', {}),
                'top_k': persona_config.get('top_k', 10),
                'min_relevance_score': persona_config.get('min_relevance_score', 0.6),
                'enable_self_rag': persona_config.get('enable_self_rag', False)
            },
            'options': {
                'enable_streaming': True,
                'include_citations': True,
                'enable_memory': True
            }
        }
        
        # Stage 2: Retrieving
        send_to_connection(endpoint, connection_id, {
            'type': 'stage',
            'stage': 'retrieving',
            'message': 'Searching documentation and knowledge base...'
        })

        # Invoke Lambda 06
        response = get_lambda_client().invoke(
            FunctionName=Config.LAMBDA_06_ARN,
            InvocationType='RequestResponse',
            Payload=json.dumps(lambda_payload)
        )
        
        # Parse response
        response_payload = json.loads(response['Payload'].read().decode('utf-8'))
        
        if response.get('FunctionError'):
            log_event('LAMBDA_06_ERROR', 'Lambda 06 returned error',
                     error=str(response_payload))
            send_to_connection(endpoint, connection_id, {
                'type': 'error',
                'error': 'Failed to process query'
            })
            return {'statusCode': 500, 'body': 'Lambda error'}
        
        # Stage 3: Generating
        send_to_connection(endpoint, connection_id, {
            'type': 'stage',
            'stage': 'generating',
            'message': 'Generating response from retrieved context...'
        })

        # Extract answer
        result = response_payload.get('body', response_payload)
        if isinstance(result, str):
            result = json.loads(result)
        
        answer = result.get('answer', result.get('response', ''))
        sources = result.get('sources', []) or []
        confidence = (result.get('confidence') or {}).get('score') or 0.0
        
        # Stage 4: Reviewing
        send_to_connection(endpoint, connection_id, {
            'type': 'stage',
            'stage': 'reviewing',
            'message': 'Verifying answer quality and grounding...'
        })

        # Stream the response
        stream_response(endpoint, connection_id, answer)
        
        # Send final message with metadata
        send_to_connection(endpoint, connection_id, {
            'type': 'complete',
            'session_id': session_id,
            # Backwards-compatible: some clients may still read `citations`
            'citations': sources[:5],
            'sources': sources,
            'confidence': confidence,
            'persona': {
                'id': persona,
                'name': PERSONAS.get(persona, {}).get('name', 'Assistant')
            },
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
        
        log_event('QUERY_SUCCESS', 'Query processed and streamed',
                 connection_id=connection_id, session_id=session_id,
                 answer_length=len(answer), citation_count=len(sources))
        
        return {'statusCode': 200, 'body': 'Message sent'}
        
    except Exception as e:
        log_event('QUERY_ERROR', f'Error processing query: {str(e)}',
                 connection_id=connection_id)
        send_to_connection(endpoint, connection_id, {
            'type': 'error',
            'error': f'Error: {str(e)}'
        })
        return {'statusCode': 500, 'body': str(e)}


def handle_ping(event: Dict[str, Any]) -> Dict[str, Any]:
    """Handle ping action for keep-alive."""
    connection_id = event['requestContext']['connectionId']
    domain_name = event['requestContext']['domainName']
    stage = event['requestContext']['stage']
    endpoint = f"https://{domain_name}/{stage}"
    
    send_to_connection(endpoint, connection_id, {
        'type': 'pong',
        'timestamp': datetime.now(timezone.utc).isoformat()
    })
    
    return {'statusCode': 200, 'body': 'pong'}


# ============================================================================
# MAIN HANDLER
# ============================================================================

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for WebSocket events.
    
    Routes events to appropriate handlers based on route key.
    
    Args:
        event: API Gateway WebSocket event
        context: Lambda context
    
    Returns:
        API Gateway response
    """
    # Set correlation ID
    request_id = context.aws_request_id if context else str(time.time())
    set_correlation_id(request_id)
    
    # Get route key
    route_key = event['requestContext'].get('routeKey', '')
    
    log_event('WEBSOCKET_EVENT', f'Received event: {route_key}',
             connection_id=event['requestContext'].get('connectionId'))
    
    try:
        # Route to appropriate handler
        if route_key == '$connect':
            return handle_connect(event)
        
        elif route_key == '$disconnect':
            return handle_disconnect(event)
        
        elif route_key == 'sendMessage':
            return handle_send_message(event)
        
        elif route_key == 'ping':
            return handle_ping(event)
        
        elif route_key == '$default':
            # Handle default route - attempt to parse action from body
            try:
                body = json.loads(event.get('body', '{}'))
                action = body.get('action', '')
                
                if action == 'sendMessage':
                    return handle_send_message(event)
                elif action == 'ping':
                    return handle_ping(event)
                    
            except json.JSONDecodeError:
                pass
            
            log_event('DEFAULT_ROUTE', 'Received message on $default route')
            return {'statusCode': 200, 'body': 'OK'}
        
        else:
            # Handle custom actions from body
            try:
                body = json.loads(event.get('body', '{}'))
                action = body.get('action', '')
                
                if action == 'sendMessage':
                    return handle_send_message(event)
                elif action == 'ping':
                    return handle_ping(event)
                    
            except json.JSONDecodeError:
                pass
            
            log_event('UNKNOWN_ROUTE', f'Unknown route: {route_key}')
            return {
                'statusCode': 400,
                'body': f'Unknown route: {route_key}'
            }
            
    except Exception as e:
        log_event('HANDLER_ERROR', f'Unhandled error: {str(e)}',
                 route_key=route_key)
        return {
            'statusCode': 500,
            'body': f'Internal error: {str(e)}'
        }

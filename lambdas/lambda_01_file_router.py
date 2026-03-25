"""
Lambda 01: File Router
======================
MECH Avatar - BMO Lab Account (939620275271)

Entry point for S3 events. Routes documents to Step Functions pipeline
and Q&A Excel files to batch processor.

S3 Structure:
    mechavatar-lab-cac1-s3-inbound/
    ├── documents/   -> Step Functions (document pipeline)
    ├── questions/   -> Lambda 08 (batch Q&A)
    ├── outputs/     -> Q&A results (no trigger)
    ├── processed/   -> Completed documents (no trigger)
    └── chunks/      -> Intermediate data (no trigger)

Environment Variables:
    AWS_DEFAULT_REGION       = ca-central-1
    AWS_ACCOUNT_ID           = 939620275271
    BUCKET_NAME              = mechavatar-lab-cac1-s3-inbound
    DOCUMENTS_TABLE          = mechavatar-lab-cac1-mech-documents-metadata
    PIPELINE_TABLE           = mechavatar-lab-cac1-mech-processing-pipeline
    DEDUP_TABLE              = mechavatar-lab-cac1-mech-deduplication-index
    STATE_MACHINE_ARN        = arn:aws:states:ca-central-1:939620275271:stateMachine:mechavatar-lab-cac1-document-processing-stepfn
    BATCH_QA_LAMBDA_ARN      = arn:aws:lambda:ca-central-1:939620275271:function:mechavatar-lab-cac1-batch-qa-processor-lambda

Lambda Configuration:
    Memory: 1024 MB | Timeout: 300s | Runtime: Python 3.11
    Trigger: S3 ObjectCreated event on documents/ and questions/
"""

import json
import boto3
import os
import urllib.parse
import hashlib
import re
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from enum import Enum
import logging
from pydantic import BaseModel, ConfigDict, Field, ValidationError

# Import centralized BMO configuration
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from config import (
        AWS_REGION, AWS_ACCOUNT_ID, BUCKET_NAME,
        STATE_MACHINE_ARN, DOCUMENTS_TABLE, PIPELINE_TABLE,
        LAMBDA_ARNS
    )
    BATCH_QA_LAMBDA_ARN = LAMBDA_ARNS.get('batch_qa_processor')
except ImportError:
    AWS_REGION = os.environ.get('AWS_DEFAULT_REGION', 'ca-central-1')
    AWS_ACCOUNT_ID = os.environ.get('AWS_ACCOUNT_ID', '939620275271')
    BUCKET_NAME = os.environ.get('BUCKET_NAME', 'mechavatar-lab-cac1-s3-inbound')
    STATE_MACHINE_ARN = os.environ.get('STATE_MACHINE_ARN', 'arn:aws:states:ca-central-1:939620275271:stateMachine:mechavatar-lab-cac1-document-processing-stepfn')
    BATCH_QA_LAMBDA_ARN = os.environ.get('BATCH_QA_LAMBDA_ARN', 'arn:aws:lambda:ca-central-1:939620275271:function:mechavatar-lab-cac1-batch-qa-processor-lambda')
    DOCUMENTS_TABLE = os.environ.get('DOCUMENTS_TABLE', 'mechavatar-lab-cac1-mech-documents-metadata')
    PIPELINE_TABLE = os.environ.get('PIPELINE_TABLE', 'mechavatar-lab-cac1-mech-processing-pipeline')

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


class WorkflowType(Enum):
    """Workflow types based on S3 folder."""
    DOCUMENT_PIPELINE = "document_pipeline"
    QA_BATCH = "qa_batch"
    IGNORE = "ignore"


class FileType(Enum):
    """Supported file types."""
    PDF = "pdf"
    DOCX = "docx"
    DOC_LEGACY = "doc_legacy"
    XLSX = "xlsx"
    XLS_LEGACY = "xls_legacy"
    PPTX = "pptx"
    PPT_LEGACY = "ppt_legacy"
    TEXT = "text"
    HTML = "html"
    XML = "xml"
    IMAGE = "image"
    UNSUPPORTED = "unsupported"


# File type configuration with max sizes and handlers
FILE_CONFIG: Dict[str, Dict[str, Any]] = {
    '.pdf': {'type': FileType.PDF, 'max_mb': 100, 'mime': 'application/pdf'},
    '.docx': {'type': FileType.DOCX, 'max_mb': 50, 'mime': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'},
    '.doc': {'type': FileType.DOC_LEGACY, 'max_mb': 50, 'mime': 'application/msword'},
    '.xlsx': {'type': FileType.XLSX, 'max_mb': 50, 'mime': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'},
    '.xls': {'type': FileType.XLS_LEGACY, 'max_mb': 50, 'mime': 'application/vnd.ms-excel'},
    '.pptx': {'type': FileType.PPTX, 'max_mb': 100, 'mime': 'application/vnd.openxmlformats-officedocument.presentationml.presentation'},
    '.ppt': {'type': FileType.PPT_LEGACY, 'max_mb': 100, 'mime': 'application/vnd.ms-powerpoint'},
    '.txt': {'type': FileType.TEXT, 'max_mb': 10, 'mime': 'text/plain'},
    '.md': {'type': FileType.TEXT, 'max_mb': 10, 'mime': 'text/markdown'},
    '.html': {'type': FileType.HTML, 'max_mb': 10, 'mime': 'text/html'},
    '.htm': {'type': FileType.HTML, 'max_mb': 10, 'mime': 'text/html'},
    '.xml': {'type': FileType.XML, 'max_mb': 10, 'mime': 'application/xml'},
    '.xsd': {'type': FileType.XML, 'max_mb': 10, 'mime': 'application/xml'},
    '.json': {'type': FileType.TEXT, 'max_mb': 10, 'mime': 'application/json'},
    '.csv': {'type': FileType.TEXT, 'max_mb': 50, 'mime': 'text/csv'},
    '.png': {'type': FileType.IMAGE, 'max_mb': 20, 'mime': 'image/png'},
    '.jpg': {'type': FileType.IMAGE, 'max_mb': 20, 'mime': 'image/jpeg'},
    '.jpeg': {'type': FileType.IMAGE, 'max_mb': 20, 'mime': 'image/jpeg'},
    '.bmp': {'type': FileType.IMAGE, 'max_mb': 20, 'mime': 'image/bmp'},
    '.tiff': {'type': FileType.IMAGE, 'max_mb': 20, 'mime': 'image/tiff'},
    '.gif': {'type': FileType.IMAGE, 'max_mb': 10, 'mime': 'image/gif'},
}

# Folders to ignore
IGNORE_FOLDERS = {'.git', '__MACOSX', '__pycache__', 'node_modules', '.venv', 'venv', '.DS_Store'}

# ============================================================================
# AWS CLIENT MANAGEMENT (Lazy Loading)
# ============================================================================

_clients: Dict[str, Any] = {}


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


class S3ObjectModel(BaseModel):
    model_config = ConfigDict(extra='allow')
    key: str
    size: Optional[int] = None


class S3BucketModel(BaseModel):
    model_config = ConfigDict(extra='allow')
    name: str


class S3RecordModel(BaseModel):
    model_config = ConfigDict(extra='allow')
    eventSource: str = Field(..., alias='eventSource')
    eventName: str = Field(..., alias='eventName')
    s3: Dict[str, Any]


class S3EventModel(BaseModel):
    model_config = ConfigDict(extra='allow')
    Records: List[S3RecordModel]


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def generate_document_id(bucket: str, key: str) -> str:
    """Generate deterministic document ID from bucket/key."""
    content = f"{bucket}/{key}"
    hash_value = hashlib.md5(content.encode()).hexdigest()[:12]
    return f"doc-{hash_value}"


def get_file_extension(file_path: str) -> str:
    """Extract lowercase file extension."""
    if '.' in file_path:
        return '.' + file_path.rsplit('.', 1)[-1].lower()
    return ''


def get_file_name(key: str) -> str:
    """Extract file name from S3 key."""
    return os.path.basename(key)


def determine_workflow(key: str) -> WorkflowType:
    """
    Determine workflow type based on S3 key prefix.
    
    Routing:
        documents/* → Document processing pipeline
        questions/* → Q&A batch processing
        * → Ignore (outputs, processed, chunks, embeddings, etc.)
    """
    key_lower = key.lower()
    
    if key_lower.startswith('documents/'):
        return WorkflowType.DOCUMENT_PIPELINE
    elif key_lower.startswith('questions/'):
        return WorkflowType.QA_BATCH
    else:
        return WorkflowType.IGNORE


def get_file_type(extension: str) -> Tuple[FileType, Dict[str, Any]]:
    """Get file type and configuration from extension."""
    config = FILE_CONFIG.get(extension.lower())
    if config:
        return config['type'], config
    return FileType.UNSUPPORTED, {}


# ============================================================================
# VALIDATION
# ============================================================================

def validate_file(key: str, size_bytes: int) -> Dict[str, Any]:
    """
    Comprehensive file validation.
    
    Returns:
        {
            'valid': bool,
            'reason': str (if invalid),
            'file_type': FileType,
            'file_config': dict,
            'file_name': str,
            'extension': str
        }
    """
    file_name = get_file_name(key)
    extension = get_file_extension(key)
    
    # Check for hidden/system files
    if file_name.startswith('.') or file_name.startswith('~'):
        return {'valid': False, 'reason': 'hidden_file', 'file_name': file_name}
    
    # Check for ignored folders
    for folder in IGNORE_FOLDERS:
        if folder in key:
            return {'valid': False, 'reason': f'ignored_folder:{folder}', 'file_name': file_name}
    
    # Check extension
    if not extension:
        return {'valid': False, 'reason': 'no_extension', 'file_name': file_name}
    
    file_type, config = get_file_type(extension)
    
    if file_type == FileType.UNSUPPORTED:
        return {'valid': False, 'reason': f'unsupported_extension:{extension}', 'file_name': file_name}
    
    # Check file size
    if size_bytes == 0:
        return {'valid': False, 'reason': 'empty_file', 'file_name': file_name}
    
    max_bytes = config.get('max_mb', 100) * 1024 * 1024
    if size_bytes > max_bytes:
        return {
            'valid': False, 
            'reason': f'file_too_large:{size_bytes}>{max_bytes}',
            'file_name': file_name
        }
    
    return {
        'valid': True,
        'file_type': file_type,
        'file_config': config,
        'file_name': file_name,
        'extension': extension,
        'size_bytes': size_bytes
    }


# ============================================================================
# S3 OPERATIONS
# ============================================================================

def get_s3_object_metadata(bucket: str, key: str) -> Dict[str, Any]:
    """Get S3 object metadata."""
    try:
        response = get_client('s3').head_object(Bucket=bucket, Key=key)
        return {
            'content_type': response.get('ContentType', 'application/octet-stream'),
            'content_length': response.get('ContentLength', 0),
            'last_modified': response.get('LastModified').isoformat() if response.get('LastModified') else None,
            'etag': response.get('ETag', '').strip('"'),
            'metadata': response.get('Metadata', {})
        }
    except Exception as e:
        logger.warning(f"Error getting S3 metadata for {bucket}/{key}: {e}")
        return {}


# ============================================================================
# DYNAMODB OPERATIONS
# ============================================================================

def create_document_record(document_id: str, bucket: str, key: str, 
                           validation: Dict[str, Any], workflow: WorkflowType,
                           content_hash: Optional[str] = None) -> bool:
    """Create initial document record in DynamoDB."""
    try:
        table = get_resource('dynamodb').Table(DOCUMENTS_TABLE)
        
        item = {
            'document_id': document_id,
            's3_bucket': bucket,
            's3_key': key,
            'file_name': validation.get('file_name', 'unknown'),
            'file_type': validation.get('file_type', FileType.UNSUPPORTED).value,
            'file_extension': validation.get('extension', ''),
            'file_size_bytes': validation.get('size_bytes', 0),
            'workflow_type': workflow.value,
            'status': 'pending',
            'created_at': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat()
        }
        
        # Add content hash if computed
        if content_hash:
            item['content_hash'] = content_hash
        
        table.put_item(Item=item)
        logger.info(f"Created document record: {document_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error creating document record: {e}")
        return False


def create_pipeline_record(document_id: str, workflow: WorkflowType, 
                           input_data: Dict[str, Any]) -> bool:
    """Create pipeline tracking record in DynamoDB."""
    try:
        table = get_resource('dynamodb').Table(PIPELINE_TABLE)
        
        # Custom JSON encoder for enums
        def enum_default(obj):
            if isinstance(obj, Enum):
                return obj.value
            raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
        
        item = {
            'document_id': document_id,
            'workflow_type': workflow.value,
            'status': 'started',
            'stage': 'file_routing',
            'input_data': json.dumps(input_data, default=enum_default),
            'started_at': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat()
        }
        
        table.put_item(Item=item)
        logger.info(f"Created pipeline record: {document_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error creating pipeline record: {e}")
        return False


def check_duplicate(document_id: str) -> bool:
    """Check if document already exists and is processed."""
    try:
        table = get_resource('dynamodb').Table(DOCUMENTS_TABLE)
        response = table.get_item(
            Key={'document_id': document_id},
            ProjectionExpression='document_id, #s',
            ExpressionAttributeNames={'#s': 'status'}
        )
        
        if 'Item' in response:
            status = response['Item'].get('status', '')
            if status in ('completed', 'processing', 'indexing'):
                logger.info(f"Duplicate document {document_id} with status {status}")
                return True
        return False
        
    except Exception as e:
        logger.warning(f"Error checking duplicate: {e}")
        return False


def compute_content_hash(bucket: str, key: str, max_bytes: int = 10 * 1024 * 1024) -> Optional[str]:
    """
    Compute SHA256 hash of S3 object content for deduplication.
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        max_bytes: Maximum bytes to read (default 10MB) - for large files, hash partial content
    
    Returns:
        SHA256 hex digest or None if error
    """
    try:
        s3 = get_client('s3')
        
        # Get object size first
        head = s3.head_object(Bucket=bucket, Key=key)
        size = head.get('ContentLength', 0)
        
        if size <= max_bytes:
            # Hash entire file
            response = s3.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read()
        else:
            # Hash first portion + last portion + size for large files
            # This catches most duplicates while being efficient
            response = s3.get_object(Bucket=bucket, Key=key, Range=f'bytes=0-{max_bytes//2}')
            first_half = response['Body'].read()
            
            response = s3.get_object(Bucket=bucket, Key=key, Range=f'bytes={size - max_bytes//2}-{size}')
            second_half = response['Body'].read()
            
            content = first_half + second_half + str(size).encode()
        
        content_hash = hashlib.sha256(content).hexdigest()
        logger.info(f"Computed content hash for {key}: {content_hash[:16]}...")
        return content_hash
        
    except Exception as e:
        logger.warning(f"Error computing content hash for {key}: {e}")
        return None


def check_content_duplicate(content_hash: str, current_doc_id: str) -> Optional[Dict[str, Any]]:
    """
    Check if a document with the same content hash already exists.
    
    Args:
        content_hash: SHA256 hash of content
        current_doc_id: Current document ID to exclude from check
    
    Returns:
        Existing document info if duplicate found, None otherwise
    """
    if not content_hash:
        return None
    
    try:
        dynamodb = get_client('dynamodb')
        
        # Query using GSI on content_hash (if exists)
        try:
            response = dynamodb.query(
                TableName=DOCUMENTS_TABLE,
                IndexName='content-hash-index',
                KeyConditionExpression='content_hash = :hash',
                ExpressionAttributeValues={':hash': {'S': content_hash}},
                Limit=2  # Only need to find one match
            )
            
            for item in response.get('Items', []):
                doc_id = item.get('document_id', {}).get('S')
                status = item.get('status', {}).get('S', '')
                
                # Skip current document and failed documents
                if doc_id != current_doc_id and status in ('completed', 'processing', 'indexing'):
                    logger.info(f"Content duplicate found: {doc_id} (hash: {content_hash[:16]}...)")
                    return {
                        'document_id': doc_id,
                        'status': status,
                        'file_name': item.get('file_name', {}).get('S'),
                        's3_key': item.get('s3_key', {}).get('S')
                    }
                    
        except Exception as index_error:
            # GSI not available or query failed - skip content dedup gracefully
            if 'ValidationException' in str(index_error) or 'ResourceNotFoundException' in str(index_error):
                logger.debug(f"Content hash index not available: {index_error}")
            else:
                logger.warning(f"Content hash query failed: {index_error}")
            return None
            
        return None
        
    except Exception as e:
        logger.debug(f"Content dedup check skipped: {e}")
        return None


# ============================================================================
# STEP FUNCTIONS
# ============================================================================

def start_document_pipeline(document_id: str, bucket: str, key: str, 
                            validation: Dict[str, Any]) -> Dict[str, Any]:
    """Start document processing Step Functions workflow."""
    if not STATE_MACHINE_ARN:
        raise ValueError("STATE_MACHINE_ARN environment variable not set")
    
    input_payload = {
        'document_id': document_id,
        's3_bucket': bucket,
        's3_key': key,
        'file_name': validation.get('file_name'),
        'file_type': validation.get('file_type', FileType.UNSUPPORTED).value,
        'file_extension': validation.get('extension'),
        'file_size_bytes': validation.get('size_bytes'),
        'workflow_type': WorkflowType.DOCUMENT_PIPELINE.value,
        'timestamp': datetime.utcnow().isoformat()
    }
    
    execution_name = f"{document_id}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    
    response = get_client('stepfunctions').start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=execution_name,
        input=json.dumps(input_payload)
    )
    
    logger.info(f"Started document pipeline: {response['executionArn']}")
    
    return {
        'execution_arn': response['executionArn'],
        'execution_name': execution_name,
        'input_payload': input_payload
    }


def start_qa_pipeline(document_id: str, bucket: str, key: str,
                      validation: Dict[str, Any]) -> Dict[str, Any]:
    """
    Start Q&A batch processing by directly invoking Lambda 08.
    
    No separate Step Function needed - Lambda 08 handles the entire Q&A workflow.
    """
    input_payload = {
        'document_id': document_id,
        's3_bucket': bucket,
        's3_key': key,
        'file_name': validation.get('file_name'),
        'file_type': validation.get('file_type', FileType.UNSUPPORTED).value,
        'workflow_type': WorkflowType.QA_BATCH.value,
        'timestamp': datetime.utcnow().isoformat()
    }
    
    if BATCH_QA_LAMBDA_ARN:
        # Async invoke Lambda 08 (Batch Q&A Processor)
        response = get_client('lambda').invoke(
            FunctionName=BATCH_QA_LAMBDA_ARN,
            InvocationType='Event',  # Async - don't wait for response
            Payload=json.dumps(input_payload)
        )
        
        logger.info(f"Invoked Batch Q&A Lambda async: {BATCH_QA_LAMBDA_ARN}")
        
        return {
            'invocation_type': 'lambda_async',
            'lambda_arn': BATCH_QA_LAMBDA_ARN,
            'status_code': response.get('StatusCode', 0),
            'input_payload': input_payload
        }
    else:
        # No Lambda ARN configured - log warning and return payload for manual processing
        logger.warning("BATCH_QA_LAMBDA_ARN not set - Q&A processing not started")
        
        return {
            'invocation_type': 'not_configured',
            'message': 'BATCH_QA_LAMBDA_ARN not set - configure to enable Q&A processing',
            'input_payload': input_payload
        }


# ============================================================================
# EVENT PROCESSING
# ============================================================================

def process_s3_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a single S3 event record.
    
    Returns processing result with status and details.
    """
    try:
        # Extract S3 event details
        s3_info = record.get('s3', {})
        bucket = s3_info.get('bucket', {}).get('name', '')
        key = urllib.parse.unquote_plus(s3_info.get('object', {}).get('key', ''))
        size = s3_info.get('object', {}).get('size', 0)
        event_name = record.get('eventName', '')
        
        logger.info(f"Processing: {bucket}/{key} ({size} bytes) - Event: {event_name}")
        
        # Determine workflow based on folder
        workflow = determine_workflow(key)
        
        if workflow == WorkflowType.IGNORE:
            logger.info(f"Ignoring file in non-trigger folder: {key}")
            return {
                'status': 'ignored',
                'reason': 'non_trigger_folder',
                'bucket': bucket,
                'key': key
            }
        
        # Validate file
        validation = validate_file(key, size)
        
        if not validation.get('valid'):
            logger.warning(f"Invalid file: {key} - {validation.get('reason')}")
            return {
                'status': 'skipped',
                'reason': validation.get('reason'),
                'bucket': bucket,
                'key': key
            }
        
        # Generate document ID
        document_id = generate_document_id(bucket, key)
        
        # Check for duplicates by document ID
        if check_duplicate(document_id):
            logger.info(f"Duplicate document skipped: {document_id}")
            return {
                'status': 'duplicate',
                'document_id': document_id,
                'bucket': bucket,
                'key': key
            }
        
        # Compute content hash for content-based deduplication
        content_hash = compute_content_hash(bucket, key)
        
        # Check for content duplicates (same file with different name)
        if content_hash:
            existing = check_content_duplicate(content_hash, document_id)
            if existing:
                logger.info(f"Content duplicate found: {key} matches {existing['s3_key']}")
                return {
                    'status': 'content_duplicate',
                    'document_id': document_id,
                    'existing_document_id': existing['document_id'],
                    'existing_file': existing.get('file_name'),
                    'bucket': bucket,
                    'key': key
                }
        
        # Create tracking records with content hash
        create_document_record(document_id, bucket, key, validation, workflow, content_hash)
        create_pipeline_record(document_id, workflow, {
            'bucket': bucket,
            'key': key,
            'validation': {
                k: v.value if isinstance(v, (FileType, WorkflowType)) else v 
                for k, v in validation.items()
            }
        })
        
        # Start appropriate workflow
        if workflow == WorkflowType.DOCUMENT_PIPELINE:
            execution = start_document_pipeline(document_id, bucket, key, validation)
        elif workflow == WorkflowType.QA_BATCH:
            execution = start_qa_pipeline(document_id, bucket, key, validation)
        else:
            raise ValueError(f"Unknown workflow type: {workflow}")
        
        return {
            'status': 'started',
            'document_id': document_id,
            'workflow': workflow.value,
            'execution_arn': execution.get('execution_arn'),
            'bucket': bucket,
            'key': key
        }
        
    except Exception as e:
        logger.error(f"Error processing record: {e}", exc_info=True)
        return {
            'status': 'error',
            'error': str(e),
            'bucket': record.get('s3', {}).get('bucket', {}).get('name', ''),
            'key': record.get('s3', {}).get('object', {}).get('key', '')
        }


# ============================================================================
# LAMBDA HANDLER
# ============================================================================

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for S3 events.
    
    Receives S3 ObjectCreated events and routes to appropriate workflow.
    
    Event Structure:
    {
        "Records": [
            {
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": "mechavatar-lab-cac1-s3-inbound"},
                    "object": {"key": "documents/file.pdf", "size": 12345}
                }
            }
        ]
    }
    """
    start_time = datetime.utcnow()
    request_id = context.aws_request_id if context else 'local'
    set_correlation_id(request_id)
    try:
        event = S3EventModel.model_validate(event).model_dump(by_alias=True)
    except ValidationError as ve:
        log_event('validation_error', 'Invalid S3 event payload', errors=ve.errors())
        return {'statusCode': 400, 'body': {'error': 'Invalid S3 event payload'}}

    log_event('request_start', 'File Router invoked', record_count=len(event.get('Records', [])))
    
    results = []
    
    for record in event.get('Records', []):
        # Only process S3 events
        if record.get('eventSource') != 'aws:s3':
            log_event('record_skipped', 'Skipping non-S3 event', event_source=record.get('eventSource'))
            continue
        
        result = process_s3_record(record)
        results.append(result)
    
    # Summary
    started = sum(1 for r in results if r.get('status') == 'started')
    skipped = sum(1 for r in results if r.get('status') in ('skipped', 'ignored', 'duplicate'))
    errors = sum(1 for r in results if r.get('status') == 'error')
    
    duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
    log_event('request_complete', 'Processing complete', started=started, skipped=skipped, errors=errors, duration_ms=duration_ms)
    
    return {
        'statusCode': 200,
        'body': {
            'processed': len(results),
            'started': started,
            'skipped': skipped,
            'errors': errors,
            'results': results
        }
    }

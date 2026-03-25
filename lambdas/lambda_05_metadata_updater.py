"""
Lambda 5: Metadata Updater & Pipeline Finalizer (Unified S3 Architecture)
=========================================================================
Finalizes document processing and updates all metadata.
Also handles reprocessing of failed documents.

Trigger:
    - Step Functions finalize step or manual invocation

Actions:
    - finalize (default): Complete document processing and update metadata
    - reprocess_single: Reprocess a single failed document by ID
    - reprocess_failed: Reprocess all failed documents
    - get_failed_summary: Get count of failed documents by stage

Flow:
    - Receives completion signal from Step Functions
    - Updates document metadata in DynamoDB
    - Marks pipeline as complete
    - Generates processing summary

Configuration:
    - Memory: 512 MB
    - Timeout: 60 seconds
    - Runtime: Python 3.11
    - Layers: None (boto3 only)
    - Trigger: Step Functions finalize step or manual invoke

Environment Variables:
    - AWS_DEFAULT_REGION: ca-central-1
    - AWS_ACCOUNT_ID: 939620275271
    - BUCKET_NAME: mechavatar-lab-cac1-s3-inbound
    - DOCUMENTS_TABLE: mechavatar-lab-cac1-mech-documents-metadata
    - PIPELINE_TABLE: mechavatar-lab-cac1-mech-processing-pipeline
    - STATE_MACHINE_ARN: arn:aws:states:ca-central-1:939620275271:stateMachine:mechavatar-lab-cac1-document-processing-stepfn

Settings:
    - Idempotent skip if already completed
    - Reprocess batch limit defaults to 20
"""

import json
import boto3
import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from pydantic import BaseModel, ConfigDict, ValidationError
from botocore.exceptions import BotoCoreError, ClientError

# Import centralized BMO configuration
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from config import (
        AWS_REGION, AWS_ACCOUNT_ID, BUCKET_NAME,
        DOCUMENTS_TABLE, PIPELINE_TABLE, STATE_MACHINE_ARN,
        IDENTIFIERS_TABLE, DOC_CODE_LINKS_TABLE,
        ENABLE_IDENTIFIER_INDEX, ENABLE_DOC_CODE_LINKING
    )
except ImportError:
    # Fallback to environment variables if config not available
    AWS_REGION = os.environ.get('AWS_DEFAULT_REGION', 'ca-central-1')
    AWS_ACCOUNT_ID = os.environ.get('AWS_ACCOUNT_ID', '939620275271')
    DOCUMENTS_TABLE = os.environ.get('DOCUMENTS_TABLE', 'mechavatar-lab-cac1-mech-documents-metadata')
    PIPELINE_TABLE = os.environ.get('PIPELINE_TABLE', 'mechavatar-lab-cac1-mech-processing-pipeline')
    STATE_MACHINE_ARN = os.environ.get('STATE_MACHINE_ARN', '')
    BUCKET_NAME = os.environ.get('S3_BUCKET', 'mechavatar-lab-cac1-s3-inbound')
    IDENTIFIERS_TABLE = os.environ.get('IDENTIFIERS_TABLE', 'mechavatar-lab-cac1-mech-deduplication-index')
    DOC_CODE_LINKS_TABLE = os.environ.get('DOC_CODE_LINKS_TABLE', 'mechavatar-lab-cac1-mech-deduplication-index')
    ENABLE_IDENTIFIER_INDEX = os.environ.get('ENABLE_IDENTIFIER_INDEX', 'true').lower() == 'true'
    ENABLE_DOC_CODE_LINKING = os.environ.get('ENABLE_DOC_CODE_LINKING', 'true').lower() == 'true'

import re

MAX_REPROCESS_BATCH = 20
REPROCESS_COOLDOWN_HOURS = 1

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
# AWS CLIENTS (Lazy Loading)
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

class MetadataEvent(BaseModel):
    model_config = ConfigDict(extra='allow')
    action: Optional[str] = 'finalize'
    document_id: Optional[str] = None
    limit: Optional[int] = None
    file_name: Optional[str] = None
    file_type: Optional[str] = None
    chunk_count: Optional[int] = None
    indexed_count: Optional[int] = None

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
# METADATA OPERATIONS
# ============================================================================

def update_document_metadata(document_id: str, metadata: Dict[str, Any]) -> bool:
    """Update document metadata in DynamoDB documents table."""
    try:
        table = get_resource('dynamodb').Table(DOCUMENTS_TABLE)
        
        update_expr = """
            SET #status = :status,
                file_name = :file_name,
                file_type = :file_type,
                chunk_count = :chunk_count,
                indexed_count = :indexed_count,
                completion_time = :completion_time,
                total_processing_ms = :total_ms,
                is_searchable = :searchable,
                extraction_method = :extraction_method,
                word_count = :word_count,
                char_count = :char_count,
                token_count = :token_count,
                last_updated = :updated
        """
        
        table.update_item(
            Key={'document_id': document_id},
            UpdateExpression=update_expr,
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={
                ':status': 'completed',
                ':file_name': metadata.get('file_name', 'unknown'),
                ':file_type': metadata.get('file_type', 'unknown'),
                ':chunk_count': metadata.get('chunk_count', 0),
                ':indexed_count': metadata.get('indexed_count', 0),
                ':completion_time': datetime.utcnow().isoformat(),
                ':total_ms': metadata.get('total_processing_ms', 0),
                ':searchable': True,
                ':extraction_method': metadata.get('extraction_method', 'unknown'),
                ':word_count': metadata.get('word_count', 0),
                ':char_count': metadata.get('char_count', 0),
                ':token_count': metadata.get('token_count', 0),
                ':updated': datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Updated document metadata: {document_id}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to update document metadata: {e}")
        return False

def update_pipeline_status(document_id: str, status: str, 
                           metrics: Dict[str, Any] = None) -> bool:
    """Update pipeline status to completed."""
    try:
        table = get_resource('dynamodb').Table(PIPELINE_TABLE)
        
        update_expr = """
            SET #status = :status,
                completion_time = :completion_time,
                last_updated = :updated
        """
        expr_values = {
            ':status': status,
            ':completion_time': datetime.utcnow().isoformat(),
            ':updated': datetime.utcnow().isoformat()
        }
        expr_names = {'#status': 'status'}
        
        if metrics:
            update_expr += ', processing_metrics = :metrics'
            expr_values[':metrics'] = json.dumps(metrics)
        
        table.update_item(
            Key={'document_id': document_id},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_values
        )
        
        logger.info(f"Updated pipeline status: {document_id} -> {status}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to update pipeline status: {e}")
        return False

def calculate_total_metrics(event: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate comprehensive processing metrics from event data."""
    
    # Extract metrics from various stages
    extraction = event.get('extraction', {})
    metrics = event.get('metrics', {})
    
    processing_ms = metrics.get('processing_ms', 0)
    embedding_ms = metrics.get('embedding_ms', 0)
    indexing_ms = metrics.get('indexing_ms', 0)
    
    total_ms = processing_ms + embedding_ms + indexing_ms
    
    return {
        'processing_ms': processing_ms,
        'embedding_ms': embedding_ms,
        'indexing_ms': indexing_ms,
        'total_processing_ms': total_ms,
        'token_count': metrics.get('token_count', 0),
        'chunk_count': event.get('chunk_count', 0),
        'indexed_count': event.get('indexed_count', event.get('chunk_count', 0)),
        'word_count': extraction.get('word_count', 0),
        'char_count': extraction.get('char_count', 0),
        'pages': extraction.get('pages', 0),
        'tables': extraction.get('tables', 0),
        'extraction_method': extraction.get('method', 'unknown')
    }

# ============================================================================
# IDENTIFIER EXTRACTION (COBOL, JCL, TABLES, FILES)
# ============================================================================

# Mainframe identifier patterns
IDENTIFIER_PATTERNS = {
    # COBOL programs
    'cobol_program': [
        r'\bPROGRAM-ID[.\s]+([A-Z][A-Z0-9]{1,7})',  # PROGRAM-ID. PROGNAME
        r'\bCALL\s+["\']([A-Z][A-Z0-9]{1,7})["\']',  # CALL 'PROGNAME'
        r'\bCALL\s+([A-Z][A-Z0-9]{1,7})\b(?!\s*USING)',  # CALL PROGNAME (no quotes)
    ],
    
    # JCL jobs and procs
    'jcl_job': [
        r'^//([A-Z][A-Z0-9]{1,7})\s+JOB\b',  # //JOBNAME JOB
        r'^//\s*EXEC\s+([A-Z][A-Z0-9]{1,7})\b',  # // EXEC PROCNAME
        r'\bEXEC\s+PGM=([A-Z][A-Z0-9]{1,7})\b',  # EXEC PGM=PROGNAME
    ],
    
    # DB2/BDD tables (MECH-specific patterns)
    'table_name': [
        r'\bMY[A-Z]{2,6}\d?\b',  # MYTP03, MYBFTA, MYASSH
        r'\bBDD[A-Z0-9]{2,8}\b',  # BDDOPTR, BDDMSPA
        r'\bFSFL[A-Z]{2,4}\b',  # FSFLRTB, FSFLLAN
        r'\bFROM\s+([A-Z][A-Z0-9_]{2,20})\b',  # FROM TABLENAME
        r'\bINTO\s+([A-Z][A-Z0-9_]{2,20})\b',  # INTO TABLENAME
        r'\bUPDATE\s+([A-Z][A-Z0-9_]{2,20})\b',  # UPDATE TABLENAME
        r'\bJOIN\s+([A-Z][A-Z0-9_]{2,20})\b',  # JOIN TABLENAME
    ],
    
    # Dataset/file names
    'file_name': [
        r'\bDSN=([A-Z][A-Z0-9.]{5,44})\b',  # DSN=HLQ.QUAL.DSNAME
        r'\bDD\s+DSN=([A-Z][A-Z0-9.]{5,44})\b',  # DD DSN=...
        r'\bCOPY\s+([A-Z][A-Z0-9]{1,8})\b',  # COPY COPYBOOK
        r'\bINCLUDE\s+MEMBER=([A-Z][A-Z0-9]{1,8})\b',  # INCLUDE MEMBER=...
    ],
    
    # CICS transactions
    'cics_txn': [
        r'\bEXEC\s+CICS\s+([A-Z0-9]{4})\b',  # EXEC CICS TRAN
        r'\bTRANSACTION[=\s]+([A-Z0-9]{4})\b',  # TRANSACTION=XXXX
    ],
    
    # Field names (MECH-specific)
    'field_name': [
        r'\bDABI[A-Z]{2,6}\b',  # DABISEC, DABITAP
        r'\bOPTR[A-Z]{2,5}\b',  # OPTRTYP
        r'\bOPDM[A-Z]{2,5}\b',  # OPDMLGT
        r'\bSPTL[A-Z]{2,6}\b',  # SPTLNCT
        r'\bJIEN[A-Z]{2,4}\b',  # JIENAMT, JIENPOS
    ],
}

def extract_identifiers(text: str) -> Dict[str, List[str]]:
    """
    Extract mainframe identifiers from text using regex patterns.
    
    Returns:
        Dict with identifier types as keys and lists of unique identifiers as values.
    """
    if not text:
        return {}
    
    # Normalize text
    text_upper = text.upper()
    
    identifiers = {}
    
    for id_type, patterns in IDENTIFIER_PATTERNS.items():
        found = set()
        for pattern in patterns:
            try:
                matches = re.findall(pattern, text_upper, re.MULTILINE | re.IGNORECASE)
                for match in matches:
                    # Handle tuple results from groups
                    if isinstance(match, tuple):
                        match = match[0] if match[0] else match[-1]
                    if match and len(match) >= 2:
                        found.add(match.strip())
            except re.error:
                continue
        
        if found:
            identifiers[id_type] = sorted(list(found))
    
    return identifiers

def write_identifiers_to_dynamodb(document_id: str, file_name: str, 
                                   identifiers: Dict[str, List[str]]) -> int:
    """
    Write extracted identifiers to DynamoDB inverted index.
    
    Schema: identifier (PK) -> list of document_ids containing it
    
    Returns: Number of identifiers indexed
    """
    if not ENABLE_IDENTIFIER_INDEX or not identifiers:
        return 0
    
    table = get_resource('dynamodb').Table(IDENTIFIERS_TABLE)
    now = datetime.utcnow().isoformat()
    count = 0
    
    for id_type, id_list in identifiers.items():
        for identifier in id_list:
            try:
                # Composite key: type#identifier
                pk = f"{id_type}#{identifier}"
                
                # Update with SET add to document list (idempotent)
                table.update_item(
                    Key={'identifier_key': pk},
                    UpdateExpression='''
                        SET identifier_type = :type,
                            identifier_value = :value,
                            updated_at = :ts
                        ADD document_ids :doc_set
                    ''',
                    ExpressionAttributeValues={
                        ':type': id_type,
                        ':value': identifier,
                        ':ts': now,
                        ':doc_set': {document_id}
                    }
                )
                count += 1
            except Exception as e:
                logger.warning(f"Failed to index identifier {identifier}: {e}")
    
    logger.info(f"Indexed {count} identifiers for document {document_id}")
    return count

def write_doc_code_links(document_id: str, file_name: str,
                          identifiers: Dict[str, List[str]]) -> int:
    """
    Write bidirectional document-code links to DynamoDB.
    
    Enables queries like:
    - "Which documents mention program MECH001?"
    - "What programs are referenced in document X?"
    
    Returns: Number of links created
    """
    if not ENABLE_DOC_CODE_LINKING or not identifiers:
        return 0
    
    table = get_resource('dynamodb').Table(DOC_CODE_LINKS_TABLE)
    now = datetime.utcnow().isoformat()
    count = 0
    
    # Flatten all identifiers
    all_identifiers = []
    for id_type, id_list in identifiers.items():
        for ident in id_list:
            all_identifiers.append({
                'type': id_type,
                'value': ident
            })
    
    try:
        # Write document -> identifiers mapping
        table.put_item(
            Item={
                'pk': f"DOC#{document_id}",
                'sk': 'IDENTIFIERS',
                'document_id': document_id,
                'file_name': file_name,
                'identifiers': all_identifiers,
                'identifier_count': len(all_identifiers),
                'cobol_programs': identifiers.get('cobol_program', []),
                'jcl_jobs': identifiers.get('jcl_job', []),
                'tables': identifiers.get('table_name', []),
                'files': identifiers.get('file_name', []),
                'cics_transactions': identifiers.get('cics_txn', []),
                'updated_at': now
            }
        )
        count += 1
        
        # Write identifier -> document mappings (for reverse lookup)
        for id_type, id_list in identifiers.items():
            for ident in id_list:
                try:
                    table.update_item(
                        Key={
                            'pk': f"ID#{id_type}#{ident}",
                            'sk': f"DOC#{document_id}"
                        },
                        UpdateExpression='''
                            SET document_id = :doc_id,
                                file_name = :fname,
                                identifier_type = :type,
                                identifier_value = :value,
                                updated_at = :ts
                        ''',
                        ExpressionAttributeValues={
                            ':doc_id': document_id,
                            ':fname': file_name,
                            ':type': id_type,
                            ':value': ident,
                            ':ts': now
                        }
                    )
                    count += 1
                except Exception as e:
                    logger.warning(f"Failed to create link for {ident}: {e}")
    except Exception as e:
        logger.error(f"Failed to write doc-code links: {e}")
    
    logger.info(f"Created {count} doc-code links for document {document_id}")
    return count

def process_identifiers_for_document(document_id: str, file_name: str, 
                                      text_content: str = None,
                                      chunks_s3_key: str = None) -> Dict[str, Any]:
    """
    Process identifier extraction for a document.
    
    Can extract from:
    - Provided text content directly
    - Chunks file from S3
    
    Returns: Summary of extracted identifiers
    """
    text = text_content or ''
    
    # If no text provided, try to load from chunks file
    if not text and chunks_s3_key:
        try:
            s3 = get_client('s3')
            response = s3.get_object(Bucket=BUCKET_NAME, Key=chunks_s3_key)
            content = response['Body'].read().decode('utf-8')
            
            # Concatenate all chunk texts
            chunks_text = []
            for line in content.strip().split('\n'):
                if line.strip():
                    try:
                        chunk = json.loads(line)
                        chunks_text.append(chunk.get('text', ''))
                    except json.JSONDecodeError:
                        continue
            text = '\n'.join(chunks_text)
        except Exception as e:
            logger.warning(f"Could not load chunks from {chunks_s3_key}: {e}")
    
    if not text:
        return {
            'document_id': document_id,
            'identifiers_found': 0,
            'identifiers': {},
            'links_created': 0
        }
    
    # Extract identifiers
    identifiers = extract_identifiers(text)
    
    # Write to DynamoDB
    indexed_count = write_identifiers_to_dynamodb(document_id, file_name, identifiers)
    links_count = write_doc_code_links(document_id, file_name, identifiers)
    
    # Count total identifiers
    total_identifiers = sum(len(v) for v in identifiers.values())
    
    return {
        'document_id': document_id,
        'identifiers_found': total_identifiers,
        'identifiers': identifiers,
        'indexed_count': indexed_count,
        'links_created': links_count
    }

def lookup_documents_by_identifier(identifier_type: str, identifier_value: str) -> List[str]:
    """
    Look up documents containing a specific identifier.
    
    Args:
        identifier_type: cobol_program, jcl_job, table_name, file_name, cics_txn
        identifier_value: The identifier to search for
    
    Returns: List of document_ids
    """
    table = get_resource('dynamodb').Table(IDENTIFIERS_TABLE)
    
    pk = f"{identifier_type}#{identifier_value.upper()}"
    
    try:
        response = table.get_item(Key={'identifier_key': pk})
        if 'Item' in response:
            return list(response['Item'].get('document_ids', set()))
        return []
    except Exception as e:
        logger.error(f"Error looking up identifier {pk}: {e}")
        return []

def handle_extract_identifiers(event: Dict[str, Any]) -> Dict[str, Any]:
    """Handler for identifier extraction action."""
    document_id = event.get('document_id')
    file_name = event.get('file_name', 'unknown')
    text_content = event.get('text_content')
    chunks_s3_key = event.get('chunks_s3_key')
    
    if not document_id:
        return {'statusCode': 400, 'error': 'document_id required'}
    
    # If no text or chunks key, try to construct chunks key
    if not text_content and not chunks_s3_key:
        chunks_s3_key = f"chunks/{document_id}.jsonl"
    
    result = process_identifiers_for_document(
        document_id, file_name, text_content, chunks_s3_key
    )
    
    return {
        'statusCode': 200,
        'document_id': document_id,
        'result': result
    }

# ============================================================================
# REPROCESSING FUNCTIONS
# ============================================================================

def get_failed_documents(limit: int = MAX_REPROCESS_BATCH) -> list:
    """Get documents that failed processing."""
    dynamodb = get_client('dynamodb')
    
    failed_statuses = [
        'extraction_failed', 'embedding_failed', 'indexing_failed', 
        'metadata_failed', 'failed', 'finalization_failed'
    ]
    
    failed_docs = []
    
    for status in failed_statuses:
        try:
            # Try GSI first
            response = dynamodb.query(
                TableName=DOCUMENTS_TABLE,
                IndexName='status-index',
                KeyConditionExpression='#status = :status',
                ExpressionAttributeNames={'#status': 'status'},
                ExpressionAttributeValues={':status': {'S': status}},
                Limit=limit - len(failed_docs)
            )
            
            for item in response.get('Items', []):
                failed_docs.append({
                    'document_id': item.get('document_id', {}).get('S'),
                    'status': item.get('status', {}).get('S'),
                    's3_key': item.get('s3_key', {}).get('S'),
                    's3_bucket': item.get('s3_bucket', {}).get('S', BUCKET_NAME),
                    'file_name': item.get('file_name', {}).get('S'),
                    'file_type': item.get('file_type', {}).get('S'),
                    'error': item.get('error', {}).get('S', ''),
                })
                if len(failed_docs) >= limit:
                    break
                    
        except Exception as e:
            # GSI might not exist, use scan as fallback
            if 'ResourceNotFoundException' in str(type(e).__name__) or 'ValidationException' in str(e):
                try:
                    response = dynamodb.scan(
                        TableName=DOCUMENTS_TABLE,
                        FilterExpression='#status = :status',
                        ExpressionAttributeNames={'#status': 'status'},
                        ExpressionAttributeValues={':status': {'S': status}},
                        Limit=limit
                    )
                    for item in response.get('Items', []):
                        failed_docs.append({
                            'document_id': item.get('document_id', {}).get('S'),
                            'status': item.get('status', {}).get('S'),
                            's3_key': item.get('s3_key', {}).get('S'),
                            's3_bucket': item.get('s3_bucket', {}).get('S', BUCKET_NAME),
                            'file_name': item.get('file_name', {}).get('S'),
                            'file_type': item.get('file_type', {}).get('S'),
                        })
                except Exception:
                    pass
            else:
                logger.warning(f"Error querying status {status}: {e}")
        
        if len(failed_docs) >= limit:
            break
    
    return failed_docs[:limit]

def get_document_by_id(document_id: str) -> Optional[Dict[str, Any]]:
    """Get a specific document by ID."""
    try:
        table = get_resource('dynamodb').Table(DOCUMENTS_TABLE)
        response = table.get_item(Key={'document_id': document_id})
        
        if 'Item' not in response:
            return None
        
        item = response['Item']
        return {
            'document_id': item.get('document_id'),
            'status': item.get('status'),
            's3_key': item.get('s3_key'),
            's3_bucket': item.get('s3_bucket', BUCKET_NAME),
            'file_name': item.get('file_name'),
            'file_type': item.get('file_type'),
            'error': item.get('error', ''),
        }
    except Exception as e:
        logger.error(f"Error getting document {document_id}: {e}")
        return None

def trigger_reprocessing(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Trigger Step Functions to reprocess a document."""
    sfn = get_client('stepfunctions')
    
    if not STATE_MACHINE_ARN:
        return {
            'document_id': doc['document_id'],
            'success': False,
            'error': 'STATE_MACHINE_ARN not configured'
        }
    
    # Prepare input for Step Functions
    sfn_input = {
        'document_id': doc['document_id'],
        's3_bucket': doc.get('s3_bucket', BUCKET_NAME),
        's3_key': doc['s3_key'],
        'file_name': doc.get('file_name', os.path.basename(doc['s3_key'] or '')),
        'file_type': doc.get('file_type', 'unknown'),
        'reprocess': True,
    }
    
    # Handle file extension
    file_name = sfn_input['file_name']
    ext = os.path.splitext(file_name)[1].lower() if file_name else ''
    sfn_input['file_extension'] = ext
    
    if ext == '.doc':
        sfn_input['file_type'] = 'doc_legacy'
    
    try:
        # Update status to reprocessing
        table = get_resource('dynamodb').Table(DOCUMENTS_TABLE)
        table.update_item(
            Key={'document_id': doc['document_id']},
            UpdateExpression='SET #status = :status, last_attempt = :ts',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={
                ':status': 'reprocessing',
                ':ts': datetime.utcnow().isoformat()
            }
        )
        
        # Start execution
        execution_name = f"reprocess-{doc['document_id']}-{int(datetime.utcnow().timestamp())}"
        
        response = sfn.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=execution_name,
            input=json.dumps(sfn_input)
        )
        
        logger.info(f"Started reprocessing: {doc['document_id']}")
        
        return {
            'document_id': doc['document_id'],
            'success': True,
            'execution_arn': response['executionArn']
        }
        
    except Exception as e:
        logger.error(f"Error triggering reprocessing: {e}")
        return {
            'document_id': doc['document_id'],
            'success': False,
            'error': str(e)
        }

def handle_reprocess_single(document_id: str) -> Dict[str, Any]:
    """Reprocess a single document."""
    doc = get_document_by_id(document_id)
    
    if not doc:
        return {'statusCode': 404, 'error': f'Document {document_id} not found'}
    
    if doc.get('status') == 'completed':
        return {'statusCode': 400, 'error': 'Document already completed'}
    
    if not doc.get('s3_key'):
        return {'statusCode': 400, 'error': 'Document has no S3 key'}
    
    result = trigger_reprocessing(doc)
    return {
        'statusCode': 200 if result['success'] else 500,
        'document_id': document_id,
        'result': result
    }

def handle_reprocess_failed(limit: int = MAX_REPROCESS_BATCH) -> Dict[str, Any]:
    """Reprocess all failed documents."""
    failed_docs = get_failed_documents(limit=limit)
    
    if not failed_docs:
        return {'statusCode': 200, 'message': 'No failed documents', 'processed': 0}
    
    results = []
    success_count = 0
    
    for doc in failed_docs:
        if doc.get('s3_key'):
            result = trigger_reprocessing(doc)
            results.append(result)
            if result['success']:
                success_count += 1
    
    return {
        'statusCode': 200,
        'message': f'Triggered {success_count}/{len(failed_docs)} documents',
        'processed': success_count,
        'total_found': len(failed_docs),
        'results': results
    }

def handle_get_failed_summary() -> Dict[str, Any]:
    """Get summary of failed documents."""
    dynamodb = get_client('dynamodb')
    
    statuses = ['extraction_failed', 'embedding_failed', 'indexing_failed', 'failed']
    summary = {}
    
    for status in statuses:
        try:
            response = dynamodb.scan(
                TableName=DOCUMENTS_TABLE,
                FilterExpression='#status = :status',
                ExpressionAttributeNames={'#status': 'status'},
                ExpressionAttributeValues={':status': {'S': status}},
                Select='COUNT'
            )
            summary[status] = response.get('Count', 0)
        except Exception:
            summary[status] = 0
    
    return {
        'statusCode': 200,
        'total_failed': sum(summary.values()),
        'by_stage': summary
    }

# ============================================================================
# LAMBDA HANDLER
# ============================================================================

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for metadata finalization and reprocessing.
    
    Actions:
        - finalize (default): Finalize document processing (called by Step Functions)
        - reprocess_single: Reprocess a single failed document
        - reprocess_failed: Reprocess all failed documents
        - get_failed_summary: Get count of failed documents by stage
    
    Input for finalize (from Step Functions):
    {
        "document_id": "doc-abc123",
        "file_name": "document.pdf",
        "file_type": "pdf",
        "chunk_count": 42,
        ...
    }
    
    Input for reprocess actions (manual invocation):
    {
        "action": "reprocess_single",
        "document_id": "doc-abc123"
    }
    or
    {
        "action": "reprocess_failed",
        "limit": 10
    }
    """
    request_id = context.aws_request_id if context else 'local'
    set_correlation_id(request_id)
    
    try:
        event = MetadataEvent.model_validate(event).model_dump()
    except ValidationError as e:
        log_event('validation_error', 'Invalid input payload', errors=e.errors())
        return {'statusCode': 400, 'error': 'Invalid input payload'}

    # Check for action-based routing
    action = event.get('action', 'finalize')
    start_time = datetime.utcnow()
    log_event('request_start', 'Metadata Updater invoked', action=action, request_id=request_id)
    
    # Handle reprocessing actions
    if action == 'reprocess_single':
        document_id = event.get('document_id')
        if not document_id:
            log_event('validation_error', 'document_id required for reprocess_single')
            return {'statusCode': 400, 'error': 'document_id required'}
        result = handle_reprocess_single(document_id)
        duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
        success = result.get('statusCode', 500) < 400
        log_event('request_complete', 'Reprocess single complete',
                  document_id=document_id, duration_ms=duration_ms, success=success)
        return result
    
    elif action == 'reprocess_failed':
        limit = int(event.get('limit', MAX_REPROCESS_BATCH))
        result = handle_reprocess_failed(limit)
        duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
        success = result.get('statusCode', 500) < 400
        log_event('request_complete', 'Reprocess failed complete',
                  duration_ms=duration_ms, success=success, limit=limit)
        return result
    
    elif action == 'get_failed_summary':
        result = handle_get_failed_summary()
        duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
        success = result.get('statusCode', 500) < 400
        log_event('request_complete', 'Get failed summary complete',
                  duration_ms=duration_ms, success=success)
        return result
    
    elif action == 'extract_identifiers':
        result = handle_extract_identifiers(event)
        duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
        success = result.get('statusCode', 500) < 400
        log_event('request_complete', 'Extract identifiers complete',
                  duration_ms=duration_ms, success=success,
                  identifiers_found=result.get('result', {}).get('identifiers_found', 0))
        return result
    
    # Default: finalize action (original behavior)
    
    try:
        # Extract parameters
        document_id = event.get('document_id')
        file_name = event.get('file_name', 'unknown')
        file_type = event.get('file_type', 'unknown')
        chunk_count = event.get('chunk_count', 0)
        indexed_count = event.get('indexed_count', chunk_count)
        
        log_event('parameters_parsed', 'Finalize parameters',
              document_id=document_id, file_name=file_name, file_type=file_type,
              chunk_count=chunk_count, indexed_count=indexed_count)
        
        if not document_id:
            log_event('validation_error', 'Missing required parameter: document_id')
            raise ValueError("Missing required parameter: document_id")

        current_status = get_pipeline_status(document_id)
        if current_status == 'completed':
            log_event('idempotent_skip', 'Metadata already finalized',
                      document_id=document_id, status=current_status)
            return {
                'statusCode': 200,
                'document_id': document_id,
                'status': current_status,
                'message': 'Metadata already finalized'
            }
        
        # Calculate comprehensive metrics
        metrics = calculate_total_metrics(event)
        
        # Prepare document metadata
        doc_metadata = {
            'file_name': file_name,
            'file_type': file_type,
            'chunk_count': chunk_count,
            'indexed_count': indexed_count,
            'total_processing_ms': metrics['total_processing_ms'],
            'extraction_method': metrics['extraction_method'],
            'word_count': metrics['word_count'],
            'char_count': metrics['char_count'],
            'token_count': metrics['token_count']
        }
        
        # Update document metadata
        if not update_document_metadata(document_id, doc_metadata):
            raise Exception("Failed to update document metadata")
        
        # Extract identifiers from chunks (COBOL, JCL, tables, files)
        identifier_result = {'identifiers_found': 0, 'identifiers': {}}
        if ENABLE_IDENTIFIER_INDEX or ENABLE_DOC_CODE_LINKING:
            try:
                chunks_s3_key = f"chunks/{document_id}.jsonl"
                identifier_result = process_identifiers_for_document(
                    document_id, file_name, 
                    text_content=None, 
                    chunks_s3_key=chunks_s3_key
                )
                log_event('identifiers_extracted', 'Identifier extraction complete',
                          document_id=document_id,
                          identifiers_found=identifier_result.get('identifiers_found', 0))
            except Exception as e:
                logger.warning(f"Identifier extraction failed (non-blocking): {e}")
        
        # Update pipeline status to completed
        if not update_pipeline_status(document_id, 'completed', metrics):
            raise Exception("Failed to update pipeline status")
        
        # Calculate finalization time
        finalization_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
        
        logger.info(f"Document {document_id} processing completed in {metrics['total_processing_ms']}ms")

        log_event('request_complete', 'Metadata finalization complete',
              document_id=document_id, duration_ms=finalization_ms)
        
        return {
            'statusCode': 200,
            'document_id': document_id,
            'status': 'completed',
            'message': 'Document processing completed successfully',
            'summary': {
                'file_name': file_name,
                'file_type': file_type,
                'chunk_count': chunk_count,
                'indexed_count': indexed_count,
                'total_processing_ms': metrics['total_processing_ms'],
                'token_count': metrics['token_count'],
                'extraction_method': metrics['extraction_method'],
                'pages': metrics['pages'],
                'tables': metrics['tables'],
                'finalization_ms': finalization_ms,
                'identifiers_found': identifier_result.get('identifiers_found', 0),
                'identifier_types': list(identifier_result.get('identifiers', {}).keys())
            }
        }
        
    except (ClientError, BotoCoreError) as e:
        log_event('request_error', f"AWS service error: {str(e)}")
        return {
            'statusCode': 502,
            'error': str(e),
            'document_id': event.get('document_id', 'unknown') if isinstance(event, dict) else 'unknown'
        }

    except Exception as e:
        log_event('request_error', f"Metadata finalization failed: {e}")
        logger.exception("Metadata finalization failed")
        
        if 'document_id' in event:
            update_pipeline_status(event['document_id'], 'finalization_failed')
        
        return {
            'statusCode': 500,
            'error': str(e),
            'document_id': event.get('document_id', 'unknown')
        }

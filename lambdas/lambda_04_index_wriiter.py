"""
Lambda 4: OpenSearch Index Writer (Unified S3 Architecture)
============================================================
Writes document chunks with embeddings to OpenSearch Serverless.

Trigger:
    - Step Functions after Lambda 03 embeddings

Flow:
    Input:  mechavatar-lab-cac1-s3-inbound/embeddings/{document_id}.jsonl
    Output: OpenSearch index (mechavatar-lab-cac1-documents)
    - Ensure index exists
    - Load embeddings from S3
    - Prepare documents and bulk index
    - Update pipeline status

Configuration:
    - Memory: 2048 MB
    - Timeout: 600 seconds
    - Runtime: Python 3.11
    - Layers: Layer3 (opensearch-py, requests-aws4auth)
    - Trigger: Step Functions after Lambda 03 embeddings

Environment Variables:
    - AWS_DEFAULT_REGION: ca-central-1
    - AWS_ACCOUNT_ID: 939620275271
    - BUCKET_NAME: mechavatar-lab-cac1-s3-inbound
    - PIPELINE_TABLE: mechavatar-lab-cac1-mech-processing-pipeline
    - OPENSEARCH_HOST: (your OpenSearch endpoint)
    - OPENSEARCH_INDEX: mechavatar-lab-cac1-documents (index NAME, not ARN)
    - PAGEINDEX_INDEX: mechavatar-lab-cac1-pageindex
    - OPENSEARCH_MEMORY_INDEX: mechavatar-lab-cac1-memory
    - EMBEDDING_DIMENSIONS: 1024
    - ENABLE_PAGEINDEX: true
    - BATCH_SIZE, MAX_RETRIES, RETRY_DELAY

Settings:
    - Idempotent skip if already indexed
    - Circuit breaker wraps OpenSearch calls
"""

from __future__ import annotations

import json
import boto3
import os
import logging
import time
import hashlib
from datetime import datetime
from typing import Dict, List, Any, Optional
from pydantic import BaseModel, ConfigDict, ValidationError
from botocore.exceptions import BotoCoreError, ClientError

# External dependencies (Layer 3)
import requests
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection, helpers

# Import centralized BMO configuration
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from config import (
        AWS_REGION, AWS_ACCOUNT_ID, BUCKET_NAME, PIPELINE_TABLE,
        OPENSEARCH_HOST, OPENSEARCH_INDEX, PAGEINDEX_INDEX, OPENSEARCH_MEMORY_INDEX,
        ENABLE_PAGEINDEX, EMBEDDING_DIMENSIONS
    )
except ImportError:
    # Fallback to environment variables if config not available
    AWS_REGION = os.environ.get('AWS_DEFAULT_REGION', 'ca-central-1')
    AWS_ACCOUNT_ID = os.environ.get('AWS_ACCOUNT_ID', '939620275271')
    BUCKET_NAME = os.environ.get('BUCKET_NAME', 'mechavatar-lab-cac1-s3-inbound')
    PIPELINE_TABLE = os.environ.get('PIPELINE_TABLE', 'mechavatar-lab-cac1-mech-processing-pipeline')
    OPENSEARCH_HOST = os.environ.get('OPENSEARCH_HOST', '')
    OPENSEARCH_INDEX = os.environ.get('OPENSEARCH_INDEX', 'mechavatar-lab-cac1-documents')
    PAGEINDEX_INDEX = os.environ.get('PAGEINDEX_INDEX', 'mechavatar-lab-cac1-pageindex')
    OPENSEARCH_MEMORY_INDEX = os.environ.get('OPENSEARCH_MEMORY_INDEX', 'mechavatar-lab-cac1-memory')
    ENABLE_PAGEINDEX = os.environ.get('ENABLE_PAGEINDEX', 'true').lower() == 'true'
    EMBEDDING_DIMENSIONS = int(os.environ.get('EMBEDDING_DIMENSIONS', 1024))

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
# INDEXING CONFIGURATION (env vars for tuning)
# ============================================================================

BATCH_SIZE = int(os.environ.get('BATCH_SIZE', 100))
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', 3))
RETRY_DELAY = float(os.environ.get('RETRY_DELAY', 1.0))

# ============================================================================
# AWS CLIENTS (Lazy Loading)
# ============================================================================

_clients: Dict[str, Any] = {}
_opensearch_client = None
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

class IndexWriterEvent(BaseModel):
    model_config = ConfigDict(extra='allow')
    document_id: str
    embeddings_s3_key: str
    embedding_count: Optional[int] = 0
    file_name: Optional[str] = None
    file_type: Optional[str] = None
    chunk_count: Optional[int] = None

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

def get_opensearch_client() -> OpenSearch:
    """Get or create OpenSearch client with IAM authentication."""
    global _opensearch_client
    
    if _opensearch_client is None:
        if not OPENSEARCH_HOST:
            raise ValueError("OPENSEARCH_HOST environment variable not set")
        
        # Get AWS credentials
        credentials = boto3.Session().get_credentials()
        
        # Create AWS4Auth for OpenSearch Serverless
        auth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            AWS_REGION,
            'aoss',  # OpenSearch Serverless service name
            session_token=credentials.token
        )
        
        # Parse host
        host = OPENSEARCH_HOST.replace('https://', '').replace('http://', '').rstrip('/')
        
        _opensearch_client = OpenSearch(
            hosts=[{'host': host, 'port': 443}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=30
        )
        
        logger.info(f"OpenSearch client initialized for: {host}")
    
    return _opensearch_client

# ============================================================================
# INDEX MANAGEMENT
# ============================================================================

def ensure_index_exists():
    """
    Ensure OpenSearch index exists with proper mapping.
    
    Index mapping for vector search:
    - embedding: k-NN vector field (1024 dimensions)
    - text: Full text content
    - document_id, chunk_id: Identifiers
    - metadata: Document metadata
    """
    client = get_opensearch_client()
    
    # Check if index exists
    if call_with_circuit_breaker('opensearch', client.indices.exists, index=OPENSEARCH_INDEX):
        logger.info(f"Index {OPENSEARCH_INDEX} already exists")
        return True
    
    # Create index with k-NN settings + BM25 for hybrid search
    # PHASE 1.5: Enhanced mappings for hybrid search + rich metadata
    index_body = {
        "settings": {
            "index": {
                "knn": True,
                "knn.algo_param.ef_search": 100
            },
            # BM25 settings for keyword search
            "analysis": {
                "analyzer": {
                    "mech_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "stop", "snowball"]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                # Core identifiers
                "document_id": {"type": "keyword"},
                "chunk_id": {"type": "keyword"},
                "chunk_number": {"type": "integer"},
                
                # Text field with BM25 for hybrid search
                "text": {
                    "type": "text",
                    "analyzer": "mech_analyzer",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256}
                    }
                },
                
                # Vector embedding for semantic search
                "embedding": {
                    "type": "knn_vector",
                    "dimension": 1024,
                    "method": {
                        "name": "hnsw",
                        "space_type": "cosinesimil",
                        "engine": "nmslib",
                        "parameters": {
                            "ef_construction": 128,
                            "m": 24
                        }
                    }
                },
                
                # Document metadata
                "file_name": {"type": "keyword"},
                "file_type": {"type": "keyword"},
                "page_number": {"type": "integer"},
                "section": {"type": "keyword"},
                "word_count": {"type": "integer"},
                "char_count": {"type": "integer"},
                "created_at": {"type": "date"},
                
                # PHASE 1.5: Rich metadata fields
                "section_title": {"type": "keyword"},
                "content_type": {"type": "keyword"},  # procedure|troubleshooting|definition|code|overview
                "entities": {"type": "keyword"},       # Array of extracted entities (TSS, CICS, etc.)
                "topics": {"type": "keyword"},         # Array of topics
                "sensitivity": {"type": "keyword"},    # public|internal|confidential
                "department": {"type": "keyword"},     # For future RBAC filtering
                "upload_date": {"type": "date"},
                "source": {"type": "keyword"},         # SharePoint, S3, etc.
                
                # GENERIC DYNAMIC METADATA FIELDS (from LLM extraction)
                # These are NOT hardcoded categories - they are dynamically extracted
                "doc_summary": {"type": "text"},       # Document-level summary
                "primary_topics": {"type": "keyword"}, # LLM-extracted topics
                "key_terms": {"type": "keyword"},      # Important technical terms
                "doc_type_inferred": {"type": "keyword"},  # LLM-inferred doc type
                "domain_indicators": {"type": "keyword"},  # Domain-specific identifiers

                # MECH-SPECIFIC METADATA FIELDS (for exact filtering)
                "mech_domain": {"type": "keyword"},
                "bdd_tables": {"type": "keyword"},
                "field_names": {"type": "keyword"},
                "shf_segment": {"type": "keyword"},
                "xml_tags": {"type": "keyword"},
                "program_chain": {"type": "keyword"},
                "fsf_fields": {"type": "keyword"},
                "contains_txnlog": {"type": "boolean"},
                "contains_rl03": {"type": "boolean"},
                "contains_xxa": {"type": "boolean"},
                "contains_hex": {"type": "boolean"},
                "contains_jcl": {"type": "boolean"},
                "contains_macro": {"type": "boolean"},
                "is_mapping": {"type": "boolean"},
                "is_procedure": {"type": "boolean"},
                "identifier_count": {"type": "integer"},
                "identifier_density": {"type": "float"},
                
                # Legacy metadata object (for backward compatibility)
                "metadata": {"type": "object", "enabled": False}
            }
        }
    }
    
    try:
        call_with_circuit_breaker(
            'opensearch',
            client.indices.create,
            index=OPENSEARCH_INDEX,
            body=index_body
        )
        logger.info(f"Created index: {OPENSEARCH_INDEX}")
        return True
    except Exception as e:
        if 'resource_already_exists_exception' in str(e).lower():
            logger.info(f"Index {OPENSEARCH_INDEX} already exists (race condition)")
            return True
        raise

def ensure_pageindex_exists() -> bool:
    """
    Ensure PageIndex exists for hierarchical chunk navigation.
    
    PageIndex supports:
    - 3-level hierarchy (section → semantic → child chunks)
    - Parent-child relationships for drill-down
    - Token-aware chunking metadata
    
    Schema aligns with HybridChunker output.
    """
    client = get_opensearch_client()
    
    # Check if index exists
    if call_with_circuit_breaker('opensearch', client.indices.exists, index=PAGEINDEX_INDEX):
        logger.info(f"PageIndex {PAGEINDEX_INDEX} already exists")
        return True
    
    index_body = {
        "settings": {
            "index": {
                "knn": True,
                "knn.algo_param.ef_search": 100
            },
            "analysis": {
                "analyzer": {
                    "mech_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "stop", "snowball"]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                # Core identifiers
                "document_id": {"type": "keyword"},
                "chunk_id": {"type": "keyword"},
                
                # Hierarchy fields (PageIndex)
                "hierarchy_level": {"type": "keyword"},  # section|semantic|child
                "parent_chunk_id": {"type": "keyword"},
                "child_chunk_ids": {"type": "keyword"},  # Array
                "section_number": {"type": "integer"},
                "semantic_chunk_number": {"type": "integer"},
                "child_number": {"type": "integer"},
                
                # Text content
                "text": {
                    "type": "text",
                    "analyzer": "mech_analyzer",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256}
                    }
                },
                
                # Vector embedding (1024 Titan V2)
                "embedding": {
                    "type": "knn_vector",
                    "dimension": EMBEDDING_DIMENSIONS,
                    "method": {
                        "name": "hnsw",
                        "space_type": "cosinesimil",
                        "engine": "nmslib",
                        "parameters": {
                            "ef_construction": 128,
                            "m": 24
                        }
                    }
                },
                
                # Token-aware metadata
                "token_count": {"type": "integer"},
                "char_count": {"type": "integer"},
                "word_count": {"type": "integer"},
                "sentence_count": {"type": "integer"},
                
                # Document context
                "file_name": {"type": "keyword"},
                "file_type": {"type": "keyword"},
                "page_numbers": {"type": "integer"},  # Array for multi-page chunks
                "section_title": {"type": "keyword"},
                "section_prefix": {"type": "text"},  # Contextual prefix from parent
                
                # Content classification  
                "content_type": {"type": "keyword"},  # code|prose|table|mixed
                "is_code_chunk": {"type": "boolean"},
                "code_type": {"type": "keyword"},  # COBOL|JCL|REXX|SQL
                
                # MECH-specific fields
                "program_names": {"type": "keyword"},  # Array
                "table_names": {"type": "keyword"},    # Array
                "file_names": {"type": "keyword"},     # Array
                "identifiers": {"type": "keyword"},    # All extracted identifiers
                
                # Timestamps
                "created_at": {"type": "date"},
                "updated_at": {"type": "date"}
            }
        }
    }
    
    try:
        call_with_circuit_breaker(
            'opensearch',
            client.indices.create,
            index=PAGEINDEX_INDEX,
            body=index_body
        )
        logger.info(f"Created PageIndex: {PAGEINDEX_INDEX}")
        return True
    except Exception as e:
        if 'resource_already_exists_exception' in str(e).lower():
            logger.info(f"PageIndex {PAGEINDEX_INDEX} already exists (race condition)")
            return True
        raise

def ensure_memory_index_exists() -> bool:
    """
    Ensure Memory Index exists for Q&A conversation history.
    
    Memory Index supports:
    - Q&A pair embeddings for semantic memory search
    - Batch session tracking for Excel Q&A workflows
    - TTL-based expiration for cleanup
    
    Used by Lambda 07 (Memory Search) and Lambda 09 (Memory Writer).
    """
    client = get_opensearch_client()
    
    # Check if index exists
    if call_with_circuit_breaker('opensearch', client.indices.exists, index=OPENSEARCH_MEMORY_INDEX):
        logger.info(f"Memory Index {OPENSEARCH_MEMORY_INDEX} already exists")
        return True
    
    index_body = {
        "settings": {
            "index": {
                "knn": True,
                "knn.algo_param.ef_search": 100
            },
            "analysis": {
                "analyzer": {
                    "mech_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "stop", "snowball"]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                # Core identifiers
                "memory_id": {"type": "keyword"},
                "session_id": {"type": "keyword"},
                "batch_session_id": {"type": "keyword"},  # For Excel Q&A batches
                
                # Q&A content
                "question": {
                    "type": "text",
                    "analyzer": "mech_analyzer",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 512}
                    }
                },
                "answer": {
                    "type": "text",
                    "analyzer": "mech_analyzer"
                },
                "combined_text": {
                    "type": "text",
                    "analyzer": "mech_analyzer"
                },
                
                # Vector embedding (for Q&A pair)
                "embedding": {
                    "type": "knn_vector",
                    "dimension": EMBEDDING_DIMENSIONS,
                    "method": {
                        "name": "hnsw",
                        "space_type": "cosinesimil",
                        "engine": "nmslib",
                        "parameters": {
                            "ef_construction": 128,
                            "m": 24
                        }
                    }
                },
                
                # Context and sources
                "source_documents": {"type": "keyword"},  # Array of document_ids used
                "source_chunks": {"type": "keyword"},     # Array of chunk_ids used
                "intent_type": {"type": "keyword"},       # Query intent classification
                "confidence_score": {"type": "float"},
                
                # Batch Q&A metadata
                "batch_file_name": {"type": "keyword"},   # Input Excel file name
                "question_number": {"type": "integer"},   # Row number in Excel
                "total_questions": {"type": "integer"},
                
                # Timestamps and TTL
                "timestamp": {"type": "date"},
                "created_at": {"type": "date"},
                "expires_at": {"type": "date"},  # For TTL-based cleanup
                "ttl_hours": {"type": "integer"},
                
                # Feedback (optional, for future improvement)
                "user_feedback": {"type": "keyword"},  # positive|negative|none
                "feedback_notes": {"type": "text"}
            }
        }
    }
    
    try:
        call_with_circuit_breaker(
            'opensearch',
            client.indices.create,
            index=OPENSEARCH_MEMORY_INDEX,
            body=index_body
        )
        logger.info(f"Created Memory Index: {OPENSEARCH_MEMORY_INDEX}")
        return True
    except Exception as e:
        if 'resource_already_exists_exception' in str(e).lower():
            logger.info(f"Memory Index {OPENSEARCH_MEMORY_INDEX} already exists (race condition)")
            return True
        raise

def ensure_all_indexes_exist() -> Dict[str, bool]:
    """
    Orchestrator to ensure all 3 OpenSearch indexes exist.
    
    Creates (if not present):
    1. documents - Main document chunks with embeddings
    2. pageindex - Hierarchical chunk navigation
    3. memory - Q&A conversation history
    
    Returns: Dict with status for each index
    """
    results = {}
    
    # Main documents index
    try:
        results['documents'] = ensure_index_exists()
    except Exception as e:
        logger.error(f"Failed to ensure documents index: {e}")
        results['documents'] = False
    
    # PageIndex for hierarchical navigation
    if ENABLE_PAGEINDEX:
        try:
            results['pageindex'] = ensure_pageindex_exists()
        except Exception as e:
            logger.error(f"Failed to ensure pageindex: {e}")
            results['pageindex'] = False
    else:
        results['pageindex'] = None  # Disabled
    
    # Memory index for Q&A history
    try:
        results['memory'] = ensure_memory_index_exists()
    except Exception as e:
        logger.error(f"Failed to ensure memory index: {e}")
        results['memory'] = False
    
    logger.info(f"Index status: {results}")
    return results

# ============================================================================
# INDEXING OPERATIONS
# ============================================================================

def prepare_document_for_indexing(chunk: Dict[str, Any], 
                                  file_name: str, 
                                  file_type: str) -> Dict[str, Any]:
    """
    Prepare a chunk document for OpenSearch indexing.
    
    PHASE 1.5: Enhanced with rich metadata fields for:
    - Hybrid search (text is properly analyzed for BM25)
    - Content-type filtering
    - Entity/topic boosting
    - Future RBAC support
    
    Note: OpenSearch Serverless does NOT support explicit document IDs (_id)
    in index operations. IDs are auto-generated. We keep chunk_id in the
    document body for traceability and querying.
    """
    embedding = chunk.get('embedding')
    
    if not embedding or not isinstance(embedding, list):
        logger.warning(f"Chunk {chunk.get('chunk_id')} has no valid embedding")
        return None
    
    # Extract enhanced metadata if available (from Lambda 02 enrichment)
    chunk_metadata = chunk.get('metadata', {})
    
    # OpenSearch Serverless: Do NOT include _id - it's not supported
    # The chunk_id is stored in the document body for querying
    return {
        '_index': OPENSEARCH_INDEX,
        '_source': {
            # Core identifiers
            'document_id': chunk.get('document_id'),
            'chunk_id': chunk.get('chunk_id'),
            'chunk_number': chunk.get('chunk_number', 0),
            
            # Text content (indexed for both KNN and BM25)
            'text': chunk.get('text', ''),
            'embedding': embedding,
            
            # Document metadata
            'file_name': file_name,
            'file_type': file_type,
            'page_number': chunk.get('page_number'),
            'section': chunk.get('section'),
            'word_count': chunk.get('word_count', 0),
            'char_count': chunk.get('char_count', 0),
            'created_at': datetime.utcnow().isoformat(),
            
            # PHASE 1.5: Rich metadata fields
            'section_title': chunk_metadata.get('section_title'),
            'content_type': chunk_metadata.get('content_type', 'unknown'),
            'entities': chunk_metadata.get('entities', []),
            'topics': chunk_metadata.get('topics', []),
            'sensitivity': chunk_metadata.get('sensitivity', 'internal'),
            'department': chunk_metadata.get('department', 'mainframe'),
            'upload_date': chunk_metadata.get('upload_date', datetime.utcnow().isoformat()),
            'source': chunk_metadata.get('source', 's3'),
            
            # GENERIC DYNAMIC METADATA FIELDS (LLM-extracted)
            # These enable semantic matching without hardcoded categories
            'doc_summary': chunk_metadata.get('doc_summary', ''),
            'primary_topics': chunk_metadata.get('primary_topics', []),
            'key_terms': chunk_metadata.get('key_terms', []),
            'doc_type_inferred': chunk_metadata.get('doc_type_inferred', 'unknown'),
            'domain_indicators': chunk_metadata.get('domain_indicators', []),

            # MECH-specific metadata for precise filtering
            'mech_domain': chunk_metadata.get('mech_domain'),
            'bdd_tables': chunk_metadata.get('bdd_tables', []),
            'field_names': chunk_metadata.get('field_names', []),
            'shf_segment': chunk_metadata.get('shf_segment'),
            'xml_tags': chunk_metadata.get('xml_tags', []),
            'program_chain': chunk_metadata.get('program_chain', []),
            'fsf_fields': chunk_metadata.get('fsf_fields', []),
            'contains_txnlog': chunk_metadata.get('contains_txnlog', False),
            'contains_rl03': chunk_metadata.get('contains_rl03', False),
            'contains_xxa': chunk_metadata.get('contains_xxa', False),
            'contains_hex': chunk_metadata.get('contains_hex', False),
            'contains_jcl': chunk_metadata.get('contains_jcl', False),
            'contains_macro': chunk_metadata.get('contains_macro', False),
            'is_mapping': chunk_metadata.get('is_mapping', False),
            'is_procedure': chunk_metadata.get('is_procedure', False),
            'identifier_count': chunk_metadata.get('identifier_count', 0),
            'identifier_density': chunk_metadata.get('identifier_density', 0.0),
            
            # Legacy metadata object (for backward compatibility)
            'metadata': chunk_metadata
        }
    }

def bulk_index_documents(documents: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Bulk index documents to OpenSearch with retry logic.
    
    Returns: {'indexed': N, 'failed': M, 'errors': [...]}
    """
    client = get_opensearch_client()
    
    indexed = 0
    failed = 0
    errors = []
    
    # Process in batches
    for i in range(0, len(documents), BATCH_SIZE):
        batch = documents[i:i + BATCH_SIZE]
        
        for retry in range(MAX_RETRIES):
            try:
                # Use bulk helper
                success, failed_items = call_with_circuit_breaker(
                    'opensearch',
                    helpers.bulk,
                    client,
                    batch,
                    raise_on_error=False,
                    raise_on_exception=False
                )
                
                indexed += success
                
                if failed_items:
                    for idx, item in enumerate(failed_items):
                        error_info = item.get('index', {}).get('error', {})
                        # Get chunk_id from the batch item's source if available
                        chunk_id = 'unknown'
                        if i + idx < len(documents):
                            chunk_id = documents[i + idx].get('_source', {}).get('chunk_id', 'unknown')
                        errors.append({
                            'id': chunk_id,
                            'error': error_info.get('reason', 'Unknown error')
                        })
                        failed += 1
                
                logger.info(f"Batch {i // BATCH_SIZE + 1}: indexed {success}, " 
                           f"failed {len(failed_items) if failed_items else 0}")
                break
                
            except Exception as e:
                logger.warning(f"Batch indexing retry {retry + 1}/{MAX_RETRIES}: {e}")
                
                if retry < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (2 ** retry))
                else:
                    # Mark entire batch as failed
                    failed += len(batch)
                    errors.append({
                        'batch': i // BATCH_SIZE + 1,
                        'error': str(e),
                        'count': len(batch)
                    })
    
    return {
        'indexed': indexed,
        'failed': failed,
        'errors': errors[:10]  # Limit error details
    }

# ============================================================================
# S3 OPERATIONS
# ============================================================================

def load_embeddings_from_s3(s3_key: str) -> List[Dict[str, Any]]:
    """Load chunks with embeddings from JSONL file in S3."""
    try:
        s3 = get_client('s3')
        response = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        
        chunks = []
        for line in content.strip().split('\n'):
            if line.strip():
                chunks.append(json.loads(line))
        
        logger.info(f"Loaded {len(chunks)} chunks from s3://{BUCKET_NAME}/{s3_key}")
        return chunks
        
    except Exception as e:
        logger.error(f"Error loading embeddings from S3: {e}")
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
    Main Lambda handler for OpenSearch indexing.
    
    Input from Step Functions:
    {
        "document_id": "doc-abc123",
        "embeddings_s3_key": "embeddings/doc-abc123.jsonl",
        "embedding_count": 42,
        "file_name": "document.pdf",
        "file_type": "pdf",
        "chunk_count": 42
    }
    
    Output:
    {
        "statusCode": 200,
        "document_id": "doc-abc123",
        "indexed_count": 42,
        "failed_count": 0,
        "metrics": {...}
    }
    """
    start_time = time.time()
    request_id = context.aws_request_id if context else 'local'
    set_correlation_id(request_id)
    log_event('request_start', 'Index Writer invoked', request_id=request_id)
    
    try:
        event = IndexWriterEvent.model_validate(event).model_dump()

        # Extract parameters
        document_id = event.get('document_id')
        embeddings_s3_key = event.get('embeddings_s3_key')
        file_name = event.get('file_name', 'unknown')
        file_type = event.get('file_type', 'unknown')
        expected_count = event.get('embedding_count', 0)
        
        log_event('parameters_parsed', 'Indexing request parameters',
              document_id=document_id, file_name=file_name, file_type=file_type,
              embedding_count=expected_count)
        
        if not all([document_id, embeddings_s3_key]):
            log_event('validation_error', 'Missing required parameters')
            raise ValueError("Missing required parameters: document_id, embeddings_s3_key")

        current_status = get_pipeline_status(document_id)
        completed_statuses = {'indexed', 'partially_indexed', 'completed'}
        if current_status in completed_statuses:
            log_event('idempotent_skip', 'Indexing already completed',
                      document_id=document_id, status=current_status)
            return {
                'statusCode': 200,
                'document_id': document_id,
                'status': current_status,
                'message': 'Indexing already completed'
            }
        
        # Update status
        update_pipeline_status(
            document_id,
            'indexing',
            f'Loading embeddings'
        )
        
        # Ensure all indexes exist (documents, pageindex, memory)
        index_status = ensure_all_indexes_exist()
        log_event('indexes_ready', 'OpenSearch indexes ensured', status=index_status)
        
        # Load embeddings from S3
        chunks = load_embeddings_from_s3(embeddings_s3_key)
        
        if not chunks:
            raise ValueError(f"No chunks found in {embeddings_s3_key}")
        
        # Prepare documents for indexing
        update_pipeline_status(
            document_id,
            'indexing',
            f'Preparing {len(chunks)} documents for indexing'
        )
        
        documents = []
        skipped = 0
        
        for chunk in chunks:
            doc = prepare_document_for_indexing(chunk, file_name, file_type)
            if doc:
                documents.append(doc)
            else:
                skipped += 1
        
        if not documents:
            raise ValueError("No valid documents to index (all chunks missing embeddings)")
        
        # Bulk index
        update_pipeline_status(
            document_id,
            'indexing',
            f'Indexing {len(documents)} documents to OpenSearch'
        )
        
        result = bulk_index_documents(documents)
        
        # Calculate metrics
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        # Update status
        status = 'indexed' if result['failed'] == 0 else 'partially_indexed'
        
        update_pipeline_status(
            document_id,
            status,
            f"Indexed {result['indexed']}/{len(documents)} chunks",
            metadata={
                'indexed_count': result['indexed'],
                'failed_count': result['failed'],
                'skipped_count': skipped,
                'indexing_ms': processing_time_ms
            }
        )

        log_event('request_complete', 'Indexing complete',
                  document_id=document_id, duration_ms=processing_time_ms,
                  indexed=result['indexed'], failed=result['failed'], skipped=skipped)
        
        return {
            'statusCode': 200,
            'document_id': document_id,
            'indexed_count': result['indexed'],
            'failed_count': result['failed'],
            'skipped_count': skipped,
            'total_chunks': len(chunks),
            'file_name': file_name,
            'file_type': file_type,
            'errors': result.get('errors', []),
            'metrics': {
                'indexing_ms': processing_time_ms,
                'indexed': result['indexed'],
                'failed': result['failed']
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
        log_event('request_error', f"Indexing failed: {e}")
        logger.exception("Indexing failed")
        
        if 'document_id' in event:
            update_pipeline_status(
                event['document_id'],
                'indexing_failed',
                str(e)
            )
        
        return {
            'statusCode': 500,
            'error': str(e),
            'document_id': event.get('document_id')
        }

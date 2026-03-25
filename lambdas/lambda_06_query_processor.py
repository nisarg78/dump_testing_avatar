"""
Lambda 6: Query Processor - Enterprise RAG Engine
==================================================
Production-grade query processing with advanced retrieval capabilities.

Architecture:
    - Bedrock Titan V2 (1024-dim embeddings)
    - Bedrock Claude Sonnet (response generation)
    - Bedrock Claude Haiku (intent/expansion)
    - OpenSearch Serverless (hybrid KNN+BM25 index)
    - DynamoDB (conversation memory persistence)

Trigger:
    - API invocation from app or Step Functions query flow

Flow:
    - Validate input and configuration
    - Resolve conversational context (optional)
    - Run parallel LLM tasks (intent, embedding, requirements)
    - Retrieve (hybrid or semantic) and re-rank
    - Generate grounded answer and verify
    - Return response with metrics and citations

Capabilities:
    - Hybrid search (KNN + BM25)
    - Intent classification and query expansion
    - Reranking and decomposition
    - Grounding verification and confidence calibration

Configuration:
    - Memory: 3072 MB (increased for multi-hop + code gen + Haiku-as-judge)
    - Timeout: 300 seconds (increased for multi-hop entity traversal + two-stage retrieval)
    - Runtime: Python 3.12
    - Layers: Layer3 (opensearch-py, requests-aws4auth)
    - Trigger: API Gateway or direct invocation

Environment Variables:
    - AWS_DEFAULT_REGION: AWS region (default: ca-central-1)
    - OPENSEARCH_HOST, OPENSEARCH_INDEX
    - MEMORY_TABLE
    - EMBEDDING_MODEL, GENERATION_MODEL, HAIKU_MODEL
    - TOP_K, MIN_SCORE, SEMANTIC_WEIGHT, KEYWORD_WEIGHT
    - MAX_TOKENS, TEMPERATURE
    - ENABLE_HYBRID_SEARCH, ENABLE_INTENT_CLASSIFICATION
    - ENABLE_QUERY_EXPANSION, ENABLE_MEMORY, ENABLE_EXPLANATIONS
    - ENABLE_EMBEDDING_CACHE, ENABLE_PARALLEL_LLM, ENABLE_RRF_SCORING
    - ENABLE_CROSS_ENCODER, ENABLE_CLOUDWATCH_METRICS
    - EMBEDDING_CACHE_SIZE, PARALLEL_TIMEOUT_SECONDS
    - RRF_K, CROSS_ENCODER_MIN_CONFIDENCE, CROSS_ENCODER_TOP_N
    - CIRCUIT_BREAKER_THRESHOLD, CIRCUIT_BREAKER_RECOVERY_SECONDS
    - DLQ_URL (optional), ENABLE_DLQ (default: true)

    - ENABLE_MECH_CONTEXT (default: false)
    - MECH_CONTEXT_MAX_CHARS (default: 6000)
    
Notes:
    - MECH context is embedded directly in this Lambda (no S3 dependency).

Settings:
    - No PII processing; data remains in ca-central-1
    - Circuit breaker protects downstream services

Author: MECH Avatar Team | Version: 2.0.0
"""

from __future__ import annotations

import json
import boto3
import os
import logging
import time
import hashlib
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple
from enum import Enum
from dataclasses import Field, dataclass, asdict, field
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
import threading
from pydantic import BaseModel, ConfigDict, ValidationError, Field
from botocore.exceptions import BotoCoreError, ClientError

# External dependencies (Layer 3)
import requests
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection
from boto3.dynamodb.conditions import Key

# ============================================================================
# STRUCTURED LOGGING
# ============================================================================

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Correlation ID for distributed tracing
_correlation_id: str = None


def set_correlation_id(request_id: str) -> None:
    """Set correlation ID for request tracing."""
    global _correlation_id
    _correlation_id = request_id


def log_event(event_type: str, message: str, **kwargs) -> None:
    """Structured logging with correlation ID and metrics."""
    log_data = {
        'correlation_id': _correlation_id,
        'event': event_type,
        'message': message,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        **kwargs
    }
    logger.info(json.dumps(log_data))


def _truncate_text(text: Optional[str], limit: int = 200) -> str:
    """Return a compact preview for logs."""
    if not text:
        return ''
    if len(text) <= limit:
        return text
    return f"{text[:limit]}..."


# ============================================================================
# CONFIGURATION & FEATURE FLAGS
# ============================================================================

class Config:
    """Centralized configuration with validation."""
    
    # AWS Region
    AWS_REGION = os.environ.get('AWS_DEFAULT_REGION', 'ca-central-1')
    
    # OpenSearch - Updated to new naming convention
    OPENSEARCH_HOST = os.environ.get('OPENSEARCH_HOST', '')
    OPENSEARCH_INDEX = os.environ.get('OPENSEARCH_INDEX', 'mechavatar-lab-cac1-documents')
    
    # DynamoDB - Updated to new naming convention
    MEMORY_TABLE = os.environ.get('MEMORY_TABLE', 'mechavatar-lab-cac1-mech-chunks-metadata')

    # Optional Doc↔Code links table (identifier -> linked chunk_ids)
    ENABLE_DOC_CODE_LINKS = os.environ.get('ENABLE_DOC_CODE_LINKS', 'true').lower() == 'true'
    DOC_CODE_LINKS_TABLE = os.environ.get('DOC_CODE_LINKS_TABLE', 'mechavatar-lab-cac1-mech-deduplication-index')

    # Code-aware retrieval enhancements
    ENABLE_IDENTIFIER_NARROWING = os.environ.get('ENABLE_IDENTIFIER_NARROWING', 'true').lower() == 'true'
    IDENTIFIER_NARROWING_MAX_IDS = int(os.environ.get('IDENTIFIER_NARROWING_MAX_IDS', 12))
    LINK_EXPANSION_MAX_CHUNKS = int(os.environ.get('LINK_EXPANSION_MAX_CHUNKS', 30))

    # PageIndex navigation (section drill-down using heading hierarchy) - Updated
    PAGEINDEX_INDEX = os.environ.get('PAGEINDEX_INDEX', 'mechavatar-lab-cac1-pageindex')
    ENABLE_PAGEINDEX_NAVIGATION = os.environ.get('ENABLE_PAGEINDEX_NAVIGATION', 'true').lower() == 'true'

    # Async memory writer Lambda (optional)
    MEMORY_WRITER_LAMBDA_ARN = os.environ.get('MEMORY_WRITER_LAMBDA_ARN', '')

    # Persistent project context ("MECH_CONTEXT.md")
    ENABLE_MECH_CONTEXT = os.environ.get('ENABLE_MECH_CONTEXT', 'true').lower() == 'true'
    MECH_CONTEXT_MAX_CHARS = int(os.environ.get('MECH_CONTEXT_MAX_CHARS', 6000))
    
    # Bedrock Models
    EMBEDDING_MODEL = (
        os.environ.get('EMBEDDING_INFERENCE_PROFILE_ARN')
        or os.environ.get('EMBEDDING_MODEL', 'amazon.titan-embed-text-v2:0')
    )
    GENERATION_MODEL = (
        os.environ.get('GENERATION_INFERENCE_PROFILE_ARN')
        or os.environ.get('GENERATION_MODEL', 'anthropic.claude-sonnet-4-20250514-v1:0')
    )
    HAIKU_MODEL = (
        os.environ.get('HAIKU_INFERENCE_PROFILE_ARN')
        or os.environ.get('HAIKU_MODEL', 'anthropic.claude-3-haiku-20240307-v1:0')
    )
    
    # Search Configuration
    # INCREASED from 8 to 12 for better multi-document coverage
    TOP_K = int(os.environ.get('TOP_K', 12))
    MIN_SCORE = float(os.environ.get('MIN_SCORE', 0.25))
    
    # Hybrid Search Weights (must sum to 1.0)
    SEMANTIC_WEIGHT = float(os.environ.get('SEMANTIC_WEIGHT', 0.7))
    KEYWORD_WEIGHT = float(os.environ.get('KEYWORD_WEIGHT', 0.3))
    
    # Response Configuration
    MAX_TOKENS = int(os.environ.get('MAX_TOKENS', 2000))
    TEMPERATURE = float(os.environ.get('TEMPERATURE', 0.2))
    
    # Feature Flags (enterprise toggle capability)
    ENABLE_HYBRID_SEARCH = os.environ.get('ENABLE_HYBRID_SEARCH', 'true').lower() == 'true'
    ENABLE_INTENT_CLASSIFICATION = os.environ.get('ENABLE_INTENT_CLASSIFICATION', 'true').lower() == 'true'
    ENABLE_QUERY_EXPANSION = os.environ.get('ENABLE_QUERY_EXPANSION', 'true').lower() == 'true'
    ENABLE_MEMORY = os.environ.get('ENABLE_MEMORY', 'true').lower() == 'true'
    ENABLE_EXPLANATIONS = os.environ.get('ENABLE_EXPLANATIONS', 'true').lower() == 'true'

    # Phase 2 Feature Flags (Performance & Quality Enhancements)
    ENABLE_EMBEDDING_CACHE = os.environ.get('ENABLE_EMBEDDING_CACHE', 'true').lower() == 'true'
    ENABLE_PARALLEL_LLM = os.environ.get('ENABLE_PARALLEL_LLM', 'true').lower() == 'true'
    ENABLE_RRF_SCORING = os.environ.get('ENABLE_RRF_SCORING', 'true').lower() == 'true'
    ENABLE_CROSS_ENCODER = os.environ.get('ENABLE_CROSS_ENCODER', 'true').lower() == 'true'  
    ENABLE_CLOUDWATCH_METRICS = os.environ.get('ENABLE_CLOUDWATCH_METRICS', 'true').lower() == 'true'

    # DLQ
    DLQ_URL = os.environ.get('DLQ_URL')
    ENABLE_DLQ = os.environ.get('ENABLE_DLQ', 'true').lower() == 'true'

    # Cache Configuration
    EMBEDDING_CACHE_SIZE = int(os.environ.get('EMBEDDING_CACHE_SIZE', 100))

    # Parallel Processing
    PARALLEL_TIMEOUT_SECONDS = float(os.environ.get('PARALLEL_TIMEOUT_SECONDS', 5.0))

    # RRF Configuration
    RRF_K = int(os.environ.get('RRF_K', 60))  # Standard RRF constant

    # Cross-Encoder (only for high-stakes queries)
    CROSS_ENCODER_MIN_CONFIDENCE = float(os.environ.get('CROSS_ENCODER_MIN_CONFIDENCE', 0.6))
    CROSS_ENCODER_TOP_N = int(os.environ.get('CROSS_ENCODER_TOP_N', 5))

    # Circuit Breaker Configuration
    CIRCUIT_BREAKER_THRESHOLD = int(os.environ.get('CIRCUIT_BREAKER_THRESHOLD', 5))
    CIRCUIT_BREAKER_RECOVERY_SECONDS = int(os.environ.get('CIRCUIT_BREAKER_RECOVERY_SECONDS', 30))

    # Rate Limiting
    MAX_RETRIES = 3
    RETRY_BASE_DELAY = 0.5
    
    # =========================================================================
    # ADVANCED RAG FEATURE FLAGS (Phase 3 Enhancement)
    # =========================================================================
    
    # Entity Extraction - Extract mainframe entities (TSS IDs, tables, error codes)
    ENABLE_ENTITY_EXTRACTION = os.environ.get('ENABLE_ENTITY_EXTRACTION', 'true').lower() == 'true'
    ENTITY_BOOST_WEIGHT = float(os.environ.get('ENTITY_BOOST_WEIGHT', 1.5))  # Boost chunks with entities
    
    # Query Result Caching - Cache responses in DynamoDB - Updated
    ENABLE_QUERY_CACHE = os.environ.get('ENABLE_QUERY_CACHE', 'true').lower() == 'true'
    QUERY_CACHE_TABLE = os.environ.get('QUERY_CACHE_TABLE', 'mechavatar-lab-cac1-mech-system-config')
    CACHE_TTL_HOURS = int(os.environ.get('CACHE_TTL_HOURS', 72))
    CACHE_MIN_CONFIDENCE = float(os.environ.get('CACHE_MIN_CONFIDENCE', 0.7))  # Only cache high-confidence
    
    # Entity-Aware Reranking
    ENABLE_ENTITY_RERANKING = os.environ.get('ENABLE_ENTITY_RERANKING', 'true').lower() == 'true'
    
    # =========================================================================
    # PHASE 2 ENHANCEMENTS: Self-RAG Verification
    # =========================================================================
    
    # -------------------------------------------------------------------------
    # BEDROCK RERANK API (DISABLED)
    # -------------------------------------------------------------------------
    # ENABLE_BEDROCK_RERANK = os.environ.get('ENABLE_BEDROCK_RERANK', 'false').lower() == 'true'
    # RERANK_MODEL = os.environ.get('RERANK_MODEL', 'amazon.rerank-v1:0')
    # RERANK_TOP_N = int(os.environ.get('RERANK_TOP_N', 10))
    # RERANK_MIN_SCORE = float(os.environ.get('RERANK_MIN_SCORE', 0.1))
    ENABLE_BEDROCK_RERANK = False  # Disabled - Bedrock Rerank not available in this project
    
    # Self-RAG Verification Loop
    ENABLE_SELF_RAG = os.environ.get('ENABLE_SELF_RAG', 'true').lower() == 'true'
    SELF_RAG_MAX_ITERATIONS = int(os.environ.get('SELF_RAG_MAX_ITERATIONS', 2))
    SELF_RAG_MIN_CONFIDENCE = float(os.environ.get('SELF_RAG_MIN_CONFIDENCE', 0.7))
    SELF_RAG_RETRY_THRESHOLD = float(os.environ.get('SELF_RAG_RETRY_THRESHOLD', 0.5))
    
    # =========================================================================
    # PHASE 2B/2C: CODE GENERATION & ENTITY-AWARE RETRIEVAL
    # =========================================================================
    
    # Entity Registry DynamoDB tables - Updated
    ENTITY_REGISTRY_TABLE = os.environ.get('ENTITY_REGISTRY_TABLE', 'mechavatar-lab-cac1-mech-processing-pipeline')
    RELATIONSHIPS_TABLE = os.environ.get('RELATIONSHIPS_TABLE', 'mechavatar-lab-cac1-mech-deduplication-index')
    ENABLE_ENTITY_LOOKUP = os.environ.get('ENABLE_ENTITY_LOOKUP', 'true').lower() == 'true'
    
    # Enhanced RRF Boost Multipliers (Phase 2B)
    ENTITY_BOOST = float(os.environ.get('ENTITY_BOOST', 3.0))  # Chunks mentioning query entities
    RELATIONSHIP_BOOST = float(os.environ.get('RELATIONSHIP_BOOST', 1.8))  # Related entity chunks
    TYPE_BOOST = float(os.environ.get('TYPE_BOOST', 2.0))  # Matching content type
    RECENCY_BOOST = float(os.environ.get('RECENCY_BOOST', 1.3))  # Recently updated docs
    PROPOSITION_BOOST = float(os.environ.get('PROPOSITION_BOOST', 1.5))  # Proposition chunks
    CODE_LINE_BOOST = float(os.environ.get('CODE_LINE_BOOST', 1.4))  # Full-line code chunks
    
    # Code Generation Pipeline (Phase 2C)
    ENABLE_CODE_GENERATION = os.environ.get('ENABLE_CODE_GENERATION', 'true').lower() == 'true'
    CODE_GEN_MODEL = (
        os.environ.get('CODE_GEN_INFERENCE_PROFILE_ARN')
        or os.environ.get('CODE_GEN_MODEL', 'anthropic.claude-sonnet-4-20250514-v1:0')
    )
    CODE_GEN_MAX_TOKENS = int(os.environ.get('CODE_GEN_MAX_TOKENS', 4000))
    CODE_GEN_TEMPERATURE = float(os.environ.get('CODE_GEN_TEMPERATURE', 0.1))
    
    # Impact Analysis (Phase 3)
    ENABLE_IMPACT_ANALYSIS = os.environ.get('ENABLE_IMPACT_ANALYSIS', 'true').lower() == 'true'
    IMPACT_MAX_HOPS = int(os.environ.get('IMPACT_MAX_HOPS', 3))  # Max relationship traversal depth
    IMPACT_MAX_RESULTS = int(os.environ.get('IMPACT_MAX_RESULTS', 50))  # Max affected entities
    
    @classmethod
    def validate(cls) -> Tuple[bool, str]:
        """Validate critical configuration."""
        if not cls.OPENSEARCH_HOST:
            return False, "OPENSEARCH_HOST not configured"
        if cls.SEMANTIC_WEIGHT + cls.KEYWORD_WEIGHT != 1.0:
            logger.warning(f"Search weights don't sum to 1.0, normalizing...")
        return True, "OK"


# ============================================================================
# CLOUDWATCH METRICS (Phase 2 Enhancement)
# ============================================================================

_cloudwatch_client = None
_dlq_client = None


# ============================================================================
# MECH CONTEXT (Persistent Project Memory - Embedded)
# ============================================================================

# Keep this concise. It is injected into the SYSTEM prompt as background context
MECH_CONTEXT_EMBEDDED = """\
MECH Avatar is a Retrieval-Augmented Generation (RAG) assistant for the MECH mainframe documentation and code corpus.

Runtime flow:
- Ingestion: S3 upload -> Lambda 01-05 pipeline -> OpenSearch index
- Query: API Gateway (REST/WebSocket) -> Lambda 08/09 -> Lambda 06 -> OpenSearch + Bedrock -> response with sources

Corpus types:
- Functional specifications/requirements (business rules)
- Technical overviews/designs (architecture/integration)
- User guides/runbooks (operations)
- Data dictionary/tables/field definitions
- Mainframe source artifacts (COBOL/JCL/ASM/copybooks)

Grounding expectations:
- Prefer citing the exact specification section when asked "what is the rule?"
- Prefer citing code chunks when asked "where is it implemented?"
- When both exist, answer with business rule + code location.

Naming/entity patterns (common examples):
- Procedures/programs: uppercase identifiers like PROC_XYZ, MYTP03
- Tables: MY*, BDD*
- Segments/structures: SHF01, SHF03

Chunk semantics:
- `text` is embedding/search text (may include contextual prefix)
- `text_display` is UI/citation text (clean human-readable)
- Proposition chunks may be resolved to their parent `text_display` for citations
"""


def build_mech_context_system_addition() -> str:
    """Build a system-prompt addition containing embedded project memory."""
    if not Config.ENABLE_MECH_CONTEXT:
        return ''

    ctx = (MECH_CONTEXT_EMBEDDED or '').strip()
    if not ctx:
        return ''

    if Config.MECH_CONTEXT_MAX_CHARS and len(ctx) > Config.MECH_CONTEXT_MAX_CHARS:
        ctx = ctx[:Config.MECH_CONTEXT_MAX_CHARS].rstrip() + "\n\n[...truncated...]"

    return (
        "\n\n"
        "PROJECT CONTEXT (persistent memory):\n"
        "- Background project knowledge for MECH Avatar.\n"
        "- DO NOT cite this section as a source document.\n"
        "- Use retrieved SOURCES for citations/quotes.\n\n"
        f"{ctx}\n"
    )


def get_cloudwatch_client():
    """Get or create CloudWatch client."""
    global _cloudwatch_client
    if _cloudwatch_client is None:
        _cloudwatch_client = boto3.client('cloudwatch', region_name=Config.AWS_REGION)
    return _cloudwatch_client


def get_dlq_client():
    """Get or create SQS client for DLQ dispatch."""
    global _dlq_client
    if _dlq_client is None:
        _dlq_client = boto3.client('sqs', region_name=Config.AWS_REGION)
    return _dlq_client


def send_to_dlq(payload: Dict[str, Any], error: Exception, error_type: str) -> None:
    """Send failed events to DLQ for offline inspection."""
    if not Config.ENABLE_DLQ or not Config.DLQ_URL:
        return
    try:
        message = {
            'error_type': error_type,
            'error_message': str(error),
            'payload': payload,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        get_dlq_client().send_message(
            QueueUrl=Config.DLQ_URL,
            MessageBody=json.dumps(message, default=str)
        )
    except Exception as dlq_error:
        logger.warning(f"DLQ send failed: {dlq_error}")


class QueryEvent(BaseModel):
    model_config = ConfigDict(extra='allow')
    query: str
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    top_k: Optional[int] = None
    filters: Optional[Dict[str, Any]] = Field(default_factory=dict)
    options: Optional[Dict[str, Any]] = Field(default_factory=dict)
    system_prompt: Optional[str] = None
    persona_config: Optional[Dict[str, Any]] = Field(default_factory=dict)


def emit_metric(metric_name: str, value: float, unit: str = 'Count',
                dimensions: Dict[str, str] = None) -> None:
    """
    Emit CloudWatch custom metric for observability.

    Args:
        metric_name: Name of the metric (e.g., 'QueryLatency', 'CacheHitRate')
        value: Metric value
        unit: CloudWatch unit (Count, Milliseconds, Percent, etc.)
        dimensions: Optional dimension key-value pairs
    """
    if not Config.ENABLE_CLOUDWATCH_METRICS:
        return

    try:
        metric_data = {
            'MetricName': metric_name,
            'Value': value,
            'Unit': unit,
            'Timestamp': datetime.now(timezone.utc)
        }

        if dimensions:
            metric_data['Dimensions'] = [
                {'Name': k, 'Value': str(v)} for k, v in dimensions.items()
            ]

        get_cloudwatch_client().put_metric_data(
            Namespace='MECH-Avatar/QueryProcessor',
            MetricData=[metric_data]
        )

    except Exception as e:
        # Non-blocking - log but don't fail
        log_event('metric_error', f"Failed to emit metric {metric_name}: {e}")


def emit_metrics_batch(metrics: List[Dict[str, Any]]) -> None:
    """Emit multiple metrics in a single batch call."""
    if not Config.ENABLE_CLOUDWATCH_METRICS or not metrics:
        return

    try:
        metric_data = []
        for m in metrics[:20]:  # CloudWatch limit is 20 per call
            data = {
                'MetricName': m['name'],
                'Value': m['value'],
                'Unit': m.get('unit', 'Count'),
                'Timestamp': datetime.now(timezone.utc)
            }
            if m.get('dimensions'):
                data['Dimensions'] = [
                    {'Name': k, 'Value': str(v)} for k, v in m['dimensions'].items()
                ]
            metric_data.append(data)

        get_cloudwatch_client().put_metric_data(
            Namespace='MECH-Avatar/QueryProcessor',
            MetricData=metric_data
        )

    except Exception as e:
        log_event('metric_batch_error', f"Failed to emit metrics batch: {e}")


# ============================================================================
# CIRCUIT BREAKER (Phase 2 Enhancement)
# ============================================================================

class CircuitBreaker:
    """
    Circuit breaker pattern for external service calls.

    Prevents cascade failures by temporarily stopping calls to failing services.
    States: CLOSED (normal), OPEN (failing), HALF_OPEN (testing recovery)
    """

    def __init__(self, name: str, threshold: int = None, recovery_seconds: int = None):
        self.name = name
        self.threshold = threshold or Config.CIRCUIT_BREAKER_THRESHOLD
        self.recovery_seconds = recovery_seconds or Config.CIRCUIT_BREAKER_RECOVERY_SECONDS
        self.failures = 0
        self.last_failure_time: Optional[float] = None
        self.state = 'CLOSED'
        self._lock = threading.Lock()

    def can_execute(self) -> bool:
        """Check if the circuit allows execution."""
        with self._lock:
            if self.state == 'CLOSED':
                return True

            if self.state == 'OPEN':
                # Check if recovery time has passed
                if self.last_failure_time and \
                   (time.time() - self.last_failure_time) > self.recovery_seconds:
                    self.state = 'HALF_OPEN'
                    log_event('circuit_half_open', f"Circuit {self.name} entering half-open state")
                    return True
                return False

            # HALF_OPEN - allow one test request
            return True

    def record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            if self.state == 'HALF_OPEN':
                self.state = 'CLOSED'
                self.failures = 0
                log_event('circuit_closed', f"Circuit {self.name} recovered")
                emit_metric('CircuitBreakerRecovery', 1, dimensions={'circuit': self.name})
            elif self.state == 'CLOSED':
                self.failures = 0

    def record_failure(self) -> None:
        """Record a failed call."""
        with self._lock:
            self.failures += 1
            self.last_failure_time = time.time()

            if self.state == 'HALF_OPEN':
                self.state = 'OPEN'
                log_event('circuit_open', f"Circuit {self.name} reopened after half-open failure")
                emit_metric('CircuitBreakerOpen', 1, dimensions={'circuit': self.name})
            elif self.failures >= self.threshold:
                self.state = 'OPEN'
                log_event('circuit_open', f"Circuit {self.name} opened after {self.failures} failures")
                emit_metric('CircuitBreakerOpen', 1, dimensions={'circuit': self.name})

    def get_state(self) -> Dict[str, Any]:
        """Get current circuit state."""
        with self._lock:
            return {
                'name': self.name,
                'state': self.state,
                'failures': self.failures,
                'last_failure': self.last_failure_time
            }


# Global circuit breakers for external services
_circuit_breakers: Dict[str, CircuitBreaker] = {}


def get_circuit_breaker(service: str) -> CircuitBreaker:
    """Get or create circuit breaker for a service."""
    if service not in _circuit_breakers:
        _circuit_breakers[service] = CircuitBreaker(service)
    return _circuit_breakers[service]


# ============================================================================
# PARALLEL LLM EXECUTION (Phase 2 Enhancement)
# ============================================================================

def execute_parallel_llm_tasks(tasks: Dict[str, callable],
                                timeout: float = None) -> Dict[str, Any]:
    """
    Execute multiple LLM tasks in parallel using ThreadPoolExecutor.

    This significantly reduces latency when multiple independent LLM calls
    are needed (intent classification, query expansion, requirements extraction).

    Args:
        tasks: Dict of {task_name: callable} where callable takes no args
        timeout: Max seconds to wait (default: Config.PARALLEL_TIMEOUT_SECONDS)

    Returns:
        Dict of {task_name: result or None on error}
    """
    if not Config.ENABLE_PARALLEL_LLM:
        # Sequential fallback
        results = {}
        for name, func in tasks.items():
            try:
                results[name] = func()
            except Exception as e:
                log_event('parallel_task_error', f"Task {name} failed: {e}")
                results[name] = None
        return results

    timeout = timeout or Config.PARALLEL_TIMEOUT_SECONDS
    results = {}
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
        futures = {executor.submit(func): name for name, func in tasks.items()}

        for future in futures:
            name = futures[future]
            try:
                remaining_timeout = max(0.1, timeout - (time.time() - start_time))
                results[name] = future.result(timeout=remaining_timeout)
            except FuturesTimeoutError:
                log_event('parallel_timeout', f"Task {name} timed out after {timeout}s")
                emit_metric('ParallelTaskTimeout', 1, dimensions={'task': name})
                results[name] = None
            except Exception as e:
                log_event('parallel_error', f"Task {name} failed: {e}")
                emit_metric('ParallelTaskError', 1, dimensions={'task': name})
                results[name] = None

    parallel_time_ms = int((time.time() - start_time) * 1000)
    log_event('parallel_complete',
              f"Executed {len(tasks)} tasks in {parallel_time_ms}ms",
              task_count=len(tasks), duration_ms=parallel_time_ms)
    emit_metric('ParallelExecutionTime', parallel_time_ms, 'Milliseconds')

    return results


# ============================================================================
# EMBEDDING CACHE (Phase 2 Enhancement)
# ============================================================================

# Thread-safe embedding cache using LRU
_embedding_cache: Dict[str, List[float]] = {}
_cache_lock = threading.Lock()
_cache_hits = 0
_cache_misses = 0


def _compute_query_hash(query: str) -> str:
    """Compute stable hash for query caching."""
    # Normalize query for better cache hits
    normalized = query.lower().strip()
    return hashlib.md5(normalized.encode('utf-8')).hexdigest()


def get_cached_embedding(query: str) -> Optional[List[float]]:
    """
    Get embedding from cache if available.

    Returns:
        Cached embedding or None if not found
    """
    global _cache_hits, _cache_misses

    if not Config.ENABLE_EMBEDDING_CACHE:
        return None

    query_hash = _compute_query_hash(query)

    with _cache_lock:
        if query_hash in _embedding_cache:
            _cache_hits += 1
            log_event('cache_hit', f"Embedding cache hit", query_hash=query_hash[:8])
            emit_metric('EmbeddingCacheHit', 1)
            return _embedding_cache[query_hash]

        _cache_misses += 1
        emit_metric('EmbeddingCacheMiss', 1)
        return None


def store_cached_embedding(query: str, embedding: List[float]) -> None:
    """
    Store embedding in cache.

    Uses LRU-style eviction when cache is full.
    """
    if not Config.ENABLE_EMBEDDING_CACHE:
        return

    query_hash = _compute_query_hash(query)

    with _cache_lock:
        # Simple LRU: remove oldest entry if cache is full
        if len(_embedding_cache) >= Config.EMBEDDING_CACHE_SIZE:
            # Remove first (oldest) entry
            oldest_key = next(iter(_embedding_cache))
            del _embedding_cache[oldest_key]
            log_event('cache_eviction', f"Evicted oldest embedding from cache")

        _embedding_cache[query_hash] = embedding


def get_cache_stats() -> Dict[str, Any]:
    """Get cache statistics for monitoring."""
    with _cache_lock:
        total = _cache_hits + _cache_misses
        hit_rate = (_cache_hits / total * 100) if total > 0 else 0
        return {
            'hits': _cache_hits,
            'misses': _cache_misses,
            'hit_rate_pct': round(hit_rate, 2),
            'size': len(_embedding_cache),
            'max_size': Config.EMBEDDING_CACHE_SIZE
        }


# ============================================================================
# QUERY RESULT CACHING (Phase 3 Advanced RAG)
# ============================================================================

# Lazy-loaded DynamoDB resource for query cache
_query_cache_dynamodb = None


def _get_query_cache_table():
    """Get DynamoDB table for query caching."""
    global _query_cache_dynamodb
    if _query_cache_dynamodb is None:
        _query_cache_dynamodb = boto3.resource(
            'dynamodb', 
            region_name=Config.AWS_REGION
        ).Table(Config.QUERY_CACHE_TABLE)
    return _query_cache_dynamodb


def generate_query_cache_key(query: str, filters: Dict[str, Any], top_k: int) -> str:
    """
    Generate deterministic cache key for query.
    
    Args:
        query: Normalized query text
        filters: Search filters
        top_k: Number of results
        
    Returns:
        SHA256 hash of query parameters
    """
    # Normalize query (lowercase, strip, collapse whitespace)
    normalized_query = ' '.join(query.lower().strip().split())
    
    if not isinstance(filters, dict):
        filters = {}

    # Create cache key components
    cache_data = {
        'query': normalized_query,
        'filters': sorted(filters.items()) if filters else [],
        'top_k': top_k
    }
    
    # Generate hash
    cache_string = json.dumps(cache_data, sort_keys=True)
    cache_hash = hashlib.sha256(cache_string.encode()).hexdigest()
    
    return cache_hash


def get_cached_query_response(cache_key: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve cached query response from DynamoDB.
    
    Args:
        cache_key: Cache key hash
        
    Returns:
        Cached response or None if not found/expired
    """
    if not Config.ENABLE_QUERY_CACHE:
        return None
    
    try:
        table = _get_query_cache_table()
        response = table.get_item(Key={'cache_key': cache_key})
        
        if 'Item' not in response:
            log_event('query_cache_miss', f"Cache miss: {cache_key[:16]}...")
            emit_metric('QueryCacheMiss', 1)
            return None
        
        item = response['Item']
        
        # Check if expired (belt-and-suspenders with TTL)
        cached_at = datetime.fromisoformat(item['cached_at'].replace('Z', '+00:00'))
        expiry_time = cached_at + timedelta(hours=Config.CACHE_TTL_HOURS)
        
        if datetime.now(timezone.utc) > expiry_time:
            log_event('query_cache_expired', f"Cache expired: {cache_key[:16]}...")
            return None
        
        age_seconds = (datetime.now(timezone.utc) - cached_at).total_seconds()
        log_event('query_cache_hit', 
                  f"Cache hit: {cache_key[:16]}...",
                  age_seconds=int(age_seconds))
        emit_metric('QueryCacheHit', 1)
        
        # Return cached data
        return {
            'answer': item['answer'],
            'sources': json.loads(item['sources']),
            'source_count': item.get('source_count', 0),
            'intent': item.get('intent'),
            'entities': json.loads(item.get('entities', '{}')),
            'cached': True,
            'cached_at': item['cached_at']
        }
        
    except Exception as e:
        log_event('query_cache_error', f"Cache retrieval error: {e}")
        return None


def cache_query_response(cache_key: str, query: str, response_data: Dict[str, Any],
                        groundedness: float = 0.0) -> None:
    """
    Cache query response in DynamoDB with TTL.
    
    Args:
        cache_key: Cache key hash
        query: Original query
        response_data: Response to cache (answer, sources, etc.)
        groundedness: Groundedness score (only cache if >= threshold)
    """
    if not Config.ENABLE_QUERY_CACHE:
        return
    
    # Only cache high-confidence responses
    if groundedness < Config.CACHE_MIN_CONFIDENCE:
        log_event('query_cache_skip', 
                  f"Skipping cache: low confidence ({groundedness:.2f})")
        return
    
    try:
        table = _get_query_cache_table()
        
        # Calculate TTL (Unix timestamp)
        expires_at = int((datetime.now(timezone.utc) + 
                         timedelta(hours=Config.CACHE_TTL_HOURS)).timestamp())
        
        item = {
            'cache_key': cache_key,
            'query': query[:500],  # Truncate for space
            'answer': response_data.get('answer', ''),
            'sources': json.dumps(response_data.get('sources', [])),
            'source_count': response_data.get('source_count', 0),
            'intent': response_data.get('intent'),
            'entities': json.dumps(response_data.get('entities', {})),
            'cached_at': datetime.now(timezone.utc).isoformat(),
            'expires_at': expires_at,  # DynamoDB TTL field
        }
        
        table.put_item(Item=item)
        
        log_event('query_cached', 
                  f"Cached response: {cache_key[:16]}...",
                  ttl_hours=Config.CACHE_TTL_HOURS)
        emit_metric('QueryCacheWrite', 1)
        
    except Exception as e:
        log_event('query_cache_write_error', f"Cache write error: {e}")


# ============================================================================
# INTENT TAXONOMY
# ============================================================================

class QueryIntent(Enum):
    """Mainframe documentation intent taxonomy."""
    DEFINITION = "definition"
    PROCEDURE = "procedure"
    TROUBLESHOOTING = "troubleshooting"
    COMPARISON = "comparison"
    CODE_EXPLANATION = "code_explanation"
    BEST_PRACTICE = "best_practice"
    CONFIGURATION = "configuration"
    SECURITY = "security"
    OPEN_ENDED = "open_ended"


# ============================================================================
# RETRIEVAL MODE (Phase 4: Hybrid Retrieval Router)
# ============================================================================

class RetrievalMode(Enum):
    """Explicit retrieval routing mode for hybrid search."""
    CODE = "code"         # Code-only retrieval (COBOL, JCL, copybooks)
    DOC = "doc"           # Documentation-only retrieval
    MIXED = "mixed"       # Explicit mixed retrieval (equal weight)
    AUTO = "auto"         # Auto-detect based on query/intent


def detect_retrieval_mode(query: str, intent: QueryIntent) -> Tuple[RetrievalMode, Dict[str, Any]]:
    """
    Phase 4: Detect retrieval mode and return routing hints.
    
    Routing logic:
    1. CODE mode: Query explicitly asks for code, implementation, source
    2. DOC mode: Query asks for documentation, guide, overview, definition
    3. MIXED mode: Query naturally combines both (how does X work, show me code AND docs)
    4. AUTO mode: Fallback, let intent guide content_type_boost
    
    Args:
        query: User query text
        intent: Classified query intent
    
    Returns:
        Tuple of (RetrievalMode, routing_hints dict with filters/boosts)
    """
    q_lower = query.lower()
    
    # Keywords indicating CODE mode
    code_keywords = {
        'source code', 'cobol', 'jcl', 'copybook', 'csect', 'proc',
        'implementation', 'show me the code', 'code for', 'lines of code',
        'snippet', 'where is the code', 'procedure division', 'working-storage',
        'linkage section', 'data division'
    }
    
    # Keywords indicating DOC mode
    doc_keywords = {
        'documentation', 'guide', 'manual', 'overview', 'explain', 'definition',
        'what is', 'describe', 'help me understand', 'tutorial',
        'user guide', 'specification', 'design document'
    }
    
    # Keywords indicating MIXED mode (explicit combination)
    mixed_keywords = {
        'code and documentation', 'implementation and docs', 'with examples',
        'show me both', 'code along with', 'explain with code'
    }
    
    # Check for mixed first (most specific)
    if any(kw in q_lower for kw in mixed_keywords):
        return RetrievalMode.MIXED, {
            'chunk_type_filter': None,  # No filter, get both
            'content_type_boost': ['code', 'reference', 'procedure'],
            'retrieval_strategy': 'interleaved'  # Code + Doc interleaved
        }
    
    # Check for CODE mode
    code_score = sum(1 for kw in code_keywords if kw in q_lower)
    if code_score >= 2 or intent == QueryIntent.CODE_EXPLANATION:
        return RetrievalMode.CODE, {
            'chunk_type_filter': ['code', 'copybook'],
            'content_type_boost': ['code', 'reference'],
            'retrieval_strategy': 'code_first'
        }
    
    # Check for DOC mode
    doc_score = sum(1 for kw in doc_keywords if kw in q_lower)
    if doc_score >= 2 or intent in [QueryIntent.DEFINITION, QueryIntent.PROCEDURE]:
        return RetrievalMode.DOC, {
            'chunk_type_filter_exclude': ['code'],  # Exclude code chunks
            'content_type_boost': ['documentation', 'procedure', 'guide'],
            'retrieval_strategy': 'doc_first'
        }
    
    # Default to AUTO
    return RetrievalMode.AUTO, {
        'chunk_type_filter': None,
        'content_type_boost': INTENT_CONFIG.get(intent, {}).get('content_type_boost'),
        'retrieval_strategy': 'auto'
    }


INTENT_CONFIG = {
    QueryIntent.DEFINITION: {
        "top_k": 8,
        "content_type_boost": ["definition", "overview"],
        "response_style": "concise",
        "max_tokens": 500
    },
    QueryIntent.PROCEDURE: {
        "top_k": 15,  # Increased for multi-step procedures
        "content_type_boost": ["procedure", "configuration"],
        "response_style": "step_by_step",
        "max_tokens": 2500
    },
    QueryIntent.TROUBLESHOOTING: {
        "top_k": 18,  # Increased for comprehensive diagnostic info
        "content_type_boost": ["troubleshooting", "error", "procedure"],
        "response_style": "diagnostic",
        "max_tokens": 2500
    },
    QueryIntent.COMPARISON: {
        "top_k": 12,  # Increased for comparing multiple sources
        "content_type_boost": None,
        "response_style": "comparative",
        "max_tokens": 2000
    },
    QueryIntent.CODE_EXPLANATION: {
        "top_k": 8,
        "content_type_boost": ["code", "reference"],
        "response_style": "technical",
        "max_tokens": 1500
    },
    QueryIntent.BEST_PRACTICE: {
        "top_k": 10,
        "content_type_boost": ["best_practice", "procedure"],
        "response_style": "advisory",
        "max_tokens": 1500
    },
    QueryIntent.CONFIGURATION: {
        "top_k": 15,  # Increased for comprehensive config details
        "content_type_boost": ["configuration", "procedure"],
        "response_style": "step_by_step",
        "max_tokens": 2500
    },
    QueryIntent.SECURITY: {
        "top_k": 10,
        "content_type_boost": ["security", "procedure"],
        "response_style": "security_focused",
        "max_tokens": 1500
    },
    QueryIntent.OPEN_ENDED: {
        "top_k": Config.TOP_K,
        "content_type_boost": None,
        "response_style": "comprehensive",
        "max_tokens": Config.MAX_TOKENS
    }
}


# ============================================================================
# MAINFRAME TERMINOLOGY KNOWLEDGE BASE
# ============================================================================

MAINFRAME_SYNONYMS = {
    # Security Systems
    "tss": ["top secret security", "ca-tss", "ca top secret", "topsecret"],
    "racf": ["resource access control facility", "ibm racf"],
    "acf2": ["ca-acf2", "access control facility"],
    
    # Transaction Processing
    "cics": ["customer information control system", "ibm cics"],
    "ims": ["information management system", "ibm ims", "input message segment"],
    "mq": ["websphere mq", "ibm mq", "message queue"],
    
    # Job Control
    "jcl": ["job control language"],
    "jes": ["job entry subsystem", "jes2", "jes3"],
    "sdsf": ["system display and search facility"],
    
    # Errors & Diagnostics
    "abend": ["abnormal end", "abnormal termination", "system completion code"],
    "soc7": ["s0c7", "data exception", "decimal divide exception"],
    "soc4": ["s0c4", "protection exception", "addressing exception"],
    "rc": ["return code", "retcode"],
    
    # Storage
    "vsam": ["virtual storage access method"],
    "db2": ["ibm db2", "database 2"],
    "ims db": ["ims database"],
    
    # BMO Specific - General
    "mech": ["mainframe enterprise core hub", "bmo mainframe"],
    "shf": ["system history file", "standard history file"],
    "cdic": ["canada deposit insurance corporation"],
    "cads": ["customer account data system"],
    "sage": ["savings account general environment"],
    
    # BMO MECH Files - Customer & Account
    "cif": ["customer information file"],
    "caf$": ["customer account file"],
    "fca$": ["foreign currency account file"],
    "dda": ["demand deposit accounting file"],
    "sdf": ["special deposit file"],
    
    # BMO MECH Files - Transaction & Journal
    "txf$": ["transaction file"],
    "ejf$": ["electronic journal file"],
    "jrnl": ["journal file"],
    "rjf": ["reconstructed journal file"],
    
    # BMO MECH Files - Balancing & Consolidation
    "cbof": ["consolidated balances online file"],
    "rcbof": ["reformatted consolidated balances online file"],
    "cdtf": ["consolidated balances transaction file"],
    "mbuf": ["merge balance update file"],
    
    # BMO MECH Files - Cheque Processing
    "chif": ["cardholder information file"],
    "cicf": ["cheque input control file"],
    "bcfaf": ["bulk cheque filing advertising master file"],
    "bcfauf": ["bulk cheque filing advertising update file"],
    
    # BMO MECH Files - Back Items
    "bimf": ["back item master file"],
    "bdif": ["back item daily file"],
    "biuf": ["back items update file"],
    
    # BMO MECH Files - Control & Cycle
    "rcf": ["run control file"],
    "ccf": ["critical cycle file", "critical merge cycle file"],
    "mcf": ["merge cycle file"],
    "scf": ["shf control file"],
    "cscf": ["common system cycle file"],
    "ascf": ["archived shf sort control file"],
    "iscf": ["ifs sort cycle file"],
    
    # BMO MECH Files - Trigger Files
    "trig": ["trigger file"],
    "arpstf": ["arp statement trigger file"],
    "dstf": ["daily statement trigger file"],
    "csaa": ["transmitted trigger file"],
    
    # BMO MECH Files - Reports & Output
    "rif": ["report information file"],
    "crif": ["critical report information file"],
    "crf": ["customer report file"],
    "crfu": ["customer report file update file"],
    "sof": ["statement output file"],
    "rof": ["report output file"],
    
    # BMO MECH Files - Store & Forward
    "sf1$": ["store and forward file mech east"],
    "sf2$": ["store and forward file mech west"],
    "sf6$": ["store and forward file sage"],
    
    # BMO MECH Files - Terminal & Branch
    "tcf$": ["terminal control file"],
    "tif$": ["terminal information file"],
    "brn$": ["branch file"],
    
    # BMO MECH Files - Inflight & Response
    "iff$": ["inflight file"],
    "irf$": ["inflight response file"],
    
    # BMO MECH Files - Miscellaneous
    "csf": ["common system file"],
    "sif": ["sundry information file"],
    "sssf": ["sub-system support file"],
    "guf$": ["general ledger unpostable verification file"],
    "dpf$": ["depository file"],
    "ccf$": ["corporate concentrator file"],
    "edf$": ["transfer rollover file"],
    "edov": ["transfer rollover overflow file"],
    "dbrf": ["data base recovery file"],
    "spn": ["standard pre-nati file"],
    "ttsf": ["teller training segment file"],
}

MAINFRAME_ACRONYMS = {
    # Standard Mainframe Systems
    "TSS", "RACF", "ACF2", "CICS", "IMS", "JCL", "JES", "SDSF",
    "VSAM", "DB2", "COBOL", "PL/I", "REXX", "ISPF", "OMEGAMON",
    "SMP/E", "DFSORT", "ICETOOL", "IDCAMS", "IPCS", "SYSVIEW",
    
    # BMO-Specific Files & Systems
    "MECH", "SHF", "CDIC", "CADS", "SAGE", "DDA",
    
    # MECH Files (alphabetical)
    "ARPSTF", "ASCF", "BCFAF", "BCFAUF", "BDIF", "BIMF", "BIUF",
    "BRN$", "CAF$", "CBOF", "CCF", "CCF$", "CDTF", "CHIF", "CICF",
    "CIF", "CRF", "CRFU", "CRIF", "CSAA", "CSCF", "CSF",
    "DBRF", "DPF$", "DSTF", "EDF$", "EDOV", "EJF$",
    "FCA$", "GUF$", "IFF$", "IRF$", "ISCF", "ISIF",
    "JRNL", "MBUF", "MCF", "MIF",
    "RCBOF", "RCF", "RIF", "RJF", "ROF",
    "SCF", "SDF", "SF1$", "SF2$", "SF6$", "SIF", "SOF", "SPN", "SSSF",
    "TCF$", "TIF$", "TRIG", "TTSF", "TXF$"
}


# ============================================================================
# MAINFRAME ENTITY EXTRACTION (Phase 3 Advanced RAG)
# ============================================================================

class EntityType(Enum):
    """Types of mainframe entities that can be extracted from queries."""
    TSS_ID = "tss_id"                    # Top Secret Security User ID
    TABLE_NAME = "table_name"            # Database table (MY*, BDD*, etc.)
    PROGRAM_NAME = "program_name"        # COBOL/Assembler program
    JCL_PROCEDURE = "jcl_procedure"      # JCL proc (@1*, @2*, etc.)
    CICS_TRANSACTION = "cics_transaction" # CICS transaction code
    ACID = "acid"                        # ACID (Application Customer ID)
    ERROR_CODE = "error_code"            # System error code (S0C7, etc.)
    HEX_VALUE = "hex_value"              # Hexadecimal value
    CHECKPOINT = "checkpoint"            # Checkpoint file (GCHK*)
    SHF_SEGMENT = "shf_segment"          # SHF segment (SHF03, SHF04, etc.)
    VSAM_DATASET = "vsam_dataset"        # VSAM dataset
    JOB_NAME = "job_name"                # Batch job name


@dataclass
class ExtractedEntity:
    """Represents an extracted mainframe entity."""
    text: str                            # Original text
    entity_type: EntityType              # Type of entity
    confidence: float                    # Extraction confidence (0.0-1.0)
    normalized: str = None               # Normalized form


# Entity extraction patterns (regex-based) with confidence scores
ENTITY_PATTERNS = {
    EntityType.TSS_ID: [
        (r'\b[A-Z]{3}\d{4}\b', 0.9),                    # ABC1234
        (r'\b[A-Z]{2}\d{5}\b', 0.8),                    # AB12345
        (r'\buser\s+id[:\s]+([A-Z0-9]{5,8})\b', 0.95),  # user id: ABC1234
    ],
    EntityType.TABLE_NAME: [
        (r'\bMY[A-Z]{2,6}\d?\b', 0.95),                 # MYABC, MYABCD1
        (r'\bBDD[A-Z0-9]{2,6}\b', 0.95),                # BDDABC, BDD123
        (r'\bDABI[A-Z]{2,6}\b', 0.9),                   # DABIABC
        (r'\bFSFL[A-Z]{2,6}\b', 0.9),                   # FSFLABC
        (r'\bOPTR[A-Z]{2,6}\b', 0.9),                   # OPTRABC
        (r'\bDDA[A-Z]{2,6}\b', 0.85),                   # DDAABC
    ],
    EntityType.PROGRAM_NAME: [
        (r'\bM[A-Z]{4,7}\b', 0.85),                     # MABC, MABCDEF
        (r'\bP[A-Z]{4,7}\b', 0.8),                      # PABC, PABCDEF
        (r'\b[A-Z]{3}\d{3}[A-Z]?\b', 0.7),              # ABC123, ABC123D
    ],
    EntityType.JCL_PROCEDURE: [
        (r'@[123][A-Z]+\b', 0.95),                      # @1ABC, @2DEF
        (r'\bPROC\s+([A-Z0-9]+)\b', 0.9),               # PROC ABC
    ],
    EntityType.CICS_TRANSACTION: [
        (r'\b[A-Z]{4}\s+transaction\b', 0.9),           # ABCD transaction
        (r'\btranid[:\s]+([A-Z]{4})\b', 0.95),          # tranid: ABCD
    ],
    EntityType.ACID: [
        (r'\bACID[:\s]+(\d{4,6})\b', 0.95),             # ACID: 12345
        (r'\bacid\s+(\d{4,6})\b', 0.9),                 # acid 12345
    ],
    EntityType.ERROR_CODE: [
        (r'\bS0[CD][0-9A-F]\b', 0.95),                  # S0C7, S0D1
        (r'\bU\d{4}\b', 0.9),                           # U0001
        (r'\bIEC\d{3}[A-Z]\b', 0.9),                    # IEC141I
        (r'\bTSS\d{4}[A-Z]?\b', 0.95),                  # TSS0401
    ],
    EntityType.HEX_VALUE: [
        (r"X'[0-9A-Fa-f]+'", 0.95),                     # X'C1C2'
        (r"0x[0-9A-Fa-f]+", 0.9),                       # 0xC1C2
    ],
    EntityType.CHECKPOINT: [
        (r'\bGCHK\d{1,3}\b', 0.95),                     # GCHK1, GCHK123
    ],
    EntityType.SHF_SEGMENT: [
        (r'\bSHF[-_]?0[1349]\b', 0.95),                 # SHF03, SHF-04, SHF_14
    ],
    EntityType.VSAM_DATASET: [
        (r'\b[A-Z]+\.VSAM\.[A-Z0-9.]+\b', 0.9),         # PROD.VSAM.KSDS
    ],
    EntityType.JOB_NAME: [
        (r'\bTLG\d{3}\b', 0.9),                         # TLG123
        (r'\b[A-Z]{3}\d{4}[A-Z]?\b', 0.7),              # ABC1234J
    ],
}


def extract_mainframe_entities(query: str) -> List[ExtractedEntity]:
    """
    Extract mainframe-specific entities from query text.
    
    Args:
        query: User query text
        
    Returns:
        List of extracted entities with confidence scores
    """
    if not Config.ENABLE_ENTITY_EXTRACTION:
        return []
    
    entities = []
    query_upper = query.upper()
    
    for entity_type, patterns in ENTITY_PATTERNS.items():
        for pattern, confidence in patterns:
            matches = re.finditer(pattern, query_upper, re.IGNORECASE)
            for match in matches:
                entity_text = match.group(0)
                normalized = entity_text.strip().upper()
                
                entities.append(ExtractedEntity(
                    text=entity_text,
                    entity_type=entity_type,
                    confidence=confidence,
                    normalized=normalized
                ))
    
    # Deduplicate by normalized form and type
    seen = set()
    unique_entities = []
    for entity in entities:
        key = (entity.normalized, entity.entity_type)
        if key not in seen:
            seen.add(key)
            unique_entities.append(entity)
    
    # Sort by confidence
    unique_entities.sort(key=lambda e: e.confidence, reverse=True)
    
    if unique_entities:
        log_event('entities_extracted', 
                  f"Found {len(unique_entities)} entities",
                  entities=[e.text for e in unique_entities[:5]])
    
    return unique_entities


def format_entities_for_display(entities: List[ExtractedEntity]) -> Dict[str, List[str]]:
    """
    Format entities for API response.
    
    Returns:
        {
            'tss_ids': ['ABC1234'],
            'table_names': ['MYABC', 'BDDXYZ'],
            'error_codes': ['S0C7'],
            ...
        }
    """
    formatted = {}
    
    for entity in entities:
        type_key = entity.entity_type.value + 's'  # Pluralize
        if type_key not in formatted:
            formatted[type_key] = []
        formatted[type_key].append(entity.normalized)
    
    return formatted


def entity_aware_rerank(query: str, chunks: List[Dict[str, Any]], 
                       entities: List[ExtractedEntity],
                       top_k: int = 12) -> List[Dict[str, Any]]:
    """
    Rerank chunks based on presence of extracted entities.
    
    Chunks containing query entities are boosted, improving precision for
    entity-centric queries (e.g., "What tables does MYABC use?").
    
    Args:
        query: Original query
        chunks: Search results to rerank
        entities: Extracted entities from query
        top_k: Number of results to return
        
    Returns:
        Reranked chunks with boosted scores
    """
    if not entities or not Config.ENABLE_ENTITY_RERANKING:
        return chunks[:top_k]
    
    # Create entity lookup
    entity_texts = {e.normalized for e in entities}
    
    reranked_chunks = []
    boosted_count = 0
    
    for chunk in chunks:
        chunk_text = chunk.get('text', '').upper()
        score = chunk.get('score', 0.5)
        
        # Count entity matches in chunk
        entities_found = sum(1 for entity in entity_texts if entity in chunk_text)
        
        # Apply boost
        if entities_found > 0:
            boost_factor = 1 + (entities_found * (Config.ENTITY_BOOST_WEIGHT - 1))
            boosted_score = min(score * boost_factor, 1.0)  # Cap at 1.0
            
            chunk['score'] = boosted_score
            chunk['entity_boost'] = round(boost_factor, 2)
            chunk['entities_matched'] = entities_found
            boosted_count += 1
        
        reranked_chunks.append(chunk)
    
    # Sort by boosted scores
    reranked_chunks.sort(key=lambda c: c.get('score', 0), reverse=True)
    
    if boosted_count > 0:
        log_event('entity_reranking', 
                  f"Boosted {boosted_count} chunks with entity matches",
                  entity_count=len(entities), chunks_boosted=boosted_count)
    
    return reranked_chunks[:top_k]


# ============================================================================
# MECH QUERY UNDERSTANDING (Layer 5 Integration)
# ============================================================================

class MechQueryDomain(Enum):
    """MECH-specific query domains for routing."""
    SHF_PROCESSING = "shf_processing"
    TXNLOG_OPERATIONS = "txnlog_operations"
    RL03_XML = "rl03_xml"
    BACK_ITEM_MAPPING = "back_item_mapping"
    STATEMENT_TESTING = "statement_testing"
    XXA_TRANSACTIONS = "xxa_transactions"
    GENERAL = "general"


# Domain detection patterns - generic patterns for query routing
MECH_DOMAIN_PATTERNS = {
    MechQueryDomain.SHF_PROCESSING: [
        r'\bSHF[-_]?\d{2}\b',                       # SHF segments (SHF03, SHF04, etc.)
        r'\bMSHFP[A-Z]\b',                          # SHF processor programs
        r'\b(?:segment|processor|formatted?)\b.*\bSHF\b',
    ],
    MechQueryDomain.TXNLOG_OPERATIONS: [
        r'\bTXNLOG|MECH_TXNLOG\b',                  # Transaction log
        r'\bpartition\s+rotat',                     # Partition rotation
        r'\bTLG\d{3}\b',                            # Job names TLGnnn
        r'\bGMT[_\s]?(?:time|ts|timestamp)\b',
    ],
    MechQueryDomain.RL03_XML: [
        r'\bRL[-\s]?03\b',                          # RL03 reporting
        r'\bRevenue\s+Qu[eéè]bec\b',               # Quebec revenue
        r'\bXML\s+(?:tag|header|schema)\b',
        r'\btransmission|feedback|certification\b',
    ],
    MechQueryDomain.BACK_ITEM_MAPPING: [
        r'\bDABI[A-Z]+\b',                          # DABI fields (generic pattern)
        r'\bOPTR(?:TYP)?\b',                        # OPTR fields
        r'\bBDDOP[A-Z0-9]+\b',                      # BDD mapping fields  
        r'\bback\s+item|optional\s+(?:item|segment)\b',
    ],
    MechQueryDomain.STATEMENT_TESTING: [
        r'\bFSF[A-Z]+\b',                           # FSF fields (generic)
        r'\bSTMTTST\b',
        r'\b@[123][A-Z]+\b',                        # JCL procedures @1*, @2*, @3*
        r'\bGCHK\d*\b',                             # Checkpoint files GCHK*
        r'\bstatement\s+(?:test|language|message)\b',
    ],
    MechQueryDomain.XXA_TRANSACTIONS: [
        r'\bXXA\b',                                  # XXA transactions
        r'\bprogram\s+call\s+chain\b',
        r'\bSTDPC\s+TYPE=',                          # STDPC macro
    ],
}


def detect_mech_domain(query: str) -> Tuple[MechQueryDomain, Dict]:
    """
    Detect MECH domain and generate appropriate filters.
    
    Uses pattern matching on query text to route to appropriate domain.
    """
    scores = {}
    
    for domain, patterns in MECH_DOMAIN_PATTERNS.items():
        score = 0
        for pattern in patterns:
            if re.search(pattern, query, re.IGNORECASE):
                score += 1
        scores[domain] = score
    
    best_domain = max(scores, key=scores.get) if max(scores.values()) > 0 else MechQueryDomain.GENERAL
    
    # Generate filters based on explicit segment mentions in query
    filters = {}
    query_upper = query.upper()
    
    # SHF segment detection - only if explicitly mentioned in query
    if best_domain == MechQueryDomain.SHF_PROCESSING or 'SHF' in query_upper:
        # Detect specific SHF segment from query text
        shf_match = re.search(r'\bSHF[-_]?(\d{2})\b', query_upper)
        if shf_match:
            segment = f"SHF{shf_match.group(1)}"
            filters['shf_segment'] = segment
            # Add negative filter for other common segments to avoid cross-contamination  
            other_segments = ['SHF03', 'SHF04', 'SHF14']
            for other in other_segments:
                if other != segment:
                    filters['shf_segment_ne'] = other
                    break
    
    # TXNLOG filter
    if best_domain == MechQueryDomain.TXNLOG_OPERATIONS:
        filters['contains_txnlog'] = True
    
    # XXA filter
    if best_domain == MechQueryDomain.XXA_TRANSACTIONS:
        filters['contains_xxa'] = True
    
    return best_domain, filters


def extract_required_literals(query: str) -> List[str]:
    """Extract literals from query that should appear in answer."""
    patterns = [
        r'\bMY[A-Z]{2,6}\d?\b',      # Table names
        r'\bBDD[A-Z0-9]+\b',         # BDD mappings
        r'\bDABI[A-Z]+\b',           # DABI fields
        r'\bFSFL[A-Z]+\b',           # FSF fields
        r'\bSHF0[134]\b',            # SHF segments
        r'\bOPTR[A-Z]+\b',           # OPTR fields
        r"X'[0-9A-Fa-f]+'",          # Hex values
        r'\bM[A-Z]{4,7}\b',          # Program names
        r'\bTLG\d{3}\b',             # Job names
        r'\bGCHK\d?\b',              # Checkpoint files
        r'@[123][A-Z]+',             # JCL procedures
    ]
    
    literals = []
    for pattern in patterns:
        matches = re.findall(pattern, query)
        literals.extend(matches)
    
    return list(set(literals))


# ============================================================================
# GROUNDING VERIFICATION (Layer 6 Integration)
# ============================================================================

class VerificationStatus(Enum):
    """Status of answer verification."""
    FULLY_GROUNDED = "fully_grounded"
    PARTIALLY_GROUNDED = "partially_grounded"
    WEAKLY_GROUNDED = "weakly_grounded"
    UNGROUNDED = "ungrounded"


def verify_answer_grounding(answer: str, context_chunks: List[Dict], 
                           query: str, required_literals: List[str] = None) -> Dict[str, Any]:
    """
    Verify that answer is grounded in provided context.
    
    Addresses failure mode: "pipeline marked every answer 'success' even when wrong"
    
    Returns:
        {
            'status': VerificationStatus,
            'groundedness': float (0.0-1.0),
            'grounded_sentences': int,
            'total_sentences': int,
            'citations_found': int,
            'literals_found': List[str],
            'literals_missing': List[str],
            'should_add_warning': bool,
            'warning_message': str
        }
    """
    if required_literals is None:
        required_literals = extract_required_literals(query)
    
    # Concatenate context for searching
    context_text = '\n'.join(c.get('text', '') for c in context_chunks)
    
    # Split answer into sentences
    sentences = re.split(r'[.!?]+\s+', answer)
    sentences = [s.strip() for s in sentences if len(s.strip()) > 10]
    
    grounded_count = 0
    
    for sentence in sentences:
        # Extract key terms from sentence (skip stop words)
        words = re.findall(r'\b[A-Za-z0-9_-]{3,}\b', sentence)
        key_terms = [w for w in words if w.lower() not in {
            'the', 'and', 'for', 'are', 'was', 'were', 'has', 'have', 'this', 'that',
            'with', 'from', 'not', 'but', 'can', 'will', 'would', 'should', 'could'
        }]
        
        if not key_terms:
            grounded_count += 1  # No key terms = filler sentence
            continue
        
        # Check how many key terms appear in context
        terms_found = sum(1 for term in key_terms[:10] if term.lower() in context_text.lower())
        if terms_found / len(key_terms[:10]) >= 0.5:
            grounded_count += 1
    
    total_sentences = len(sentences) if sentences else 1
    groundedness = grounded_count / total_sentences
    
    # Check citations
    citations = re.findall(r'\[Source[:\s]*(\d+)\]', answer)
    
    # Check literals
    literals_found = [lit for lit in required_literals if lit in answer]
    literals_missing = [lit for lit in required_literals if lit not in answer and lit in context_text]
    
    # Determine status
    if groundedness >= 0.9:
        status = VerificationStatus.FULLY_GROUNDED
    elif groundedness >= 0.7:
        status = VerificationStatus.PARTIALLY_GROUNDED
    elif groundedness >= 0.5:
        status = VerificationStatus.WEAKLY_GROUNDED
    else:
        status = VerificationStatus.UNGROUNDED
    
    # Entity Registry Validation: Check that technical entities in the answer
    # actually exist in the entity registry (prevents hallucinated identifiers)
    entity_validation_penalty = 0.0
    hallucinated_entities = []
    if Config.ENABLE_ENTITY_LOOKUP:
        # Extract technical identifiers from the answer
        answer_identifiers = re.findall(r'\b[A-Z][A-Z0-9_@#$-]{2,30}\b', answer)
        answer_identifiers = [aid for aid in answer_identifiers if len(aid) >= 4 and aid not in {
            'NOTE', 'WITH', 'FROM', 'INTO', 'THAT', 'THIS', 'WHEN', 'DOES', 'HAVE',
            'WILL', 'EACH', 'ALSO', 'USED', 'USES', 'THEN', 'ELSE', 'TRUE', 'FALSE',
            'NULL', 'NONE', 'COBOL', 'TABLE', 'INDEX', 'ERROR', 'FIELD', 'VALUE'
        }]
        
        if answer_identifiers:
            try:
                entity_context = lookup_entity_context(
                    [f"COBOL:{aid}" for aid in answer_identifiers[:10]]
                )
                known_entities = {e.get('entity_key', '').split(':')[-1] 
                                 for e in entity_context.get('entities', [])}
                
                # Check if answer references identifiers not in registry or context
                for aid in answer_identifiers[:10]:
                    in_registry = aid in known_entities
                    in_context = aid in context_text.upper()
                    if not in_registry and not in_context:
                        hallucinated_entities.append(aid)
                
                if hallucinated_entities:
                    ratio = len(hallucinated_entities) / len(answer_identifiers[:10])
                    entity_validation_penalty = min(ratio * 0.2, 0.15)
                    groundedness = max(0.0, groundedness - entity_validation_penalty)
                    # Re-evaluate status after penalty
                    if groundedness >= 0.9:
                        status = VerificationStatus.FULLY_GROUNDED
                    elif groundedness >= 0.7:
                        status = VerificationStatus.PARTIALLY_GROUNDED
                    elif groundedness >= 0.5:
                        status = VerificationStatus.WEAKLY_GROUNDED
                    else:
                        status = VerificationStatus.UNGROUNDED
            except Exception:
                pass  # Entity lookup failure should not block grounding
    
    # Determine if we should add a warning
    should_add_warning = False
    warning_message = ""
    
    if status == VerificationStatus.UNGROUNDED:
        should_add_warning = True
        warning_message = "Note: This answer may contain information not fully verified against available documentation."
    elif literals_missing and len(literals_missing) >= 2:
        should_add_warning = True
        warning_message = f"Note: Some key terms from the question ({', '.join(literals_missing[:3])}) were found in context but may not be fully addressed."
    
    is_grounded = status in [VerificationStatus.FULLY_GROUNDED, VerificationStatus.PARTIALLY_GROUNDED]

    return {
        'status': status.value,
        'groundedness': round(groundedness, 3),
        'is_grounded': is_grounded,
        'grounded_sentences': grounded_count,
        'total_sentences': total_sentences,
        'citations_found': len(citations),
        'literals_found': literals_found,
        'literals_missing': literals_missing,
        'hallucinated_entities': hallucinated_entities,
        'entity_validation_penalty': round(entity_validation_penalty, 3),
        'should_add_warning': should_add_warning,
        'warning_message': warning_message,
    }


# Faithfulness-enforcing system prompt (Layer 8 Integration)
# Generic for all MECH documentation questions
FAITHFUL_SYSTEM_PROMPT_ADDITION = """

<CRITICAL_FAITHFULNESS_REQUIREMENTS>

<rule id="1" name="COMPLETE_ENUMERATION">
WHEN asked for lists (XML tags, tables, fields, mappings):
- Count ALL items found in context FIRST
- Provide the COMPLETE list, not "such as" or "including" partial lists
- Format: "Found X items in the documentation: [full list]"

GOOD: "Found 5 XML tags: <HEADER>, <BODY>, <FOOTER>, <TIMESTAMP>, <CHECKSUM>"
BAD: "Tags include <HEADER>, <BODY>, and others"
</rule>

<rule id="2" name="SEGMENT_BOUNDARIES">
WHEN question specifies a segment (SHF03, SHF04, Module X):
- ONLY include content FROM that specific segment
- NEVER mix fields/tables from different segments
- When uncertain: cite source document explicitly

GOOD: "[From SHF03 specification] The fields are: FIELD1, FIELD2"
BAD: "The fields are FIELD1 (from SHF03), FIELD5 (from SHF04)..." (mixed sources)
</rule>

<rule id="3" name="MAPPING_COMPLETENESS">
WHEN asked for field mappings or value definitions:
- Provide the COMPLETE mapping table
- Preserve exact formats: hex values (X'01'), special characters
- Show full relationship: field → value → meaning

GOOD: "OPTRTYP values: X'01'=Optional, X'02'=Required, X'03'=Conditional"
BAD: "OPTRTYP can be Optional, Required, or other values"
</rule>

<rule id="4" name="CALL_CHAIN_ACCURACY">
WHEN asked about program/call relationships:
- Trace the ACTUAL flow from documentation
- Show which program calls which (not just names)
- Cite section for each relationship

GOOD: "MAINPGM calls SUBPGM1 (line 150), which calls UTIL01 (per Section 3.2)"
BAD: "Related programs: MAINPGM, SUBPGM1, UTIL01"
</rule>

<rule id="5" name="FALSE_NEGATIVE_PREVENTION">
BEFORE saying "not found":
- Search ALL provided chunks thoroughly
- Check alternate spellings, abbreviations, related terms
- If partial info exists, provide it + identify specific gaps

GOOD: "Found partial info about X: [details]. NOT FOUND: [specific missing item]"
BAD: "Information not found" (without confirming all chunks checked)
</rule>

<rule id="6" name="EXACT_TECHNICAL_IDENTIFIERS">
Technical identifiers MUST be copied EXACTLY:
- Field names, table names, program names: exact spelling
- Hex formats (X'xx'), casing, special characters: preserved
- Never paraphrase or abbreviate technical names
</rule>

<rule id="7" name="MANDATORY_CITATIONS">
Every factual claim REQUIRES a citation:
- Single source: [Source: Document N, filename]
- Combined sources: "Combining Documents X and Y: [synthesis]"
- Unverified claims: Must be flagged as such
</rule>

</CRITICAL_FAITHFULNESS_REQUIREMENTS>

<response_when_not_found>
"Searched all {N} provided documents covering [list main topics]. 
Found: [any partial info]
NOT FOUND: [specific item requested]"
</response_when_not_found>

<absolute_prohibition>
NEVER fabricate identifiers, field names, values, or relationships not explicitly present in the provided context.
</absolute_prohibition>"""


# ============================================================================
# AWS CLIENT MANAGEMENT
# ============================================================================

_clients: Dict[str, Any] = {}
_opensearch_client: Optional[OpenSearch] = None


def get_client(service: str) -> Any:
    """Thread-safe AWS client factory with connection pooling."""
    if service not in _clients:
        _clients[service] = boto3.client(
            service, 
            region_name=Config.AWS_REGION,
            config=boto3.session.Config(
                retries={'max_attempts': Config.MAX_RETRIES, 'mode': 'adaptive'},
                connect_timeout=5,
                read_timeout=30
            )
        )
    return _clients[service]


def get_resource(service: str) -> Any:
    """Get AWS resource for DynamoDB operations."""
    key = f"{service}_resource"
    if key not in _clients:
        _clients[key] = boto3.resource(service, region_name=Config.AWS_REGION)
    return _clients[key]


def get_opensearch_client() -> OpenSearch:
    """
    OpenSearch Serverless client with IAM authentication.
    
    Uses AWS4Auth for AOSS (OpenSearch Serverless) service signing.
    Connection is cached for Lambda container reuse.
    """
    global _opensearch_client
    
    if _opensearch_client is None:
        if not Config.OPENSEARCH_HOST:
            raise ConnectionError("OPENSEARCH_HOST environment variable not configured")
        
        credentials = boto3.Session().get_credentials()
        
        auth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            Config.AWS_REGION,
            'aoss',  # OpenSearch Serverless service identifier
            session_token=credentials.token
        )
        
        host = Config.OPENSEARCH_HOST.replace('https://', '').replace('http://', '').rstrip('/')
        
        _opensearch_client = OpenSearch(
            hosts=[{'host': host, 'port': 443}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=30,
            max_retries=Config.MAX_RETRIES,
            retry_on_timeout=True
        )
        
        log_event('opensearch_init', f"Client initialized for {host}")
    
    return _opensearch_client


# ============================================================================
# CONVERSATIONAL MEMORY LAYER
# ============================================================================

@dataclass
class ConversationTurn:
    """Represents a single conversation turn."""
    query: str
    response: str
    intent: str
    entities: List[str]
    timestamp: str
    document_ids: List[str]


def get_conversation_history(session_id: str, limit: int = 5) -> List[Dict]:
    """
    Retrieve conversation history from DynamoDB for context resolution.
    
    Args:
        session_id: Unique session identifier
        limit: Maximum turns to retrieve (default: 5)
    
    Returns:
        List of recent conversation turns, most recent first
    """
    if not Config.ENABLE_MEMORY or not session_id:
        return []
    
    try:
        table = get_resource('dynamodb').Table(Config.MEMORY_TABLE)
        
        response = table.query(
            KeyConditionExpression=Key('session_id').eq(session_id),
            ScanIndexForward=False,  # Descending (most recent first)
            Limit=limit,
            ProjectionExpression='#q, answer, #ts, intent, entities_discussed, retrieved_documents',
            ExpressionAttributeNames={
                '#q': 'query',
                '#ts': 'timestamp'
            }
        )
        
        history = response.get('Items', [])
        log_event('memory_retrieved', f"Retrieved {len(history)} turns", session_id=session_id)
        return history
        
    except Exception as e:
        log_event('memory_error', f"Failed to retrieve history: {e}", session_id=session_id)
        return []


def resolve_contextual_references(query: str, history: List[Dict]) -> Tuple[str, Optional[str]]:
    """
    Resolve pronouns and contextual references using conversation history.
    
    Handles: "What about that?", "How do I fix it?", "Tell me more", etc.
    
    Returns:
        Tuple of (resolved_query, resolution_explanation or None)
    """
    if not history or not Config.ENABLE_MEMORY:
        return query, None
    
    # Quick heuristic: check for ambiguous references
    ambiguous_patterns = [
        'it', 'that', 'this', 'they', 'them', 'those', 'these',
        'its', 'their', 'the same', 'more about', 'tell me more',
        'what about', 'how about', 'and also', 'related to'
    ]
    
    query_lower = query.lower()
    needs_resolution = any(
        f' {p} ' in f' {query_lower} ' or
        query_lower.startswith(f'{p} ') or
        query_lower.endswith(f' {p}')
        for p in ambiguous_patterns
    )
    
    if not needs_resolution:
        return query, None
    
    try:
        bedrock = get_client('bedrock-runtime')
        
        # Build compact history context
        history_context = "\n".join([
            f"Q: {h.get('query', '')}\nA: {h.get('answer', '')[:300]}..."
            for h in history[:3]
        ])
        
        prompt = f"""<task>Resolve pronouns and references in the new query using conversation context.</task>

<conversation_history>
{history_context}
</conversation_history>

<new_query>{query}</new_query>

<resolution_rules>
1. PRONOUNS: Replace "it", "this", "that", "they", "them" with specific referents from history
2. IMPLICIT REFERENCES: "tell me more", "what about", "and also" - expand with previous topic
3. ELLIPSIS: Incomplete queries - complete with context from previous Q&A
4. ALREADY CLEAR: If query is self-contained, return it unchanged
</resolution_rules>

<examples>
History: Q: "What is the SHF switch?" A: "The SHF switch controls..."
New: "How do I activate it?" becomes "How do I activate the SHF switch?"

History: Q: "Explain TSS permissions" A: "TSS uses..."
New: "What about RACF?" becomes "What about RACF permissions compared to TSS?"

History: Q: "Steps to submit a JCL job" A: "1. First..."
New: "Can you explain step 3?" becomes "Can you explain step 3 of submitting a JCL job?"
</examples>

<output>Return ONLY the rewritten query (no explanation, no quotes):</output>"""

        response = bedrock.invoke_model(
            modelId=Config.HAIKU_MODEL,
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 200,
                "temperature": 0,
                "messages": [{"role": "user", "content": prompt}]
            })
        )
        
        result = json.loads(response['body'].read())
        resolved = result['content'][0]['text'].strip().strip('"')
        
        if resolved.lower() != query.lower():
            explanation = f"Resolved: '{query}' → '{resolved}'"
            log_event('query_resolved', explanation)
            return resolved, explanation
        
        return query, None
        
    except Exception as e:
        log_event('resolution_error', f"Context resolution failed: {e}")
        return query, None


def store_conversation_async(user_id: str, session_id: str, query: str,
                             answer: str, sources: List[Dict], intent: str,
                             entities: List[str], persona: str = None) -> None:
    """
    Asynchronously store conversation turn via Lambda 09.
    
    Non-blocking to avoid latency impact on response.
    
    Args:
        user_id: User identifier
        session_id: Session identifier
        query: Original user query
        answer: Generated answer
        sources: Retrieved source chunks
        intent: Classified query intent
        entities: Discussed entities
        persona: Persona ID used for this turn (Task 3.4)
    """
    if not Config.ENABLE_MEMORY:
        return
    
    try:
        lambda_client = get_client('lambda')
        
        payload = {
            'user_id': user_id,
            'session_id': session_id,
            'query': query,
            'answer': answer[:2000],  # Truncate for storage efficiency
            'intent': intent,
            'entities_discussed': entities[:20],
            'retrieved_documents': [s.get('document_id') for s in sources[:10]],
            'persona': persona,  # Task 3.4: Include persona in memory
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

        function_name = Config.MEMORY_WRITER_LAMBDA_ARN or 'mechavatar-lab-cac1-lambda-07-memory-writer'
        
        lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',  # Async - fire and forget
            Payload=json.dumps(payload)
        )
        
        log_event('memory_stored', 'Async memory write invoked', session_id=session_id)
        
    except Exception as e:
        # Non-blocking - log but don't fail
        log_event('memory_store_error', f"Failed to store memory: {e}")


# ============================================================================
# INTENT CLASSIFICATION ENGINE
# ============================================================================

def classify_query_intent(query: str) -> Tuple[QueryIntent, float, List[str]]:
    """
    Classify user query intent using Claude Haiku.
    
    Returns:
        Tuple of (intent, confidence, extracted_keywords)
    """
    if not Config.ENABLE_INTENT_CLASSIFICATION:
        return QueryIntent.OPEN_ENDED, 0.5, []
    
    log_event('intent_classification', f"Using model: {Config.HAIKU_MODEL}")
    
    try:
        bedrock = get_client('bedrock-runtime')
        
        prompt = f"""<task>Classify this mainframe documentation query into exactly ONE intent category.</task>

<query>{query}</query>

<categories>
DEFINITION: Questions asking "What is X?", "Define Y", "Explain the concept of Z"
PROCEDURE: Action-oriented questions like "How do I...", "Steps to...", "Process for...", "Walk me through..."
TROUBLESHOOTING: Contains error codes (S0C7, ABEND, RC=12), symptoms like "not working", "failed", "fix", "error"
COMPARISON: "Difference between X and Y", "X vs Y", "Compare", "Which is better"
CODE_EXPLANATION: "What does this code/JCL/COBOL do", "Explain this macro", "Parse this syntax"
BEST_PRACTICE: "Best way to...", "Recommended approach", "Standard for...", "Should I..."
CONFIGURATION: "How to configure/setup", "Parameters for", "Settings", "Initialize"
SECURITY: Access control, permissions, TSS, RACF, ACF2, authorization, authentication
OPEN_ENDED: Broad exploration, general information, multiple possible answers
</categories>

<examples>
Query: "What is the SHF switch?" → DEFINITION (asking for concept explanation)
Query: "How do I submit a JCL job?" → PROCEDURE (step-by-step action)
Query: "Getting S0C7 abend in batch job" → TROUBLESHOOTING (error symptom)
Query: "VSAM vs DB2 for this use case" → COMPARISON (comparing options)
Query: "TSS permission to access dataset" → SECURITY (access control)
</examples>

<rules>
1. Choose the SINGLE most specific category that matches
2. If query spans multiple categories, pick the PRIMARY intent
3. Confidence: 0.9+ = clear match, 0.7-0.9 = likely match, <0.7 = uncertain
4. Extract 2-5 technical keywords from the query
</rules>

<output_format>Return ONLY valid JSON, no explanation:
{{"intent": "CATEGORY_NAME", "confidence": 0.0-1.0, "keywords": ["term1", "term2"]}}</output_format>"""

        response = bedrock.invoke_model(
            modelId=Config.HAIKU_MODEL,
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 150,
                "temperature": 0,
                "messages": [{"role": "user", "content": prompt}]
            })
        )
        
        result = json.loads(response['body'].read())
        intent_json = result['content'][0]['text'].strip()
        
        # Parse response
        parsed = json.loads(intent_json)
        intent_str = parsed.get('intent', 'OPEN_ENDED').upper()
        confidence = float(parsed.get('confidence', 0.5))
        keywords = parsed.get('keywords', [])
        
        # Map to enum
        intent_map = {
            'DEFINITION': QueryIntent.DEFINITION,
            'PROCEDURE': QueryIntent.PROCEDURE,
            'TROUBLESHOOTING': QueryIntent.TROUBLESHOOTING,
            'COMPARISON': QueryIntent.COMPARISON,
            'CODE_EXPLANATION': QueryIntent.CODE_EXPLANATION,
            'BEST_PRACTICE': QueryIntent.BEST_PRACTICE,
            'CONFIGURATION': QueryIntent.CONFIGURATION,
            'SECURITY': QueryIntent.SECURITY,
            'OPEN_ENDED': QueryIntent.OPEN_ENDED
        }
        
        intent = intent_map.get(intent_str, QueryIntent.OPEN_ENDED)
        
        log_event('intent_classified', f"Intent: {intent.value}", 
                  confidence=confidence, keywords=keywords)
        
        return intent, confidence, keywords
        
    except json.JSONDecodeError as e:
        log_event('intent_parse_error', f"JSON parse failed: {e}")
        return QueryIntent.OPEN_ENDED, 0.3, []
    except Exception as e:
        log_event('intent_error', f"Classification failed: {e}")
        return QueryIntent.OPEN_ENDED, 0.3, []


# ============================================================================
# QUERY EXPANSION ENGINE
# ============================================================================

def expand_query_terms(query: str, keywords: List[str] = None) -> List[str]:
    """
    Expand query with mainframe terminology synonyms and acronym variations.
    
    Uses both static knowledge base and LLM for edge cases.
    
    Returns:
        List of query variations [original, expanded1, expanded2]
    """
    if not Config.ENABLE_QUERY_EXPANSION:
        return [query]
    
    expansions = [query]
    query_lower = query.lower()
    
    # Static expansion from knowledge base
    expanded_terms = []
    for term, synonyms in MAINFRAME_SYNONYMS.items():
        if term in query_lower:
            expanded_terms.extend(synonyms[:2])  # Limit to prevent explosion
    
    if expanded_terms:
        # Create variation with expanded terms
        expanded_query = query
        for term in expanded_terms[:3]:
            if term.lower() not in query_lower:
                expanded_query = f"{query} {term}"
                break
        if expanded_query != query:
            expansions.append(expanded_query)
    
    # LLM expansion for complex queries
    if len(query.split()) > 3 and Config.ENABLE_QUERY_EXPANSION:
        try:
            bedrock = get_client('bedrock-runtime')
            
            prompt = f"""<task>Expand this mainframe query with semantic variations for better search coverage.</task>

<query>{query}</query>

<expansion_rules>
1. ACRONYM EXPANSION: Expand mainframe acronyms to full forms
   - TSS → Top Secret Security
   - CICS → Customer Information Control System
   - JCL → Job Control Language
   - VSAM → Virtual Storage Access Method
   - SHF → System History File

2. SYNONYM VARIATIONS: Use alternate technical terms
   - "abend" → "abnormal end" / "system failure"
   - "job" → "batch job" / "JCL job"
   - "dataset" → "file" / "data set"

3. DOMAIN-SPECIFIC EXPANSION:
   - Add BMO/MECH-specific context if relevant
   - Include related system names (CADS, SAGE, DDA)

4. PRESERVE MEANING: Never change the query's intent
</expansion_rules>

<examples>
Input: "TSS permission denied error"
Output: ["Top Secret Security permission denied error", "TSS access authorization failure"]

Input: "How to fix S0C7 abend"
Output: ["How to fix S0C7 data exception", "How to resolve S0C7 abnormal termination"]
</examples>

<output>Return ONLY a JSON array with 1-2 variations:
["variation 1", "variation 2"]</output>"""

            response = bedrock.invoke_model(
                modelId=Config.HAIKU_MODEL,
                body=json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 200,
                    "temperature": 0,
                    "messages": [{"role": "user", "content": prompt}]
                })
            )
            
            result = json.loads(response['body'].read())
            llm_expansions = json.loads(result['content'][0]['text'].strip())
            
            if isinstance(llm_expansions, list):
                expansions.extend(llm_expansions[:2])
                
        except Exception as e:
            log_event('expansion_error', f"LLM expansion failed: {e}")
    
    # Deduplicate while preserving order
    seen = set()
    unique = []
    for exp in expansions:
        exp_lower = exp.lower()
        if exp_lower not in seen:
            seen.add(exp_lower)
            unique.append(exp)
    
    log_event(
        'query_expanded',
        f"Generated {len(unique)} variations",
        variations=[_truncate_text(exp, 200) for exp in unique[:4]]
    )
    return unique[:4]  # Max 4 variations


# ============================================================================
# EMBEDDING GENERATION
# ============================================================================

def generate_query_embedding(query: str) -> Optional[List[float]]:
    """
    Generate vector embedding using Amazon Titan V2.

    Includes:
    - Embedding cache for repeated queries (Phase 2 Enhancement)
    - Circuit breaker for Bedrock failures
    - Retry logic with exponential backoff for throttling

    Returns:
        1024-dimensional embedding vector or None on failure
    """
    # Phase 2: Check cache first
    cached = get_cached_embedding(query)
    if cached is not None:
        return cached
    
    log_event('embedding_generation', f"Using embedding model: {Config.EMBEDDING_MODEL}")

    # Phase 2: Check circuit breaker
    circuit = get_circuit_breaker('bedrock-embedding')
    if not circuit.can_execute():
        log_event('circuit_open_skip', 'Bedrock embedding circuit is open, skipping')
        emit_metric('CircuitBreakerSkip', 1, dimensions={'service': 'bedrock-embedding'})
        return None

    client = get_client('bedrock-runtime')
    start_time = time.time()

    for attempt in range(Config.MAX_RETRIES):
        try:
            response = client.invoke_model(
                modelId=Config.EMBEDDING_MODEL,
                contentType='application/json',
                body=json.dumps({"inputText": query[:8000]})  # Titan limit
            )

            result = json.loads(response['body'].read().decode('utf-8'))
            embedding = result.get('embedding', [])

            # Record success
            circuit.record_success()
            latency_ms = int((time.time() - start_time) * 1000)
            emit_metric('EmbeddingLatency', latency_ms, 'Milliseconds')

            log_event('embedding_generated', f"Dimensions: {len(embedding)}")

            # Phase 2: Store in cache
            store_cached_embedding(query, embedding)

            return embedding

        except client.exceptions.ThrottlingException:
            wait_time = Config.RETRY_BASE_DELAY * (2 ** attempt)
            log_event('embedding_throttled', f"Retry {attempt + 1}, waiting {wait_time}s")
            emit_metric('BedrockThrottling', 1, dimensions={'model': 'titan-embed'})
            time.sleep(wait_time)

        except Exception as e:
            if 'throttl' in str(e).lower() and attempt < Config.MAX_RETRIES - 1:
                time.sleep(Config.RETRY_BASE_DELAY * (2 ** attempt))
            else:
                circuit.record_failure()
                log_event('embedding_error', f"Failed: {e}")
                emit_metric('EmbeddingError', 1)
                raise

    return None


# Backwards-compatible alias used by some helper paths
def generate_embedding(query: str) -> Optional[List[float]]:
    return generate_query_embedding(query)


# ============================================================================
# GENERIC SEMANTIC RETRIEVAL ENHANCEMENT (NO HARDCODED DOCUMENT TYPES)
# ============================================================================

def extract_query_requirements(query: str) -> Dict[str, Any]:
    """
    GENERIC LLM-based analysis of query requirements.
    
    NO HARDCODED CATEGORIES - dynamically extracts what the query needs.
    Uses LLM to understand what topics, terms, and document types are relevant.
    
    Returns:
        {
            'required_topics': List[str],      # Topics the query is asking about
            'key_terms': List[str],             # Technical terms to match
            'expected_doc_types': List[str],   # Types of docs likely to have answer
            'importance_indicators': List[str], # Terms that indicate important content
            'multi_doc_required': bool          # Whether multiple sources needed
        }
    """
    log_event('requirements_extraction', f"Using model: {Config.HAIKU_MODEL}")
    
    try:
        bedrock = get_client('bedrock-runtime')
        
        prompt = f"""<task>Analyze this query to determine retrieval requirements for optimal document search.</task>

<query>{query}</query>

<analysis_instructions>
Think step-by-step:
1. What TOPICS does this query relate to? (systems, processes, concepts)
2. What TECHNICAL TERMS should we match exactly?
3. What DOCUMENT TYPES would contain this information?
4. What UNIQUE IDENTIFIERS would indicate high relevance?
5. Does answering this require MULTIPLE SOURCES?
</analysis_instructions>

<extraction_fields>
required_topics: 3-5 high-level topics (e.g., "security", "batch_processing", "error_handling")
key_terms: 5-10 technical terms, acronyms, system names (extract EXACTLY from query)
expected_doc_types: Document types likely to have answer (procedure, specification, guide, reference, troubleshooting, configuration, code_sample, overview)
importance_indicators: Unique identifiers that indicate high relevance (program names, error codes, file names, table names)
multi_doc_required: true if query asks for comparison, combination, or comprehensive info; false for simple lookups
</extraction_fields>

<examples>
Query: "How do I configure TSS permissions for CICS transaction ABCD?"
→ {{"required_topics": ["security", "TSS_configuration", "CICS_transactions"], "key_terms": ["TSS", "CICS", "transaction", "ABCD", "permissions", "configure"], "expected_doc_types": ["procedure", "configuration", "security_guide"], "importance_indicators": ["ABCD", "TSS", "CICS"], "multi_doc_required": false}}

Query: "Compare SHF03 and SHF04 record structures for journal entries"
→ {{"required_topics": ["SHF_processing", "record_structures", "journal_entries"], "key_terms": ["SHF03", "SHF04", "record", "structure", "journal", "entries"], "expected_doc_types": ["specification", "reference", "overview"], "importance_indicators": ["SHF03", "SHF04"], "multi_doc_required": true}}
</examples>

<output>Return ONLY valid JSON (no explanation):
{{"required_topics": [...], "key_terms": [...], "expected_doc_types": [...], "importance_indicators": [...], "multi_doc_required": true/false}}</output>"""

        response = bedrock.invoke_model(
            modelId=Config.HAIKU_MODEL,
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 300,
                "temperature": 0,
                "messages": [{"role": "user", "content": prompt}]
            })
        )
        
        result = json.loads(response['body'].read())
        analysis = json.loads(result['content'][0]['text'].strip())
        
        log_event(
            'query_analyzed',
            f"Topics: {len(analysis.get('required_topics', []))}, "
            f"Terms: {len(analysis.get('key_terms', []))}, "
            f"Multi-doc: {analysis.get('multi_doc_required', False)}",
            key_terms=analysis.get('key_terms', [])[:10],
            expected_doc_types=analysis.get('expected_doc_types', [])[:10],
            importance_indicators=analysis.get('importance_indicators', [])[:10]
        )
        
        return analysis
        
    except Exception as e:
        log_event('query_analysis_error', f"Query analysis failed: {e}")
        # Fallback to basic extraction
        return {
            'required_topics': [],
            'key_terms': query.split()[:10],
            'expected_doc_types': [],
            'importance_indicators': [],
            'multi_doc_required': len(query.split()) > 10
        }


# ============================================================================
# PHASE 3: ADVANCED RETRIEVAL TECHNIQUES
# ============================================================================

# Feature Flags for Phase 3
ENABLE_HYDE = os.environ.get('ENABLE_HYDE', 'true').lower() == 'true'
ENABLE_QUERY_DECOMPOSITION = os.environ.get('ENABLE_QUERY_DECOMPOSITION', 'true').lower() == 'true'
ENABLE_CONFIDENCE_CALIBRATION = os.environ.get('ENABLE_CONFIDENCE_CALIBRATION', 'true').lower() == 'true'
ENABLE_SELF_CONSISTENCY = os.environ.get('ENABLE_SELF_CONSISTENCY', 'true').lower() == 'true'  # Cost-heavy
ENABLE_CROSS_ENCODER = os.environ.get('ENABLE_CROSS_ENCODER', 'true').lower() == 'true'  # LLM-based reranking

# ============================================================================
# 1. CROSS-ENCODER RERANKING (LLM-based scoring)
# ============================================================================
# Note: Bedrock Rerank API is disabled. Using cross-encoder as primary reranking method.
# To enable Bedrock Rerank in future, uncomment Config.ENABLE_BEDROCK_RERANK and 
# the bedrock_rerank() function below, then update the retrieval pipeline to call it.

def cross_encoder_rerank(query: str, results: List[Dict[str, Any]], 
                         top_n: int = None) -> List[Dict[str, Any]]:
    """
    Use Claude Haiku as a cross-encoder to score query-document relevance.
    
    Logic:
    1. Take top candidates from initial retrieval
    2. Score each (query, document) pair with LLM
    3. Return reordered by LLM relevance score
    
    This is more accurate than embedding similarity because:
    - LLM understands semantic nuance
    - Can catch false positives from keyword matching
    - Better at multi-hop reasoning connections
    
    Cost: ~$0.0003 per document scored (Haiku)
    """
    if not Config.ENABLE_CROSS_ENCODER or not results:
        return results
    
    log_event('cross_encoder_rerank', f"Using model: {Config.HAIKU_MODEL} for reranking")
    
    top_n = top_n or Config.CROSS_ENCODER_TOP_N
    candidates = results[:min(len(results), top_n * 2)]  # Score 2x candidates
    
    try:
        bedrock = get_client('bedrock-runtime')
        
        # Batch scoring prompt - score multiple docs at once for efficiency
        docs_text = ""
        for i, doc in enumerate(candidates[:10]):  # Max 10 to control cost
            text_preview = doc.get('text', '')[:500]  # 500 chars per doc
            docs_text += f"\n[DOC_{i}]\n{text_preview}\n"
        
        prompt = f"""<task>Score each document's relevance to the query (0.0-1.0).</task>

<query>{query}</query>

<documents>{docs_text}</documents>

<scoring_criteria>
1.0 = Directly answers the query with specific information
0.8 = Contains highly relevant information, may need minor inference
0.6 = Partially relevant, contains related but not exact information
0.4 = Tangentially related, same domain but different topic
0.2 = Minimal relevance, only keyword matches
0.0 = Not relevant at all
</scoring_criteria>

<output>Return ONLY JSON object with document scores:
{{"DOC_0": 0.9, "DOC_1": 0.7, "DOC_2": 0.4, ...}}</output>"""

        response = bedrock.invoke_model(
            modelId=Config.HAIKU_MODEL,
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 200,
                "temperature": 0,
                "messages": [{"role": "user", "content": prompt}]
            })
        )
        
        result_text = json.loads(response['body'].read())['content'][0]['text'].strip()
        
        # Parse scores
        import re
        json_match = re.search(r'\{.*\}', result_text, re.DOTALL)
        if json_match:
            scores = json.loads(json_match.group(0))
        else:
            scores = {}
        
        # Apply cross-encoder scores
        for i, doc in enumerate(candidates[:10]):
            ce_score = scores.get(f"DOC_{i}", 0.5)
            doc['cross_encoder_score'] = ce_score
            # Combine with original score (weighted average)
            original_score = doc.get('score', 0.5)
            doc['final_score'] = 0.4 * original_score + 0.6 * ce_score
        
        # Sort by final score
        candidates.sort(key=lambda x: x.get('final_score', x.get('score', 0)), reverse=True)
        
        log_event('cross_encoder_complete', 
                  f"Reranked {len(candidates)} docs, top score: {candidates[0].get('final_score', 0):.3f}")
        emit_metric('CrossEncoderRerank', len(candidates))
        
        return candidates[:top_n]
        
    except Exception as e:
        log_event('cross_encoder_error', f"Cross-encoder failed: {e}")
        return results[:top_n]


# ============================================================================
# 1.5 BEDROCK RERANK API (DISABLED)
# ============================================================================
# 
# def bedrock_rerank(query: str, results: List[Dict[str, Any]], 
#                    top_n: int = None) -> List[Dict[str, Any]]:
#     """
#     Use Bedrock Rerank API for fast, accurate document reranking.
#     
#     Bedrock Rerank is faster and cheaper than LLM-based cross-encoder:
#     - amazon.rerank-v1:0 (Amazon's model)
#     - cohere.rerank-v3-5:0 (Cohere's model on Bedrock)
#     
#     Args:
#         query: User query
#         results: Initial retrieval results
#         top_n: Number of top results to return
#     
#     Returns:
#         Reranked results with relevance scores
#     """
#     if not Config.ENABLE_BEDROCK_RERANK or not results:
#         return results
#     
#     top_n = top_n or Config.RERANK_TOP_N
#     
#     try:
#         bedrock = get_client('bedrock-runtime')
#         
#         # Prepare documents for reranking
#         documents = []
#         for doc in results[:min(len(results), 25)]:  # Rerank API limit
#             text = doc.get('text', doc.get('content', ''))[:4000]  # Max chars
#             documents.append({
#                 "textDocument": {"text": text}
#             })
#         
#         # Call Bedrock Rerank API
#         response = bedrock.invoke_model(
#             modelId=Config.RERANK_MODEL,
#             body=json.dumps({
#                 "query": query,
#                 "documents": documents,
#                 "topN": top_n
#             })
#         )
#         
#         rerank_response = json.loads(response['body'].read())
#         
#         # Apply rerank scores to original results
#         reranked_results = []
#         for item in rerank_response.get('results', []):
#             idx = item.get('index', 0)
#             relevance_score = item.get('relevanceScore', 0)
#             
#             if idx < len(results) and relevance_score >= Config.RERANK_MIN_SCORE:
#                 doc = results[idx].copy()
#                 doc['rerank_score'] = relevance_score
#                 doc['original_score'] = doc.get('score', 0)
#                 doc['score'] = relevance_score  # Use rerank score as primary
#                 reranked_results.append(doc)
#         
#         log_event('bedrock_rerank_complete', 
#                   f"Reranked {len(results)} -> {len(reranked_results)} docs",
#                   model=Config.RERANK_MODEL,
#                   top_score=reranked_results[0]['rerank_score'] if reranked_results else 0)
#         emit_metric('BedrockRerank', len(reranked_results))
#         
#         return reranked_results if reranked_results else results[:top_n]
#         
#     except Exception as e:
#         log_event('bedrock_rerank_error', f"Bedrock Rerank failed: {e}")
#         # Fallback to cross-encoder if available
#         if Config.ENABLE_CROSS_ENCODER:
#             return cross_encoder_rerank(query, results, top_n)
#         return results[:top_n]


# ============================================================================
# 1.6 SELF-RAG VERIFICATION LOOP (Phase 2 Enhancement)
# ============================================================================

@dataclass
class SelfRAGResult:
    """Result from Self-RAG verification."""
    answer: str
    confidence: float
    is_grounded: bool
    verification_notes: str
    retrieval_needed: bool
    improved_query: Optional[str] = None
    iteration: int = 0


def self_rag_verify(query: str, answer: str, sources: List[Dict[str, Any]], 
                    iteration: int = 0) -> SelfRAGResult:
    """
    Self-RAG: Verify and potentially improve the generated answer.
    
    Self-RAG adds retrieval-aware self-reflection:
    1. ISREL - Is retrieval needed for this query?
    2. ISSUP - Is the evidence sufficient?
    3. ISUSE - Is the answer useful and grounded?
    
    If confidence is low, it can suggest better queries or flag for retry.
    
    Reference: "Self-RAG: Learning to Retrieve, Generate, and Critique" (Asai et al.)
    """
    if not Config.ENABLE_SELF_RAG:
        return SelfRAGResult(
            answer=answer,
            confidence=0.8,  # Default confidence
            is_grounded=True,
            verification_notes="Self-RAG disabled",
            retrieval_needed=False,
            iteration=iteration
        )
    
    if iteration >= Config.SELF_RAG_MAX_ITERATIONS:
        return SelfRAGResult(
            answer=answer,
            confidence=0.5,
            is_grounded=False,
            verification_notes="Max iterations reached",
            retrieval_needed=False,
            iteration=iteration
        )
    
    log_event('self_rag_verify', f"Iteration {iteration + 1}/{Config.SELF_RAG_MAX_ITERATIONS}")
    
    try:
        bedrock = get_client('bedrock-runtime')
        
        # Prepare source context
        source_texts = []
        for i, src in enumerate(sources[:5]):  # Top 5 sources
            text = src.get('text', src.get('content', ''))[:500]
            source_texts.append(f"[Source {i+1}]: {text}")
        sources_context = "\n".join(source_texts) if source_texts else "No sources available"
        
        prompt = f"""<task>Evaluate this RAG answer for quality and grounding.</task>

<user_query>{query}</user_query>

<sources>
{sources_context}
</sources>

<generated_answer>{answer[:2000]}</generated_answer>

<evaluation_criteria>
1. GROUNDING (0.0-1.0): Is the answer supported by the sources?
2. COMPLETENESS (0.0-1.0): Does it fully address the query?
3. ACCURACY (0.0-1.0): Are the facts correct per the sources?
4. RELEVANCE (0.0-1.0): Is the answer focused on the query?
</evaluation_criteria>

<output_format>
Return ONLY this JSON:
{{
    "grounding_score": 0.8,
    "completeness_score": 0.7,
    "accuracy_score": 0.9,
    "relevance_score": 0.85,
    "is_grounded": true,
    "needs_more_retrieval": false,
    "improved_query": null,
    "verification_notes": "Answer is well-supported by sources 1 and 3."
}}
</output_format>"""

        response = bedrock.invoke_model(
            modelId=Config.HAIKU_MODEL,
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 300,
                "temperature": 0,
                "messages": [{"role": "user", "content": prompt}]
            })
        )
        
        result_text = json.loads(response['body'].read())['content'][0]['text'].strip()
        
        # Parse response
        json_match = re.search(r'\{.*\}', result_text, re.DOTALL)
        if json_match:
            eval_result = json.loads(json_match.group(0))
        else:
            eval_result = {}
        
        # Calculate overall confidence
        grounding = eval_result.get('grounding_score', 0.5)
        completeness = eval_result.get('completeness_score', 0.5)
        accuracy = eval_result.get('accuracy_score', 0.5)
        relevance = eval_result.get('relevance_score', 0.5)
        
        # Weighted confidence calculation
        confidence = (grounding * 0.3 + completeness * 0.2 + accuracy * 0.3 + relevance * 0.2)
        
        is_grounded = eval_result.get('is_grounded', confidence >= Config.SELF_RAG_MIN_CONFIDENCE)
        needs_retry = eval_result.get('needs_more_retrieval', confidence < Config.SELF_RAG_RETRY_THRESHOLD)
        improved_query = eval_result.get('improved_query')
        
        log_event('self_rag_result', 
                  f"Confidence: {confidence:.2f}, Grounded: {is_grounded}, Retry: {needs_retry}",
                  scores={
                      'grounding': grounding,
                      'completeness': completeness,
                      'accuracy': accuracy,
                      'relevance': relevance
                  })
        emit_metric('SelfRAGConfidence', confidence)
        
        return SelfRAGResult(
            answer=answer,
            confidence=confidence,
            is_grounded=is_grounded,
            verification_notes=eval_result.get('verification_notes', ''),
            retrieval_needed=needs_retry,
            improved_query=improved_query if needs_retry else None,
            iteration=iteration
        )
        
    except Exception as e:
        log_event('self_rag_error', f"Self-RAG verification failed: {e}")
        return SelfRAGResult(
            answer=answer,
            confidence=0.6,  # Moderate confidence on error
            is_grounded=True,
            verification_notes=f"Verification error: {str(e)}",
            retrieval_needed=False,
            iteration=iteration
        )


def self_rag_loop(query: str, retrieve_fn, generate_fn) -> Tuple[str, float, List[Dict]]:
    """
    Run the Self-RAG loop: retrieve → generate → verify → possibly retry.
    
    Args:
        query: User query
        retrieve_fn: Function to retrieve documents (returns List[Dict])
        generate_fn: Function to generate answer (takes query, docs, returns str)
    
    Returns:
        Tuple of (final_answer, confidence, sources)
    """
    current_query = query
    best_answer = None
    best_confidence = 0.0
    best_sources = []
    
    for iteration in range(Config.SELF_RAG_MAX_ITERATIONS + 1):
        # Retrieve
        sources = retrieve_fn(current_query)
        
        # Generate
        answer = generate_fn(current_query, sources)
        
        # Verify
        result = self_rag_verify(current_query, answer, sources, iteration)
        
        # Track best result
        if result.confidence > best_confidence:
            best_answer = result.answer
            best_confidence = result.confidence
            best_sources = sources
        
        # Check if we should retry
        if result.confidence >= Config.SELF_RAG_MIN_CONFIDENCE or not result.retrieval_needed:
            log_event('self_rag_complete', 
                      f"Completed in {iteration + 1} iterations, confidence: {result.confidence:.2f}")
            return result.answer, result.confidence, sources
        
        # Use improved query if available
        if result.improved_query:
            log_event('self_rag_retry', 
                      f"Retrying with improved query: {result.improved_query[:100]}")
            current_query = result.improved_query
        else:
            # No improvement possible, return best result
            break
    
    log_event('self_rag_fallback', f"Returning best result, confidence: {best_confidence:.2f}")
    return best_answer or answer, best_confidence, best_sources


# ============================================================================
# 2. HyDE - HYPOTHETICAL DOCUMENT EMBEDDINGS
# ============================================================================

def generate_hyde_embedding(query: str) -> Optional[List[float]]:
    """
    HyDE: Generate a hypothetical document that would answer the query,
    then embed THAT document for retrieval.
    
    Logic:
    1. LLM generates what an ideal answer document would look like
    2. Embed the hypothetical document
    3. Search using hypothetical embedding (better semantic match)
    
    Why it works:
    - Query embeddings often don't match document embeddings well
    - "What is SHF?" embeds differently than "SHF is the System History File..."
    - HyDE bridges this query-document vocabulary gap
    
    Research: "Precise Zero-Shot Dense Retrieval without Relevance Labels" (Gao et al.)
    """
    if not ENABLE_HYDE:
        return None
    
    log_event('hyde_generation', f"Using LLM: {Config.HAIKU_MODEL}, Embedding: {Config.EMBEDDING_MODEL}")
    
    try:
        bedrock = get_client('bedrock-runtime')
        
        prompt = f"""<task>Write a short technical documentation passage that would directly answer this query.</task>

<query>{query}</query>

<instructions>
- Write 2-3 sentences as if from an official MECH/mainframe documentation manual
- Include specific technical details, field names, system names
- Use formal technical writing style
- Do NOT say "this document explains" - just provide the content directly
</instructions>

<example>
Query: "What is the SHF switch process?"
Output: "The SHF Switch is a critical MECH batch process that transitions the System History File from online to offline mode. It is triggered by job MSHFSW01 and coordinates with NATI processing. The switch operates in either dependent mode (waits for prior jobs) or independent mode (runs immediately)."
</example>

Write the hypothetical passage:"""

        response = bedrock.invoke_model(
            modelId=Config.HAIKU_MODEL,
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 300,
                "temperature": 0.3,  # Slight creativity for realistic doc
                "messages": [{"role": "user", "content": prompt}]
            })
        )
        
        hypothetical_doc = json.loads(response['body'].read())['content'][0]['text'].strip()
        
        log_event(
            'hyde_generated',
            f"Generated hypothetical doc: {len(hypothetical_doc)} chars",
            preview=_truncate_text(hypothetical_doc, 240)
        )
        
        # Embed the hypothetical document
        hyde_embedding = generate_query_embedding(hypothetical_doc)
        
        if hyde_embedding:
            emit_metric('HyDEGenerated', 1)
            return hyde_embedding
        
        return None
        
    except Exception as e:
        log_event('hyde_error', f"HyDE generation failed: {e}")
        return None


def hybrid_search_with_hyde(query: str, top_k: int = None) -> List[Dict[str, Any]]:
    """
    Enhanced search combining:
    1. Regular query embedding
    2. HyDE embedding
    3. BM25 keyword search
    
    Uses RRF to fuse all three result sets.
    """
    top_k = top_k or Config.TOP_K
    
    # Get regular query embedding
    query_embedding = generate_query_embedding(query)
    
    # Get HyDE embedding
    hyde_embedding = generate_hyde_embedding(query)
    
    rankings = []
    
    # Search with regular embedding
    if query_embedding:
        regular_results = execute_semantic_search(query_embedding, top_k=top_k * 2)
        if regular_results:
            rankings.append(regular_results)
    
    # Search with HyDE embedding
    if hyde_embedding:
        hyde_results = execute_semantic_search(hyde_embedding, top_k=top_k * 2)
        if hyde_results:
            rankings.append(hyde_results)
    
    # Fuse using RRF
    if len(rankings) > 1:
        return reciprocal_rank_fusion(rankings)[:top_k]
    elif rankings:
        return rankings[0][:top_k]
    else:
        return []


# ============================================================================
# 3. QUERY DECOMPOSITION FOR MULTI-HOP REASONING
# ============================================================================

def decompose_complex_query(query: str) -> List[str]:
    """
    Break complex queries into simpler sub-queries for multi-hop retrieval.
    
    Logic:
    1. Detect if query requires multiple pieces of information
    2. Generate sub-queries that can be answered independently
    3. Retrieve for each sub-query, then synthesize
    
    Example:
    Query: "What is the difference between SHF03 and SHF04 record formats?"
    Sub-queries:
    - "What is the SHF03 record format structure?"
    - "What is the SHF04 record format structure?"
    
    Why it works:
    - Single retrieval may miss one of the compared items
    - Each sub-query targets specific information
    - Final synthesis has complete context
    """
    if not ENABLE_QUERY_DECOMPOSITION:
        return [query]
    
    # Quick heuristics for decomposition candidates
    decomposition_signals = [
        'difference between', 'compare', 'vs', 'versus',
        'relationship between', 'how does .* affect',
        'what are all', 'list all', 'complete list',
        ' and ', ' or ', 'both', 'each'
    ]
    
    needs_decomposition = any(signal in query.lower() for signal in decomposition_signals)
    
    # Also check for multi-part questions
    if not needs_decomposition:
        multi_part_patterns = [r'\ba\)', r'\b1\.', r'first.*second', r'multiple']
        needs_decomposition = any(re.search(p, query.lower()) for p in multi_part_patterns)
    
    if not needs_decomposition:
        return [query]
    
    try:
        bedrock = get_client('bedrock-runtime')
        
        prompt = f"""<task>Decompose this complex query into 2-4 simpler sub-queries.</task>

<query>{query}</query>

<rules>
1. Each sub-query should be answerable from a SINGLE document chunk
2. Sub-queries should cover ALL aspects of the original question
3. Preserve technical terms exactly (SHF03, MYPTLA, TSS, etc.)
4. If query is already simple, return just the original query
</rules>

<examples>
Query: "Compare the SHF03 and SHF14 segment structures"
Sub-queries: ["What is the SHF03 segment structure?", "What is the SHF14 segment structure?"]

Query: "How to configure TSS permissions and what are the audit requirements?"
Sub-queries: ["How to configure TSS permissions?", "What are TSS audit requirements?"]

Query: "What is the MYPTLA table?"
Sub-queries: ["What is the MYPTLA table?"]  # Already simple
</examples>

<output>Return ONLY JSON array of sub-queries:
["sub-query 1", "sub-query 2", ...]</output>"""

        response = bedrock.invoke_model(
            modelId=Config.HAIKU_MODEL,
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 300,
                "temperature": 0,
                "messages": [{"role": "user", "content": prompt}]
            })
        )
        
        result_text = json.loads(response['body'].read())['content'][0]['text'].strip()
        
        # Parse JSON array
        json_match = re.search(r'\[.*\]', result_text, re.DOTALL)
        if json_match:
            sub_queries = json.loads(json_match.group(0))
            if isinstance(sub_queries, list) and len(sub_queries) > 0:
                log_event(
                    'query_decomposed',
                    f"Decomposed into {len(sub_queries)} sub-queries",
                    sub_queries=[_truncate_text(q, 200) for q in sub_queries[:4]]
                )
                emit_metric('QueryDecomposition', len(sub_queries))
                return sub_queries
        
        return [query]
        
    except Exception as e:
        log_event('decomposition_error', f"Query decomposition failed: {e}")
        return [query]


def retrieve_for_decomposed_query(query: str, top_k: int = None) -> List[Dict[str, Any]]:
    """
    Retrieve documents for a potentially complex query using decomposition.
    
    1. Decompose query into sub-queries
    2. Retrieve for each sub-query
    3. Merge and deduplicate results
    """
    top_k = top_k or Config.TOP_K
    sub_queries = decompose_complex_query(query)
    
    if len(sub_queries) == 1:
        # No decomposition needed, use regular retrieval
        return None  # Signal to use normal path
    
    all_results = []
    seen_chunks = set()
    
    for sub_query in sub_queries:
        # Retrieve for sub-query
        sub_embedding = generate_query_embedding(sub_query)
        if sub_embedding:
            sub_results = execute_semantic_search(sub_embedding, top_k=top_k // len(sub_queries) + 2)
            
            for result in sub_results:
                chunk_id = result.get('chunk_id', result.get('text', '')[:100])
                if chunk_id not in seen_chunks:
                    result['source_subquery'] = sub_query
                    all_results.append(result)
                    seen_chunks.add(chunk_id)
    
    # Sort by score
    all_results.sort(key=lambda x: x.get('score', 0), reverse=True)
    
    log_event('multi_hop_retrieval', 
              f"Retrieved {len(all_results)} unique chunks from {len(sub_queries)} sub-queries")
    
    return all_results[:top_k]


# ============================================================================
# PHASE 5: BOUNDED MULTI-HOP RETRIEVAL LOOP
# ============================================================================

ENABLE_MULTI_HOP_RETRIEVAL = os.environ.get('ENABLE_MULTI_HOP_RETRIEVAL', 'true').lower() == 'true'
MULTI_HOP_MAX_ITERATIONS = int(os.environ.get('MULTI_HOP_MAX_ITERATIONS', '3'))
MULTI_HOP_MIN_NEW_CHUNKS = int(os.environ.get('MULTI_HOP_MIN_NEW_CHUNKS', '2'))


def multi_hop_retrieval_loop(
    query: str,
    initial_chunks: List[Dict[str, Any]],
    query_embedding: List[float],
    top_k: int = None,
    max_iterations: int = None
) -> Dict[str, Any]:
    """
    Phase 5: Bounded multi-hop retrieval loop.
    
    Process:
    1. Start with initial retrieval results
    2. Generate partial answer / identify gaps
    3. If gaps detected, formulate follow-up queries
    4. Retrieve additional context
    5. Repeat until satisfied or max iterations reached
    
    This is different from Self-RAG (which verifies existing answers).
    Multi-hop actively explores the knowledge space to fill gaps.
    
    Args:
        query: Original user query
        initial_chunks: First round of retrieved chunks
        query_embedding: Original query embedding
        top_k: Maximum chunks per iteration
        max_iterations: Maximum retrieval rounds
    
    Returns:
        Dict with enriched chunks and hop metadata
    """
    if not ENABLE_MULTI_HOP_RETRIEVAL:
        return {
            'chunks': initial_chunks,
            'iterations': 0,
            'multi_hop_enabled': False
        }
    
    max_iterations = max_iterations or MULTI_HOP_MAX_ITERATIONS
    top_k = top_k or Config.TOP_K
    start_time = time.time()
    
    all_chunks = list(initial_chunks)
    seen_chunk_ids = {c.get('chunk_id') for c in all_chunks if c.get('chunk_id')}
    iteration_history = []
    
    for iteration in range(max_iterations):
        # Step 1: Identify gaps in current context
        gap_analysis = _identify_retrieval_gaps(query, all_chunks)
        
        if not gap_analysis.get('has_gaps'):
            log_event('multi_hop_sufficient', 
                      f"Context sufficient after {iteration} iterations")
            break
        
        missing_aspects = gap_analysis.get('missing_aspects', [])
        if not missing_aspects:
            break
        
        # Step 2: Formulate follow-up queries for gaps
        follow_up_queries = _generate_follow_up_queries(query, missing_aspects)
        
        iteration_history.append({
            'iteration': iteration + 1,
            'gaps_identified': missing_aspects[:3],
            'follow_ups': [q[:100] for q in follow_up_queries[:2]]
        })
        
        # Step 3: Retrieve for follow-up queries
        new_chunks = []
        for fq in follow_up_queries[:2]:  # Limit follow-ups per iteration
            fq_embedding = generate_query_embedding(fq)
            if not fq_embedding:
                continue
            
            fq_results = execute_hybrid_search(
                query_text=fq,
                query_embedding=fq_embedding,
                top_k=top_k // 2
            )
            
            for chunk in fq_results:
                chunk_id = chunk.get('chunk_id')
                if chunk_id and chunk_id not in seen_chunk_ids:
                    chunk['hop_iteration'] = iteration + 1
                    chunk['hop_query'] = fq[:100]
                    new_chunks.append(chunk)
                    seen_chunk_ids.add(chunk_id)
        
        if len(new_chunks) < MULTI_HOP_MIN_NEW_CHUNKS:
            log_event('multi_hop_stop', 
                      f"Too few new chunks ({len(new_chunks)}), stopping")
            break
        
        all_chunks.extend(new_chunks)
        log_event('multi_hop_iteration', 
                  f"Iteration {iteration + 1}: added {len(new_chunks)} new chunks")
    
    # Re-sort all chunks by score
    all_chunks.sort(key=lambda x: x.get('score', 0), reverse=True)
    
    elapsed_ms = int((time.time() - start_time) * 1000)
    
    log_event('multi_hop_complete',
              f"Completed {len(iteration_history)} hops, "
              f"total chunks: {len(all_chunks)}, {elapsed_ms}ms")
    
    return {
        'chunks': all_chunks[:top_k * 2],  # Return top_k * 2 for re-ranking
        'iterations': len(iteration_history),
        'iteration_history': iteration_history,
        'multi_hop_enabled': True,
        'elapsed_ms': elapsed_ms
    }


def _identify_retrieval_gaps(query: str, chunks: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Identify gaps in current retrieval context.
    
    Uses Haiku for fast gap analysis.
    """
    if not chunks:
        return {'has_gaps': True, 'missing_aspects': [query]}
    
    try:
        bedrock = get_client('bedrock-runtime')
        
        # Summarize current context
        context_summary = []
        for i, chunk in enumerate(chunks[:5]):
            summary = f"Doc {i+1}: {chunk.get('text', '')[:200]}..."
            context_summary.append(summary)
        
        prompt = f"""Analyze if this context is sufficient to answer the query.

Query: "{query}"

Current context:
{chr(10).join(context_summary)}

Identify what information is MISSING to fully answer the query.
Return JSON:
{{"has_gaps": true/false, "missing_aspects": ["aspect1", "aspect2"]}}

Be conservative - only flag CRITICAL gaps. Minor details are OK."""

        response = bedrock.invoke_model(
            modelId=Config.HAIKU_MODEL,
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 200,
                "temperature": 0,
                "messages": [{"role": "user", "content": prompt}]
            })
        )
        
        result_text = json.loads(response['body'].read())['content'][0]['text'].strip()
        
        # Parse JSON
        json_match = re.search(r'\{.*\}', result_text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group(0))
        
        return {'has_gaps': False, 'missing_aspects': []}
        
    except Exception as e:
        log_event('gap_analysis_error', f"Gap analysis failed: {e}")
        return {'has_gaps': False, 'missing_aspects': []}


def _generate_follow_up_queries(original_query: str, missing_aspects: List[str]) -> List[str]:
    """Generate targeted follow-up queries for missing aspects."""
    queries = []
    
    for aspect in missing_aspects[:3]:
        # Create focused query for each missing aspect
        follow_up = f"{original_query} specifically {aspect}"
        queries.append(follow_up)
    
    return queries


# ============================================================================
# 4. CONFIDENCE CALIBRATION (Know When to Refuse)
# ============================================================================

def calculate_answer_confidence(query: str, context_chunks: List[Dict], 
                                 answer: str) -> Dict[str, Any]:
    """
    Calculate confidence score for generated answer.
    
    Signals for LOW confidence (should add disclaimer or refuse):
    1. Low retrieval scores (no good matches found)
    2. Answer contains hedging language
    3. Required entities from query not found in context
    4. Answer is very short for complex query
    
    Returns confidence score and recommendation.
    """
    if not ENABLE_CONFIDENCE_CALIBRATION:
        return {'score': 1.0, 'action': 'proceed', 'reasons': []}
    
    confidence_score = 1.0
    penalty_reasons = []
    
    # Factor 1: Retrieval Quality
    if context_chunks:
        top_scores = [c.get('score', 0) for c in context_chunks[:3]]
        avg_top_score = sum(top_scores) / len(top_scores) if top_scores else 0
        
        if avg_top_score < 0.3:
            confidence_score -= 0.3
            penalty_reasons.append(f"low_retrieval_scores:{avg_top_score:.2f}")
        elif avg_top_score < 0.5:
            confidence_score -= 0.15
            penalty_reasons.append(f"moderate_retrieval_scores:{avg_top_score:.2f}")
    else:
        confidence_score -= 0.5
        penalty_reasons.append("no_context_retrieved")
    
    # Factor 2: Hedging Language Detection
    hedging_phrases = [
        'i am not sure', 'i cannot find', 'may not be', 'might be',
        'possibly', 'perhaps', 'unclear', 'not certain',
        'i don\'t have', 'no information', 'not found in',
        'unable to determine', 'cannot confirm'
    ]
    answer_lower = answer.lower()
    hedging_count = sum(1 for phrase in hedging_phrases if phrase in answer_lower)
    if hedging_count > 0:
        confidence_score -= min(hedging_count * 0.1, 0.3)
        penalty_reasons.append(f"hedging_language:{hedging_count}")
    
    # Factor 3: Query Entity Coverage
    query_upper = query.upper()
    # Extract technical identifiers from query
    tech_terms = re.findall(r'\b[A-Z]{2,}[0-9]*\b', query_upper)
    tech_terms = [t for t in tech_terms if len(t) >= 3]  # Filter short ones
    
    if tech_terms:
        context_text = ' '.join(c.get('text', '') for c in context_chunks)
        terms_found = sum(1 for term in tech_terms if term in context_text.upper())
        coverage = terms_found / len(tech_terms)
        
        if coverage < 0.5:
            confidence_score -= 0.2
            penalty_reasons.append(f"low_term_coverage:{coverage:.0%}")
    
    # Factor 4: Answer Length vs Query Complexity
    query_words = len(query.split())
    answer_words = len(answer.split())
    
    if query_words > 15 and answer_words < 50:
        confidence_score -= 0.15
        penalty_reasons.append("short_answer_for_complex_query")
    
    # Factor 5: Code Correctness (for code generation responses)
    # Check if the answer contains code blocks and validate basic correctness
    code_blocks = re.findall(r'```(?:cobol|jcl|sql)?\s*\n(.*?)```', answer, re.DOTALL | re.IGNORECASE)
    if code_blocks:
        code_issues = 0
        for block in code_blocks:
            block_upper = block.upper()
            # Check for common COBOL structure issues
            if 'PROCEDURE DIVISION' in block_upper or 'IDENTIFICATION DIVISION' in block_upper:
                # Full program - check for matching END markers
                if block_upper.count('IF ') > block_upper.count('END-IF'):
                    code_issues += 1
                if block_upper.count('PERFORM ') > 0 and 'END-PERFORM' not in block_upper and 'THRU' not in block_upper:
                    code_issues += 1
            # Check for truncated code
            if block.rstrip().endswith(('...', '// ...', '* ...')):
                code_issues += 1
        
        if code_issues > 0:
            penalty = min(code_issues * 0.08, 0.2)
            confidence_score -= penalty
            penalty_reasons.append(f"code_correctness_issues:{code_issues}")
    
    # Clamp score
    confidence_score = max(0.0, min(1.0, confidence_score))
    
    # Determine action
    if confidence_score >= 0.7:
        action = 'proceed'
    elif confidence_score >= 0.5:
        action = 'add_disclaimer'
    else:
        action = 'low_confidence_warning'
    
    return {
        'score': round(confidence_score, 3),
        'action': action,
        'reasons': penalty_reasons,
        'metrics': {
            'retrieval_score': avg_top_score if context_chunks else 0,
            'hedging_count': hedging_count,
            'term_coverage': coverage if tech_terms else 1.0
        }
    }


def apply_confidence_calibration(answer: str, confidence: Dict[str, Any]) -> str:
    """
    Apply confidence calibration to answer.
    
    - High confidence: Return as-is
    - Medium: Add disclaimer
    - Low: Add strong warning
    """
    if confidence['action'] == 'proceed':
        return answer
    
    if confidence['action'] == 'add_disclaimer':
        disclaimer = "\n\n---\n*Note: This answer is based on available documentation. " \
                     "Some details may require verification with source systems.*"
        return answer + disclaimer
    
    if confidence['action'] == 'low_confidence_warning':
        warning = f"\n\n---\n⚠️ **Low Confidence Warning** (Score: {confidence['score']:.0%})\n" \
                  f"The available documentation may not fully address this query. " \
                  f"Reasons: {', '.join(confidence['reasons'])}\n" \
                  f"Please verify with subject matter experts or additional sources."
        return answer + warning
    
    return answer


# ============================================================================
# 5. SELF-CONSISTENCY CHECKING (Optional - Cost Heavy)
# ============================================================================

def generate_with_self_consistency(query: str, context: str, 
                                    intent: 'QueryIntent', 
                                    num_samples: int = 3) -> Dict[str, Any]:
    """
    Generate multiple answers and check for consistency.
    
    Logic:
    1. Generate N answers with temperature > 0
    2. Compare answers for consistency
    3. Return majority answer or flag inconsistency
    
    Why it works:
    - If model is confident, different samples converge
    - Inconsistent samples indicate uncertainty
    - Can catch hallucinations that aren't reproducible
    
    Cost: N * normal_generation_cost (use sparingly)
    """
    if not ENABLE_SELF_CONSISTENCY:
        return None  # Signal to use normal generation
    
    try:
        bedrock = get_client('bedrock-runtime')
        system_prompt = get_intent_system_prompt(intent)
        
        samples = []
        
        for i in range(num_samples):
            response = bedrock.invoke_model(
                modelId=Config.GENERATION_MODEL,
                body=json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": Config.MAX_TOKENS,
                    "temperature": 0.4,  # Higher temp for diversity
                    "system": system_prompt,
                    "messages": [{"role": "user", "content": f"Context:\n{context}\n\nQuestion: {query}"}]
                })
            )
            
            answer = json.loads(response['body'].read())['content'][0]['text']
            samples.append(answer)
        
        # Check consistency - simple approach: compare key facts
        consistency_score = check_answer_consistency(samples)
        
        if consistency_score >= 0.8:
            # High consistency - return first (or most common) answer
            return {
                'answer': samples[0],
                'consistency_score': consistency_score,
                'num_samples': num_samples,
                'consistent': True
            }
        else:
            # Low consistency - return with warning
            return {
                'answer': samples[0],
                'consistency_score': consistency_score,
                'num_samples': num_samples,
                'consistent': False,
                'warning': "Multiple answer variations detected - verify critical facts"
            }
        
    except Exception as e:
        log_event('self_consistency_error', f"Self-consistency check failed: {e}")
        return None


def check_answer_consistency(samples: List[str]) -> float:
    """
    Check consistency between multiple answer samples.
    
    Simple approach: Extract key facts and compare overlap.
    """
    if len(samples) < 2:
        return 1.0
    
    # Extract technical terms from each sample
    term_sets = []
    for sample in samples:
        # Find technical identifiers, numbers, and key phrases
        terms = set(re.findall(r'\b[A-Z]{2,}[0-9]*\b', sample.upper()))
        terms.update(re.findall(r'\d+(?:\.\d+)?', sample))  # Numbers
        term_sets.append(terms)
    
    # Calculate pairwise overlap
    overlaps = []
    for i in range(len(term_sets)):
        for j in range(i + 1, len(term_sets)):
            if term_sets[i] and term_sets[j]:
                intersection = len(term_sets[i].intersection(term_sets[j]))
                union = len(term_sets[i].union(term_sets[j]))
                overlaps.append(intersection / union if union > 0 else 0)
    
    return sum(overlaps) / len(overlaps) if overlaps else 1.0


# ============================================================================
# RECIPROCAL RANK FUSION (Phase 2 Enhancement)
# ============================================================================

def reciprocal_rank_fusion(rankings: List[List[Dict[str, Any]]],
                           k: int = None,
                           query: str = None,
                           query_entities: List[str] = None,
                           related_entities: List[str] = None,
                           expected_content_type: str = None) -> List[Dict[str, Any]]:
    """
    ENHANCED Reciprocal Rank Fusion with Phase 2B Boost Multipliers.

    RRF is a proven technique for combining ranked results from different
    retrieval methods (semantic, keyword, etc.) without needing score calibration.

    Formula: RRF_score = sum(1 / (k + rank)) * boost_multiplier

    Boost Multipliers (Phase 2B Enhancement):
        - Entity Boost (3.0x): Chunks mentioning query entities
        - Relationship Boost (1.8x): Chunks with related entities
        - Type Boost (2.0x): Matching content type (code/doc/table)
        - Recency Boost (1.3x): Recently updated documents
        - Proposition Boost (1.5x): Proposition chunks (atomic facts)
        - Code Line Boost (1.4x): Full-line code chunks

    Args:
        rankings: List of ranked result lists (each sorted by score descending)
        k: RRF constant (default: Config.RRF_K = 60)
        query: Original query string for entity matching
        query_entities: List of entity qualified names from query
        related_entities: List of related entity names from DynamoDB traversal
        expected_content_type: Expected content type (code, documentation, table, etc.)

    Returns:
        Combined results sorted by boosted RRF score
    """
    if not Config.ENABLE_RRF_SCORING:
        # Fallback: return first ranking list
        return rankings[0] if rankings else []

    k = k or Config.RRF_K
    scores: Dict[str, Dict[str, Any]] = {}
    
    # Prepare entity sets for matching
    query_entity_set = set(e.upper() for e in (query_entities or []))
    related_entity_set = set(e.upper() for e in (related_entities or []))
    query_terms = set((query or '').upper().split()) if query else set()

    for ranking_idx, ranking in enumerate(rankings):
        for rank, doc in enumerate(ranking):
            # Use chunk_id or document_id as key
            doc_key = doc.get('chunk_id') or doc.get('document_id') or f"doc_{id(doc)}"

            if doc_key not in scores:
                scores[doc_key] = {
                    'doc': doc.copy(),
                    'rrf_score': 0.0,
                    'rank_contributions': [],
                    'boosts_applied': []
                }

            # RRF formula: 1 / (k + rank + 1)
            contribution = 1.0 / (k + rank + 1)
            scores[doc_key]['rrf_score'] += contribution
            scores[doc_key]['rank_contributions'].append({
                'ranking': ranking_idx,
                'rank': rank,
                'contribution': round(contribution, 6)
            })

    # Apply Phase 2B Boost Multipliers
    for doc_key, item in scores.items():
        doc = item['doc']
        boost_multiplier = 1.0
        boosts = []
        
        text_upper = (doc.get('text', '') or '').upper()
        metadata = doc.get('metadata', {}) or {}
        
        # 1. ENTITY BOOST (3.0x) - Chunks mentioning query entities directly
        if query_entity_set:
            entity_matches = sum(1 for e in query_entity_set if e in text_upper)
            if entity_matches > 0:
                boost_multiplier *= Config.ENTITY_BOOST
                boosts.append(f"entity:{entity_matches}")
        
        # Also check mentioned_identifiers field
        mentioned_ids = set(str(i).upper() for i in (doc.get('mentioned_identifiers') or []))
        if mentioned_ids & query_entity_set:
            boost_multiplier *= min(Config.ENTITY_BOOST, 2.0)  # Cap at 2x if already entity-boosted
            boosts.append("mentioned_id")
        
        # 2. RELATIONSHIP BOOST (1.8x) - Chunks with related entities from graph traversal
        if related_entity_set:
            related_matches = sum(1 for e in related_entity_set if e in text_upper)
            if related_matches > 0:
                boost_multiplier *= Config.RELATIONSHIP_BOOST
                boosts.append(f"related:{related_matches}")
        
        # 3. TYPE BOOST (2.0x) - Matching content type
        if expected_content_type:
            doc_type = (
                doc.get('chunk_type') or 
                metadata.get('content_type') or 
                doc.get('file_type', '')
            ).lower()
            
            if expected_content_type.lower() in doc_type:
                boost_multiplier *= Config.TYPE_BOOST
                boosts.append(f"type:{expected_content_type}")
            elif expected_content_type.lower() == 'code' and any(
                kw in text_upper for kw in ['COBOL', 'JCL', 'IDENTIFICATION DIVISION', 'PROGRAM-ID', '//JOB']
            ):
                boost_multiplier *= Config.TYPE_BOOST
                boosts.append("type:code_detected")
        
        # 4. RECENCY BOOST (1.3x) - Recently updated documents
        updated_at = doc.get('updated_at') or metadata.get('updated_at') or metadata.get('indexed_at')
        if updated_at:
            try:
                if isinstance(updated_at, str):
                    updated_dt = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
                else:
                    updated_dt = updated_at
                # Boost if updated within last 30 days
                if (datetime.now(timezone.utc) - updated_dt).days <= 30:
                    boost_multiplier *= Config.RECENCY_BOOST
                    boosts.append("recency")
            except (ValueError, TypeError):
                pass
        
        # 5. PROPOSITION BOOST (1.5x) - Atomic fact chunks
        is_proposition = doc.get('is_proposition') or metadata.get('is_proposition', False)
        if is_proposition:
            boost_multiplier *= Config.PROPOSITION_BOOST
            boosts.append("proposition")
        
        # 6. CODE LINE BOOST (1.4x) - Full-line code chunks with line numbers
        has_line_numbers = doc.get('line_start') or doc.get('line_end') or metadata.get('line_start')
        if has_line_numbers:
            boost_multiplier *= Config.CODE_LINE_BOOST
            boosts.append("code_line")
        
        # Apply boost to RRF score
        item['rrf_score'] *= boost_multiplier
        item['boost_multiplier'] = round(boost_multiplier, 3)
        item['boosts_applied'] = boosts

    # Sort by boosted RRF score
    sorted_results = sorted(scores.values(), key=lambda x: x['rrf_score'], reverse=True)

    # Flatten and add RRF metadata
    results = []
    for item in sorted_results:
        doc = item['doc']
        doc['rrf_score'] = round(item['rrf_score'], 6)
        doc['rrf_rank_contributions'] = item['rank_contributions']
        doc['rrf_boost_multiplier'] = item.get('boost_multiplier', 1.0)
        doc['rrf_boosts_applied'] = item.get('boosts_applied', [])
        results.append(doc)

    # Log boost statistics
    boosted_count = sum(1 for r in results if r.get('rrf_boost_multiplier', 1.0) > 1.0)
    log_event('rrf_fusion_complete',
              f"Fused {len(rankings)} rankings into {len(results)} results ({boosted_count} boosted)",
              top_rrf_score=results[0]['rrf_score'] if results else 0,
              boosted_count=boosted_count)

    emit_metric('RRFResultCount', len(results))
    emit_metric('RRFBoostedCount', boosted_count)

    return results


# ============================================================================
# PHASE 2B: ENTITY LOOKUP HELPERS (DynamoDB Integration)
# ============================================================================

_dynamodb_resource = None


def get_dynamodb_resource():
    """Get or create DynamoDB resource for entity lookup."""
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource('dynamodb', region_name=Config.AWS_REGION)
    return _dynamodb_resource


def lookup_entity_context(entity_names: List[str]) -> Dict[str, Any]:
    """
    Lookup entity context from DynamoDB Entity Registry with multi-hop traversal.
    Performs up to IMPACT_MAX_HOPS hops through the relationship graph to find
    transitively related entities for comprehensive RRF boosting.
    """
    if not Config.ENABLE_ENTITY_LOOKUP or not entity_names:
        return {'entities': [], 'related': []}
    
    try:
        dynamodb = get_dynamodb_resource()
        table = dynamodb.Table(Config.ENTITY_REGISTRY_TABLE)
        
        entities = []
        for name in entity_names[:20]:  # Limit to 20 entities
            # Try qualified name lookup
            try:
                response = table.get_item(Key={'entity_key': name})
                if 'Item' in response:
                    entities.append(response['Item'])
            except Exception:
                pass
        
        # Multi-hop relationship traversal (2-3 hops per blueprint)
        max_hops = min(Config.IMPACT_MAX_HOPS, 3)
        rel_table = dynamodb.Table(Config.RELATIONSHIPS_TABLE)
        
        all_related: set = set()
        frontier = {e.get('entity_key', '') for e in entities if e.get('entity_key')}
        visited: set = set()
        
        for hop in range(max_hops):
            next_frontier: set = set()
            for entity_key in frontier:
                if entity_key in visited or not entity_key:
                    continue
                visited.add(entity_key)
                try:
                    response = rel_table.query(
                        KeyConditionExpression=Key('source_entity').eq(entity_key),
                        Limit=10
                    )
                    for item in response.get('Items', []):
                        target = item.get('target_entity', '')
                        if target and target not in visited:
                            all_related.add(target)
                            next_frontier.add(target)
                except Exception:
                    pass
            
            frontier = next_frontier
            if not frontier:
                break  # No more entities to explore
        
        return {
            'entities': entities,
            'related': list(all_related),
            'hops_completed': min(len(visited), max_hops)
        }
    
    except Exception as e:
        logger.warning(f"Entity lookup failed: {e}")
        return {'entities': [], 'related': []}


def extract_entities_from_query(query: str) -> List[str]:
    """
    Extract potential entity names from a query string.
    Returns list of entity qualified names to lookup.
    """
    entities = []
    query_upper = query.upper()
    
    # COBOL program patterns
    for match in re.finditer(r'\b[A-Z][A-Z0-9]{3,7}\b', query_upper):
        name = match.group(0)
        if not name in ('WHERE', 'WHAT', 'WHEN', 'WITH', 'FROM', 'INTO', 'WHICH', 'THAT', 'THIS', 'DOES', 'HAVE', 'ABOUT'):
            entities.append(f"COBOL:{name}")
            entities.append(f"JOB:{name}")
            entities.append(f"TABLE:{name}")
    
    # Table patterns (MY*, BDD*)
    for match in re.finditer(r'\b(MY[A-Z]{2,6}\d?|BDD[A-Z0-9]{2,8})\b', query_upper):
        entities.append(f"TABLE:{match.group(1)}")
    
    # Field patterns
    for match in re.finditer(r'\b(DABI[A-Z]{2,6}|OPTR[A-Z]{2,5})\b', query_upper):
        entities.append(f"FIELD:{match.group(1)}")
    
    return list(set(entities))


# ============================================================================
# PHASE 2C: CODE GENERATION PIPELINE
# ============================================================================

class TaskType(Enum):
    """Task types for 6-way router in code generation."""
    Q_AND_A = "q_and_a"  # Factual questions
    EXPLAIN_CODE = "explain_code"  # Code explanation
    EXPLAIN_RULE = "explain_rule"  # Business rule explanation
    GENERATE_CODE = "generate_code"  # New code generation
    MODIFY_CODE = "modify_code"  # Code modification
    DEBUG_ISSUE = "debug_issue"  # Debug/troubleshoot


def classify_task_type(query: str) -> TaskType:
    """
    Classify query into one of 6 task types using keyword patterns.
    Uses Haiku for ambiguous cases.
    """
    query_lower = query.lower()
    
    # Pattern-based classification
    if any(kw in query_lower for kw in ['what is', 'what are', 'describe', 'list', 'how many', 'which']):
        return TaskType.Q_AND_A
    
    if any(kw in query_lower for kw in ['explain code', 'what does this code', 'walk through', 'trace', 'step by step']):
        return TaskType.EXPLAIN_CODE
    
    if any(kw in query_lower for kw in ['explain rule', 'business rule', 'why is', 'logic behind', 'requirement']):
        return TaskType.EXPLAIN_RULE
    
    if any(kw in query_lower for kw in ['write', 'create', 'generate', 'implement', 'build', 'add new']):
        return TaskType.GENERATE_CODE
    
    if any(kw in query_lower for kw in ['modify', 'change', 'update', 'fix', 'refactor', 'enhance', 'edit']):
        return TaskType.MODIFY_CODE
    
    if any(kw in query_lower for kw in ['debug', 'error', 'issue', 'problem', 'not working', 'fails', 'exception', 'abend']):
        return TaskType.DEBUG_ISSUE
    
    # Default to Q&A for ambiguous queries
    return TaskType.Q_AND_A


def plan_code_generation(query: str, task_type: TaskType, context_chunks: List[Dict]) -> Dict[str, Any]:
    """
    Step 1 of code generation pipeline: Plan the generation using Haiku.
    Returns a structured plan with steps, required patterns, and constraints.
    """
    if not Config.ENABLE_CODE_GENERATION:
        return {'enabled': False}
    
    # Extract relevant code context from chunks
    code_context = []
    for chunk in context_chunks[:5]:
        text = chunk.get('text', '') or chunk.get('text_display', '')
        if 'COBOL' in text.upper() or 'JCL' in text.upper() or 'IDENTIFICATION DIVISION' in text.upper():
            code_context.append(text[:2000])
    
    prompt = f"""Analyze this {task_type.value} request and create a generation plan.

REQUEST: {query}

RELEVANT CODE CONTEXT:
{chr(10).join(code_context[:3]) if code_context else 'No code context available'}

Provide a JSON plan with:
1. "steps": Array of generation steps
2. "patterns_needed": Code patterns to follow (copybooks, naming conventions)
3. "constraints": Technical constraints to respect
4. "risk_level": "low", "medium", or "high"
5. "confidence": 0.0-1.0

Respond with ONLY valid JSON."""

    try:
        bedrock = get_client('bedrock-runtime')
        response = bedrock.invoke_model(
            modelId=Config.HAIKU_MODEL,
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1000,
                "temperature": 0.1,
                "messages": [{"role": "user", "content": prompt}]
            })
        )
        
        result = json.loads(response['body'].read())
        content = result.get('content', [{}])[0].get('text', '{}')
        
        # Parse JSON from response
        try:
            # Handle potential markdown code blocks
            if '```json' in content:
                content = content.split('```json')[1].split('```')[0]
            elif '```' in content:
                content = content.split('```')[1].split('```')[0]
            
            plan = json.loads(content.strip())
            plan['task_type'] = task_type.value
            plan['enabled'] = True
            return plan
        except json.JSONDecodeError:
            return {
                'enabled': True,
                'task_type': task_type.value,
                'steps': ['Analyze request', 'Generate code', 'Review output'],
                'patterns_needed': [],
                'constraints': [],
                'risk_level': 'medium',
                'confidence': 0.5
            }
            
    except Exception as e:
        logger.warning(f"Code generation planning failed: {e}")
        return {
            'enabled': True,
            'error': str(e),
            'task_type': task_type.value,
            'steps': ['Direct generation'],
            'patterns_needed': [],
            'constraints': [],
            'risk_level': 'medium',
            'confidence': 0.3
        }


def generate_code(query: str, plan: Dict[str, Any], context_chunks: List[Dict]) -> Dict[str, Any]:
    """
    Step 2 of code generation pipeline: Generate code using Sonnet.
    Returns generated code with explanations and confidence score.
    """
    if not Config.ENABLE_CODE_GENERATION or not plan.get('enabled', False):
        return {'generated': False}
    
    task_type = plan.get('task_type', 'q_and_a')
    
    # Build context from chunks
    context_text = ""
    for chunk in context_chunks[:8]:
        text = chunk.get('text_display', chunk.get('text', ''))[:1500]
        source = chunk.get('file_name', chunk.get('document_title', 'Unknown'))
        context_text += f"\n--- Source: {source} ---\n{text}\n"
    
    # Task-specific prompts
    if task_type == 'generate_code':
        system_prompt = """You are a COBOL/JCL mainframe code generator. Generate production-quality code that:
1. Follows COBOL-85 or Enterprise COBOL standards
2. Uses proper copybooks when referenced
3. Includes inline comments explaining logic
4. Handles error conditions appropriately
5. Follows the patterns shown in the context"""
    elif task_type == 'modify_code':
        system_prompt = """You are a COBOL/JCL code modifier. Make targeted changes that:
1. Preserve existing program structure
2. Follow established patterns in the codebase
3. Add clear comments for modifications
4. Minimize impact on surrounding code"""
    elif task_type == 'debug_issue':
        system_prompt = """You are a mainframe debugging expert. Analyze the issue and:
1. Identify the root cause
2. Suggest specific code fixes
3. Explain the debugging approach
4. Provide test scenarios"""
    else:
        system_prompt = """You are a mainframe code expert. Provide clear, accurate assistance with COBOL, JCL, and mainframe systems."""
    
    user_prompt = f"""Plan: {json.dumps(plan.get('steps', []))}
Constraints: {json.dumps(plan.get('constraints', []))}

CONTEXT:
{context_text}

REQUEST: {query}

Provide your response with:
1. Generated code (if applicable) in proper format
2. Clear explanations
3. Any assumptions made"""

    try:
        bedrock = get_client('bedrock-runtime')
        response = bedrock.invoke_model(
            modelId=Config.CODE_GEN_MODEL,
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": Config.CODE_GEN_MAX_TOKENS,
                "temperature": Config.CODE_GEN_TEMPERATURE,
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_prompt}]
            })
        )
        
        result = json.loads(response['body'].read())
        content = result.get('content', [{}])[0].get('text', '')
        
        return {
            'generated': True,
            'task_type': task_type,
            'content': content,
            'plan': plan,
            'context_chunks_used': len(context_chunks),
            'model': Config.CODE_GEN_MODEL
        }
        
    except Exception as e:
        logger.error(f"Code generation failed: {e}")
        return {
            'generated': False,
            'error': str(e),
            'task_type': task_type
        }


def review_generated_code(generated: Dict[str, Any], query: str) -> Dict[str, Any]:
    """
    Step 3 of code generation pipeline: Review with Haiku as judge.
    Validates code quality, correctness, and alignment with request.
    """
    if not generated.get('generated', False):
        return {'reviewed': False, 'reason': 'No code to review'}
    
    content = generated.get('content', '')
    if len(content) < 50:
        return {'reviewed': True, 'score': 0.5, 'feedback': 'Content too short for meaningful review'}
    
    prompt = f"""Review this generated code/response for a mainframe development request.

ORIGINAL REQUEST: {query}

GENERATED RESPONSE:
{content[:4000]}

Evaluate on these criteria (score 0-10 each):
1. correctness: Does it correctly address the request?
2. completeness: Are all aspects covered?
3. quality: Is the code/response well-structured?
4. safety: Are there any risky patterns or potential issues?

Respond with JSON:
{{
  "correctness": <0-10>,
  "completeness": <0-10>,
  "quality": <0-10>,
  "safety": <0-10>,
  "overall_score": <0-1.0>,
  "issues": ["list of any issues"],
  "suggestions": ["improvement suggestions"],
  "approved": true/false
}}"""

    try:
        bedrock = get_client('bedrock-runtime')
        response = bedrock.invoke_model(
            modelId=Config.HAIKU_MODEL,
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 800,
                "temperature": 0.0,
                "messages": [{"role": "user", "content": prompt}]
            })
        )
        
        result = json.loads(response['body'].read())
        review_text = result.get('content', [{}])[0].get('text', '{}')
        
        # Parse review JSON
        try:
            if '```json' in review_text:
                review_text = review_text.split('```json')[1].split('```')[0]
            elif '```' in review_text:
                review_text = review_text.split('```')[1].split('```')[0]
            
            review = json.loads(review_text.strip())
            review['reviewed'] = True
            return review
        except json.JSONDecodeError:
            return {
                'reviewed': True,
                'overall_score': 0.6,
                'approved': True,
                'feedback': 'Review parsing failed, default approval'
            }
            
    except Exception as e:
        logger.warning(f"Code review failed: {e}")
        return {
            'reviewed': False,
            'error': str(e),
            'approved': True  # Default approve on review failure
        }


def code_generation_pipeline(query: str, context_chunks: List[Dict]) -> Dict[str, Any]:
    """
    Main entry point for Phase 2C code generation.
    Orchestrates: Classify -> Plan -> Generate -> Review
    """
    if not Config.ENABLE_CODE_GENERATION:
        return {'enabled': False}
    
    start_time = time.time()
    
    # Step 1: Classify task type
    task_type = classify_task_type(query)
    log_event('code_gen_classify', f'Classified as {task_type.value}', task_type=task_type.value)
    
    # Skip code generation for simple Q&A
    if task_type == TaskType.Q_AND_A:
        return {
            'enabled': True,
            'task_type': task_type.value,
            'skipped': True,
            'reason': 'Simple Q&A does not require code generation pipeline'
        }
    
    # Step 2: Plan generation
    plan = plan_code_generation(query, task_type, context_chunks)
    
    # Step 3: Generate code
    generated = generate_code(query, plan, context_chunks)
    
    # Step 4: Review output
    review = review_generated_code(generated, query)
    
    processing_time_ms = int((time.time() - start_time) * 1000)
    
    return {
        'enabled': True,
        'task_type': task_type.value,
        'plan': plan,
        'generated': generated.get('generated', False),
        'content': generated.get('content', ''),
        'review': review,
        'approved': review.get('approved', True),
        'overall_score': review.get('overall_score', 0.0),
        'processing_ms': processing_time_ms
    }


# ============================================================================
# PHASE 3: IMPACT ANALYSIS
# ============================================================================

def traverse_relationships(entity_key: str, max_hops: int = None, max_results: int = None) -> List[Dict[str, Any]]:
    """
    Traverse entity relationships in DynamoDB to find affected entities.
    Returns list of affected entities with hop distance.
    """
    if not Config.ENABLE_IMPACT_ANALYSIS:
        return []
    
    max_hops = max_hops or Config.IMPACT_MAX_HOPS
    max_results = max_results or Config.IMPACT_MAX_RESULTS
    
    try:
        dynamodb = get_dynamodb_resource()
        rel_table = dynamodb.Table(Config.RELATIONSHIPS_TABLE)
        
        visited = set()
        affected = []
        current_level = [entity_key]
        
        for hop in range(max_hops):
            if len(affected) >= max_results:
                break
                
            next_level = []
            
            for source in current_level:
                if source in visited:
                    continue
                visited.add(source)
                
                # Query relationships where this entity is the source
                try:
                    response = rel_table.query(
                        KeyConditionExpression=Key('source_entity').eq(source),
                        Limit=20
                    )
                    
                    for item in response.get('Items', []):
                        target = item.get('target_entity', '')
                        if target and target not in visited:
                            affected.append({
                                'entity': target,
                                'relationship': item.get('relationship_type', 'unknown'),
                                'from_entity': source,
                                'hop': hop + 1,
                                'confidence': float(item.get('confidence', 1.0))
                            })
                            next_level.append(target)
                            
                            if len(affected) >= max_results:
                                break
                                
                except Exception as e:
                    logger.warning(f"Relationship query failed for {source}: {e}")
            
            current_level = next_level
            if not current_level:
                break
        
        log_event('impact_traversal_complete', 
                 f'Found {len(affected)} affected entities in {hop + 1} hops',
                 root_entity=entity_key, affected_count=len(affected))
        
        return affected
        
    except Exception as e:
        logger.warning(f"Relationship traversal failed: {e}")
        return []


def assess_risk_level(affected_entities: List[Dict[str, Any]], root_entity: str) -> Dict[str, Any]:
    """
    Assess the risk level of a proposed change based on affected entities.
    """
    if not affected_entities:
        return {
            'risk_level': 'low',
            'score': 0.1,
            'factors': ['No downstream dependencies detected']
        }
    
    factors = []
    risk_score = 0.0
    
    # Factor 1: Number of affected entities
    count = len(affected_entities)
    if count > 20:
        risk_score += 0.4
        factors.append(f'High blast radius: {count} affected entities')
    elif count > 10:
        risk_score += 0.25
        factors.append(f'Medium blast radius: {count} affected entities')
    elif count > 5:
        risk_score += 0.1
        factors.append(f'Limited blast radius: {count} affected entities')
    
    # Factor 2: Hop distance (closer = higher risk)
    hop_1_count = sum(1 for e in affected_entities if e.get('hop', 0) == 1)
    if hop_1_count > 5:
        risk_score += 0.2
        factors.append(f'{hop_1_count} direct dependencies')
    
    # Factor 3: Entity types
    entity_types = set(e.get('entity', '').split(':')[0] for e in affected_entities)
    if 'TABLE' in entity_types:
        risk_score += 0.15
        factors.append('Database table dependencies detected')
    if 'JOB' in entity_types:
        risk_score += 0.1
        factors.append('JCL job dependencies detected')
    
    # Determine risk level
    if risk_score >= 0.6:
        risk_level = 'high'
    elif risk_score >= 0.3:
        risk_level = 'medium'
    else:
        risk_level = 'low'
    
    return {
        'risk_level': risk_level,
        'score': round(min(risk_score, 1.0), 2),
        'factors': factors,
        'affected_count': count,
        'direct_dependencies': hop_1_count
    }


def generate_impact_report(root_entity: str, affected_entities: List[Dict[str, Any]], 
                          risk_assessment: Dict[str, Any]) -> str:
    """
    Generate a human-readable impact analysis report.
    """
    report_lines = [
        f"## Impact Analysis Report",
        f"**Entity Under Change:** `{root_entity}`",
        f"**Risk Level:** {risk_assessment.get('risk_level', 'unknown').upper()}",
        f"**Risk Score:** {risk_assessment.get('score', 0):.2f}",
        "",
        "### Risk Factors:",
    ]
    
    for factor in risk_assessment.get('factors', []):
        report_lines.append(f"- {factor}")
    
    if affected_entities:
        report_lines.extend([
            "",
            "### Affected Entities:",
            ""
        ])
        
        # Group by hop
        by_hop = {}
        for entity in affected_entities:
            hop = entity.get('hop', 0)
            if hop not in by_hop:
                by_hop[hop] = []
            by_hop[hop].append(entity)
        
        for hop in sorted(by_hop.keys()):
            report_lines.append(f"**Hop {hop} (Direct Dependencies):** " if hop == 1 else f"**Hop {hop}:**")
            for entity in by_hop[hop][:10]:  # Limit to 10 per hop
                report_lines.append(f"- `{entity.get('entity', 'unknown')}` via {entity.get('relationship', 'unknown')}")
            if len(by_hop[hop]) > 10:
                report_lines.append(f"- ... and {len(by_hop[hop]) - 10} more")
    
    report_lines.extend([
        "",
        "### Recommendations:",
    ])
    
    if risk_assessment.get('risk_level') == 'high':
        report_lines.extend([
            "- **Requires thorough testing** in all affected systems",
            "- **Schedule during maintenance window** if possible",
            "- **Notify downstream teams** before deployment",
            "- **Prepare rollback plan** before proceeding"
        ])
    elif risk_assessment.get('risk_level') == 'medium':
        report_lines.extend([
            "- **Review affected dependencies** before deployment",
            "- **Test key integration points**",
            "- **Monitor for issues** post-deployment"
        ])
    else:
        report_lines.extend([
            "- Standard testing procedures should be sufficient",
            "- Monitor application logs post-deployment"
        ])
    
    return "\n".join(report_lines)


def impact_analysis_pipeline(entity_name: str, change_type: str = "modify") -> Dict[str, Any]:
    """
    Main entry point for Phase 3 impact analysis.
    Analyzes the impact of changing a specific entity.
    """
    if not Config.ENABLE_IMPACT_ANALYSIS:
        return {'enabled': False}
    
    start_time = time.time()
    
    # Normalize entity key
    entity_key = entity_name.upper()
    if ':' not in entity_key:
        # Try to infer entity type
        if entity_key.startswith('MY') or entity_key.startswith('BDD'):
            entity_key = f"TABLE:{entity_key}"
        elif entity_key.startswith('M') and len(entity_key) <= 8:
            entity_key = f"COBOL:{entity_key}"
        else:
            entity_key = f"COBOL:{entity_key}"
    
    # Traverse relationships
    affected = traverse_relationships(entity_key)
    
    # Assess risk
    risk = assess_risk_level(affected, entity_key)
    
    # Generate report
    report = generate_impact_report(entity_key, affected, risk)
    
    processing_time_ms = int((time.time() - start_time) * 1000)
    
    return {
        'enabled': True,
        'entity': entity_key,
        'change_type': change_type,
        'affected_entities': affected,
        'affected_count': len(affected),
        'risk_assessment': risk,
        'report': report,
        'processing_ms': processing_time_ms
    }


def semantic_rerank(query: str, results: List[Dict[str, Any]], 
                    query_requirements: Dict[str, Any] = None,
                    top_k: int = None) -> List[Dict[str, Any]]:
    """
    GENERIC semantic re-ranking based on content matching.
    
    NO HARDCODED DOCUMENT CATEGORIES - uses dynamic matching of:
    1. Query terms vs document terms
    2. Query topics vs document topics  
    3. Content type alignment with query intent
    4. Entity/term overlap scoring
    
    This is a TRUE content-based matching approach.
    """
    if not results:
        return results
    
    # Get query requirements if not provided
    if query_requirements is None:
        query_requirements = extract_query_requirements(query)
    
    query_lower = query.lower()
    query_terms = set(query_lower.split())
    
    # Extract query characteristics
    required_topics = set([t.lower() for t in query_requirements.get('required_topics', [])])
    key_terms = set([t.lower() for t in query_requirements.get('key_terms', [])])
    expected_types = set([t.lower() for t in query_requirements.get('expected_doc_types', [])])
    importance_indicators = set([t.lower() for t in query_requirements.get('importance_indicators', [])])
    
    reranked = []
    
    for result in results:
        text_lower = result['text'].lower()
        base_score = result.get('score', 0.5)
        rerank_score = base_score
        boost_reasons = []
        
        # ------------------------------------------------------------------
        # Factor 1: Term Density (BM25-style)
        # ------------------------------------------------------------------
        term_matches = sum(1 for term in query_terms if term in text_lower)
        term_density = term_matches / len(query_terms) if query_terms else 0
        rerank_score += term_density * 0.25
        
        # ------------------------------------------------------------------
        # Factor 2: Key Term Matching (extracted from query)
        # These are the important technical terms the LLM identified
        # ------------------------------------------------------------------
        doc_text = text_lower + ' ' + ' '.join(result.get('entities', [])).lower()
        key_term_matches = sum(1 for term in key_terms if term in doc_text)
        if key_term_matches > 0:
            key_term_boost = min(key_term_matches * 0.15, 0.5)  # Cap at 0.5
            rerank_score += key_term_boost
            boost_reasons.append(f"key_terms:{key_term_matches}")
        
        # ------------------------------------------------------------------
        # Factor 3: Topic Overlap (semantic matching)
        # Match query topics against document's primary_topics
        # ------------------------------------------------------------------
        doc_topics = set([t.lower() for t in result.get('primary_topics', [])])
        doc_topics.update([t.lower() for t in result.get('topics', [])])
        topic_overlap = len(required_topics.intersection(doc_topics))
        if topic_overlap > 0:
            topic_boost = min(topic_overlap * 0.2, 0.6)
            rerank_score += topic_boost
            boost_reasons.append(f"topics:{topic_overlap}")
        
        # ------------------------------------------------------------------
        # Factor 4: Document Type Alignment
        # Match expected doc types against inferred doc type
        # ------------------------------------------------------------------
        doc_type = result.get('doc_type_inferred', result.get('content_type', 'unknown')).lower()
        if doc_type in expected_types:
            rerank_score += 0.2
            boost_reasons.append(f"type_match:{doc_type}")
        
        # ------------------------------------------------------------------
        # Factor 5: Domain Indicator Matching
        # These are specific identifiers that strongly indicate relevance
        # ------------------------------------------------------------------
        doc_indicators = set([i.lower() for i in result.get('domain_indicators', [])])
        doc_indicators.update([i.lower() for i in result.get('key_terms', [])])
        indicator_overlap = len(importance_indicators.intersection(doc_indicators))
        if indicator_overlap > 0:
            indicator_boost = min(indicator_overlap * 0.25, 0.75)
            rerank_score += indicator_boost
            boost_reasons.append(f"indicators:{indicator_overlap}")
        
        # ------------------------------------------------------------------
        # Factor 6: Entity Overlap (normalized)
        # Match entities extracted from query vs document
        # ------------------------------------------------------------------
        doc_entities = set([e.lower() for e in result.get('entities', [])])
        query_entities = key_terms.union(importance_indicators)
        entity_overlap = len(doc_entities.intersection(query_entities))
        if entity_overlap > 0:
            entity_boost = min(entity_overlap * 0.1, 0.4)
            rerank_score += entity_boost
            boost_reasons.append(f"entities:{entity_overlap}")
        
        # ------------------------------------------------------------------
        # Factor 7: Bigram/Phrase Matching
        # ------------------------------------------------------------------
        query_words = list(query_terms)
        if len(query_words) > 1:
            bigrams = [f"{query_words[i]} {query_words[i+1]}" for i in range(len(query_words) - 1)]
            bigram_matches = sum(1 for bg in bigrams if bg in text_lower)
            if bigram_matches > 0:
                rerank_score += bigram_matches * 0.15
                boost_reasons.append(f"phrases:{bigram_matches}")
        
        result['rerank_score'] = round(rerank_score, 4)
        result['original_score'] = base_score
        result['boost_reasons'] = boost_reasons
        reranked.append(result)
    
    # Sort by rerank score
    reranked.sort(key=lambda x: x['rerank_score'], reverse=True)
    
    log_event('semantic_rerank_complete', 
              f"Reranked {len(reranked)} results, "
              f"top boosted: {reranked[0].get('boost_reasons', []) if reranked else 'none'}")
    
    # Return top_k
    if top_k:
        return reranked[:top_k]
    
    return reranked


def ensure_document_diversity(results: List[Dict[str, Any]], 
                              min_unique_docs: int = 3,
                              top_k: int = None) -> List[Dict[str, Any]]:
    """
    Ensure result set includes diverse documents.
    
    If query likely needs multiple documents, ensure we don't return
    chunks from just one document.
    """
    if not results or len(results) < min_unique_docs:
        return results
    
    seen_docs = set()
    diverse_results = []
    backfill = []
    
    # First pass: take best chunk from each unique document
    for result in results:
        doc_id = result.get('document_id', result.get('file_name', 'unknown'))
        
        if doc_id not in seen_docs:
            diverse_results.append(result)
            seen_docs.add(doc_id)
        else:
            backfill.append(result)
        
        # Stop if we have enough unique docs
        if len(seen_docs) >= min_unique_docs and len(diverse_results) >= (top_k or len(results)) // 2:
            break
    
    # Backfill with remaining high-scoring chunks
    remaining_slots = (top_k or len(results)) - len(diverse_results)
    if remaining_slots > 0:
        diverse_results.extend(backfill[:remaining_slots])
    
    log_event('diversity_applied', 
              f"Ensured {len(seen_docs)} unique docs in top {len(diverse_results)} results")
    
    return diverse_results


# ============================================================================
# PERSONA KNOWLEDGE WEIGHTS (Task 3.2)
# ============================================================================

# Mapping of knowledge weight categories to chunk attributes
KNOWLEDGE_WEIGHT_MAPPINGS = {
    # Code-related
    'cobol_references': {'content_types': ['code', 'copybook', 'cobol'], 'topics': ['cobol', 'copybook', 'working-storage', 'linkage']},
    'jcl_procedures': {'content_types': ['jcl', 'proc', 'job'], 'topics': ['jcl', 'job', 'proc', 'step', 'dd']},
    'data_structures': {'content_types': ['copybook', 'data'], 'topics': ['data', 'structure', 'record', 'field', 'layout']},
    'error_handling': {'content_types': ['code'], 'topics': ['error', 'abend', 'exception', 'return code', 'condition']},
    
    # Documentation-related
    'technical_docs': {'content_types': ['technical', 'specification', 'design'], 'topics': ['specification', 'design', 'architecture']},
    'user_guides': {'content_types': ['guide', 'manual', 'user'], 'topics': ['guide', 'how-to', 'procedure', 'steps']},
    'security_docs': {'content_types': ['security'], 'topics': ['security', 'tss', 'racf', 'authorization', 'access']},
    'compliance_docs': {'content_types': ['compliance', 'audit', 'policy'], 'topics': ['compliance', 'audit', 'policy', 'regulation', 'cdic']},
    
    # System-specific
    'cics_docs': {'content_types': ['cics'], 'topics': ['cics', 'transaction', 'bms', 'map']},
    'db2_docs': {'content_types': ['db2', 'sql'], 'topics': ['db2', 'sql', 'table', 'query']},
    'batch_processing': {'content_types': ['batch', 'jcl'], 'topics': ['batch', 'job', 'schedule', 'twsz']},
    'online_processing': {'content_types': ['online', 'cics'], 'topics': ['online', 'cics', 'transaction', 'screen']},
}


def apply_knowledge_weights(chunks: List[Dict[str, Any]], 
                            knowledge_weights: Dict[str, float]) -> List[Dict[str, Any]]:
    """
    Apply persona-specific knowledge weights to boost relevant content types.
    
    Persona knowledge_weights dict maps categories to weight multipliers:
    {
        "cobol_references": 1.5,    # Boost COBOL code/docs by 50%
        "data_structures": 1.3,     # Boost data structure docs by 30%
        "security_docs": 1.2,       # Boost security docs by 20%
    }
    
    Weight > 1.0 = boost, Weight < 1.0 = demote, Weight = 1.0 = no change
    
    Args:
        chunks: Retrieved chunks with scores
        knowledge_weights: Category -> weight multiplier mapping
    
    Returns:
        Chunks with adjusted scores based on knowledge weights
    """
    if not chunks or not knowledge_weights:
        return chunks
    
    weighted_chunks = []
    total_boost = 0.0
    boosted_count = 0
    
    for chunk in chunks:
        chunk_copy = chunk.copy()
        base_score = chunk_copy.get('rerank_score', chunk_copy.get('score', 0.5))
        weight_multiplier = 1.0
        applied_weights = []
        
        # Get chunk attributes for matching
        content_type = (chunk_copy.get('content_type') or '').lower()
        chunk_type = (chunk_copy.get('chunk_type') or '').lower()
        topics = [t.lower() for t in (chunk_copy.get('topics') or [])]
        entities = [e.lower() for e in (chunk_copy.get('entities') or [])]
        text_lower = chunk_copy.get('text', '').lower()[:500]  # Sample first 500 chars
        
        # Check each knowledge weight category
        for category, weight in knowledge_weights.items():
            if weight == 1.0:
                continue
                
            mapping = KNOWLEDGE_WEIGHT_MAPPINGS.get(category, {})
            match_content_types = mapping.get('content_types', [])
            match_topics = mapping.get('topics', [])
            
            matches = False
            
            # Check content type match
            if match_content_types:
                if any(ct in content_type or ct in chunk_type for ct in match_content_types):
                    matches = True
            
            # Check topic match
            if match_topics and not matches:
                if any(t in topics for t in match_topics):
                    matches = True
                # Also check if topic keywords appear in text
                elif any(t in text_lower for t in match_topics):
                    matches = True
            
            # Check entity match for specific categories
            if category in ['cobol_references', 'jcl_procedures'] and not matches:
                # Code identifiers often appear in entities
                if entities and any(e.isupper() and len(e) >= 3 for e in entities):
                    matches = True
            
            if matches:
                weight_multiplier *= weight
                applied_weights.append(f"{category}:{weight}")
        
        # Apply combined weight
        if weight_multiplier != 1.0:
            new_score = base_score * weight_multiplier
            # Cap the boost to prevent runaway scores
            new_score = min(new_score, base_score * 2.0)  # Max 2x boost
            new_score = max(new_score, base_score * 0.5)  # Min 0.5x 
            
            chunk_copy['weighted_score'] = round(new_score, 4)
            chunk_copy['weight_multiplier'] = round(weight_multiplier, 3)
            chunk_copy['applied_weights'] = applied_weights
            chunk_copy['rerank_score'] = new_score  # Update rerank_score for sorting
            
            total_boost += (new_score - base_score)
            boosted_count += 1
        else:
            chunk_copy['weighted_score'] = base_score
            chunk_copy['weight_multiplier'] = 1.0
        
        weighted_chunks.append(chunk_copy)
    
    # Re-sort by weighted score
    weighted_chunks.sort(key=lambda x: x.get('weighted_score', x.get('rerank_score', 0)), reverse=True)
    
    if boosted_count > 0:
        log_event('knowledge_weights_applied', 
                  f"Applied weights to {boosted_count}/{len(chunks)} chunks, "
                  f"avg boost: {total_boost/boosted_count:.3f}" if boosted_count else "0",
                  categories=list(knowledge_weights.keys()))
    
    return weighted_chunks


# ============================================================================
# HYBRID SEARCH ENGINE (KNN + BM25)
# ============================================================================

def _build_filter_clauses(filters: Dict[str, Any]) -> Dict[str, Any]:
    """Build OpenSearch bool filter clauses from filter dict."""
    if not filters:
        return {}

    filter_clauses = []
    must_not_clauses = []

    for key, value in filters.items():
        if value is None:
            continue

        # Support negation filters like shf_segment_ne
        if key.endswith('_ne'):
            field = key[:-3]
            clause = {"terms": {field: value}} if isinstance(value, list) else {"term": {field: value}}
            must_not_clauses.append(clause)
            continue

        clause = {"terms": {key: value}} if isinstance(value, list) else {"term": {key: value}}
        filter_clauses.append(clause)

    bool_filters: Dict[str, Any] = {}
    if filter_clauses:
        bool_filters["filter"] = filter_clauses
    if must_not_clauses:
        bool_filters["must_not"] = must_not_clauses

    return bool_filters


def merge_filters(base: Dict[str, Any], extra: Dict[str, Any]) -> Dict[str, Any]:
    """Merge filters with conservative narrowing for list values."""
    if not extra:
        return base

    merged = base.copy() if base else {}
    for key, value in extra.items():
        if key not in merged or merged[key] is None:
            merged[key] = value
            continue

        existing = merged[key]
        if isinstance(existing, list) and isinstance(value, list):
            intersection = [v for v in existing if v in value]
            merged[key] = intersection if intersection else value
        elif isinstance(existing, list):
            merged[key] = [v for v in existing if v == value] or [value]
        elif isinstance(value, list):
            merged[key] = [v for v in value if v == existing] or [existing]
        else:
            merged[key] = value

    return merged

def execute_hybrid_search(query_text: str,
                          query_embedding: List[float],
                          top_k: int = Config.TOP_K,
                          content_type_boost: List[str] = None,
                          expanded_queries: List[str] = None,
                          filters: Dict = None) -> List[Dict[str, Any]]:
    """
    Execute hybrid search combining semantic KNN and keyword BM25.
    
    Scoring Strategy:
        - KNN (semantic): Captures meaning and context
        - BM25 (keyword): Exact term matching for technical accuracy
        - Content-type boosting: Prioritizes relevant document types
    
    Args:
        query_text: Original query for BM25 matching
        query_embedding: Vector for KNN similarity
        top_k: Number of results
        content_type_boost: Document types to prioritize
        expanded_queries: Additional query variations
        filters: Hard filters (document_id, file_type)
    
    Returns:
        Ranked list of document chunks with hybrid scores
    """
    try:
        client = get_opensearch_client()
        
        # Build hybrid query with should clauses
        should_clauses = []
        
        # 1. KNN Semantic Search (primary signal)
        # Increased over-fetch from 3x to 5x for better multi-doc coverage
        should_clauses.append({
            "knn": {
                "embedding": {
                    "vector": query_embedding,
                    "k": top_k * 5  # Aggressive over-fetch for re-ranking
                }
            }
        })
        
        # 2. BM25 Keyword Search - primary query
        bm25_boost = Config.KEYWORD_WEIGHT / Config.SEMANTIC_WEIGHT
        should_clauses.append({
            "match": {
                "text": {
                    "query": query_text,
                    "boost": bm25_boost,
                    "fuzziness": "AUTO"  # Handle typos
                }
            }
        })
        
        # 3. BM25 for expanded queries (lower boost)
        if expanded_queries and Config.ENABLE_QUERY_EXPANSION:
            for eq in expanded_queries[1:3]:  # Skip original, limit to 2
                should_clauses.append({
                    "match": {
                        "text": {
                            "query": eq,
                            "boost": bm25_boost * 0.5
                        }
                    }
                })
        
        # 4. Content-type boosting (intent-aware)
        if content_type_boost:
            should_clauses.append({
                "terms": {
                    "content_type": content_type_boost,
                    "boost": 1.5
                }
            })
        
        # 5. Entity matching boost (for technical terms)
        extracted_entities = extract_entities_from_query(query_text)
        if extracted_entities:
            should_clauses.append({
                "terms": {
                    "entities": extracted_entities,
                    "boost": 2.0
                }
            })
        
        # Build complete query
        query_body = {
            "size": top_k,
            "query": {
                "bool": {
                    "should": should_clauses,
                    "minimum_should_match": 1
                }
            },
            "_source": {
                "excludes": ["embedding"]
            },
            "highlight": {
                "fields": {
                    "text": {
                        "fragment_size": 150,
                        "number_of_fragments": 2
                    }
                }
            }
        }
        
        # Apply hard filters (including MECH-specific filters)
        filter_query = _build_filter_clauses(filters)
        if filter_query:
            query_body["query"]["bool"].update(filter_query)
        
        # Execute search
        response = client.search(index=Config.OPENSEARCH_INDEX, body=query_body)
        
        hits = response.get('hits', {}).get('hits', [])
        log_event('hybrid_search_complete', f"Raw hits: {len(hits)}")
        
        # Process and filter results
        results = []
        for hit in hits:
            score = hit.get('_score', 0)
            
            if score >= Config.MIN_SCORE:
                source = hit['_source']
                highlight = hit.get('highlight', {}).get('text', [])
                
                results.append({
                    'chunk_id': source.get('chunk_id'),
                    'document_id': source.get('document_id'),
                    'text': source.get('text', ''),
                    'text_display': source.get('text_display', source.get('text', '')),
                    'chunk_type': source.get('chunk_type'),
                    'parent_chunk_id': source.get('parent_chunk_id'),
                    'is_proposition': source.get('is_proposition', False),
                    'score': round(score, 4),
                    'file_name': source.get('file_name'),
                    'file_type': source.get('file_type'),
                    'page_number': source.get('page_number'),
                    'section': source.get('section'),
                    'section_title': source.get('section_title'),
                    'content_type': source.get('content_type'),
                    'entities': source.get('entities', []),
                    'topics': source.get('topics', []),
                    'doc_category': source.get('doc_category', 'GENERAL'),  # For re-ranking
                    'doc_tags': source.get('doc_tags', []),
                    'line_start': _safe_int(source.get('line_start')),
                    'line_end': _safe_int(source.get('line_end')),
                    'identifiers': source.get('identifiers', []) or [],
                    'heading_hierarchy': source.get('heading_hierarchy') or source.get('heading_path'),
                    'pageindex_node_id': source.get('pageindex_node_id'),
                    'mentioned_identifiers': source.get('mentioned_identifiers', []) or [],
                    'highlight': ' ... '.join(highlight) if highlight else None
                })
        
        log_event('search_filtered', f"Returned {len(results)} results (min_score: {Config.MIN_SCORE})")
        return results
        
    except Exception as e:
        log_event('search_error', f"Hybrid search failed: {e}")
        return []


def execute_semantic_search(query_embedding: List[float],
                            top_k: int = Config.TOP_K,
                            filters: Dict = None) -> List[Dict[str, Any]]:
    """
    Fallback: KNN-only semantic search.
    
    Used when hybrid search is disabled or as fallback.
    """
    try:
        client = get_opensearch_client()
        
        query_body = {
            "size": top_k,
            "query": {
                "knn": {
                    "embedding": {
                        "vector": query_embedding,
                        "k": top_k
                    }
                }
            },
            "_source": {"excludes": ["embedding"]}
        }
        
        # Apply filters (including MECH-specific filters)
        if filters:
            filter_query = _build_filter_clauses(filters)
            query_body["query"] = {
                "bool": {
                    "must": [
                        {"knn": {"embedding": {"vector": query_embedding, "k": top_k}}}
                    ],
                    **filter_query
                }
            }
        
        response = client.search(index=Config.OPENSEARCH_INDEX, body=query_body)
        hits = response.get('hits', {}).get('hits', [])
        
        results = []
        for hit in hits:
            score = hit.get('_score', 0)
            if score >= Config.MIN_SCORE:
                source = hit['_source']
                results.append({
                    'chunk_id': source.get('chunk_id'),
                    'document_id': source.get('document_id'),
                    'text': source.get('text', ''),
                    'text_display': source.get('text_display', source.get('text', '')),
                    'chunk_type': source.get('chunk_type'),
                    'parent_chunk_id': source.get('parent_chunk_id'),
                    'is_proposition': source.get('is_proposition', False),
                    'score': round(score, 4),
                    'file_name': source.get('file_name'),
                    'file_type': source.get('file_type'),
                    'page_number': source.get('page_number'),
                    'section': source.get('section'),
                    'content_type': source.get('content_type'),
                    'entities': source.get('entities', []),
                    'topics': source.get('topics', []),
                    'doc_category': source.get('doc_category', 'GENERAL'),
                    'doc_tags': source.get('doc_tags', []),
                    'line_start': _safe_int(source.get('line_start')),
                    'line_end': _safe_int(source.get('line_end')),
                    'identifiers': source.get('identifiers', []) or [],
                    'heading_hierarchy': source.get('heading_hierarchy') or source.get('heading_path'),
                    'pageindex_node_id': source.get('pageindex_node_id'),
                    'mentioned_identifiers': source.get('mentioned_identifiers', []) or [],
                })
        
        return results
        
    except Exception as e:
        log_event('semantic_search_error', f"Search failed: {e}")
        return []


def extract_entities_from_query(query: str) -> List[str]:
    """Extract known mainframe entities from query for boosting."""
    entities = []
    query_upper = query.upper()
    
    for acronym in MAINFRAME_ACRONYMS:
        if acronym in query_upper:
            entities.append(acronym)
    
    # Check for known terms
    for term in MAINFRAME_SYNONYMS.keys():
        if term.upper() in query_upper:
            entities.append(term.upper())
    
    return list(set(entities))[:10]


def _is_proposition_chunk(chunk: Dict[str, Any]) -> bool:
    return bool(
        chunk.get('is_proposition')
        or chunk.get('chunk_type') == 'proposition'
        or chunk.get('type') == 'proposition'
    )


def _fetch_chunks_by_chunk_id(chunk_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Fetch chunks by chunk_id (since AOSS doesn't support using chunk_id as _id)."""
    if not chunk_ids:
        return {}

    try:
        client = get_opensearch_client()
        # Keep request small and predictable
        unique_ids = list(dict.fromkeys([cid for cid in chunk_ids if cid]))[:200]
        if not unique_ids:
            return {}

        query_body = {
            "size": len(unique_ids),
            "query": {
                "bool": {
                    "filter": [
                        {"terms": {"chunk_id": unique_ids}}
                    ]
                }
            },
            "_source": {
                "includes": [
                    "chunk_id",
                    "document_id",
                    "text",
                    "text_display",
                    "file_name",
                    "file_type",
                    "page_number",
                    "section",
                    "section_title",
                    "content_type",
                    "entities",
                    "topics",
                    "chunk_type",
                    "parent_chunk_id",
                    "is_proposition",
                    "line_start",
                    "line_end",
                    "identifiers",
                    "heading_hierarchy",
                    "heading_path",
                    "pageindex_node_id",
                    "mentioned_identifiers",
                    "doc_category",
                    "doc_tags",
                ]
            },
        }

        response = client.search(index=Config.OPENSEARCH_INDEX, body=query_body)
        hits = response.get('hits', {}).get('hits', [])

        found: Dict[str, Dict[str, Any]] = {}
        for hit in hits:
            src = hit.get('_source') or {}
            cid = src.get('chunk_id')
            if cid:
                found[cid] = src
        return found
    except Exception as e:
        log_event('parent_fetch_error', f"Failed to fetch parent chunks: {e}")
        return {}


def resolve_proposition_citations(chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """For proposition chunks, use the parent chunk's text_display for citations/UI."""
    if not chunks:
        return chunks

    parent_ids = []
    for c in chunks:
        if _is_proposition_chunk(c) and c.get('parent_chunk_id'):
            parent_ids.append(c['parent_chunk_id'])

    if not parent_ids:
        return chunks

    parents = _fetch_chunks_by_chunk_id(parent_ids)
    if not parents:
        return chunks

    for c in chunks:
        if not _is_proposition_chunk(c):
            continue
        parent_id = c.get('parent_chunk_id')
        parent = parents.get(parent_id)
        if not parent:
            continue

        parent_display = parent.get('text_display') or parent.get('text')
        if parent_display:
            c['text_display'] = parent_display

        # Fill missing citation metadata from parent (do not overwrite existing)
        for key in ('file_name', 'file_type', 'page_number', 'section', 'section_title', 'content_type'):
            if not c.get(key) and parent.get(key):
                c[key] = parent.get(key)

    return chunks


# ============================================================================
# CONTEXT BUILDER
# ============================================================================

def build_retrieval_context(chunks: List[Dict[str, Any]], 
                           include_metadata: bool = True) -> str:
    """
    Build structured context from retrieved chunks.
    
    Formats chunks with source attribution for LLM consumption.
    """
    if not chunks:
        return "No relevant documentation found in the knowledge base."
    
    context_parts = []
    
    for i, chunk in enumerate(chunks, 1):
        # Build source attribution
        source_parts = [f"Source: {chunk.get('file_name', 'Unknown')}"]
        
        if chunk.get('page_number'):
            source_parts.append(f"Page {chunk['page_number']}")
        line_start = _safe_int(chunk.get('line_start'))
        line_end = _safe_int(chunk.get('line_end'))
        if line_start is not None and line_end is not None:
            source_parts.append(f"Lines {line_start}-{line_end}")
        if chunk.get('section_title'):
            source_parts.append(f"Section: {chunk['section_title']}")
        if include_metadata and chunk.get('content_type'):
            source_parts.append(f"Type: {chunk['content_type']}")
        
        source_parts.append(f"Relevance: {chunk.get('score', 0):.2f}")
        
        source_line = f"[{', '.join(source_parts)}]"
        
        # Include highlight if available
        text = chunk.get('text', '')
        if chunk.get('highlight'):
            text = f"KEY EXCERPT: {chunk['highlight']}\n\nFULL CONTENT:\n{text}"
        
        context_parts.append(f"=== DOCUMENT {i} {source_line} ===\n{text}\n")
    
    return '\n'.join(context_parts)


# ============================================================================
# INTENT-AWARE RESPONSE GENERATION
# ============================================================================

def get_intent_system_prompt(intent: QueryIntent) -> str:
    """
    Generate intent-specific system prompt for optimal response quality.
    """
    base_prompt = """<role>You are MECH Avatar, BMO's enterprise mainframe documentation assistant specializing in legacy banking systems, COBOL/JCL, and MECH platform documentation.</role>

<core_responsibilities>
1. Provide accurate, well-sourced answers from provided documentation
2. Synthesize information across multiple documents when needed
3. Preserve technical precision (exact field names, codes, formats)
4. Guide users through complex mainframe concepts clearly
</core_responsibilities>

<multi_document_processing>
CRITICAL: For every query, you MUST:
1. READ ALL provided document chunks completely - do not stop at first match
2. EXTRACT information from EVERY relevant chunk (tables, lists, code blocks, procedures)
3. SYNTHESIZE complementary details from different sources
4. CITE sources with document name and section/page when available
5. For multi-part questions (a,b,c,d): search ALL documents for EACH part

When documents provide different perspectives or details:
- Note which document provides which information
- Combine non-contradictory information
- Flag any contradictions found
</multi_document_processing>

<technical_standards>
- PRESERVE exact terminology: MYPTLA, MYNBTA, TSS, CICS, JCL, ABEND codes, SHF segments
- QUOTE relevant passages using "exact text" format
- INCLUDE code examples, parameter lists, return codes when applicable
- USE markdown formatting: headers, bullets, code blocks, tables
- STRUCTURE complex answers with clear sections
</technical_standards>

<information_not_found_protocol>
Only state "information not found" AFTER confirming:
- ALL provided chunks were searched
- Alternate spellings/abbreviations checked
- Related terms considered
Format: "Searched N documents. Found [partial]. NOT FOUND: [specific item]"
</information_not_found_protocol>"""

    intent_specifics = {
        QueryIntent.DEFINITION: """

<response_style>CONCISE DEFINITION</response_style>
<format>
1. Clear 1-2 sentence definition
2. Key characteristics (3-5 bullet points)
3. Related concepts or prerequisites
4. Example usage if applicable
</format>
<constraints>Maximum 200 words unless complexity requires more</constraints>""",

        QueryIntent.PROCEDURE: """

<response_style>STEP-BY-STEP PROCEDURE</response_style>
<format>
## Prerequisites
- What's needed before starting

## Steps
1. [Numbered, actionable instruction]
   - Sub-details if needed
2. [Next step with exact commands/parameters]

## Verification
- How to confirm success
- Expected outputs/results

## Common Issues
- Potential problems and solutions
</format>
<constraints>Include exact commands, parameters, JCL syntax, expected outputs</constraints>""",

        QueryIntent.TROUBLESHOOTING: """

<response_style>DIAGNOSTIC TROUBLESHOOTING</response_style>
<format>
## Error Analysis
- What the error/symptom indicates
- Error code meaning (if applicable)

## Root Cause
- Most likely cause(s) ranked by probability

## Resolution Steps
1. [Ordered by likelihood of success]
2. [Include specific commands/checks]

## Prevention
- How to avoid in future

## Escalation
- When to contact support
- What information to gather
</format>
<constraints>Include specific error codes, return codes, ABEND meanings, recovery commands</constraints>""",

        QueryIntent.COMPARISON: """

<response_style>STRUCTURED COMPARISON</response_style>
<format>
## Comparison: [Option A] vs [Option B]

| Aspect | Option A | Option B |
|--------|----------|----------|
| [Key differences with specific details] |

## Use Cases
- When to use Option A
- When to use Option B

## Trade-offs
- Performance considerations
- Security implications
- Complexity factors

## Recommendation
[If appropriate, with justification]
</format>""",

        QueryIntent.CODE_EXPLANATION: """

<response_style>TECHNICAL CODE ANALYSIS</response_style>
<format>
## Purpose
- Overall function of this code

## Structure Breakdown
```
[Key sections with annotations]
```

## Key Elements
- Important parameters and their effects
- Control flow explanation

## Mainframe-Specific Constructs
- JCL/COBOL/Assembler specific elements explained

## Potential Issues
- Edge cases or optimizations to consider
</format>""",

        QueryIntent.BEST_PRACTICE: """

<response_style>EXPERT RECOMMENDATION</response_style>
<format>
## Recommended Approach
[Lead with the best practice]

## Rationale
- Why this is recommended (security, performance, maintainability)

## Alternatives
- Other options with trade-offs

## BMO Standards
- Relevant BMO-specific policies or standards

## Implementation Notes
- Practical considerations for adoption
</format>""",

        QueryIntent.CONFIGURATION: """

<response_style>CONFIGURATION GUIDE</response_style>
<format>
## Parameters
| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|

## Syntax
```
[Exact format required]
```

## Examples
```
[Working configuration samples]
```

## Dependencies
- Related configurations that must be set
- Order of configuration if relevant

## Validation
- How to verify configuration is correct
</format>""",

        QueryIntent.SECURITY: """

<response_style>SECURITY-FOCUSED</response_style>
<format>
## Security Context
- Why this matters from a security perspective

## Access Requirements
- Required permissions/authorizations
- TSS/RACF/ACF2 specific settings

## Compliance Considerations
- Audit requirements
- Regulatory implications

## BMO Security Standards
- Applicable internal policies

## Risk Mitigation
- Potential risks of misconfiguration
- Safeguards to implement
</format>""",

        QueryIntent.OPEN_ENDED: """

<response_style>COMPREHENSIVE OVERVIEW</response_style>
<format>
## Overview
[High-level introduction]

## Key Aspects
### [Subtopic 1]
[Details]

### [Subtopic 2]
[Details]

## Practical Applications
[Real-world usage examples]

## Related Topics
[Cross-references to related documentation]

## Further Reading
[Suggestions for deeper exploration]
</format>"""
    }
    
    specific = intent_specifics.get(intent, intent_specifics[QueryIntent.OPEN_ENDED])
    return f"{base_prompt}{specific}"


def generate_llm_response(query: str, 
                          context: str, 
                          intent: QueryIntent,
                          custom_prompt: str = None,
                          enforce_faithfulness: bool = True) -> Dict[str, Any]:
    """
    Generate response using Claude Sonnet via Bedrock.
    
    Args:
        query: User's question
        context: Retrieved documentation context
        intent: Classified query intent
        custom_prompt: Optional override system prompt
        enforce_faithfulness: Add faithfulness requirements to prompt (Layer 8)
    
    Returns:
        Dict with answer, token usage, and model info
    """
    log_event('llm_response_generation', f'Using generation model: {Config.GENERATION_MODEL}')
    
    system_prompt = custom_prompt or get_intent_system_prompt(intent)
    
    # Layer 8 Enhancement: Add faithfulness requirements
    if enforce_faithfulness:
        system_prompt = system_prompt + FAITHFUL_SYSTEM_PROMPT_ADDITION
    
    intent_config = INTENT_CONFIG.get(intent, INTENT_CONFIG[QueryIntent.OPEN_ENDED])
    max_tokens = intent_config.get('max_tokens', Config.MAX_TOKENS)
    
    user_prompt = f"""<documentation_context>
{context}
</documentation_context>

<user_question>{query}</user_question>

<processing_instructions>
**STEP 1: Document Analysis**
- Count total document chunks provided
- Note document types/sources available
- Identify which chunks are most relevant to the question

**STEP 2: Information Extraction**
- For EACH relevant chunk, extract pertinent information
- Look for: tables, lists, code blocks, procedures, definitions
- Note the source document for each extracted fact

**STEP 3: Multi-Part Question Handling**
IF question has multiple parts (a, b, c, d) or asks about multiple items:
- Search ALL documents for EACH part separately
- Clearly label answers: (a), (b), (c), etc.
- Ensure no part is skipped

**STEP 4: Source Verification**
IF question mentions specific document types (SHF, ILC, LRD):
- Verify you found content from those specific documents
- Note which document type provided which information

**STEP 5: Response Synthesis**
- Combine information from multiple sources coherently
- Use appropriate formatting (headers, bullets, code blocks)
- Include citations: [Source: Document N, filename]

**STEP 6: Completeness Check**
- Verify all parts of the question are answered
- If information is partial, note what's available and what's missing
- Only state "not found" after confirming ALL documents were searched
</processing_instructions>

<response_format>
For simple questions: Direct answer with citations
For complex questions:
## [Main Topic]
[Answer with citations]

### [Subtopic if needed]
[Details]

**Sources:** [List of documents used]
</response_format>

Provide your response:"""

    try:
        client = get_client('bedrock-runtime')
        
        response = client.invoke_model(
            modelId=Config.GENERATION_MODEL,
            contentType='application/json',
            accept='application/json',
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": max_tokens,
                "temperature": Config.TEMPERATURE,
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_prompt}]
            })
        )
        
        result = json.loads(response['body'].read().decode('utf-8'))
        
        answer = result['content'][0]['text']
        input_tokens = result.get('usage', {}).get('input_tokens', 0)
        output_tokens = result.get('usage', {}).get('output_tokens', 0)
        
        log_event('response_generated', 
                  f"Length: {len(answer)} chars",
                  input_tokens=input_tokens,
                  output_tokens=output_tokens)
        
        return {
            'answer': answer,
            'model': Config.GENERATION_MODEL,
            'input_tokens': input_tokens,
            'output_tokens': output_tokens,
            'intent_applied': intent.value
        }
        
    except Exception as e:
        log_event('generation_error', f"Response generation failed: {e}")
        raise


# ============================================================================
# EXPLAINABLE RETRIEVAL METADATA
# ============================================================================

def build_retrieval_explanation(query: str,
                                resolved_query: str,
                                query_resolution: Optional[str],
                                intent: QueryIntent,
                                intent_confidence: float,
                                expanded_queries: List[str],
                                chunks: List[Dict],
                                timing: Dict[str, int]) -> Dict[str, Any]:
    """
    Build comprehensive retrieval explanation for transparency.
    
    Enables debugging, auditing, and user understanding of search behavior.
    """
    return {
        "search_strategy": {
            "type": "hybrid" if Config.ENABLE_HYBRID_SEARCH else "semantic_only",
            "semantic_weight": Config.SEMANTIC_WEIGHT,
            "keyword_weight": Config.KEYWORD_WEIGHT,
            "min_score_threshold": Config.MIN_SCORE
        },
        "query_processing": {
            "original_query": query,
            "resolved_query": resolved_query if resolved_query != query else None,
            "resolution_reason": query_resolution,
            "expanded_to": expanded_queries if len(expanded_queries) > 1 else None
        },
        "intent_analysis": {
            "detected_intent": intent.value,
            "confidence": round(intent_confidence, 2),
            "top_k_applied": INTENT_CONFIG.get(intent, {}).get('top_k', Config.TOP_K),
            "content_type_boost": INTENT_CONFIG.get(intent, {}).get('content_type_boost')
        },
        "retrieval_results": {
            "chunks_retrieved": len(chunks),
            "score_range": {
                "min": round(min(c['score'] for c in chunks), 3) if chunks else 0,
                "max": round(max(c['score'] for c in chunks), 3) if chunks else 0
            },
            "top_sources": [
                {
                    "file": c.get('file_name'),
                    "score": round(c.get('score', 0), 3),
                    "content_type": c.get('content_type'),
                    "entities": c.get('entities', [])[:5]
                }
                for c in chunks[:5]
            ]
        },
        "feature_flags": {
            "hybrid_search": Config.ENABLE_HYBRID_SEARCH,
            "intent_classification": Config.ENABLE_INTENT_CLASSIFICATION,
            "query_expansion": Config.ENABLE_QUERY_EXPANSION,
            "conversational_memory": Config.ENABLE_MEMORY,
            "explanations": Config.ENABLE_EXPLANATIONS
        },
        "timing_ms": timing
    }


# ============================================================================
# ADVANCED RAG - CONTEXT COMPRESSION
# ============================================================================

ENABLE_CONTEXT_COMPRESSION = os.environ.get('ENABLE_CONTEXT_COMPRESSION', 'true').lower() == 'true'
MAX_COMPRESSED_TOKENS = int(os.environ.get('MAX_COMPRESSED_TOKENS', '4000'))


def compress_context(query: str, 
                     chunks: List[Dict[str, Any]], 
                     max_tokens: int = None) -> List[Dict[str, Any]]:
    """
    Compress context by extracting only query-relevant sentences.
    
    This reduces noise in the LLM context window by:
    1. Splitting chunks into sentences
    2. Scoring each sentence for relevance to query
    3. Keeping only high-relevance sentences
    4. Maintaining coherence with context sentences
    
    Based on: LongLLMLingua and RECOMP research
    
    Args:
        query: User query
        chunks: Retrieved document chunks
        max_tokens: Maximum tokens in compressed context
    
    Returns:
        Chunks with compressed (relevant-only) text
    """
    if not chunks or not ENABLE_CONTEXT_COMPRESSION:
        return chunks
    
    max_tokens = max_tokens or MAX_COMPRESSED_TOKENS
    start_time = time.time()
    
    try:
        # Use LLM to extract relevant sentences from each chunk
        client = get_client('bedrock-runtime')
        
        query_terms = set(query.lower().split())
        compressed_chunks = []
        total_tokens = 0
        
        for chunk in chunks:
            original_text = chunk.get('text', '')
            
            # Skip if already small enough
            if len(original_text.split()) < 100:
                compressed_chunks.append(chunk)
                total_tokens += len(original_text.split())
                continue
            
            # Quick relevance filter using term matching first
            sentences = _split_into_sentences(original_text)
            
            relevant_sentences = []
            for sent in sentences:
                sent_lower = sent.lower()
                # Score by query term overlap + entity presence
                term_overlap = sum(1 for t in query_terms if t in sent_lower)
                has_entity = any(e.lower() in sent_lower for e in chunk.get('entities', []))
                
                # Keep sentence if it has term overlap or entities
                if term_overlap >= 2 or has_entity:
                    relevant_sentences.append(sent)
                elif len(sent.split()) < 20:
                    # Keep short context sentences
                    relevant_sentences.append(sent)
            
            # If too few sentences, use LLM for finer extraction
            if len(relevant_sentences) < 3 and len(sentences) > 5:
                relevant_sentences = _llm_extract_relevant_sentences(
                    client, query, original_text, max_sentences=5
                )
            
            # Build compressed text
            compressed_text = ' '.join(relevant_sentences)
            
            # Limit tokens per chunk
            chunk_tokens = len(compressed_text.split())
            if total_tokens + chunk_tokens > max_tokens:
                # Truncate to fit
                remaining = max_tokens - total_tokens
                compressed_text = ' '.join(compressed_text.split()[:remaining])
                chunk_tokens = remaining
            
            if compressed_text:
                compressed_chunk = chunk.copy()
                compressed_chunk['text'] = compressed_text
                compressed_chunk['original_text'] = original_text
                compressed_chunk['compression_ratio'] = round(
                    len(compressed_text) / len(original_text), 2
                ) if original_text else 1.0
                compressed_chunks.append(compressed_chunk)
                total_tokens += chunk_tokens
            
            if total_tokens >= max_tokens:
                break
        
        elapsed_ms = int((time.time() - start_time) * 1000)
        avg_ratio = sum(c.get('compression_ratio', 1.0) for c in compressed_chunks) / len(compressed_chunks) if compressed_chunks else 1.0
        
        log_event('context_compression_complete',
                  f"Compressed {len(chunks)} chunks, avg ratio: {avg_ratio:.2f}, {elapsed_ms}ms")
        
        return compressed_chunks
        
    except Exception as e:
        log_event('context_compression_error', f"Compression failed: {e}, using original")
        return chunks


def _split_into_sentences(text: str) -> List[str]:
    """Split text into sentences using punctuation markers."""
    import re
    # Split on sentence boundaries
    sentences = re.split(r'(?<=[.!?])\s+', text)
    return [s.strip() for s in sentences if s.strip()]


def _llm_extract_relevant_sentences(client, query: str, text: str, max_sentences: int = 5) -> List[str]:
    """Use Claude Haiku to extract most relevant sentences."""
    try:
        prompt = f"""Extract the {max_sentences} most relevant sentences from the text that help answer: "{query}"

Text:
{text[:3000]}

Return ONLY the relevant sentences, one per line. No explanations."""

        response = client.invoke_model(
            modelId=Config.FAST_MODEL,
            contentType='application/json',
            accept='application/json',
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 500,
                "temperature": 0,
                "messages": [{"role": "user", "content": prompt}]
            })
        )
        
        result = json.loads(response['body'].read().decode('utf-8'))
        extracted = result['content'][0]['text'].strip()
        
        return [s.strip() for s in extracted.split('\n') if s.strip()]
        
    except Exception:
        return []


# ============================================================================
# ADVANCED RAG - LONG-CONTEXT REORDERING
# ============================================================================

ENABLE_CONTEXT_REORDERING = os.environ.get('ENABLE_CONTEXT_REORDERING', 'true').lower() == 'true'


def reorder_chunks_for_llm(chunks: List[Dict[str, Any]], 
                           query: str = None,
                           strategy: str = 'lost_in_middle') -> List[Dict[str, Any]]:
    """
    Reorder chunks to optimize LLM attention patterns.
    
    Research shows LLMs have "lost in the middle" effect where:
    - Information at the START gets high attention (primacy)
    - Information at the END gets high attention (recency)
    - Information in the MIDDLE gets less attention
    
    Strategies:
    - 'lost_in_middle': Put most relevant at start AND end
    - 'best_first': Put most relevant first (default RAG)
    - 'chronological': Order by document position/date
    - 'diverse_spread': Spread diverse sources throughout
    
    Based on: "Lost in the Middle" research (Liu et al.)
    
    Args:
        chunks: Ranked chunks (best first)
        query: Optional query for additional relevance scoring
        strategy: Reordering strategy
    
    Returns:
        Reordered chunks optimizing for LLM attention
    """
    if not chunks or len(chunks) < 3:
        return chunks
    
    if not ENABLE_CONTEXT_REORDERING:
        return chunks
    
    start_time = time.time()
    
    if strategy == 'lost_in_middle':
        # Interleave: best at start, second-best at end, rest in middle
        # Pattern: 1, 3, 5, 7, ... (middle) ..., 6, 4, 2
        n = len(chunks)
        reordered = []
        
        # Even indices go to front, odd indices go to back
        front = []
        back = []
        
        for i, chunk in enumerate(chunks):
            if i % 2 == 0:
                front.append(chunk)
            else:
                back.insert(0, chunk)  # Reverse order for back
        
        reordered = front + back
        
    elif strategy == 'diverse_spread':
        # Ensure diverse documents spread throughout
        # Group by document, then interleave
        doc_groups = {}
        for chunk in chunks:
            doc_id = chunk.get('document_id', chunk.get('file_name', 'unknown'))
            if doc_id not in doc_groups:
                doc_groups[doc_id] = []
            doc_groups[doc_id].append(chunk)
        
        # Interleave from each document
        reordered = []
        max_per_doc = max(len(v) for v in doc_groups.values())
        
        for i in range(max_per_doc):
            for doc_id, doc_chunks in doc_groups.items():
                if i < len(doc_chunks):
                    reordered.append(doc_chunks[i])
        
    elif strategy == 'chronological':
        # Sort by page number or position
        reordered = sorted(chunks, key=lambda x: (
            x.get('page_number', 0),
            x.get('chunk_id', '')
        ))
        
    else:  # best_first (default)
        reordered = chunks
    
    # Mark position for debugging
    for i, chunk in enumerate(reordered):
        chunk['reorder_position'] = i
        chunk['reorder_strategy'] = strategy
    
    elapsed_ms = int((time.time() - start_time) * 1000)
    log_event('context_reorder_complete', 
              f"Reordered {len(chunks)} chunks with '{strategy}' in {elapsed_ms}ms")
    
    return reordered


# ============================================================================
# ADVANCED RAG - SELF-RAG VERIFICATION LOOP
# ============================================================================

ENABLE_SELF_RAG = os.environ.get('ENABLE_SELF_RAG', 'true').lower() == 'true'
SELF_RAG_MAX_ITERATIONS = int(os.environ.get('SELF_RAG_MAX_ITERATIONS', '2'))
SELF_RAG_MIN_GROUNDEDNESS = float(os.environ.get('SELF_RAG_MIN_GROUNDEDNESS', '0.7'))


def self_rag_verification_loop(query: str,
                               initial_answer: str,
                               sources: List[Dict[str, Any]],
                               query_embedding: List[float] = None,
                               max_iterations: int = None) -> Dict[str, Any]:
    """
    Iterative Self-RAG: Generate → Verify → Augment → Regenerate.
    
    Process:
    1. Generate initial answer
    2. Verify grounding/factual accuracy
    3. If low confidence: retrieve additional chunks, regenerate
    4. Repeat until satisfied or max iterations
    
    Based on: Self-RAG (Asai et al.) and CRAG research
    
    Args:
        query: Original user query
        initial_answer: First generated answer
        sources: Source chunks used
        query_embedding: Query vector for additional retrieval
        max_iterations: Max refinement iterations
    
    Returns:
        Final answer with verification metadata
    """
    if not ENABLE_SELF_RAG:
        return {
            'answer': initial_answer,
            'iterations': 0,
            'self_rag_enabled': False,
            'sources': sources
        }
    
    max_iterations = max_iterations or SELF_RAG_MAX_ITERATIONS
    start_time = time.time()
    
    current_answer = initial_answer
    current_sources = sources
    iteration_history = []
    
    for iteration in range(max_iterations):
        # Step 1: Verify current answer grounding
        verification = verify_answer_grounding(current_answer, current_sources, query)
        groundedness = verification.get('groundedness', 0)
        is_grounded = verification.get('is_grounded', False)
        missing_info = verification.get('literals_missing', [])
        
        iteration_history.append({
            'iteration': iteration + 1,
            'groundedness': groundedness,
            'is_grounded': is_grounded,
            'missing_info': missing_info[:3]
        })
        
        log_event('self_rag_iteration', 
                  f"Iteration {iteration + 1}: groundedness={groundedness:.2f}, grounded={is_grounded}")
        
        # Step 2: Check if satisfied
        if groundedness >= SELF_RAG_MIN_GROUNDEDNESS and is_grounded:
            break
        
        # Step 3: If not satisfied and we have query embedding, retrieve more
        if query_embedding and missing_info:
            # Search for chunks about missing information
            additional_query = f"{query} {' '.join(missing_info[:3])}"
            additional_embedding = generate_embedding(additional_query)
            
            additional_chunks = execute_hybrid_search(
                query_text=additional_query,
                query_embedding=additional_embedding,
                top_k=3
            )
            
            if additional_chunks:
                # Add new chunks to sources (deduplicate)
                existing_ids = {c.get('chunk_id') for c in current_sources}
                for chunk in additional_chunks:
                    if chunk.get('chunk_id') not in existing_ids:
                        current_sources.append(chunk)
                        existing_ids.add(chunk.get('chunk_id'))
                
                log_event('self_rag_augment', 
                          f"Added {len(additional_chunks)} additional chunks")
        
        # Step 4: Regenerate with augmented sources
        if iteration < max_iterations - 1:  # Don't regenerate on last iteration
            context = "\n\n---\n\n".join([
                f"[Source {i+1}] {c.get('text', '')[:1000]}"
                for i, c in enumerate(current_sources[:8])
            ])
            
            regenerate_prompt = f"""Based on the following sources, provide an accurate and complete answer to: "{query}"

{context}

Previous answer had low grounding. Focus on information directly from the sources.
Missing information to address: {', '.join(missing_info[:3]) if missing_info else 'none identified'}

Provide your answer:"""

            try:
                client = get_client('bedrock-runtime')
                response = client.invoke_model(
                    modelId=Config.GENERATION_MODEL,
                    contentType='application/json',
                    accept='application/json',
                    body=json.dumps({
                        "anthropic_version": "bedrock-2023-05-31",
                        "max_tokens": 2000,
                        "temperature": 0.3,
                        "messages": [{"role": "user", "content": regenerate_prompt}]
                    })
                )
                
                result = json.loads(response['body'].read().decode('utf-8'))
                current_answer = result['content'][0]['text']
                
            except Exception as e:
                log_event('self_rag_regenerate_error', f"Regeneration failed: {e}")
                break
    
    elapsed_ms = int((time.time() - start_time) * 1000)
    final_verification = verify_answer_grounding(current_answer, current_sources, query)
    
    log_event('self_rag_complete',
              f"Completed in {len(iteration_history)} iterations, "
              f"final groundedness: {final_verification.get('groundedness', 0):.2f}, "
              f"{elapsed_ms}ms")
    
    return {
        'answer': current_answer,
        'iterations': len(iteration_history),
        'iteration_history': iteration_history,
        'final_groundedness': final_verification.get('groundedness', 0),
        'final_grounded': final_verification.get('is_grounded', False),
        'sources': current_sources,
        'self_rag_enabled': True,
        'elapsed_ms': elapsed_ms
    }


# ============================================================================
# ADVANCED RAG - DOCUMENT-CODE CORRELATION
# ============================================================================

ENABLE_DOC_CODE_CORRELATION = os.environ.get('ENABLE_DOC_CODE_CORRELATION', 'true').lower() == 'true'


def correlate_doc_and_code(query: str,
                           doc_results: List[Dict[str, Any]],
                           code_results: List[Dict[str, Any]] = None,
                           intent: QueryIntent = None) -> List[Dict[str, Any]]:
    """
    Merge and correlate document and code search results for hybrid queries.
    
    This enables answering:
    - "How is X implemented?" → doc explanation + code
    - "What does procedure Y do?" → code + documentation
    - "Show me the code for feature Z" → correlated results
    
    Args:
        query: User query
        doc_results: Results from documentation search
        code_results: Results from code search (optional, will search if needed)
        intent: Query intent (if HYBRID, do correlation)
    
    Returns:
        Correlated results with doc↔code links
    """
    if not ENABLE_DOC_CODE_CORRELATION:
        return doc_results
    
    # Only correlate for code-related intents
    if intent and intent not in [QueryIntent.CODE_EXPLANATION, QueryIntent.PROCEDURE, QueryIntent.CONFIGURATION]:
        return doc_results
    
    start_time = time.time()
    
    # Search for code if not provided
    if not code_results:
        # Look for code references in doc results
        code_refs = set()
        for doc in doc_results:
            entities = doc.get('entities', [])
            for entity in entities:
                if _looks_like_procedure_name(entity):
                    code_refs.add(entity)
            
            # Also check text for procedure patterns
            text = doc.get('text', '')
            procs = _extract_procedure_names_from_text(text)
            code_refs.update(procs)
        
        if code_refs:
            # Search for code chunks
            code_query = ' '.join(list(code_refs)[:5])
            code_embedding = generate_embedding(code_query)
            
            code_results = execute_hybrid_search(
                query_text=code_query,
                query_embedding=code_embedding,
                top_k=5,
                filters={'chunk_type': 'code'}
            )
    
    # Correlate: link docs to code
    correlated = []
    
    for doc in doc_results:
        doc_copy = doc.copy()
        doc_copy['related_code'] = []
        
        doc_entities = set(e.lower() for e in doc.get('entities', []))
        doc_text_lower = doc.get('text', '').lower()
        
        for code in (code_results or []):
            code_idents = set(i.lower() for i in (code.get('identifiers') or []))
            code_entities = set(e.lower() for e in (code.get('entities') or []))
            
            # Check correlation
            if doc_entities.intersection(code_idents.union(code_entities)):
                doc_copy['related_code'].append({
                    'procedure': (code.get('procedure_name') or next(iter(code_idents), None)),
                    'snippet': code.get('text', '')[:300],
                    'score': code.get('score', 0)
                })
            elif code_idents and any(i in doc_text_lower for i in list(code_idents)[:5]):
                doc_copy['related_code'].append({
                    'procedure': (code.get('procedure_name') or next(iter(code_idents), None)),
                    'snippet': code.get('text', '')[:300],
                    'correlation_type': 'identifier_mention'
                })
        
        correlated.append(doc_copy)
    
    # Also add standalone code results at end
    if code_results:
        for code in code_results[:3]:
            if not any(code.get('chunk_id') == c.get('chunk_id') for c in correlated):
                code_copy = code.copy()
                code_copy['is_code_result'] = True
                correlated.append(code_copy)
    
    elapsed_ms = int((time.time() - start_time) * 1000)
    code_links = sum(len(c.get('related_code', [])) for c in correlated)
    
    log_event('doc_code_correlation_complete',
              f"Correlated {len(doc_results)} docs with {len(code_results or [])} code chunks, "
              f"{code_links} links found, {elapsed_ms}ms")
    
    return correlated


def _looks_like_procedure_name(text: str) -> bool:
    """Check if text looks like a procedure/CSECT name."""
    import re
    # MECH procedure patterns: CALC_INT, MYABC01, SHF03, etc.
    return bool(re.match(r'^[A-Z][A-Z0-9_]{2,15}$', text.upper()))


def _extract_procedure_names_from_text(text: str) -> List[str]:
    """Extract potential procedure names from text."""
    import re
    # Look for patterns like "CALC_INT procedure" or "CSECT MYABC"
    patterns = [
        r'\b([A-Z][A-Z0-9_]{2,15})\s+(?:procedure|CSECT|PROC|subroutine)',
        r'(?:calls?|invoke[sd]?|reference[sd]?)\s+([A-Z][A-Z0-9_]{2,15})\b',
        r'(?:CSECT|PROC|DSECT)\s+([A-Z][A-Z0-9_]{2,15})\b'
    ]
    
    procs = set()
    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        procs.update(m.upper() for m in matches)
    
    return list(procs)


# ---------------------------------------------------------------------------
# Industrial retrieval helpers: identifiers + doc↔code linking
# ---------------------------------------------------------------------------

_MENTIONED_IDENTIFIER_RE = re.compile(r"\b[A-Z][A-Z0-9_@#$-]{2,63}\b")
_MENTIONED_IDENTIFIER_STOPWORDS = {
    'THE', 'AND', 'OR', 'NOT', 'END', 'BEGIN', 'TRUE', 'FALSE', 'NULL',
    'DIVISION', 'SECTION', 'PARAGRAPH', 'IDENTIFICATION', 'ENVIRONMENT',
    'DATA', 'PROCEDURE', 'WORKING', 'STORAGE', 'LINKAGE',
}


def extract_candidate_identifiers(text: str, limit: int = None) -> List[str]:
    """Extract code-like identifiers for identifier-first narrowing.

    This is intentionally conservative (best-effort) and is used only to
    narrow/augment retrieval; it should never be the only retrieval path.
    """
    if not isinstance(text, str) or not text.strip():
        return []
    limit = int(limit or Config.IDENTIFIER_NARROWING_MAX_IDS)
    limit = max(1, min(limit, 50))

    found: List[str] = []
    seen: set = set()
    for token in _MENTIONED_IDENTIFIER_RE.findall(text.upper()):
        if token in _MENTIONED_IDENTIFIER_STOPWORDS:
            continue
        if token in seen:
            continue
        seen.add(token)
        found.append(token)
        if len(found) >= limit:
            break
    return found


def _is_code_focused_query(query: str, intent: Optional[QueryIntent]) -> bool:
    if intent in {QueryIntent.CODE_EXPLANATION}:
        return True

    q = (query or '').lower()
    if not q:
        return False
    # Heuristic keywords indicating implementation/code lookup.
    return any(k in q for k in (
        'code', 'implement', 'implementation', 'procedure', 'csect',
        'where is', 'show me', 'snippet', 'lines', 'source',
    ))


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def lookup_doc_code_links(
    identifiers: List[str],
    *,
    document_id: Optional[str] = None,
    max_items: int = 25,
) -> Dict[str, Dict[str, List[str]]]:
    """Fetch doc↔code links from DynamoDB.

    Expected schema (minimal):
      PK: identifier (S)
      SK: document_id (S)
      attrs: code_chunk_ids (L[S]), doc_chunk_ids (L[S])
    """
    if not Config.ENABLE_DOC_CODE_LINKS:
        return {}
    if not identifiers:
        return {}
    if not Config.DOC_CODE_LINKS_TABLE:
        return {}

    max_items = max(1, int(max_items))

    out: Dict[str, Dict[str, List[str]]] = {}
    try:
        table = get_resource('dynamodb').Table(Config.DOC_CODE_LINKS_TABLE)

        for ident in identifiers[:Config.IDENTIFIER_NARROWING_MAX_IDS]:
            if not ident:
                continue

            key_expr = Key('identifier').eq(ident)
            if document_id:
                # If the table uses document_id as sort key, this efficiently narrows.
                key_expr = key_expr & Key('document_id').eq(document_id)

            resp = table.query(
                KeyConditionExpression=key_expr,
                Limit=max_items,
            )
            items = resp.get('Items', [])
            if not items:
                continue

            merged_code: List[str] = []
            merged_doc: List[str] = []
            seen_code: set = set()
            seen_doc: set = set()
            for item in items:
                for cid in (item.get('code_chunk_ids') or []):
                    if cid and cid not in seen_code:
                        seen_code.add(cid)
                        merged_code.append(cid)
                for cid in (item.get('doc_chunk_ids') or []):
                    if cid and cid not in seen_doc:
                        seen_doc.add(cid)
                        merged_doc.append(cid)

            if merged_code or merged_doc:
                out[ident] = {
                    'code_chunk_ids': merged_code,
                    'doc_chunk_ids': merged_doc,
                }

        return out
    except Exception as e:
        log_event('doc_code_links_error', f"Failed to lookup doc↔code links: {e}")
        return {}


def query_pageindex_navigation(
    query: str,
    document_id: Optional[str] = None,
    section_title: Optional[str] = None,
    max_results: int = 10,
) -> List[Dict[str, Any]]:
    """
    Query the PageIndex for section-level navigation and drill-down.
    
    Enables:
    - Finding which sections of a document are relevant to a query
    - Drilling into child sections of a matched parent section
    - Navigating document structure without retrieving full chunks
    
    Returns list of PageIndex nodes with section hierarchy info.
    """
    if not Config.ENABLE_PAGEINDEX_NAVIGATION:
        return []
    
    try:
        client = get_opensearch_client()
        
        # Build the query
        must_clauses = []
        
        # Text match against section titles and summaries
        if query:
            must_clauses.append({
                'multi_match': {
                    'query': query,
                    'fields': ['section_title^3', 'summary^2', 'heading_path'],
                    'type': 'best_fields',
                    'fuzziness': 'AUTO'
                }
            })
        
        # Filter by document_id if provided
        filter_clauses = []
        if document_id:
            filter_clauses.append({'term': {'document_id': document_id}})
        
        # Filter by section_title for drill-down
        if section_title:
            filter_clauses.append({'term': {'parent_section': section_title}})
        
        body = {
            'size': max_results,
            'query': {
                'bool': {
                    'must': must_clauses if must_clauses else [{'match_all': {}}],
                    'filter': filter_clauses
                }
            },
            '_source': [
                'node_id', 'document_id', 'section_title', 'heading_path',
                'depth', 'parent_section', 'chunk_count', 'summary',
                'child_sections', 'page_range'
            ]
        }
        
        response = client.search(index=Config.PAGEINDEX_INDEX, body=body)
        
        results = []
        for hit in response.get('hits', {}).get('hits', []):
            source = hit.get('_source', {})
            results.append({
                'node_id': source.get('node_id'),
                'document_id': source.get('document_id'),
                'section_title': source.get('section_title'),
                'heading_path': source.get('heading_path'),
                'depth': source.get('depth', 0),
                'parent_section': source.get('parent_section'),
                'chunk_count': source.get('chunk_count', 0),
                'summary': source.get('summary', ''),
                'child_sections': source.get('child_sections', []),
                'page_range': source.get('page_range'),
                'score': hit.get('_score', 0.0)
            })
        
        log_event('pageindex_query', f"Found {len(results)} navigation nodes",
                  query=query[:100] if query else '', document_id=document_id)
        
        return results
        
    except Exception as e:
        log_event('pageindex_query_error', f"PageIndex navigation query failed: {e}")
        return []


def enrich_chunks_with_pageindex(chunks: List[Dict], query: str) -> List[Dict]:
    """
    Enrich retrieval results with PageIndex navigation context.
    For each chunk that has a pageindex_node_id, look up sibling/child sections
    to provide document navigation context in the response.
    """
    if not Config.ENABLE_PAGEINDEX_NAVIGATION or not chunks:
        return chunks
    
    try:
        # Collect unique document_ids from chunks to do targeted pageindex lookups
        doc_ids = list({c.get('document_id') for c in chunks if c.get('document_id')})[:5]
        
        navigation_context = {}
        for doc_id in doc_ids:
            nav_nodes = query_pageindex_navigation(query, document_id=doc_id, max_results=5)
            if nav_nodes:
                navigation_context[doc_id] = nav_nodes
        
        # Attach navigation context to chunks
        for chunk in chunks:
            doc_id = chunk.get('document_id')
            if doc_id and doc_id in navigation_context:
                chunk['navigation_context'] = [
                    {
                        'section_title': n.get('section_title'),
                        'depth': n.get('depth'),
                        'child_sections': n.get('child_sections', [])[:5]
                    }
                    for n in navigation_context[doc_id][:3]
                ]
        
        return chunks
        
    except Exception as e:
        log_event('pageindex_enrichment_error', f"PageIndex enrichment failed: {e}")
        return chunks


def _as_chunk_result(src: Dict[str, Any], *, score: float = 0.0, reason: Optional[str] = None) -> Dict[str, Any]:
    """Normalize an OpenSearch _source doc into the chunk result shape."""
    if not isinstance(src, dict):
        return {}

    out = {
        'chunk_id': src.get('chunk_id'),
        'document_id': src.get('document_id'),
        'text': src.get('text', ''),
        'text_display': src.get('text_display', src.get('text', '')),
        'chunk_type': src.get('chunk_type'),
        'parent_chunk_id': src.get('parent_chunk_id'),
        'is_proposition': src.get('is_proposition', False),
        'score': float(score),
        'file_name': src.get('file_name'),
        'file_type': src.get('file_type'),
        'page_number': src.get('page_number'),
        'section': src.get('section'),
        'section_title': src.get('section_title'),
        'content_type': src.get('content_type'),
        'entities': src.get('entities', []) or [],
        'topics': src.get('topics', []) or [],
        'doc_category': src.get('doc_category', 'GENERAL'),
        'doc_tags': src.get('doc_tags', []) or [],
        'line_start': _safe_int(src.get('line_start')),
        'line_end': _safe_int(src.get('line_end')),
        'identifiers': src.get('identifiers', []) or [],
        'heading_hierarchy': src.get('heading_hierarchy') or src.get('heading_path'),
        'pageindex_node_id': src.get('pageindex_node_id'),
        'mentioned_identifiers': src.get('mentioned_identifiers', []) or [],
    }
    if reason:
        out['retrieval_reason'] = reason
    return out


def expand_linked_chunks(
    chunk_ids: List[str],
    *,
    reason: str,
    score: float = 0.0,
) -> List[Dict[str, Any]]:
    fetched = _fetch_chunks_by_chunk_id(chunk_ids)
    if not fetched:
        return []
    out: List[Dict[str, Any]] = []
    for cid in chunk_ids:
        src = fetched.get(cid)
        if not src:
            continue
        out.append(_as_chunk_result(src, score=score, reason=reason))
    return out


# ============================================================================
# LAMBDA HANDLER - ENTERPRISE ORCHESTRATION
# ============================================================================

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Enterprise Query Processor - Main Entry Point
    
    Orchestrates the complete RAG pipeline with all Phase 1.5 enhancements.
    
    INPUT SCHEMA:
    {
        "query": "What is TSS error 401?",           # REQUIRED
        "user_id": "user@bmo.com",                   # For memory (optional)
        "session_id": "uuid-session-id",            # For memory (optional)
        "top_k": 8,                                  # Override retrieval count
        "filters": {                                 # Hard filters
            "file_type": "pdf",
            "document_id": "doc-123",
            "department": "mainframe"
        },
        "options": {
            "include_explanation": true,            # Include retrieval metadata
            "enable_hybrid": true,                  # Override feature flags
            "enable_intent": true,
            "enable_expansion": true,
            "enable_memory": true
        },
        "system_prompt": "..."                      # Custom prompt override
    }
    
    OUTPUT SCHEMA:
    {
        "statusCode": 200,
        "body": {
            "answer": "TSS error 401 indicates...",
            "sources": [...],
            "source_count": 8,
            "intent": "troubleshooting",
            "retrieval_explanation": {...},         # If include_explanation=true
            "metrics": {
                "total_ms": 2500,
                "breakdown": {...},
                "tokens": {...}
            }
        }
    }
    """
    start_time = time.time()
    timing = {}
    
    # Set correlation ID for distributed tracing
    request_id = context.aws_request_id if context else f"local-{int(time.time())}"
    set_correlation_id(request_id)
    
    log_event('request_start', 'Query Processor invoked', request_id=request_id)
    
    # Log Model Configuration
    log_event('model_config', 
              f"Using Models - Generation: {Config.GENERATION_MODEL}, "
              f"Embedding: {Config.EMBEDDING_MODEL}, "
              f"Haiku: {Config.HAIKU_MODEL}")
    
    try:
        try:
            event = QueryEvent.model_validate(event).model_dump()
        except ValidationError as ve:
            log_event('validation_error', 'Invalid input payload', errors=ve.errors())
            send_to_dlq(event if isinstance(event, dict) else {'event': str(event)}, ve, 'validation_error')
            return _error_response(400, 'Invalid input payload')

        # =====================================================================
        # CONFIGURATION VALIDATION
        # =====================================================================
        valid, error_msg = Config.validate()
        if not valid:
            log_event('config_error', error_msg)
            return _error_response(500, error_msg)
        
        # =====================================================================
        # PARAMETER EXTRACTION
        # =====================================================================
        original_query = event.get('query', '').strip()
        user_id = event.get('user_id', 'anonymous')
        session_id = event.get('session_id', f"session-{hashlib.md5(user_id.encode()).hexdigest()[:8]}")
        top_k = event.get('top_k', Config.TOP_K)
        filters = event.get('filters')
        if not isinstance(filters, dict):
            filters = {}
        options = event.get('options')
        if not isinstance(options, dict):
            options = {}
        custom_prompt = event.get('system_prompt')
        persona_config = event.get('persona_config')
        if not isinstance(persona_config, dict):
            persona_config = {}
        
        # Feature flag overrides from request
        enable_hybrid = options.get('enable_hybrid', Config.ENABLE_HYBRID_SEARCH)
        enable_intent = options.get('enable_intent', Config.ENABLE_INTENT_CLASSIFICATION)
        enable_expansion = options.get('enable_expansion', Config.ENABLE_QUERY_EXPANSION)
        enable_memory = options.get('enable_memory', Config.ENABLE_MEMORY)
        include_explanation = options.get('include_explanation', Config.ENABLE_EXPLANATIONS)
        
        # Validation
        if not original_query:
            return _error_response(400, "Missing required parameter: query")
        
        if not isinstance(top_k, int) or top_k < 1 or top_k > 50:
            top_k = Config.TOP_K

        if isinstance(persona_config, dict):
            persona_top_k = persona_config.get('top_k')
            if persona_top_k:
                top_k = max(top_k, int(persona_top_k))
        
        log_event('parameters_parsed', f"Query: {original_query[:100]}...",
                  user_id=user_id, session_id=session_id, top_k=top_k)
        
        # =====================================================================
        # QUERY CACHE CHECK (Early Return if Cached)
        # =====================================================================
        extracted_entities = []  # Will be populated after entity extraction
        
        if Config.ENABLE_QUERY_CACHE:
            cache_key = generate_query_cache_key(original_query, user_id, filters)
            cached_response = get_cached_query_response(cache_key)
            
            if cached_response:
                log_event('cache_hit', f"Returning cached response for query")
                cached_response['body']['from_cache'] = True
                cached_response['body']['cache_key'] = cache_key
                return cached_response
        
        # =====================================================================
        # PHASE 1: CONVERSATIONAL MEMORY & CONTEXT RESOLUTION
        # =====================================================================
        memory_start = time.time()
        
        history = []
        query_resolution = None
        resolved_query = original_query
        
        if enable_memory:
            history = get_conversation_history(session_id)
            resolved_query, query_resolution = resolve_contextual_references(
                original_query, history
            )
            if resolved_query != original_query:
                log_event(
                    'query_resolved',
                    'Resolved contextual references',
                    original=_truncate_text(original_query, 200),
                    resolved=_truncate_text(resolved_query, 200),
                    resolution=query_resolution
                )
        
        timing['memory_ms'] = int((time.time() - memory_start) * 1000)

        # =====================================================================
        # PHASE 1.5: MAINFRAME ENTITY EXTRACTION
        # =====================================================================
        if Config.ENABLE_ENTITY_EXTRACTION:
            entity_start = time.time()
            extracted_entities = extract_mainframe_entities(resolved_query)
            timing['entity_extraction_ms'] = int((time.time() - entity_start) * 1000)
            
            if extracted_entities:
                log_event('entities_extracted', 
                          f"Found {len(extracted_entities)} entities: "
                          f"{[e.value for e in extracted_entities[:5]]}")

        # =====================================================================
        # PHASE 2-5: PARALLEL LLM EXECUTION (Phase 2 Enhancement)
        # =====================================================================
        # Intent classification, embedding generation, and requirements extraction
        # can all run in parallel since they only depend on resolved_query
        parallel_start = time.time()

        # Define tasks that can run in parallel
        parallel_tasks = {}

        if enable_intent:
            parallel_tasks['intent'] = lambda: classify_query_intent(resolved_query)

        # Embedding always needed
        parallel_tasks['embedding'] = lambda: generate_query_embedding(resolved_query)

        # Requirements extraction
        parallel_tasks['requirements'] = lambda: extract_query_requirements(resolved_query)

        # Execute in parallel
        parallel_results = execute_parallel_llm_tasks(parallel_tasks)

        timing['parallel_ms'] = int((time.time() - parallel_start) * 1000)

        # Extract results from parallel execution
        intent = QueryIntent.OPEN_ENDED
        intent_confidence = 0.5
        intent_keywords = []

        if enable_intent and parallel_results.get('intent'):
            intent, intent_confidence, intent_keywords = parallel_results['intent']

        intent_config = INTENT_CONFIG.get(intent, INTENT_CONFIG[QueryIntent.OPEN_ENDED])

        # Adjust top_k based on intent
        if intent_config.get('top_k'):
            top_k = max(top_k, intent_config['top_k'])

        query_embedding = parallel_results.get('embedding')
        if not query_embedding:
            return _error_response(500, "Failed to generate query embedding")

        query_requirements = parallel_results.get('requirements') or {
            'required_topics': [],
            'key_terms': resolved_query.split()[:10],
            'expected_doc_types': [],
            'importance_indicators': [],
            'multi_doc_required': False
        }
        multi_doc_required = query_requirements.get('multi_doc_required', False)

        # =====================================================================
        # PHASE 2.5: ADVANCED RAG ENHANCEMENTS
        # =====================================================================
        
        # Query Decomposition for complex multi-part questions
        sub_queries = []
        if ENABLE_QUERY_DECOMPOSITION:
            decomposition_start = time.time()
            sub_queries = decompose_complex_query(resolved_query)
            timing['decomposition_ms'] = int((time.time() - decomposition_start) * 1000)
            if sub_queries:
                log_event('query_decomposed', f"Split into {len(sub_queries)} sub-queries")
        
        # HyDE: Generate hypothetical document embedding for better retrieval
        hyde_embedding = None
        if ENABLE_HYDE:
            hyde_start = time.time()
            hyde_embedding = generate_hyde_embedding(resolved_query)
            timing['hyde_ms'] = int((time.time() - hyde_start) * 1000)
            if hyde_embedding:
                log_event('hyde_generated', 'Using HyDE embedding for retrieval')

        # =====================================================================
        # PHASE 3: QUERY EXPANSION (depends on intent keywords)
        # =====================================================================
        expansion_start = time.time()

        expanded_queries = [resolved_query]
        if enable_expansion:
            expanded_queries = expand_query_terms(resolved_query, intent_keywords)
        log_event(
            'expansion_result',
            f"Expanded queries: {len(expanded_queries)}",
            queries=[_truncate_text(q, 200) for q in expanded_queries[:4]]
        )

        timing['expansion_ms'] = int((time.time() - expansion_start) * 1000)

        # =====================================================================
        # PHASE 5.5: MECH DOMAIN DETECTION AND FILTERING (Layer 5 Integration)
        # =====================================================================
        mech_domain, domain_filters = detect_mech_domain(resolved_query)
        required_literals = extract_required_literals(resolved_query)

        persona_filters = {}
        if isinstance(persona_config, dict):
            persona_filters = persona_config.get('metadata_filters', {}) or {}
            filters = merge_filters(filters, persona_filters)
        
        # Merge domain filters with user-provided filters
        if domain_filters:
            log_event('domain_detected', 
                      f"MECH domain: {mech_domain.value}, filters: {domain_filters}")
            filters = merge_filters(filters, domain_filters)
        
        log_event('required_literals', f"Required for validation: {required_literals}")

        # =====================================================================
        # PHASE 6: HYBRID RETRIEVAL + SEMANTIC RE-RANKING
        # =====================================================================
        search_start = time.time()

        # Phase 4: Detect retrieval mode (CODE/DOC/MIXED/AUTO)
        retrieval_mode, routing_hints = detect_retrieval_mode(resolved_query, intent)
        log_event('retrieval_mode_detected', 
                  f"Mode: {retrieval_mode.value}, strategy: {routing_hints.get('retrieval_strategy')}")
        
        # Apply mode-specific filters
        mode_filters = {}
        if routing_hints.get('chunk_type_filter'):
            mode_filters['chunk_type'] = routing_hints['chunk_type_filter']
        if routing_hints.get('chunk_type_filter_exclude'):
            mode_filters['chunk_type_ne'] = routing_hints['chunk_type_filter_exclude']
        
        if mode_filters:
            filters = merge_filters(filters, mode_filters)
        
        # Use mode-specific content_type_boost if provided
        mode_content_boost = routing_hints.get('content_type_boost') or intent_config.get('content_type_boost')

        log_event(
            'retrieval_plan',
            'Starting retrieval',
            enable_hybrid=enable_hybrid,
            top_k=top_k,
            filters=filters,
            retrieval_mode=retrieval_mode.value,
            sub_queries=len(sub_queries) if sub_queries else 0,
            expanded_queries=len(expanded_queries)
        )
        
        # Use HyDE embedding if available, otherwise standard embedding
        search_embedding = hyde_embedding if hyde_embedding else query_embedding
        
        # Handle query decomposition: search for each sub-query and merge
        if sub_queries and len(sub_queries) > 1:
            all_chunks = []
            seen_chunk_ids = set()
            
            for sub_q in sub_queries[:3]:  # Limit to 3 sub-queries
                log_event('subquery_retrieval', 'Searching sub-query', sub_query=_truncate_text(sub_q, 200))
                sub_embedding = generate_query_embedding(sub_q)
                if not sub_embedding:
                    continue
                    
                if enable_hybrid:
                    sub_chunks = execute_hybrid_search(
                        query_text=sub_q,
                        query_embedding=sub_embedding,
                        top_k=top_k,
                        content_type_boost=mode_content_boost,
                        expanded_queries=[sub_q],
                        filters=filters
                    )
                else:
                    sub_chunks = execute_semantic_search(sub_embedding, top_k, filters)
                
                # Deduplicate chunks across sub-queries
                for chunk in sub_chunks:
                    chunk_id = chunk.get('chunk_id') or chunk.get('document_id', '') + str(chunk.get('page_number', ''))
                    if chunk_id not in seen_chunk_ids:
                        seen_chunk_ids.add(chunk_id)
                        all_chunks.append(chunk)
            
            chunks = all_chunks[:top_k * 2]  # Keep top_k*2 for re-ranking
            log_event('decomposed_search', f"Merged {len(chunks)} chunks from {len(sub_queries)} sub-queries")
        else:
            # Two-Stage Retrieval:
            # Stage 1: Broad retrieval with generous top_k to maximize recall
            # Stage 2: Score-based filtering to narrow to high-quality candidates
            log_event('primary_retrieval', 'Stage 1: Broad retrieval', query=_truncate_text(resolved_query, 200))
            stage1_top_k = top_k * 3  # Over-fetch broadly for recall
            if enable_hybrid:
                chunks = execute_hybrid_search(
                    query_text=resolved_query,
                    query_embedding=search_embedding,
                    top_k=stage1_top_k,
                    content_type_boost=mode_content_boost,
                    expanded_queries=expanded_queries,
                    filters=filters
                )
            else:
                chunks = execute_semantic_search(search_embedding, stage1_top_k, filters)
            
            # Stage 2: Filter to top candidates by score threshold
            if chunks and len(chunks) > top_k:
                # Keep chunks scoring above 60% of the top score, bounded by top_k*2
                top_score = max(c.get('score', 0) for c in chunks) if chunks else 0
                score_threshold = top_score * 0.6
                stage2_chunks = [c for c in chunks if c.get('score', 0) >= score_threshold]
                # Ensure we keep at least top_k, at most top_k*2
                if len(stage2_chunks) < top_k:
                    stage2_chunks = chunks[:top_k]
                else:
                    stage2_chunks = stage2_chunks[:top_k * 2]
                log_event('two_stage_filter', 
                          f"Stage 2: {len(chunks)}→{len(stage2_chunks)} chunks (threshold={score_threshold:.3f})")
                chunks = stage2_chunks

        # Phase 2: Identifier-first narrowing + doc↔code expansion (best-effort)
        try:
            code_focused = _is_code_focused_query(resolved_query, intent)
            candidate_identifiers = extract_candidate_identifiers(resolved_query) if code_focused else []

            if Config.ENABLE_IDENTIFIER_NARROWING and code_focused and candidate_identifiers:
                log_event(
                    'identifier_narrowing_start',
                    f"Identifiers: {candidate_identifiers[:6]}",
                    identifiers=candidate_identifiers[:Config.IDENTIFIER_NARROWING_MAX_IDS]
                )

                existing_ids = {c.get('chunk_id') for c in chunks if c.get('chunk_id')}

                # A) Narrowed code search: chunk_type=code + identifiers
                code_filters = merge_filters(filters, {
                    'chunk_type': 'code',
                    'identifiers': candidate_identifiers,
                })
                narrowed_code = execute_hybrid_search(
                    query_text=resolved_query,
                    query_embedding=search_embedding,
                    top_k=min(max(6, top_k), 20),
                    expanded_queries=expanded_queries,
                    filters=code_filters,
                )

                # Fallback if identifiers aren't indexed for this corpus
                if not narrowed_code:
                    code_filters = merge_filters(filters, {'chunk_type': 'code'})
                    narrowed_code = execute_hybrid_search(
                        query_text=resolved_query,
                        query_embedding=search_embedding,
                        top_k=min(max(6, top_k), 20),
                        expanded_queries=expanded_queries,
                        filters=code_filters,
                    )

                for c in narrowed_code:
                    cid = c.get('chunk_id')
                    if cid and cid not in existing_ids:
                        c['retrieval_reason'] = c.get('retrieval_reason') or 'identifier_narrowed_code'
                        chunks.append(c)
                        existing_ids.add(cid)

                # B) Linked documentation: mentioned_identifiers (OpenSearch-only fallback)
                doc_filters = merge_filters(filters, {
                    'chunk_type_ne': ['code'],
                    'mentioned_identifiers': candidate_identifiers,
                })
                mentioned_docs = execute_hybrid_search(
                    query_text=' '.join(candidate_identifiers[:6]),
                    query_embedding=search_embedding,
                    top_k=min(max(6, top_k), 20),
                    expanded_queries=[' '.join(candidate_identifiers[:6])],
                    filters=doc_filters,
                )
                for c in mentioned_docs:
                    cid = c.get('chunk_id')
                    if cid and cid not in existing_ids:
                        c['retrieval_reason'] = c.get('retrieval_reason') or 'identifier_mentioned_doc'
                        chunks.append(c)
                        existing_ids.add(cid)

                # C) DynamoDB link expansion (optional): fetch linked chunk_ids
                document_id_filter = filters.get('document_id') if isinstance(filters, dict) else None
                links = lookup_doc_code_links(candidate_identifiers, document_id=document_id_filter)
                if links:
                    linked_doc_ids: List[str] = []
                    linked_code_ids: List[str] = []
                    for payload in links.values():
                        linked_doc_ids.extend(payload.get('doc_chunk_ids') or [])
                        linked_code_ids.extend(payload.get('code_chunk_ids') or [])

                    # Keep it bounded and deterministic
                    linked_doc_ids = list(dict.fromkeys([x for x in linked_doc_ids if x]))[:Config.LINK_EXPANSION_MAX_CHUNKS]
                    linked_code_ids = list(dict.fromkeys([x for x in linked_code_ids if x]))[:Config.LINK_EXPANSION_MAX_CHUNKS]

                    expanded = []
                    if linked_doc_ids:
                        expanded.extend(expand_linked_chunks(linked_doc_ids, reason='doc_code_link_doc'))
                    if linked_code_ids:
                        expanded.extend(expand_linked_chunks(linked_code_ids, reason='doc_code_link_code'))

                    for c in expanded:
                        cid = c.get('chunk_id')
                        if cid and cid not in existing_ids:
                            chunks.append(c)
                            existing_ids.add(cid)

                log_event(
                    'identifier_narrowing_complete',
                    f"Augmented chunks: {len(chunks)}",
                    code_hits=len(narrowed_code or []),
                    mentioned_docs=len(mentioned_docs or []),
                    ddb_links=len(links or {})
                )
        except Exception as e:
            log_event('identifier_narrowing_error', f"Identifier narrowing failed: {e}")
        
        # Phase 5: Multi-hop retrieval for complex queries
        multi_hop_result = None
        is_complex = (
            len(sub_queries) > 1 or 
            intent in [QueryIntent.COMPARISON, QueryIntent.TROUBLESHOOTING] or
            multi_doc_required
        )
        
        if ENABLE_MULTI_HOP_RETRIEVAL and is_complex and chunks:
            multi_hop_start = time.time()
            multi_hop_result = multi_hop_retrieval_loop(
                query=resolved_query,
                initial_chunks=chunks,
                query_embedding=query_embedding,
                top_k=top_k
            )
            timing['multi_hop_ms'] = int((time.time() - multi_hop_start) * 1000)
            
            if multi_hop_result.get('chunks'):
                chunks = multi_hop_result['chunks']
                log_event('multi_hop_applied', 
                          f"Expanded to {len(chunks)} chunks after {multi_hop_result.get('iterations', 0)} hops")
        
        # Apply GENERIC semantic re-ranking based on content matching
        # NO hardcoded document categories - uses dynamic term/topic matching
        chunks = semantic_rerank(resolved_query, chunks, 
                                 query_requirements=query_requirements, 
                                 top_k=top_k)
        
        # Entity-Aware Reranking: Boost chunks containing query entities
        if Config.ENABLE_ENTITY_EXTRACTION and extracted_entities:
            entity_rerank_start = time.time()
            chunks = entity_aware_rerank(resolved_query, chunks, extracted_entities, top_k)
            timing['entity_rerank_ms'] = int((time.time() - entity_rerank_start) * 1000)
            log_event('entity_reranked', 
                      f"Reranked based on {len(extracted_entities)} entities")
        
        # Cross-Encoder Reranking: LLM-based fine-grained relevance scoring
        if ENABLE_CROSS_ENCODER:
            rerank_start = time.time()
            chunks = cross_encoder_rerank(resolved_query, chunks, top_n=top_k)
            timing['cross_encoder_ms'] = int((time.time() - rerank_start) * 1000)
            log_event('cross_encoder_reranked', f"Reranked to {len(chunks)} chunks")

        # PageIndex Navigation: Enrich chunks with section hierarchy context
        if Config.ENABLE_PAGEINDEX_NAVIGATION:
            try:
                pageindex_start = time.time()
                chunks = enrich_chunks_with_pageindex(chunks, resolved_query)
                timing['pageindex_ms'] = int((time.time() - pageindex_start) * 1000)
            except Exception as e:
                log_event('pageindex_enrichment_skip', f"PageIndex enrichment failed: {e}")

        # Task 3.2: Apply persona knowledge_weights to boost relevant content types
        if isinstance(persona_config, dict):
            knowledge_weights = persona_config.get('knowledge_weights')
            if knowledge_weights and isinstance(knowledge_weights, dict):
                weight_start = time.time()
                chunks = apply_knowledge_weights(chunks, knowledge_weights)
                timing['knowledge_weights_ms'] = int((time.time() - weight_start) * 1000)

        if isinstance(persona_config, dict):
            persona_min_score = persona_config.get('min_relevance_score')
            if persona_min_score is not None:
                threshold = float(persona_min_score)
                chunks = [
                    c for c in chunks
                    if c.get('rerank_score', c.get('score', 0)) >= threshold
                ]
        
        # Ensure document diversity for multi-document queries
        if multi_doc_required:
            chunks = ensure_document_diversity(chunks, min_unique_docs=3, top_k=top_k)
        
        timing['search_ms'] = int((time.time() - search_start) * 1000)
        
        log_event('retrieval_complete', 
                  f"Retrieved {len(chunks)} chunks after re-ranking (multi_doc={multi_doc_required})",
                  unique_docs=len(set(c.get('document_id') for c in chunks)))
        
        # =====================================================================
        # PHASE 6: CONTEXT BUILDING & RESPONSE GENERATION
        # =====================================================================
        generation_start = time.time()
        
        context_text = build_retrieval_context(chunks, include_metadata=True)

        # Task 3.3: Consistent prompt assembly order
        # Priority: 1) Persona prompt, 2) Custom prompt OR Intent prompt, 3) MECH context
        persona_prompt = None
        if isinstance(persona_config, dict):
            persona_prompt = persona_config.get('system_prompt')

        # Build base prompt: use custom_prompt if provided, otherwise intent-specific
        base_prompt = custom_prompt if custom_prompt else get_intent_system_prompt(intent)
        
        # Prepend persona prompt if present (persona context is highest priority)
        if persona_prompt:
            system_prompt_override = f"{persona_prompt}\n\n{base_prompt}"
        else:
            system_prompt_override = base_prompt

        # Optional persistent project memory (MECH_CONTEXT) as system addendum
        # Kept in the system prompt so it acts as background context, not a cited source.
        mech_context_addition = build_mech_context_system_addition()
        if mech_context_addition:
            system_prompt_override = f"{system_prompt_override}{mech_context_addition}"
        
        # Self-Consistency: Generate multiple samples for complex queries
        is_complex_query = (
            intent in [QueryIntent.COMPARISON] or
            multi_doc_required or
            len(sub_queries) > 1
        )
        
        if ENABLE_SELF_CONSISTENCY and is_complex_query:
            response = generate_with_self_consistency(
                query=resolved_query,
                context=context_text,
                intent=intent,
                custom_prompt=system_prompt_override,
                num_samples=3
            )
            timing['self_consistency_samples'] = 3
        else:
            response = generate_llm_response(
                query=resolved_query,
                context=context_text,
                intent=intent,
                custom_prompt=system_prompt_override
            )
        
        # =====================================================================
        # PHASE 6.4: SELF-RAG VERIFICATION LOOP (Persona-aware)
        # =====================================================================
        self_rag_result = None
        enable_self_rag = False
        if isinstance(persona_config, dict):
            enable_self_rag = persona_config.get('enable_self_rag', False)

        if enable_self_rag and Config.ENABLE_SELF_RAG:
            self_rag_start = time.time()
            self_rag_result = self_rag_verification_loop(
                query=resolved_query,
                initial_answer=response['answer'],
                sources=chunks,
                query_embedding=query_embedding
            )
            timing['self_rag_ms'] = int((time.time() - self_rag_start) * 1000)
            response['answer'] = self_rag_result['answer']
            chunks = self_rag_result['sources']

        # If proposition chunks were retrieved, prefer the parent chunk text for UI/citations
        chunks = resolve_proposition_citations(chunks)

        # =====================================================================
        # PHASE 6.5: GROUNDING VERIFICATION (Layer 6 Integration)
        # =====================================================================
        grounding_result = verify_answer_grounding(
            answer=response['answer'],
            context_chunks=chunks,
            query=resolved_query,
            required_literals=required_literals
        )
        
        log_event('grounding_verified', 
                  f"Status: {grounding_result['status']}, "
                  f"Groundedness: {grounding_result['groundedness']}, "
                  f"Literals found: {grounding_result['literals_found']}")
        
        # Add warning if answer is poorly grounded
        if grounding_result['should_add_warning']:
            response['answer'] = f"{response['answer']}\n\n_{grounding_result['warning_message']}_"
        
        # =====================================================================
        # PHASE 6.75: CONFIDENCE CALIBRATION
        # =====================================================================
        confidence_result = None
        if ENABLE_CONFIDENCE_CALIBRATION:
            confidence_start = time.time()
            confidence_result = calculate_answer_confidence(
                answer=response['answer'],
                context_chunks=chunks,
                query=resolved_query
            )
            timing['confidence_ms'] = int((time.time() - confidence_start) * 1000)
            
            # Apply confidence-based calibration to the answer
            response['answer'] = apply_confidence_calibration(
                answer=response['answer'],
                confidence =confidence_result
                
            )
            log_event('confidence_calibrated', 
                      f"Confidence: {confidence_result['score']:.2f}, Action: {confidence_result['action']}")
        
        timing['generation_ms'] = int((time.time() - generation_start) * 1000)
        
        # =====================================================================
        # PHASE 7: ASYNC MEMORY STORAGE
        # =====================================================================
        if enable_memory:
            entities = list(set(
                intent_keywords + 
                [e for c in chunks for e in c.get('entities', [])[:3]]
            ))[:20]
            
            # Task 3.4: Extract persona ID for memory persistence
            persona_id = None
            if isinstance(persona_config, dict):
                persona_id = persona_config.get('id') or persona_config.get('name')
            
            store_conversation_async(
                user_id=user_id,
                session_id=session_id,
                query=original_query,
                answer=response['answer'],
                sources=chunks,
                intent=intent.value,
                entities=entities,
                persona=persona_id
            )
        
        # =====================================================================
        # PHASE 8: RESPONSE ASSEMBLY
        # =====================================================================
        total_time = int((time.time() - start_time) * 1000)
        timing['total_ms'] = total_time
        
        # Build source citations
        sources = [
            {
                'file_name': c.get('file_name'),
                'document_id': c.get('document_id'),
                'page_number': c.get('page_number'),
                'section_title': c.get('section_title'),
                'content_type': c.get('content_type'),
                'relevance_score': c.get('score'),
                'text_display': c.get('text_display'),
                'entities': c.get('entities', [])[:5],
                'match_highlight': c.get('highlight'),
                'chunk_id': c.get('chunk_id'),
                'chunk_type': c.get('chunk_type'),
                'line_start': _safe_int(c.get('line_start')),
                'line_end': _safe_int(c.get('line_end')),
                'identifiers': (c.get('identifiers') or [])[:20],
                'heading_hierarchy': c.get('heading_hierarchy'),
                'pageindex_node_id': c.get('pageindex_node_id'),
                'mentioned_identifiers': (c.get('mentioned_identifiers') or [])[:20],
                'retrieval_reason': c.get('retrieval_reason'),
            }
            for c in chunks
        ]
        
        result = {
            'statusCode': 200,
            'body': {
                'answer': response['answer'],
                'sources': sources,
                'source_count': len(chunks),
                'intent': intent.value,
                'intent_confidence': round(intent_confidence, 2),
                'query_resolved': resolved_query if resolved_query != original_query else None,
                'mech_domain': mech_domain.value,
                'retrieval_mode': retrieval_mode.value,  # Phase 4: Explicit retrieval routing
                'grounding': {
                    'status': grounding_result['status'],
                    'groundedness_score': grounding_result['groundedness'],
                    'citations_found': grounding_result['citations_found'],
                    'literals_found': grounding_result['literals_found'],
                    'literals_missing': grounding_result['literals_missing']
                },
                'confidence': {
                    'score': round(confidence_result['score'], 3) if confidence_result else None,
                    'calibrated': ENABLE_CONFIDENCE_CALIBRATION
                },
                'advanced_rag': {
                    'hyde_used': ENABLE_HYDE and hyde_embedding is not None,
                    'query_decomposition': len(sub_queries) if sub_queries else 0,
                    'cross_encoder_reranked': ENABLE_CROSS_ENCODER,
                    'self_consistency_used': ENABLE_SELF_CONSISTENCY and is_complex_query,
                    'self_rag_used': bool(self_rag_result),
                    'multi_hop_used': bool(multi_hop_result),  # Phase 5
                    'multi_hop_iterations': multi_hop_result.get('iterations', 0) if multi_hop_result else 0,
                    'entity_extraction_used': Config.ENABLE_ENTITY_EXTRACTION,
                    'entities_found': len(extracted_entities) if extracted_entities else 0,
                    'query_cache_enabled': Config.ENABLE_QUERY_CACHE
                },
                'extracted_entities': format_entities_for_display(extracted_entities) if extracted_entities else [],
                'metrics': {
                    'total_ms': total_time,
                    'breakdown': timing,
                    'tokens': {
                        'input': response.get('input_tokens', 0),
                        'output': response.get('output_tokens', 0)
                    },
                    'model': response.get('model'),
                    # Phase 2 Enhancement: Include cache stats
                    'cache_stats': get_cache_stats()
                }
            }
        }

        # Phase 2: Emit final request metrics
        emit_metrics_batch([
            {'name': 'QueryLatency', 'value': total_time, 'unit': 'Milliseconds'},
            {'name': 'SourceCount', 'value': len(chunks)},
            {'name': 'TokensUsed', 'value': response.get('input_tokens', 0) + response.get('output_tokens', 0)},
            {'name': 'QuerySuccess', 'value': 1}
        ])
        
        # Include retrieval explanation if requested
        if include_explanation:
            result['body']['retrieval_explanation'] = build_retrieval_explanation(
                query=original_query,
                resolved_query=resolved_query,
                query_resolution=query_resolution,
                intent=intent,
                intent_confidence=intent_confidence,
                expanded_queries=expanded_queries,
                chunks=chunks,
                timing=timing
            )
        
        log_event('request_complete', f"Success in {total_time}ms",
                  sources=len(chunks), intent=intent.value)
        
        # Store result in cache for future identical queries
        if Config.ENABLE_QUERY_CACHE:
            cache_key = generate_query_cache_key(original_query, user_id, filters)
            cache_query_response(
                cache_key=cache_key,
                query=original_query,
                response_data=result,
                groundedness=grounding_result.get('groundedness') if grounding_result else 0.0
                )
            log_event('cache_stored', f"Response cached with TTL {Config.CACHE_TTL_HOURS}h")
        
        return result
        
    except (ClientError, BotoCoreError) as e:
        log_event('request_error', f"AWS service error: {e}")
        send_to_dlq(event if isinstance(event, dict) else {'event': str(event)}, e, 'aws_error')
        return _error_response(502, str(e))

    except Exception as e:
        log_event('request_error', f"Unhandled exception: {e}")
        logger.exception("Query processing failed")
        send_to_dlq(event if isinstance(event, dict) else {'event': str(event)}, e, 'unexpected_error')
        return _error_response(500, str(e))


def _error_response(status_code: int, message: str) -> Dict[str, Any]:
    """Build standardized error response."""
    return {
        'statusCode': status_code,
        'body': {
            'error': message,
            'correlation_id': _correlation_id,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    }

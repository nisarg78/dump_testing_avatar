"""
Lambda 08: Unified API Handler + 6 Personas
============================================
Production API layer for MECH Avatar Phase 2.

Consolidates:
    - API Gateway routing 
    - Persona routing logic 
    - Document upload handling

Trigger:
    - API Gateway REST API (all endpoints)

Endpoints:
    - POST /chat/query → Routes to Lambda 06 with persona context
    - GET /chat/history/{session_id} → Fetch conversation history
    - POST /documents/upload → Generate presigned S3 URL
    - GET /documents/status/{document_id} → Check processing status
    - GET /admin/analytics → Basic metrics (admin only)

Features:
    - 6 Embedded Personas with specialized prompts
    - JWT validation (Cognito)
    - CORS handling
    - Rate limiting (10 queries/min per user)
    - Presigned URL generation

Configuration:
    - Memory: 512 MB
    - Timeout: 30 seconds
    - Runtime: Python 3.12
    - Layers: None (boto3 only)

Environment Variables:
    - AWS_DEFAULT_REGION: AWS region (default: ca-central-1)
    - LAMBDA_06_ARN: Lambda 06 Query Processor ARN
    - S3_BUCKET_NAME: Document upload bucket
    - COGNITO_USER_POOL_ID: Cognito User Pool ID
    - COGNITO_APP_CLIENT_ID: Cognito App Client ID
    - DOCUMENTS_TABLE: DynamoDB documents table
    - MEMORY_TABLE: DynamoDB memory table
    - RATE_LIMIT_TABLE: DynamoDB rate limit table
    - ALLOWED_ORIGINS: Comma-separated allowed origins for CORS

Author: MECH Avatar Team | Version: 2.0.0 | Phase: 2
"""

import json
import boto3
import os
import logging
import hashlib
import hmac
import time
import base64
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Tuple
from decimal import Decimal

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_correlation_id: Optional[str] = None


def set_correlation_id(request_id: str) -> None:
    """Set correlation ID for request tracing."""
    global _correlation_id
    _correlation_id = request_id


def log_event(event_type: str, message: str, **kwargs) -> None:
    """Structured logging with correlation ID."""
    log_data = {
        'correlation_id': _correlation_id,
        'event': event_type,
        'message': message,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'lambda': 'lambda_08_unified_api_handler',
        **kwargs
    }
    logger.info(json.dumps(log_data))


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Centralized configuration."""
    
    AWS_REGION = os.environ.get('AWS_DEFAULT_REGION', 'ca-central-1')
    
    # Lambda ARNs
    LAMBDA_06_ARN = os.environ.get('LAMBDA_06_ARN', '')
    LAMBDA_07_ARN = os.environ.get('LAMBDA_07_ARN', '')  # Memory Writer (async)
    
    # S3 - Updated to new naming convention
    S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'mechavatar-lab-cac1-s3-inbound')
    
    # Cognito
    COGNITO_USER_POOL_ID = os.environ.get('COGNITO_USER_POOL_ID', '')
    COGNITO_APP_CLIENT_ID = os.environ.get('COGNITO_APP_CLIENT_ID', '')
    
    # DynamoDB Tables - Updated to new naming convention
    DOCUMENTS_TABLE = os.environ.get('DOCUMENTS_TABLE', 'mechavatar-lab-cac1-mech-documents-metadata')
    MEMORY_TABLE = os.environ.get('MEMORY_TABLE', 'mechavatar-lab-cac1-mech-chunks-metadata')
    SESSIONS_TABLE = os.environ.get('SESSIONS_TABLE', 'mechavatar-lab-cac1-user-sessions')
    RATE_LIMIT_TABLE = os.environ.get('RATE_LIMIT_TABLE', 'mechavatar-lab-cac1-rate-limits')
    
    # Auth (DynamoDB-backed — replaces Cognito/Azure AD)
    USERS_TABLE         = os.environ.get('USERS_TABLE', 'mechavatar-lab-cac1-users')
    # ⚠️  Set a strong random secret in production — store in AWS SSM / Secrets Manager
    JWT_SECRET          = os.environ.get('JWT_SECRET', 'change-me-in-production-use-ssm')
    JWT_EXPIRY_SECONDS  = int(os.environ.get('JWT_EXPIRY_SECONDS', '28800'))  # 8 h

    # CORS
    ALLOWED_ORIGINS = os.environ.get(
        'ALLOWED_ORIGINS', 
        'http://localhost:4200,https://mech-avatar-lab.bmo.com'
    ).split(',')
    
    # Rate Limiting
    RATE_LIMIT_REQUESTS = int(os.environ.get('RATE_LIMIT_REQUESTS', 10))
    RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get('RATE_LIMIT_WINDOW_SECONDS', 60))
    
    # File Upload
    MAX_FILE_SIZE_MB = int(os.environ.get('MAX_FILE_SIZE_MB', 100))
    PRESIGNED_URL_EXPIRY = int(os.environ.get('PRESIGNED_URL_EXPIRY', 300))  # 5 minutes
    
    ALLOWED_FILE_EXTENSIONS = [
        # Documents
        'pdf', 'docx', 'doc', 'xlsx', 'xls', 'pptx', 'ppt',
        'txt', 'csv', 'html', 'xml', 'json', 'md',
        # Mainframe source code (COBOL, JCL, Assembler, etc.)
        'asm', 'hlasm', 'cbl', 'cob', 'cpy', 'jcl', 'proc', 'pli', 'rexx',
        # Images
        'png', 'jpg', 'jpeg', 'gif', 'bmp', 'tiff'
    ]


# ============================================================================
# 6 PERSONAS CONFIGURATION (EMBEDDED)
# ============================================================================

PERSONAS: Dict[str, Dict[str, Any]] = {
    "developer": {
        "id": "developer",
        "name": "MECH Coder",
        "description": "Technical questions about COBOL, JCL, Assembler code, and program logic",
        "system_prompt": """You are MECH Coder, a MECH mainframe developer assistant specialized in technical documentation.

FOCUS AREAS:
- COBOL program analysis and code structure
- JCL job configurations and execution flow
- Assembler routines and low-level operations
- Database access patterns (DB2, IMS, VSAM)
- Copy books, procedures, and include files
- Program-to-program calls and linkage conventions

RESPONSE STYLE:
- Provide code examples when relevant
- Include line numbers and file references
- Explain technical concepts clearly
- Reference specific copybooks and procedures
- Use technical terminology appropriately

GROUNDING RULES:
- Only answer based on retrieved documentation
- If information is not in the context, say so clearly
- Always cite source documents with file names""",
        "metadata_filters": {
            #"document_type": ["technical_spec", "program", "procedure", "copybook"],
            #"folder": ["Mech_Documentations", "Mech Software Documentation"]
        },
        "knowledge_weights": {
            "procedures": 2.0,
            "technical_specs": 1.5,
            "programs": 2.0,
            "copybooks": 1.8
        },
        "top_k": 15,
        "min_relevance_score": 0.6,
        "enable_self_rag": False
    },
    
    "business_analyst": {
        "id": "business_analyst",
        "name": "MECH Researcher",
        "description": "Business requirements, functional specifications, and workflow analysis",
        "system_prompt": """You are MECH Researcher, a MECH business analyst assistant focused on requirements and specifications.

FOCUS AREAS:
- Business requirements and functional specifications
- Process flows and workflow documentation
- Data dictionaries and field definitions
- Business rules and validation logic
- Integration points and interfaces
- Change management documentation

RESPONSE STYLE:
- Focus on business logic and workflows
- Explain in business terms, not technical jargon
- Reference functional specs and requirements docs
- Highlight business rules and constraints
- Provide context on system interactions

GROUNDING RULES:
- Only answer based on retrieved documentation
- If information is not in the context, say so clearly
- Always cite source documents""",
        "metadata_filters": {
            "document_type": ["function_spec", "business_requirement", "data_dictionary"],
            "folder": ["A-02-Function Specifications", "Mech Data Dictionary"]
        },
        "knowledge_weights": {
            "function_specs": 2.0,
            "business_docs": 1.5,
            "data_dictionaries": 1.8
        },
        "top_k": 12,
        "min_relevance_score": 0.65,
        "enable_self_rag": False
    },
    
    "qa_tester": {
        "id": "qa_tester",
        "name": "MECH Validator",
        "description": "Test plans, validation procedures, error handling, and quality assurance",
        "system_prompt": """You are MECH Validator, a MECH QA tester assistant specialized in testing documentation.

FOCUS AREAS:
- Test plans and test cases
- Validation procedures and checklists
- Error codes and handling procedures
- Regression testing documentation
- Data validation rules
- Edge cases and boundary conditions

RESPONSE STYLE:
- Be precise about test conditions
- Reference specific error codes and messages
- Include expected vs actual behaviors
- Highlight validation rules
- Mention test data requirements

GROUNDING RULES:
- Only answer based on retrieved documentation
- If information is not in the context, say so clearly
- Always cite source documents""",
        "metadata_filters": {
            "document_type": ["test_doc", "error_table", "validation_spec"],
            "folder": ["Mech CDIC FID Playbooks", "Maintenance Shared Documents"]
        },
        "knowledge_weights": {
            "test_docs": 2.0,
            "error_tables": 1.8,
            "validation_specs": 1.5
        },
        "top_k": 12,
        "min_relevance_score": 0.6,
        "enable_self_rag": False
    },
    
    "support_engineer": {
        "id": "support_engineer",
        "name": "MECH Helper",
        "description": "User guides, operational procedures, troubleshooting, and maintenance",
        "system_prompt": """You are MECH Helper, a MECH support engineer assistant focused on operations and troubleshooting.

FOCUS AREAS:
- User guides and operating procedures
- Troubleshooting steps and runbooks
- Maintenance procedures and schedules
- System startup/shutdown procedures
- Common issues and resolutions
- Operational best practices

RESPONSE STYLE:
- Provide step-by-step instructions
- Use numbered lists for procedures
- Include warnings and cautions
- Reference relevant job names and schedules
- Suggest escalation paths when needed

GROUNDING RULES:
- Only answer based on retrieved documentation
- If information is not in the context, say so clearly
- Always cite source documents""",
        "metadata_filters": {
            "document_type": ["user_guide", "runbook", "maintenance_doc"],
            "folder": ["A-09-Users Guides", "Maintenance Shared Documents"]
        },
        "knowledge_weights": {
            "user_guides": 2.0,
            "runbooks": 1.8,
            "maintenance_docs": 1.5
        },
        "top_k": 10,
        "min_relevance_score": 0.55,
        "enable_self_rag": False
    },
    
    "architect": {
        "id": "architect",
        "name": "MECH Strategist",
        "description": "System architecture, integration patterns, design decisions, and technical overviews",
        "system_prompt": """You are MECH Strategist, a MECH system architect assistant focused on design and architecture.

FOCUS AREAS:
- System architecture and design patterns
- Integration interfaces and data flows
- Technical overviews and diagrams
- Design decisions and rationale
- Cross-system dependencies
- Modernization strategies

RESPONSE STYLE:
- Provide high-level overviews first, then details
- Explain system interactions and data flows
- Reference architecture diagrams when available
- Discuss design trade-offs
- Consider scalability and maintainability

GROUNDING RULES:
- Only answer based on retrieved documentation
- If information is not in the context, say so clearly
- Always cite source documents""",
        "metadata_filters": {
            "document_type": ["technical_overview", "design_spec", "architecture_doc"],
            "folder": ["A-08-Technical Overviews", "A-16-Detailed Design Specifications"]
        },
        "knowledge_weights": {
            "design_specs": 2.0,
            "technical_overviews": 1.8,
            "architecture_docs": 1.5
        },
        "top_k": 15,
        "min_relevance_score": 0.7,
        "enable_self_rag": False
    },
    
    "compliance_officer": {
        "id": "compliance_officer",
        "name": "MECH Auditor",
        "description": "Regulatory requirements, CDIC documentation, security policies, and audit trails",
        "system_prompt": """You are MECH Auditor, a MECH compliance officer assistant focused on regulatory and security.

FOCUS AREAS:
- CDIC regulatory requirements
- Security policies and procedures
- Audit documentation and trails
- Compliance checklists and evidence
- Data classification and handling
- Change control procedures

RESPONSE STYLE:
- Use precise, formal language
- Include specific regulation references
- Cite exact document sections
- Highlight compliance requirements
- Emphasize accuracy and traceability
- ALWAYS verify answers against multiple sources

GROUNDING RULES:
- Only answer based on retrieved documentation
- If information is not in the context, say so clearly
- Always cite source documents with section numbers
- Flag any uncertainty or gaps in documentation

SPECIAL: Self-RAG verification is ENABLED for this persona to ensure accuracy.""",
        "metadata_filters": {
            "document_type": ["compliance_doc", "regulatory_requirement", "security_policy"],
            "folder": ["CDIC Requirements Docmentation", "Mech Security Documentation"]
        },
        "knowledge_weights": {
            "compliance_docs": 2.5,
            "security_docs": 2.0,
            "regulatory_docs": 2.0,
            "general_docs": 0.5
        },
        "top_k": 20,
        "min_relevance_score": 0.8,
        "enable_self_rag": True  # Enable Self-RAG for compliance accuracy
    }
}

DEFAULT_PERSONA = "developer"
FALLBACK_PERSONA = "support_engineer"


# ============================================================================
# AWS CLIENTS (Lazy initialization)
# ============================================================================

_lambda_client = None
_s3_client = None
_dynamodb = None
_cognito_client = None


def get_lambda_client():
    global _lambda_client
    if _lambda_client is None:
        _lambda_client = boto3.client('lambda', region_name=Config.AWS_REGION)
    return _lambda_client


def get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client('s3', region_name=Config.AWS_REGION)
    return _s3_client


def get_dynamodb():
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource('dynamodb', region_name=Config.AWS_REGION)
    return _dynamodb


def get_cognito_client():
    # NOTE: Cognito/Azure AD authentication is currently disabled.
    # User authentication now uses DynamoDB (mech-avatar-users) with HMAC-SHA256 JWTs.
    # This client remains here so the import can be re-enabled in the future if needed.
    global _cognito_client
    if _cognito_client is None:
        _cognito_client = boto3.client('cognito-idp', region_name=Config.AWS_REGION)
    return _cognito_client


# ---------------------------------------------------------------------------
# JWT helpers (HS256 — stdlib only, matches Lambda 10)
# ---------------------------------------------------------------------------

def _b64url_decode_08(s: str) -> bytes:
    """URL-safe base64 decode with padding correction."""
    pad = 4 - len(s) % 4
    if pad != 4:
        s += '=' * pad
    return base64.urlsafe_b64decode(s)


def _verify_jwt_hs256(token: str) -> Optional[Dict[str, Any]]:
    """
    Verify an HS256 JWT signed with Config.JWT_SECRET.
    Returns the payload dict or None if invalid/expired.
    """
    try:
        parts = token.split('.')
        if len(parts) != 3:
            return None

        header_b64, payload_b64, sig_b64 = parts
        signing_input = f"{header_b64}.{payload_b64}"

        expected_sig = hmac.new(
            Config.JWT_SECRET.encode('utf-8'),
            signing_input.encode('utf-8'),
            hashlib.sha256,
        ).digest()

        actual_sig = _b64url_decode_08(sig_b64)
        if not hmac.compare_digest(expected_sig, actual_sig):
            log_event('JWT_SIG_INVALID', 'JWT signature mismatch')
            return None

        payload = json.loads(_b64url_decode_08(payload_b64))

        if payload.get('exp', 0) < int(time.time()):
            log_event('JWT_EXPIRED', 'JWT token expired')
            return None

        return payload

    except Exception as exc:
        log_event('JWT_ERROR', f'JWT verification error: {exc}')
        return None


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def generate_document_id(file_name: str, user_id: str) -> str:
    """Generate unique document ID."""
    timestamp = datetime.now(timezone.utc).isoformat()
    hash_input = f"{file_name}{user_id}{timestamp}"
    hash_value = hashlib.md5(hash_input.encode()).hexdigest()[:12]
    return f"doc-{hash_value}"


def generate_session_id() -> str:
    """Generate unique session ID."""
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    random_suffix = hashlib.md5(str(time.time()).encode()).hexdigest()[:8]
    return f"sess-{timestamp}-{random_suffix}"


def get_content_type(extension: str) -> str:
    """Map file extension to MIME type."""
    content_types = {
        # Documents
        'pdf': 'application/pdf',
        'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'doc': 'application/msword',
        'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'xls': 'application/vnd.ms-excel',
        'pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
        'ppt': 'application/vnd.ms-powerpoint',
        'txt': 'text/plain',
        'csv': 'text/csv',
        'html': 'text/html',
        'xml': 'application/xml',
        'json': 'application/json',
        'md': 'text/markdown',
        # Mainframe source code (COBOL, JCL, Assembler, etc.)
        'asm': 'text/plain',
        'hlasm': 'text/plain',
        'cbl': 'text/plain',
        'cob': 'text/plain',
        'cpy': 'text/plain',
        'jcl': 'text/plain',
        'proc': 'text/plain',
        'pli': 'text/plain',
        'rexx': 'text/plain',
        # Images
        'png': 'image/png',
        'jpg': 'image/jpeg',
        'jpeg': 'image/jpeg',
        'gif': 'image/gif',
        'bmp': 'image/bmp',
        'tiff': 'image/tiff'
    }
    return content_types.get(extension.lower(), 'application/octet-stream')


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder for DynamoDB Decimal types."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


# ============================================================================
# AUTHENTICATION & AUTHORIZATION
# ============================================================================

def extract_user_from_token(headers: Dict[str, str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract user_id and persona from JWT token.

    Primary path  : HS256 JWT signed by Lambda 10 (DynamoDB auth).
                    Signature is verified with Config.JWT_SECRET.
    Fallback path : Legacy Cognito base64-decoded token (no sig verify) —
                    kept for backwards compatibility during rollout.

    Returns:
        Tuple of (user_id, persona) or (None, None) if invalid.
    """
    # DEV OVERRIDES:
    # 1) `DISABLE_JWT=true` — disable JWT verification entirely (dev only).
    # 2) `ALLOW_ANONYMOUS=true` — allow anonymous access (keeps existing behavior).
    # WARNING: These bypasses should ONLY be used for local/dev testing. Do NOT enable in production.
    if os.environ.get('DISABLE_JWT', 'false').lower() in ('1', 'true', 'yes'):
        log_event('AUTH_DISABLED', 'JWT verification disabled (dev)', user_id='anon', persona=DEFAULT_PERSONA)
        return 'anon', DEFAULT_PERSONA

    if os.environ.get('ALLOW_ANONYMOUS', 'false').lower() in ('1', 'true', 'yes'):
        log_event('AUTH_BYPASS', 'Anonymous access enabled (dev)', user_id='anon', persona=DEFAULT_PERSONA)
        return 'anon', DEFAULT_PERSONA

    auth_header = headers.get('Authorization') or headers.get('authorization', '')

    if not auth_header.startswith('Bearer '):
        log_event('AUTH_ERROR', 'Missing or invalid Authorization header')
        return None, None

    token = auth_header[7:]

    # ── Primary: verify HMAC-SHA256 signature ─────────────────────────────
    if Config.JWT_SECRET and Config.JWT_SECRET != 'change-me-in-production-use-ssm':
        payload = _verify_jwt_hs256(token)
        if payload:
            user_id = payload.get('sub') or payload.get('email')
            persona = payload.get('persona', DEFAULT_PERSONA)
            if persona not in PERSONAS:
                persona = DEFAULT_PERSONA
            log_event('AUTH_SUCCESS', 'User authenticated (HS256)', user_id=user_id, persona=persona)
            return user_id, persona
        # HS256 verification failed — do NOT fall through to legacy path
        log_event('AUTH_ERROR', 'HS256 JWT verification failed')
        return None, None

    # ── Fallback: base64-decode only (Cognito / legacy tokens) ────────────
    # NOTE: This path is intentionally left in place for gradual migration.
    # Once all clients use the new DynamoDB-backed tokens, this block can
    # be removed and JWT_SECRET must always be set.
    try:
        parts = token.split('.')
        if len(parts) != 3:
            log_event('AUTH_ERROR', 'Invalid JWT format')
            return None, None

        payload = json.loads(_b64url_decode_08(parts[1]))

        user_id = payload.get('email') or payload.get('cognito:username') or payload.get('sub')

        # New JWT format: direct 'persona' field
        if payload.get('persona') and payload['persona'] in PERSONAS:
            persona = payload['persona']
        else:
            # Legacy Cognito: read from groups or custom: attribute
            persona = DEFAULT_PERSONA
            for group in payload.get('cognito:groups', []):
                if group in PERSONAS:
                    persona = group
                    break
            custom_persona = payload.get('custom:persona')
            if custom_persona and custom_persona in PERSONAS:
                persona = custom_persona

        log_event('AUTH_SUCCESS', 'User authenticated (legacy/no-verify)', user_id=user_id, persona=persona)
        return user_id, persona

    except Exception as exc:
        log_event('AUTH_ERROR', f'Failed to decode token: {exc}')
        return None, None


def check_admin_access(user_id: str, persona: str) -> bool:
    """Check if user has admin access for analytics."""
    return persona in ['architect', 'compliance_officer']


# ============================================================================
# AUTH ROUTE HANDLERS  (DynamoDB-backed, no Cognito / Azure AD)
# ============================================================================
# These mirror Lambda 10 but are embedded here so no API Gateway reconfiguration
# is needed — routes /auth/* are handled by the same Lambda 08 function.
# Lambda 10 is the standalone / dedicated version of these same routes.
# ============================================================================

def _hash_password_08(password: str):
    """Hash password using PBKDF2-HMAC-SHA256. Returns (hash_hex, salt_hex)."""
    import os as _os
    salt = _os.urandom(32)
    key  = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100_000)
    return key.hex(), salt.hex()


def _verify_password_08(password: str, stored_hash: str, salt_hex: str) -> bool:
    """Constant-time password check."""
    try:
        salt = bytes.fromhex(salt_hex)
        key  = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100_000)
        return hmac.compare_digest(key.hex(), stored_hash)
    except Exception:
        return False


def _b64url_encode_08(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode('ascii')


def _create_jwt_08(user_id: str, email: str, persona: str, name: str = '') -> str:
    """Create an HS256-signed JWT (mirrors Lambda 10 create_jwt)."""
    now = int(time.time())
    header  = _b64url_encode_08(json.dumps({'alg': 'HS256', 'typ': 'JWT'}).encode())
    payload = _b64url_encode_08(json.dumps({
        'sub':     user_id,
        'email':   email,
        'name':    name,
        'persona': persona,
        'iat':     now,
        'exp':     now + Config.JWT_EXPIRY_SECONDS,
    }).encode())
    signing_input = f"{header}.{payload}"
    sig = hmac.new(
        Config.JWT_SECRET.encode('utf-8'),
        signing_input.encode('utf-8'),
        hashlib.sha256,
    ).digest()
    return f"{signing_input}.{_b64url_encode_08(sig)}"


_VALID_PERSONAS_AUTH = {
    'developer', 'business_analyst', 'qa_tester',
    'support_engineer', 'architect', 'compliance_officer',
}


def handle_auth_register(body: str, request_origin: Optional[str] = None) -> Dict[str, Any]:
    """POST /auth/register — create account, return JWT."""
    # Debug: log raw body when anonymous/dev mode is enabled to help debugging
    if os.environ.get('ALLOW_ANONYMOUS', 'false').lower() in ('1', 'true', 'yes'):
        log_event('DEBUG_RAW_BODY', 'handle_auth_register raw body', raw_body=body)

    try:
        payload = json.loads(body) if isinstance(body, str) else (body or {})
    except json.JSONDecodeError:
        return error_response(400, 'Invalid JSON body', request_origin)

    name     = (payload.get('name')     or '').strip()
    email    = (payload.get('email')    or '').strip().lower()
    password = (payload.get('password') or '')
    persona  = payload.get('persona', DEFAULT_PERSONA)

    if not name:
        return error_response(400, 'name is required', request_origin)
    if not email or '@' not in email:
        return error_response(400, 'A valid email address is required', request_origin)
    if len(password) < 8:
        return error_response(400, 'Password must be at least 8 characters', request_origin)
    if persona not in _VALID_PERSONAS_AUTH:
        persona = DEFAULT_PERSONA

    # DEV-FALLBACK: if ALLOW_ANONYMOUS is enabled, skip DynamoDB and return a dev token
    if os.environ.get('ALLOW_ANONYMOUS', 'false').lower() in ('1', 'true', 'yes'):
        user_id = f"dev-usr-{uuid.uuid4().hex[:12]}"
        # Try to persist to DynamoDB if the table exists and permissions allow it.
        persisted = False
        try:
            table = get_dynamodb().Table(Config.USERS_TABLE)
            pass_hash, salt = _hash_password_08(password)
            now_iso = datetime.now(timezone.utc).isoformat()
            table.put_item(Item={
                'email':         email,
                'user_id':       user_id,
                'name':          name,
                'password_hash': pass_hash,
                'password_salt': salt,
                'persona':       persona,
                'created_at':    now_iso,
                'last_login':    now_iso,
                'is_active':     True,
            })
            persisted = True
        except Exception as e:
            log_event('DEV_PERSIST_WARN', f'Could not persist dev user: {str(e)}', email=email)

        token = _create_jwt_08(user_id, email, persona, name)
        log_event('REGISTER_SUCCESS_DEV', 'Dev user registered (in-memory)', user_id=user_id, email=email, persisted=persisted)
        note = 'DEV: user persisted to DynamoDB' if persisted else 'DEV: user not persisted to DynamoDB'
        return success_response({
            'user':       {'id': user_id, 'email': email, 'name': name, 'persona': persona},
            'token':      token,
            'expires_in': Config.JWT_EXPIRY_SECONDS,
            'note': note
        }, 201, request_origin)

    table = get_dynamodb().Table(Config.USERS_TABLE)

    if table.get_item(Key={'email': email}).get('Item'):
        return error_response(409, 'An account with this email already exists', request_origin)

    pass_hash, salt = _hash_password_08(password)
    user_id = f"usr-{uuid.uuid4().hex[:16]}"
    now_iso = datetime.now(timezone.utc).isoformat()

    table.put_item(Item={
        'email':         email,
        'user_id':       user_id,
        'name':          name,
        'password_hash': pass_hash,
        'password_salt': salt,
        'persona':       persona,
        'created_at':    now_iso,
        'last_login':    now_iso,
        'is_active':     True,
    })

    token = _create_jwt_08(user_id, email, persona, name)
    log_event('REGISTER_SUCCESS', 'New user registered', user_id=user_id, email=email)

    return success_response({
        'user':       {'id': user_id, 'email': email, 'name': name, 'persona': persona},
        'token':      token,
        'expires_in': Config.JWT_EXPIRY_SECONDS,
    }, 201, request_origin)


def handle_auth_login(body: str, request_origin: Optional[str] = None) -> Dict[str, Any]:
    """POST /auth/login — verify credentials, return JWT."""
    # Debug: log raw body when anonymous/dev mode is enabled to help debugging
    if os.environ.get('ALLOW_ANONYMOUS', 'false').lower() in ('1', 'true', 'yes'):
        log_event('DEBUG_RAW_BODY', 'handle_auth_login raw body', raw_body=body)

    try:
        payload = json.loads(body) if isinstance(body, str) else (body or {})
    except json.JSONDecodeError:
        return error_response(400, 'Invalid JSON body', request_origin)

    email    = (payload.get('email')    or '').strip().lower()
    password = (payload.get('password') or '')

    if not email or not password:
        return error_response(400, 'email and password are required', request_origin)

    # DEV-FALLBACK: if ALLOW_ANONYMOUS is enabled, accept any credentials and return a dev token
    if os.environ.get('ALLOW_ANONYMOUS', 'false').lower() in ('1', 'true', 'yes'):
        # If a real user exists in DynamoDB, prefer that account (and verify password if possible).
        try:
            table = get_dynamodb().Table(Config.USERS_TABLE)
            item = table.get_item(Key={'email': email}).get('Item')
            if item:
                # If password fields exist, verify; otherwise accept and issue token for stored user
                if item.get('password_hash') and item.get('password_salt'):
                    if _verify_password_08(password, item['password_hash'], item['password_salt']):
                        user_id = item['user_id']
                        persona = item.get('persona', DEFAULT_PERSONA)
                        name = item.get('name', '')
                        token = _create_jwt_08(user_id, email, persona, name)
                        log_event('LOGIN_SUCCESS_DEV', 'Dev login via persisted user', user_id=user_id, email=email)
                        return success_response({
                            'user': {'id': user_id, 'email': email, 'name': name, 'persona': persona},
                            'token': token,
                            'expires_in': Config.JWT_EXPIRY_SECONDS,
                            'note': 'DEV: login using persisted DynamoDB user'
                        }, 200, request_origin)
                else:
                    # No password stored — accept and issue token
                    user_id = item.get('user_id', f"dev-usr-{uuid.uuid4().hex[:12]}")
                    persona = item.get('persona', DEFAULT_PERSONA)
                    name = item.get('name', '')
                    token = _create_jwt_08(user_id, email, persona, name)
                    log_event('LOGIN_SUCCESS_DEV', 'Dev login using persisted user (no password)', user_id=user_id, email=email)
                    return success_response({
                        'user': {'id': user_id, 'email': email, 'name': name, 'persona': persona},
                        'token': token,
                        'expires_in': Config.JWT_EXPIRY_SECONDS,
                        'note': 'DEV: login using persisted DynamoDB user (no password check)'
                    }, 200, request_origin)
            # If user not found, create/persist a dev user to make future logins deterministic
            user_id = f"dev-usr-{uuid.uuid4().hex[:12]}"
            pass_hash, salt = _hash_password_08(password)
            now_iso = datetime.now(timezone.utc).isoformat()
            try:
                table.put_item(Item={
                    'email':         email,
                    'user_id':       user_id,
                    'name':          '',
                    'password_hash': pass_hash,
                    'password_salt': salt,
                    'persona':       DEFAULT_PERSONA,
                    'created_at':    now_iso,
                    'last_login':    now_iso,
                    'is_active':     True,
                })
                persisted = True
            except Exception as e:
                persisted = False
                log_event('DEV_PERSIST_WARN', f'Could not persist dev login user: {str(e)}', email=email)

            token = _create_jwt_08(user_id, email, DEFAULT_PERSONA, '')
            log_event('LOGIN_SUCCESS_DEV', 'Dev login issued token', user_id=user_id, email=email, persisted=persisted)
            note = 'DEV: login persisted to DynamoDB' if persisted else 'DEV: login accepted without DynamoDB'
            return success_response({
                'user':       {'id': user_id, 'email': email, 'name': '', 'persona': DEFAULT_PERSONA},
                'token':      token,
                'expires_in': Config.JWT_EXPIRY_SECONDS,
                'note': note
            }, 200, request_origin)
        except Exception:
            # If DynamoDB access isn't available, fall back to in-memory dev token
            user_id = f"dev-usr-{uuid.uuid4().hex[:12]}"
            persona = DEFAULT_PERSONA
            name = ''
            token = _create_jwt_08(user_id, email, persona, name)
            log_event('LOGIN_SUCCESS_DEV', 'Dev login issued token (no DynamoDB)', user_id=user_id, email=email)
            return success_response({
                'user':       {'id': user_id, 'email': email, 'name': name, 'persona': persona},
                'token':      token,
                'expires_in': Config.JWT_EXPIRY_SECONDS,
                'note': 'DEV: login accepted without DynamoDB'
            }, 200, request_origin)

    table = get_dynamodb().Table(Config.USERS_TABLE)
    item  = table.get_item(Key={'email': email}).get('Item')

    # Same message for not-found vs wrong-password (prevent user enumeration)
    if not item or not _verify_password_08(password, item['password_hash'], item['password_salt']):
        log_event('LOGIN_FAILED', 'Invalid credentials', email=email)
        return error_response(401, 'Invalid email or password', request_origin)

    if not item.get('is_active', True):
        return error_response(401, 'Account is disabled — contact your administrator', request_origin)

    try:
        table.update_item(
            Key={'email': email},
            UpdateExpression='SET last_login = :t',
            ExpressionAttributeValues={':t': datetime.now(timezone.utc).isoformat()},
        )
    except Exception:
        pass

    user_id = item['user_id']
    persona = item.get('persona', DEFAULT_PERSONA)
    name    = item.get('name', '')
    token   = _create_jwt_08(user_id, email, persona, name)

    log_event('LOGIN_SUCCESS', 'User logged in', user_id=user_id, email=email, persona=persona)

    return success_response({
        'user':       {'id': user_id, 'email': email, 'name': name, 'persona': persona},
        'token':      token,
        'expires_in': Config.JWT_EXPIRY_SECONDS,
    }, 200, request_origin)


def handle_auth_refresh(headers: Dict[str, str],
                        request_origin: Optional[str] = None) -> Dict[str, Any]:
    """POST /auth/refresh — verify token, issue fresh JWT."""
    auth_header = headers.get('Authorization') or headers.get('authorization', '')
    if not auth_header.startswith('Bearer '):
        return error_response(401, 'Missing or invalid Authorization header', request_origin)

    existing_payload = _verify_jwt_hs256(auth_header[7:])
    if not existing_payload:
        return error_response(401, 'Invalid or expired token', request_origin)

    new_token = _create_jwt_08(
        existing_payload['sub'],
        existing_payload['email'],
        existing_payload.get('persona', DEFAULT_PERSONA),
        existing_payload.get('name', ''),
    )
    log_event('TOKEN_REFRESHED', 'Token refreshed', user_id=existing_payload['sub'])

    return success_response({
        'token':      new_token,
        'expires_in': Config.JWT_EXPIRY_SECONDS,
    }, 200, request_origin)


# ============================================================================
# RATE LIMITING
# ============================================================================

def is_rate_limited(user_id: str) -> bool:
    """
    Check if user exceeded rate limit using DynamoDB.
    
    Rate limit: RATE_LIMIT_REQUESTS per RATE_LIMIT_WINDOW_SECONDS
    """
    # If anonymous/dev bypass is enabled, skip rate limiting entirely
    if os.environ.get('ALLOW_ANONYMOUS', 'false').lower() in ('1', 'true', 'yes'):
        return False

    try:
        table = get_dynamodb().Table(Config.RATE_LIMIT_TABLE)
        current_time = int(time.time())
        window_start = current_time - Config.RATE_LIMIT_WINDOW_SECONDS
        
        # Get current count for user
        response = table.get_item(
            Key={'user_id': user_id}
        )
        
        item = response.get('Item')
        
        if not item:
            # First request from user
            table.put_item(
                Item={
                    'user_id': user_id,
                    'request_count': 1,
                    'window_start': current_time,
                    'ttl': current_time + Config.RATE_LIMIT_WINDOW_SECONDS * 2
                }
            )
            return False
        
        # Check if window has expired
        if item.get('window_start', 0) < window_start:
            # Reset window
            table.put_item(
                Item={
                    'user_id': user_id,
                    'request_count': 1,
                    'window_start': current_time,
                    'ttl': current_time + Config.RATE_LIMIT_WINDOW_SECONDS * 2
                }
            )
            return False
        
        # Check count
        current_count = int(item.get('request_count', 0))
        
        if current_count >= Config.RATE_LIMIT_REQUESTS:
            log_event('RATE_LIMITED', f'User exceeded rate limit', 
                     user_id=user_id, count=current_count)
            return True
        
        # Increment count
        table.update_item(
            Key={'user_id': user_id},
            UpdateExpression='SET request_count = request_count + :inc',
            ExpressionAttributeValues={':inc': 1}
        )
        
        return False
        
    except Exception as e:
        # If rate limiting fails, allow the request
        log_event('RATE_LIMIT_ERROR', f'Rate limit check failed: {str(e)}')
        return False


# ============================================================================
# REQUEST HANDLERS
# ============================================================================

def handle_chat_query(body: str, user_id: str, persona: str) -> Dict[str, Any]:
    """
    Route chat query to Lambda 06 (RAG Engine) with persona context.
    
    Args:
        body: JSON request body with 'query' and optional 'session_id'
        user_id: Authenticated user ID
        persona: User's persona
    
    Returns:
        API Gateway response with RAG answer
    """
    try:
        payload = json.loads(body) if isinstance(body, str) else body
        query = payload.get('query', '').strip()
        session_id = payload.get('session_id') or generate_session_id()
        
        if not query:
            return error_response(400, 'Query is required')
        
        if len(query) > 5000:
            return error_response(400, 'Query too long (max 5000 characters)')
        
        # Get persona configuration
        persona_config = PERSONAS.get(persona, PERSONAS[DEFAULT_PERSONA])
        
        # Build enriched payload for Lambda 06
        lambda_payload = {
            'query': query,
            'session_id': session_id,
            'user_id': user_id,
            'persona': persona,
            'persona_config': {
                'name': persona_config['name'],
                'system_prompt': persona_config['system_prompt'],
                'metadata_filters': persona_config.get('metadata_filters', {}),
                'knowledge_weights': persona_config.get('knowledge_weights', {}),
                'top_k': persona_config.get('top_k', 10),
                'min_relevance_score': persona_config.get('min_relevance_score', 0.6),
                'enable_self_rag': persona_config.get('enable_self_rag', False)
            },
            'options': {
                'enable_streaming': payload.get('enable_streaming', False),
                'include_citations': payload.get('include_citations', True),
                'max_tokens': payload.get('max_tokens', 2000)
            }
        }
        
        log_event('CHAT_QUERY', 'Invoking Lambda 06', 
                 user_id=user_id, persona=persona, query_length=len(query))
        
        # Invoke Lambda 06
        response = get_lambda_client().invoke(
            FunctionName=Config.LAMBDA_06_ARN,
            InvocationType='RequestResponse',
            Payload=json.dumps(lambda_payload)
        )
        
        # Parse Lambda 06 response
        response_payload = json.loads(response['Payload'].read().decode('utf-8'))
        
        if response.get('FunctionError'):
            log_event('LAMBDA_06_ERROR', 'Lambda 06 returned error', 
                     error=response_payload)
            return error_response(500, 'Failed to process query')
        
        # Add persona info to response
        result = response_payload.get('body', response_payload)
        if isinstance(result, str):
            result = json.loads(result)
        
        result['persona'] = {
            'id': persona,
            'name': persona_config['name']
        }
        result['session_id'] = session_id
        
        log_event('CHAT_QUERY_SUCCESS', 'Query processed successfully',
                 user_id=user_id, persona=persona, session_id=session_id)
        
        return success_response(result)
        
    except json.JSONDecodeError:
        return error_response(400, 'Invalid JSON in request body')
    except Exception as e:
        log_event('CHAT_QUERY_ERROR', f'Error processing query: {str(e)}')
        return error_response(500, f'Internal error: {str(e)}')


def handle_chat_history(session_id: str, user_id: str) -> Dict[str, Any]:
    """
    Retrieve conversation history from DynamoDB.
    
    Args:
        session_id: Session ID to retrieve
        user_id: Authenticated user ID (for authorization)
    
    Returns:
        API Gateway response with conversation history
    """
    try:
        table = get_dynamodb().Table(Config.MEMORY_TABLE)
        
        # Query memory items for session
        response = table.query(
            KeyConditionExpression='session_id = :sid',
            ExpressionAttributeValues={':sid': session_id},
            ScanIndexForward=True,  # Chronological order
            Limit=50
        )
        
        items = response.get('Items', [])
        
        # Convert Decimal to float for JSON serialization
        history = []
        for item in items:
            history.append({
                'message_id': item.get('message_id'),
                'role': item.get('role'),
                'content': item.get('content'),
                'timestamp': item.get('timestamp'),
                'citations': item.get('citations', [])
            })
        
        log_event('HISTORY_RETRIEVED', f'Retrieved {len(history)} messages',
                 session_id=session_id, user_id=user_id)
        
        return success_response({
            'session_id': session_id,
            'messages': history,
            'count': len(history)
        })
        
    except Exception as e:
        log_event('HISTORY_ERROR', f'Error retrieving history: {str(e)}')
        return error_response(500, f'Failed to retrieve history: {str(e)}')


def handle_document_upload(body: str, user_id: str) -> Dict[str, Any]:
    """
    Generate presigned URL for document upload to S3.
    
    Args:
        body: JSON with file_name, file_size, file_type
        user_id: Authenticated user ID
    
    Returns:
        API Gateway response with presigned URL and document_id
    """
    try:
        payload = json.loads(body) if isinstance(body, str) else body
        
        file_name = payload.get('file_name', '').strip()
        file_size = payload.get('file_size', 0)
        file_type = payload.get('file_type', '').lower()
        
        # Validation
        if not file_name:
            return error_response(400, 'file_name is required')
        
        # Extract extension
        if '.' in file_name:
            extension = file_name.rsplit('.', 1)[-1].lower()
        else:
            extension = file_type
        
        if extension not in Config.ALLOWED_FILE_EXTENSIONS:
            return error_response(400, f'File type .{extension} not allowed')
        
        max_size_bytes = Config.MAX_FILE_SIZE_MB * 1024 * 1024
        if file_size > max_size_bytes:
            return error_response(400, f'File size exceeds {Config.MAX_FILE_SIZE_MB} MB limit')
        
        # Generate document ID and S3 key
        document_id = generate_document_id(file_name, user_id)
        s3_key = f"documents/{user_id}/{document_id}_{file_name}"
        
        # Generate presigned URL
        presigned_url = get_s3_client().generate_presigned_url(
            'put_object',
            Params={
                'Bucket': Config.S3_BUCKET_NAME,
                'Key': s3_key,
                'ContentType': get_content_type(extension)
            },
            ExpiresIn=Config.PRESIGNED_URL_EXPIRY
        )
        
        # Create document record in DynamoDB
        table = get_dynamodb().Table(Config.DOCUMENTS_TABLE)
        table.put_item(
            Item={
                'document_id': document_id,
                'file_name': file_name,
                'file_size': file_size,
                'file_type': extension,
                's3_bucket': Config.S3_BUCKET_NAME,
                's3_key': s3_key,
                'uploaded_by': user_id,
                'status': 'pending_upload',
                'created_at': datetime.now(timezone.utc).isoformat(),
                'ttl': int(time.time()) + 86400 * 30  # 30 days TTL
            }
        )
        
        log_event('UPLOAD_URL_GENERATED', 'Presigned URL generated',
                 document_id=document_id, user_id=user_id, file_name=file_name)
        
        return success_response({
            'document_id': document_id,
            'presigned_url': presigned_url,
            's3_key': s3_key,
            'expires_in': Config.PRESIGNED_URL_EXPIRY
        })
        
    except json.JSONDecodeError:
        return error_response(400, 'Invalid JSON in request body')
    except Exception as e:
        log_event('UPLOAD_ERROR', f'Error generating upload URL: {str(e)}')
        return error_response(500, f'Failed to generate upload URL: {str(e)}')


def handle_document_status(document_id: str, user_id: str) -> Dict[str, Any]:
    """
    Get document processing status.
    
    Args:
        document_id: Document ID to check
        user_id: Authenticated user ID
    
    Returns:
        API Gateway response with document status
    """
    try:
        table = get_dynamodb().Table(Config.DOCUMENTS_TABLE)
        
        response = table.get_item(
            Key={'document_id': document_id}
        )
        
        item = response.get('Item')
        
        if not item:
            return error_response(404, f'Document {document_id} not found')
        
        # Check authorization (user can only see their own documents)
        if item.get('uploaded_by') != user_id:
            return error_response(403, 'Not authorized to view this document')
        
        return success_response({
            'document_id': document_id,
            'file_name': item.get('file_name'),
            'status': item.get('status'),
            'chunk_count': item.get('chunk_count', 0),
            'created_at': item.get('created_at'),
            'processed_at': item.get('processed_at'),
            'error': item.get('error')
        })
        
    except Exception as e:
        log_event('STATUS_ERROR', f'Error getting document status: {str(e)}')
        return error_response(500, f'Failed to get status: {str(e)}')


def handle_document_list(user_id: str, query_params: Dict[str, str]) -> Dict[str, Any]:
    """
    List documents for the authenticated user (paginated).

    Args:
        user_id: Authenticated user ID
        query_params: page (default 1) and page_size (default 20, max 100)

    Returns:
        API Gateway response with paginated document list
    """
    try:
        page = max(1, int(query_params.get('page', 1)))
        page_size = min(100, max(1, int(query_params.get('page_size', 20))))

        table = get_dynamodb().Table(Config.DOCUMENTS_TABLE)

        # Scan with filter — in production, replace with a GSI on uploaded_by+created_at
        scan_kwargs: Dict[str, Any] = {
            'FilterExpression': boto3.dynamodb.conditions.Attr('uploaded_by').eq(user_id),
            'Limit': page_size * page  # over-fetch then slice (simple pagination)
        }

        result = table.scan(**scan_kwargs)
        items = result.get('Items', [])

        # Sort by created_at descending
        items.sort(key=lambda x: x.get('created_at', ''), reverse=True)

        start = (page - 1) * page_size
        page_items = items[start: start + page_size]

        documents = [
            {
                'document_id': item.get('document_id'),
                'file_name': item.get('file_name'),
                'file_type': item.get('file_type'),
                'size_bytes': item.get('size_bytes'),
                'status': item.get('status'),
                'chunk_count': item.get('chunk_count', 0),
                'created_at': item.get('created_at'),
                'processed_at': item.get('processed_at')
            }
            for item in page_items
        ]

        log_event('DOCUMENT_LIST', f'Listed {len(documents)} documents', user_id=user_id)

        return success_response({
            'documents': documents,
            'total': len(items),
            'page': page,
            'page_size': page_size,
            'has_more': (start + page_size) < len(items)
        })

    except Exception as e:
        log_event('LIST_ERROR', f'Error listing documents: {str(e)}')
        return error_response(500, f'Failed to list documents: {str(e)}')


def handle_document_delete(document_id: str, user_id: str) -> Dict[str, Any]:
    """
    Delete a document record (and optionally its S3 object).

    Args:
        document_id: Document ID to delete
        user_id: Authenticated user ID

    Returns:
        API Gateway response confirming deletion
    """
    try:
        table = get_dynamodb().Table(Config.DOCUMENTS_TABLE)

        # Verify ownership before deleting
        response = table.get_item(Key={'document_id': document_id})
        item = response.get('Item')

        if not item:
            return error_response(404, f'Document {document_id} not found')

        if item.get('uploaded_by') != user_id:
            return error_response(403, 'Not authorized to delete this document')

        # Delete from DynamoDB
        table.delete_item(Key={'document_id': document_id})

        # Best-effort delete from S3 (non-fatal if it fails)
        try:
            s3 = boto3.client('s3', region_name=Config.AWS_REGION)
            s3_key = item.get('s3_key') or f"uploads/{user_id}/{document_id}/{item.get('file_name', document_id)}"
            s3.delete_object(Bucket=Config.S3_BUCKET_NAME, Key=s3_key)
        except Exception as s3_err:
            log_event('S3_DELETE_WARN', f'Could not delete S3 object: {str(s3_err)}',
                      document_id=document_id)

        log_event('DOCUMENT_DELETED', 'Document deleted', document_id=document_id, user_id=user_id)

        return success_response({
            'document_id': document_id,
            'deleted': True,
            'message': f'Document {document_id} deleted successfully'
        })

    except Exception as e:
        log_event('DELETE_ERROR', f'Error deleting document: {str(e)}')
        return error_response(500, f'Failed to delete document: {str(e)}')


def handle_admin_analytics(user_id: str, persona: str) -> Dict[str, Any]:
    """
    Get basic analytics (admin only).
    Uses CloudWatch Logs Insights directly.
    
    Args:
        user_id: Authenticated user ID
        persona: User's persona
    
    Returns:
        API Gateway response with analytics data
    """
    if not check_admin_access(user_id, persona):
        return error_response(403, 'Admin access required')
    
    try:
        # Basic metrics from CloudWatch
        logs_client = boto3.client('logs', region_name=Config.AWS_REGION)
        
        # Query for last 24 hours
        end_time = int(time.time() * 1000)
        start_time = end_time - (24 * 60 * 60 * 1000)
        
        # Simple query for query counts by persona
        query = """
            fields @timestamp, @message
            | filter @message like /CHAT_QUERY_SUCCESS/
            | parse @message /persona=(?<persona>\\w+)/
            | stats count() as query_count by persona
        """
        
        response = logs_client.start_query(
            logGroupName=f'/aws/lambda/mech-avatar-lambda-08',
            startTime=start_time,
            endTime=end_time,
            queryString=query
        )
        
        # Note: In production, you'd poll for results
        # For now, return placeholder
        
        log_event('ANALYTICS_REQUESTED', 'Analytics query started',
                 user_id=user_id, query_id=response.get('queryId'))
        
        return success_response({
            'message': 'Analytics query started',
            'query_id': response.get('queryId'),
            'note': 'Poll /admin/analytics/{query_id} for results'
        })
        
    except Exception as e:
        log_event('ANALYTICS_ERROR', f'Error getting analytics: {str(e)}')
        return error_response(500, f'Failed to get analytics: {str(e)}')


def handle_get_personas(user_id: str) -> Dict[str, Any]:
    """
    Get available personas for the user.
    
    Returns:
        API Gateway response with persona list
    """
    personas_list = []
    for persona_id, config in PERSONAS.items():
        personas_list.append({
            'id': persona_id,
            'name': config['name'],
            'description': config['description']
        })
    
    return success_response({
        'personas': personas_list,
        'default': DEFAULT_PERSONA
    })


# ============================================================================
# RESPONSE HELPERS
# ============================================================================

def _resolve_cors_origin(request_origin: Optional[str] = None) -> str:
    """
    Return the allowed CORS origin.
    Matches the request Origin against ALLOWED_ORIGINS; falls back to the first
    allowed origin.  If ALLOWED_ORIGINS contains '*', that is returned directly.
    """
    allowed = Config.ALLOWED_ORIGINS
    if '*' in allowed:
        return '*'
    if request_origin and request_origin in allowed:
        return request_origin
    return allowed[0] if allowed else '*'


def success_response(data: Any, status_code: int = 200, request_origin: Optional[str] = None) -> Dict[str, Any]:
    """Build successful API Gateway response with CORS headers."""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': _resolve_cors_origin(request_origin),
            'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key',
            'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
            'Vary': 'Origin'
        },
        'body': json.dumps(data, cls=DecimalEncoder)
    }


def error_response(status_code: int, message: str, request_origin: Optional[str] = None) -> Dict[str, Any]:
    """Build error API Gateway response with CORS headers."""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': _resolve_cors_origin(request_origin),
            'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key',
            'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
            'Vary': 'Origin'
        },
        'body': json.dumps({
            'error': True,
            'message': message,
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
    }


def handle_options(request_origin: Optional[str] = None) -> Dict[str, Any]:
    """Handle CORS preflight OPTIONS request."""
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': _resolve_cors_origin(request_origin),
            'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
            'Access-Control-Max-Age': '86400',
            'Vary': 'Origin'
        },
        'body': ''
    }


# ============================================================================
# MAIN HANDLER
# ============================================================================

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for API Gateway requests.
    
    Routes requests to appropriate handlers based on HTTP method and resource.
    
    Args:
        event: API Gateway event
        context: Lambda context
    
    Returns:
        API Gateway response
    """
    # Set correlation ID for tracing
    request_id = context.aws_request_id if context else str(time.time())
    set_correlation_id(request_id)
    
    # Debug: when ALLOW_ANONYMOUS is enabled, log the full event to troubleshoot mapping
    if os.environ.get('ALLOW_ANONYMOUS', 'false').lower() in ('1', 'true', 'yes'):
        try:
            # Avoid logging extremely large payloads in production; this is for dev only
            log_event('DEBUG_EVENT', 'Full incoming event', event=event)
        except Exception:
            pass

    log_event('REQUEST_RECEIVED', 'Incoming API request',
             resource=event.get('resource'),
             method=event.get('httpMethod'))
    
    # Handle CORS preflight
    # Normalize event shape for REST (v1) and HTTP API (v2)
    http_method = (event.get('httpMethod') or
                   (event.get('requestContext', {})
                        .get('http', {})
                        .get('method')) or
                   '')
    http_method = (http_method or '').upper()

    headers = event.get('headers') or {}
    request_origin = headers.get('origin') or headers.get('Origin')

    # Normalize resource/path (v1 uses 'resource', v2 uses 'rawPath' or 'routeKey')
    resource = event.get('resource') or event.get('rawPath') or event.get('routeKey') or ''
    # routeKey may be in the form 'POST /chat/query' — extract path portion
    if resource and isinstance(resource, str) and ' ' in resource and resource.split(' ', 1)[0] in ('GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'PATCH'):
        resource = resource.split(' ', 1)[1]

    # Strip stage prefix if present (API Gateway may include the stage in the path, e.g. '/dev/auth/register')
    request_context = event.get('requestContext') or {}
    stage = request_context.get('stage')
    if stage and isinstance(resource, str) and resource.startswith(f"/{stage}/"):
        resource = resource[len(stage) + 1:]

    if http_method == 'OPTIONS':
        return handle_options(request_origin)

    # Extract request info (path params and body)
    path_params = event.get('pathParameters') or {}
    body = event.get('body', '')
    # If HTTP API sent base64-encoded body, decode it
    if event.get('isBase64Encoded') and body:
        try:
            body = base64.b64decode(body).decode('utf-8')
        except Exception:
            pass

    # ── Auth routes (no token required) ─────────────────────────────────────
    if resource == '/auth/login' and http_method == 'POST':
        return handle_auth_login(body, request_origin)
    elif resource == '/auth/register' and http_method == 'POST':
        return handle_auth_register(body, request_origin)
    elif resource == '/auth/refresh' and http_method == 'POST':
        return handle_auth_refresh(headers, request_origin)

    # ── All other routes require authentication ───────────────────────────
    # Authenticate user (dev bypass allowed via ALLOW_ANONYMOUS)
    user_id, persona = extract_user_from_token(headers)

    if not user_id:
        return error_response(401, 'Unauthorized - Invalid or missing token', request_origin)

    # Rate limiting (skip when anonymous dev bypass enabled)
    if is_rate_limited(user_id):
        return error_response(429, 'Too many requests - Please slow down', request_origin)

    # Route to appropriate handler
    try:
        # POST /chat/query
        if resource == '/chat/query' and http_method == 'POST':
            return handle_chat_query(body, user_id, persona)
        
        # GET /chat/history/{session_id}
        elif resource == '/chat/history/{session_id}' and http_method == 'GET':
            session_id = path_params.get('session_id', '')
            return handle_chat_history(session_id, user_id)
        
        # POST /documents/upload
        elif resource == '/documents/upload' and http_method == 'POST':
            return handle_document_upload(body, user_id)
        
        # GET /documents/status/{document_id}
        elif resource == '/documents/status/{document_id}' and http_method == 'GET':
            document_id = path_params.get('document_id', '')
            return handle_document_status(document_id, user_id)
        
        # GET /documents/list
        elif resource == '/documents/list' and http_method == 'GET':
            return handle_document_list(user_id, event.get('queryStringParameters') or {})
        
        # DELETE /documents/{document_id}
        elif resource == '/documents/{document_id}' and http_method == 'DELETE':
            document_id = path_params.get('document_id', '')
            return handle_document_delete(document_id, user_id)
        
        # GET /admin/analytics
        elif resource == '/admin/analytics' and http_method == 'GET':
            return handle_admin_analytics(user_id, persona)
        
        # GET /personas
        elif resource == '/personas' and http_method == 'GET':
            return handle_get_personas(user_id)
        
        # Unknown route
        else:
            log_event('UNKNOWN_ROUTE', f'Route not found: {http_method} {resource}')
            return error_response(404, f'Route not found: {http_method} {resource}')
            
    except Exception as e:
        log_event('HANDLER_ERROR', f'Unhandled error: {str(e)}', 
                 resource=resource, method=http_method)
        return error_response(500, f'Internal server error: {str(e)}')


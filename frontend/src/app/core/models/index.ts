/**
 * MECH Avatar - Core Data Models
 * Based on FRONTEND_HANDOFF.md specification
 */

// =============================================================================
// PERSONA TYPES (6 Official Personas)
// =============================================================================

export type PersonaType = 
  | 'developer'
  | 'business_analyst'
  | 'qa_tester'
  | 'support_engineer'
  | 'architect'
  | 'compliance_officer';

// =============================================================================
// AUTHENTICATION MODELS
// =============================================================================

export interface JwtPayload {
  sub: string;
  email: string;
  email_verified: boolean;
  'cognito:username': string;
  'cognito:groups': string[];
  'custom:persona'?: string;
  'custom:department'?: string;
  iss: string;
  aud: string;
  exp: number;
  iat: number;
  token_use: 'id' | 'access';
}

export interface User {
  id: string;
  email: string;
  name?: string;
  username: string;
  groups: string[];
  persona?: PersonaType;
  department?: string;
  isAuthenticated: boolean;
}

export type UserGroup = string;

export interface AuthTokens {
  idToken: string;
  accessToken: string;
  refreshToken: string;
  expiresIn: number;
  tokenType: string;
}

// =============================================================================
// CHAT MODELS (POST /chat/query, GET /chat/history/{session_id})
// =============================================================================

/**
 * Chat query request
 * POST /chat/query
 */
export interface ChatQueryRequest {
  query: string;              // 1-500 chars
  session_id: string;         // UUID
  persona?: PersonaType;
  filters?: {
    doc_type?: string;
    identifiers?: string[];
  };
}

/**
 * Source chunk in response
 * Field names match Lambda 06 Phase 8 response assembly.
 */
export interface SourceChunk {
  chunk_id: string;
  document_id: string;
  // File identification — backend returns file_name; document_name is an alias
  file_name?: string;          // Lambda 06 primary field
  document_name?: string;      // Alias: falls back to file_name in display
  // Titles / sections
  section_title?: string;      // Lambda 06 primary field
  title?: string;              // Alias for section_title
  section?: string;
  // Page and line references
  page_number?: number;        // Lambda 06 primary field
  page_num?: number;           // Old alias
  line_start?: number;
  line_end?: number;
  // Content
  text_display?: string;       // Lambda 06 primary display text
  content?: string;            // Alias for text_display
  content_type?: string;
  // Scoring & metadata
  relevance_score: number;
  chunk_type: 'text' | 'code' | 'table' | 'figure' | 'proposition';
  entities?: string[];
  match_highlight?: string;
}

/**
 * Chat query response
 * Lambda 08 wraps Lambda 06's output.  Shape reflects what actually arrives.
 */
export interface ChatQueryResponse {
  statusCode: number;
  answer: string;
  /**
   * REST path: Lambda 06 returns { score, calibrated }.
   * WebSocket path (Lambda 09): already normalised to a plain number.
   * ApiService.normalizeChatResponse() always maps this to a number before
   * emitting to consumers.
   */
  confidence: number;
  grounded: boolean;
  /** Populated by normalizeChatResponse() from metrics.total_ms */
  execution_time_ms: number;
  sources: SourceChunk[];
  follow_up_suggestions: string[];  // Default [] if backend omits
  session_id: string;
  // Extra fields that come through from Lambda 06 (safe to ignore in UI)
  grounding?: {
    status: string;
    groundedness_score: number;
    citations_found: number;
    literals_found?: number[];
    literals_missing?: string[];
  };
  persona?: { id: string; name: string };
  intent?: string;
  metrics?: { total_ms: number; [key: string]: unknown };
}

/**
 * Chat message for history
 */
export interface ChatMessage {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: string;
  confidence?: number;
  grounded?: boolean;
  sources?: SourceChunk[];
  follow_up_suggestions?: string[];
  followUpSuggestions?: string[];  // Alias for client-side convenience
}

/**
 * Recent conversation for sidebar
 */
export interface RecentConversation {
  id: string;
  title: string;
  timestamp: string;
  messageCount: number;
}

/**
 * Chat history response
 * GET /chat/history/{session_id}
 * Backend returns: session_id, messages (with message_id), count.
 * created_at / last_activity are optional (not in backend response).
 */
export interface ChatHistoryResponse {
  session_id: string;
  messages: ChatMessage[];
  created_at?: string;      // Not returned by backend — kept for future use
  last_activity?: string;   // Not returned by backend — kept for future use
  count?: number;           // Backend returns this
}

// =============================================================================
// DOCUMENT MODELS
// =============================================================================

export type DocumentType = 'pdf' | 'docx' | 'txt' | 'md' | 'json' | 'csv' | 'xlsx' | 'yaml' | 'py' | 'js' | 'ts' | 'java' | 'cobol';

export type DocumentStatus = 'pending' | 'processing' | 'completed' | 'failed';

/**
 * Document upload request
 * POST /documents/upload
 * Field names match Lambda 08 handle_document_upload expectations.
 */
export interface DocumentUploadRequest {
  file_name: string;      // Original file name (e.g. "report.pdf")
  file_type: string;      // File extension without dot (e.g. "pdf", "docx")
  file_size: number;      // Size in bytes
}

/**
 * Document upload response (presigned URL)
 */
export interface DocumentUploadResponse {
  document_id: string;
  presigned_url: string;
  expires_in: number;
}

/**
 * Document status response
 * GET /documents/status/{document_id}
 * Field names match Lambda 08 handle_document_status response.
 */
export interface DocumentStatusResponse {
  document_id: string;
  file_name: string;          // Matches backend 'file_name'
  status: DocumentStatus;
  chunk_count?: number;       // Matches backend 'chunk_count'
  error?: string;             // Matches backend 'error'
  created_at: string;
  processed_at?: string;      // Matches backend 'processed_at'
}

/**
 * Document list item
 * Field names match Lambda 08 handle_document_list response.
 */
export interface DocumentListItem {
  document_id: string;
  file_name: string;      // Matches backend 'file_name'
  file_type: string;
  size_bytes: number;
  status: DocumentStatus;
  chunk_count: number;    // Matches backend 'chunk_count'
  created_at: string;     // Matches backend 'created_at'
  processed_at?: string;
}

/**
 * Document list response
 * GET /documents/list
 */
export interface DocumentListResponse {
  documents: DocumentListItem[];
  total: number;
  page: number;
  page_size: number;
  has_more?: boolean;   // Backend returns this for pagination
}

/**
 * Document delete response
 * DELETE /documents/{document_id}
 * Field names match Lambda 08 handle_document_delete response.
 */
export interface DocumentDeleteResponse {
  deleted: boolean;       // Matches backend 'deleted'
  document_id: string;
  message: string;
}

// =============================================================================
// WEBSOCKET MODELS
// =============================================================================

export type WebSocketMessageType = 
  // Canonical frontend types
  | 'connection_ack'
  | 'answer_chunk'
  | 'answer_complete'
  | 'sources'
  | 'error'
  | 'ping'
  | 'pong'
  // Lambda 09 backend aliases (mapped in WebSocketService.handleMessage)
  | 'ack'       // → connection_ack
  | 'chunk'     // → answer_chunk  (payload field: content)
  | 'complete'  // → answer_complete
  | 'final';    // → answer_complete (streaming final chunk)

export interface WebSocketMessage {
  type: WebSocketMessageType;
  session_id?: string;
  // Streaming content
  delta?: string;              // frontend 'answer_chunk' field
  content?: string;            // backend 'chunk' / 'final' field
  // Completion payload
  answer?: string;             // 'answer_complete' / 'complete'
  confidence?: number;
  grounded?: boolean;
  sources?: SourceChunk[];
  citations?: SourceChunk[];   // Backend alias for sources in 'complete'
  follow_up_suggestions?: string[];
  // Error
  error?: string;
  // Ack
  message?: string;            // Backend 'ack' message field
  persona?: string;            // Backend 'ack' persona field
  timestamp?: string;
}

// =============================================================================
// ERROR MODELS
// =============================================================================

export interface ApiError {
  statusCode: number;
  error: string;
  message: string;
  details?: Record<string, unknown>;
  retry_after_seconds?: number;  // For 429
}

// =============================================================================
// VALIDATION CONSTANTS
// =============================================================================

export const VALIDATION = {
  QUERY_MIN_LENGTH: 1,
  QUERY_MAX_LENGTH: 500,
  MAX_FILE_SIZE_MB: 100,
  MAX_FILE_SIZE_BYTES: 100 * 1024 * 1024,
  RATE_LIMIT_PER_HOUR: 100
} as const;

export const ALLOWED_EXTENSIONS = [
  '.pdf', '.docx', '.txt', '.md', '.json', '.csv', 
  '.xlsx', '.yaml', '.yml', '.py', '.js', '.ts', 
  '.java', '.cobol', '.cbl', '.cob'
] as const;

// =============================================================================
// PERSONA CONFIG (6 Official Personas from Handoff Doc)
// =============================================================================

export interface PersonaConfig {
  id: string;
  name: string;
  label: string;              // Display label (same as name)
  apiPersona: PersonaType;
  description: string;
  challenges: string[];
  benefits: string[];
  example: string;
  icon: string;
  color: string;
}

// Alias for simpler usage
export type Persona = PersonaConfig;

export const PERSONAS: PersonaConfig[] = [
  {
    id: 'new-joiner',
    name: 'New Joiners',
    label: 'New Joiners',
    apiPersona: 'developer',
    description: 'Building application knowledge and exploring topics',
    challenges: [
      'Building application knowledge and exploring topics',
      'Limited SME availability for queries',
      'Rework due to lack of context'
    ],
    benefits: [
      'Accelerates learning',
      'Reduces dependency',
      'Provides contextual guidance for onboarding'
    ],
    example: 'What is TSS testing tool usage and common errors?',
    icon: 'user-plus',
    color: 'blue'
  },
  {
    id: 'designer',
    name: 'Designers',
    label: 'Designers',
    apiPersona: 'architect',
    description: 'Technical function explanations and comprehensive searches',
    challenges: [
      'Manual searches risk missing changes',
      'Explaining technical functions to other teams'
    ],
    benefits: [
      'Ensures comprehensive searches',
      'Simplifies technical explanations',
      'Reduces mentoring time'
    ],
    example: 'How do credit interest calculations work?',
    icon: 'palette',
    color: 'purple'
  },
  {
    id: 'qa-tester',
    name: 'QA Testers',
    label: 'QA Testers',
    apiPersona: 'qa_tester',
    description: 'Test case generation and coverage tracking',
    challenges: [
      'Time-intensive research on changes',
      'Error-prone manual test data creation',
      'Difficulty tracking test coverage'
    ],
    benefits: [
      'Faster, more accurate test solutions',
      'Quick access to past coverage',
      'Accelerated issue resolution'
    ],
    example: 'What are the inquiry transaction formats and field details?',
    icon: 'clipboard-check',
    color: 'orange'
  },
  {
    id: 'developer',
    name: 'Developers',
    label: 'Developers',
    apiPersona: 'developer',
    description: 'Code architecture and knowledge transfer',
    challenges: [
      'Tracking changes across multiple releases',
      'Difficulty finding unfamiliar info',
      'Knowledge transfer to multiple new joiners'
    ],
    benefits: [
      'Summarizes changes',
      'Seamless access to topics',
      'Streamlines onboarding and reduces training'
    ],
    example: 'Find why and when changes were made for MECH transaction log data requests',
    icon: 'code',
    color: 'green'
  },
  {
    id: 'tech-support',
    name: 'Technical Production Support',
    label: 'Tech Support',
    apiPersona: 'support_engineer',
    description: 'On-call support and dependency analysis',
    challenges: [
      'High-pressure searches during on-call',
      'Finding past solutions',
      'Validating processes',
      'Understanding dependencies during failures'
    ],
    benefits: [
      'Faster results with exhaustive searches',
      'Quick access to historical solutions',
      'Comprehensive dependency analysis'
    ],
    example: 'What are challenges with offline file recoveries and MECH posting?',
    icon: 'server',
    color: 'red'
  },
  {
    id: 'business-support',
    name: 'Business Production Support',
    label: 'Business Support',
    apiPersona: 'business_analyst',
    description: 'Business queries and policy updates',
    challenges: [
      'Handling routine business queries',
      'Finding latest updates on policy and operations',
      'Getting additional business rules',
      'New implementation to business-related policies'
    ],
    benefits: [
      'Faster resolution for frequent queries',
      'Latest and broader information',
      'Detailed changes in document search'
    ],
    example: 'Explain why customer financial items appear in a specific report (e.g., holds)',
    icon: 'briefcase',
    color: 'yellow'
  }
];

// =============================================================================
// UI STATE MODELS
// =============================================================================

export interface ChatSession {
  id: string;
  title: string;
  createdAt: string;
  lastActivity: string;
  messages: ChatMessage[];
  persona: string;
}

export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'error';

// =============================================================================
// AGENT EVENT MODELS (AI Thinking Overlay)
// =============================================================================

export type AgentEventType = 
  | 'orchestrator_decision'
  | 'orchestrator_routing'
  | 'orchestrator_started'
  | 'agent_decision'
  | 'agent_thinking'
  | 'agent_started'
  | 'agent_completed'
  | 'team_started'
  | 'team_completed'
  | 'error';

export interface AgentEvent {
  event_id: string;
  event_type: AgentEventType;
  source: string;
  data: Record<string, unknown>;
  timestamp: string;
}

// UI-friendly decision item (transformed from AgentEvent)
export interface DecisionItem {
  id: string;
  type: 'orchestrator' | 'agent' | 'thinking' | 'complete' | 'error';
  sourceName: string;
  text: string;
  reasoning?: string;
}

// =============================================================================
// HUMAN FEEDBACK MODELS (Human-in-the-Loop)
// =============================================================================

export interface DownloadOption {
  type: string;
  label: string;
  url: string;
  filename: string;
}

export interface FeedbackOption {
  value: string;
  label: string;
  description: string;
}

export interface HumanFeedbackRequest {
  requestId: string;
  phase: string;
  phaseDisplay: string;
  summary: string;
  options: FeedbackOption[];
  availableDownloads?: DownloadOption[];
  timeout?: number;
}

export interface HumanFeedbackResponse {
  requestId: string;
  selectedOption: string;
  userInput?: string;
  timestamp: string;
}

// =============================================================================
// RE-EXPORT API MODELS
// =============================================================================

export * from './api.models';
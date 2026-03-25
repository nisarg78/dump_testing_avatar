/**
 * MECH Avatar - API Service
 * 6 REST Endpoints as per FRONTEND_HANDOFF.md:
 * - POST /chat/query - Ask a question
 * - GET /chat/history/{session_id} - Get past messages
 * - POST /documents/upload - Start document upload
 * - GET /documents/status/{document_id} - Check upload progress
 * - GET /documents/list - List all documents
 * - DELETE /documents/{document_id} - Delete a document
 */
import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Observable, throwError, catchError, of, delay, map } from 'rxjs';
import { environment } from '@environments/environment';
import {
  ChatQueryRequest,
  ChatQueryResponse,
  ChatHistoryResponse,
  ChatMessage,
  SourceChunk,
  DocumentUploadRequest,
  DocumentUploadResponse,
  DocumentStatusResponse,
  DocumentListResponse,
  DocumentDeleteResponse,
  ApiError,
  VALIDATION
} from '@app/core/models';

@Injectable({
  providedIn: 'root'
})
export class ApiService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = environment.api.endpoint;

  // ==========================================================================
  // CHAT ENDPOINTS
  // ==========================================================================

  /**
   * POST /chat/query
   * Ask a question (1-500 chars, rate limited: 100/hour)
   */
  sendQuery(request: ChatQueryRequest): Observable<ChatQueryResponse> {
    // Validate query
    const validationError = this.validateQuery(request.query);
    if (validationError) {
      return throwError(() => ({
        statusCode: 400,
        error: 'VALIDATION_ERROR',
        message: validationError
      }));
    }

    // Check rate limit
    if (this.isRateLimited()) {
      const resetTime = this.getRateLimitResetTime();
      return throwError(() => ({
        statusCode: 429,
        error: 'RATE_LIMITED',
        message: `Rate limit exceeded. Resets at ${resetTime?.toLocaleTimeString() || 'soon'}.`
      }));
    }

    // DEV MODE: Return mock response only when explicitly enabled in the dev environment.
    if (!environment.production && environment.api?.useMock) {
      return this.getMockResponse(request);
    }
    
    return this.http.post<ChatQueryResponse>(
      `${this.baseUrl}/chat/query`,
      request
    ).pipe(
      map(r => this.normalizeChatResponse(r)),
      catchError(this.handleError.bind(this))
    );
  }

  /**
   * Mock response for development
   */
  private getMockResponse(request: ChatQueryRequest): Observable<ChatQueryResponse> {
    const mockResponses: Record<string, string> = {
      'hello': 'Hello! I\'m MECH Avatar, your AI assistant for BMO documentation and knowledge. How can I help you today?',
      'help': 'I can help you with:\n\n• **Documentation Search** - Find technical docs, procedures, and guides\n• **Code Examples** - Get code snippets and implementation details\n• **Troubleshooting** - Diagnose issues and find solutions\n• **Best Practices** - Learn recommended approaches\n\nJust ask me a question!',
      'default': `Based on your query "${request.query}", here's what I found:\n\nThis is a **mock response** for development purposes. In production, this would connect to the actual MECH Avatar backend API.\n\n**Key Points:**\n1. The system would search through indexed documents\n2. Relevant sources would be cited\n3. Confidence scores would be calculated\n\nPlease connect to the backend API for real responses.`
    };

    const query = request.query.toLowerCase();
    let answer = mockResponses['default'];
    
    if (query.includes('hello') || query.includes('hi')) {
      answer = mockResponses['hello'];
    } else if (query.includes('help')) {
      answer = mockResponses['help'];
    }

    const mockResponse: ChatQueryResponse = {
      statusCode: 200,
      answer,
      confidence: 0.85 + Math.random() * 0.1,
      grounded: true,
      execution_time_ms: 500 + Math.random() * 1000,
      sources: [
        {
          chunk_id: 'mock-chunk-1',
          document_id: 'mock-doc-1',
          document_name: 'MECH Documentation',
          title: 'Getting Started Guide',
          section: 'Overview',
          page_num: 1,
          content: 'This is mock source content for development testing.',
          text_display: 'Mock source content...',
          relevance_score: 0.92,
          chunk_type: 'text'
        }
      ],
      follow_up_suggestions: [
        'Tell me more about this topic',
        'What are the best practices?',
        'Show me an example'
      ],
      session_id: request.session_id
    };

    // Simulate network delay
    return of(mockResponse).pipe(delay(800 + Math.random() * 700));
  }

  /**
   * GET /chat/history/{session_id}
   * Get past messages for a session
   */
  getChatHistory(sessionId: string): Observable<ChatHistoryResponse> {
    if (!sessionId || sessionId.trim().length === 0) {
      return throwError(() => ({
        statusCode: 400,
        error: 'VALIDATION_ERROR',
        message: 'Session ID is required'
      }));
    }

    return this.http.get<ChatHistoryResponse>(
      `${this.baseUrl}/chat/history/${sessionId}`
    ).pipe(
      map(r => this.normalizeChatHistory(r)),
      catchError(this.handleError.bind(this))
    );
  }

  // ==========================================================================
  // DOCUMENT ENDPOINTS
  // ==========================================================================

  /**
   * POST /documents/upload
   * Start document upload (gets presigned URL).
   * Accepts either a DocumentUploadRequest directly or a File object for convenience.
   * Max file size: 100 MB
   */
  initiateUpload(fileOrRequest: File | DocumentUploadRequest): Observable<DocumentUploadResponse> {
    let request: DocumentUploadRequest;

    if (fileOrRequest instanceof File) {
      const ext = fileOrRequest.name.split('.').pop()?.toLowerCase() ?? '';
      request = {
        file_name: fileOrRequest.name,
        file_type: ext,
        file_size: fileOrRequest.size
      };
    } else {
      request = fileOrRequest;
    }

    // Validate file size (uses file_size to match backend field name)
    const maxSize = 100 * 1024 * 1024; // 100 MB
    if (request.file_size > maxSize) {
      return throwError(() => ({
        statusCode: 400,
        error: 'VALIDATION_ERROR',
        message: `File size exceeds maximum of 100 MB`
      }));
    }

    return this.http.post<DocumentUploadResponse>(
      `${this.baseUrl}/documents/upload`,
      request
    ).pipe(catchError(this.handleError.bind(this)));
  }

  /**
   * Upload file to S3 using presigned URL
   */
  uploadToS3(presignedUrl: string, file: File): Observable<void> {
    return this.http.put<void>(presignedUrl, file, {
      headers: { 'Content-Type': file.type }
    });
  }

  /**
   * GET /documents/status/{document_id}
   * Check upload/processing progress
   */
  getDocumentStatus(documentId: string): Observable<DocumentStatusResponse> {
    if (!documentId || documentId.trim().length === 0) {
      return throwError(() => ({
        statusCode: 400,
        error: 'VALIDATION_ERROR',
        message: 'Document ID is required'
      }));
    }

    return this.http.get<DocumentStatusResponse>(
      `${this.baseUrl}/documents/status/${documentId}`
    ).pipe(catchError(this.handleError.bind(this)));
  }

  /**
   * GET /documents/list
   * List all documents
   */
  listDocuments(page = 1, pageSize = 20): Observable<DocumentListResponse> {
    return this.http.get<DocumentListResponse>(
      `${this.baseUrl}/documents/list`,
      { params: { page: page.toString(), page_size: pageSize.toString() } }
    ).pipe(catchError(this.handleError.bind(this)));
  }

  /**
   * DELETE /documents/{document_id}
   * Delete a document
   */
  deleteDocument(documentId: string): Observable<DocumentDeleteResponse> {
    return this.http.delete<DocumentDeleteResponse>(
      `${this.baseUrl}/documents/${documentId}`
    ).pipe(catchError(this.handleError.bind(this)));
  }

  // ==========================================================================
  // RESPONSE NORMALIZATION
  // Transforms the raw Lambda 06/08 shape into the clean frontend interfaces.
  // ==========================================================================

  /**
   * Normalize a raw chat query response from Lambda 08 → ChatQueryResponse.
   *
   * Handles:
   *   - Lambda 08 wraps Lambda 06 response in 'body' field
   *   - confidence: Lambda 06 returns { score, calibrated }; we flatten to number.
   *   - grounded:   Lambda 06 returns grounding.status string; we map to boolean.
   *   - execution_time_ms: Backend uses metrics.total_ms.
   *   - follow_up_suggestions: Backend may omit; default to [].
   *   - sources: normalise each SourceChunk field names.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private normalizeChatResponse(raw: any): ChatQueryResponse {
    // Lambda 08 wraps Lambda 06's response in a 'body' field
    // Handle both direct response and wrapped response
    let data = raw;
    if (raw.body && typeof raw.body === 'object') {
      data = raw.body;
    } else if (raw.body && typeof raw.body === 'string') {
      try {
        data = JSON.parse(raw.body);
      } catch {
        data = raw;
      }
    }

    const confidence =
      typeof data.confidence === 'object' && data.confidence !== null
        ? (data.confidence?.score ?? 0)
        : (typeof data.confidence === 'number' ? data.confidence : 0);

    const grounded =
      typeof data.grounded === 'boolean'
        ? data.grounded
        : data.grounding?.status === 'grounded' || (data.grounding?.groundedness_score ?? 0) >= 0.7;

    const executionMs =
      data.execution_time_ms ??
      data.metrics?.total_ms ??
      0;

    return {
      statusCode:             raw.statusCode ?? data.statusCode ?? 200,
      answer:                 data.answer ?? '',
      confidence,
      grounded,
      execution_time_ms:      executionMs,
      sources:                (data.sources ?? []).map((s: any) => this.normalizeSourceChunk(s)),
      follow_up_suggestions:  data.follow_up_suggestions ?? [],
      session_id:             data.session_id ?? raw.session_id ?? '',
      grounding:              data.grounding,
      persona:                data.persona ?? raw.persona,
      intent:                 data.intent,
      metrics:                data.metrics,
    } as ChatQueryResponse;
  }

  /**
   * Normalise a single source chunk from Lambda 06 into SourceChunk.
   *
   * Lambda 06 uses: file_name, page_number, section_title, text_display
   * Frontend historically used: document_name, page_num, section, content
   *
   * We keep both so templates can use either.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private normalizeSourceChunk(src: any): SourceChunk {
    return {
      chunk_id:        src.chunk_id ?? '',
      document_id:     src.document_id ?? '',
      // File name — support both spellings
      file_name:       src.file_name ?? src.document_name,
      document_name:   src.document_name ?? src.file_name,
      // Section title — support both spellings
      section_title:   src.section_title ?? src.title ?? src.section,
      title:           src.title ?? src.section_title,
      section:         src.section ?? src.section_title,
      // Page — support both spellings
      page_number:     src.page_number ?? src.page_num,
      page_num:        src.page_num ?? src.page_number,
      line_start:      src.line_start,
      line_end:        src.line_end,
      // Content — support both spellings
      text_display:    src.text_display ?? src.content,
      content:         src.content ?? src.text_display,
      content_type:    src.content_type,
      relevance_score: src.relevance_score ?? 0,
      chunk_type:      src.chunk_type ?? 'text',
      entities:        src.entities ?? [],
      match_highlight: src.match_highlight,
    } as SourceChunk;
  }

  /**
   * Normalise chat history response from Lambda 08.
   * Backend returns messages with 'message_id' instead of 'id'.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private normalizeChatHistory(raw: any): ChatHistoryResponse {
    const messages: ChatMessage[] = (raw.messages ?? []).map((m: any) => ({
      id:        m.id ?? m.message_id ?? '',
      role:      m.role,
      content:   m.content,
      timestamp: m.timestamp ?? new Date().toISOString(),
      sources:   m.citations ? m.citations.map(this.normalizeSourceChunk) : (m.sources ?? []),
    } as ChatMessage));

    return {
      session_id:    raw.session_id,
      messages,
      count:         raw.count,
      created_at:    raw.created_at,
      last_activity: raw.last_activity,
    };
  }

  // ==========================================================================
  // RATE LIMITING
  // ==========================================================================

  private rateLimitState = {
    remaining: 100,
    resetTime: 0,
    isLimited: false
  };

  /**
   * Check if rate limited
   */
  isRateLimited(): boolean {
    if (this.rateLimitState.isLimited && Date.now() < this.rateLimitState.resetTime) {
      return true;
    }
    this.rateLimitState.isLimited = false;
    return false;
  }

  /**
   * Get remaining rate limit
   */
  getRateLimitRemaining(): number {
    return this.rateLimitState.remaining;
  }

  /**
   * Get rate limit reset time
   */
  getRateLimitResetTime(): Date | null {
    if (this.rateLimitState.resetTime > 0) {
      return new Date(this.rateLimitState.resetTime);
    }
    return null;
  }

  // ==========================================================================
  // ERROR HANDLING
  // ==========================================================================

  private handleError(error: HttpErrorResponse): Observable<never> {
    let apiError: ApiError;

    if (error.error && typeof error.error === 'object') {
      apiError = error.error as ApiError;
    } else {
      apiError = {
        statusCode: error.status,
        error: this.getErrorType(error.status),
        message: this.getErrorMessage(error.status),
        details: { originalError: error.message }
      };
    }

    // Handle rate limiting (429)
    if (error.status === 429) {
      const retryAfter = apiError.retry_after_seconds || 60;
      this.rateLimitState.isLimited = true;
      this.rateLimitState.remaining = 0;
      this.rateLimitState.resetTime = Date.now() + (retryAfter * 1000);
      apiError.message = `Rate limit exceeded. Please try again in ${retryAfter} seconds.`;
    }

    // Extract rate limit headers if present
    const remaining = error.headers?.get('X-RateLimit-Remaining');
    const reset = error.headers?.get('X-RateLimit-Reset');
    if (remaining) {
      this.rateLimitState.remaining = parseInt(remaining, 10);
    }
    if (reset) {
      this.rateLimitState.resetTime = parseInt(reset, 10) * 1000;
    }

    console.error('[API Error]', {
      status: apiError.statusCode,
      error: apiError.error,
      message: apiError.message,
      details: apiError.details
    });

    return throwError(() => apiError);
  }

  private getErrorType(status: number): string {
    const types: Record<number, string> = {
      400: 'BAD_REQUEST',
      401: 'UNAUTHORIZED',
      403: 'FORBIDDEN',
      404: 'NOT_FOUND',
      429: 'RATE_LIMITED',
      500: 'INTERNAL_ERROR',
      502: 'BAD_GATEWAY',
      503: 'SERVICE_UNAVAILABLE'
    };
    return types[status] || 'UNKNOWN_ERROR';
  }

  private getErrorMessage(status: number): string {
    const messages: Record<number, string> = {
      400: 'Invalid request. Please check your input and try again.',
      401: 'Your session has expired. Please log in again.',
      403: 'You do not have permission to access this resource.',
      404: 'The requested resource was not found.',
      429: 'Rate limit exceeded. Please wait before trying again.',
      500: 'An internal server error occurred. Please try again later.',
      502: 'Bad gateway. The server is temporarily unavailable.',
      503: 'Service temporarily unavailable. Please try again later.'
    };
    return messages[status] || 'An unexpected error occurred. Please try again.';
  }

  /**
   * Validate query input before sending
   * Query rules: 1-500 characters
   * Returns error message or null if valid
   */
  validateQuery(query: string): string | null {
    if (!query || query.trim().length === 0) {
      return 'Query cannot be empty';
    }
    if (query.length < VALIDATION.QUERY_MIN_LENGTH) {
      return `Query must be at least ${VALIDATION.QUERY_MIN_LENGTH} character`;
    }
    if (query.length > VALIDATION.QUERY_MAX_LENGTH) {
      return `Query must not exceed ${VALIDATION.QUERY_MAX_LENGTH} characters`;
    }
    return null;
  }

  /**
   * Validate file before upload
   * File rules: Max 100 MB, supported extensions only
   * Returns error message or null if valid
   */
  validateFile(file: File): string | null {
    if (file.size > VALIDATION.MAX_FILE_SIZE_BYTES) {
      const maxMB = VALIDATION.MAX_FILE_SIZE_MB;
      const currentMB = (file.size / 1024 / 1024).toFixed(2);
      return `File size exceeds maximum of ${maxMB} MB (current: ${currentMB} MB)`;
    }

    const extension = '.' + file.name.split('.').pop()?.toLowerCase();
    const allowedExtensions = [
      '.pdf', '.docx', '.txt', '.md', '.json', '.csv',
      '.xlsx', '.yaml', '.yml', '.py', '.js', '.ts',
      '.java', '.cobol', '.cbl', '.cob'
    ];

    if (!allowedExtensions.includes(extension)) {
      return `File type "${extension}" not supported. Allowed: ${allowedExtensions.join(', ')}`;
    }

    return null;
  }
}

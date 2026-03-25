/**
 * MECH Avatar - WebSocket Service
 * Real-time streaming for chat responses
 * Supports WebSocket message types: connection_ack, answer_chunk, answer_complete, sources, error, ping/pong
 */
import { Injectable, signal, OnDestroy } from '@angular/core';
import { Subject, Observable, timer } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { environment } from '@environments/environment';
import { 
  WebSocketMessage, 
  SourceChunk, 
  ConnectionState,
  VALIDATION
} from '@app/core/models';
import { AuthService } from './auth.service';

@Injectable({
  providedIn: 'root'
})
export class WebSocketService implements OnDestroy {
  private socket: WebSocket | null = null;
  private readonly destroy$ = new Subject<void>();
  private reconnectAttempts = 0;
  private readonly maxReconnectAttempts = 5;
  private heartbeatInterval: ReturnType<typeof setInterval> | null = null;

  // Signals
  private readonly _connectionState = signal<ConnectionState>('disconnected');
  readonly connectionState = this._connectionState.asReadonly();

  // Subjects for streaming
  private readonly chunkSubject = new Subject<string>();
  private readonly completeSubject = new Subject<{
    answer: string;
    confidence: number;
    grounded: boolean;
    sources: SourceChunk[];
    follow_up_suggestions: string[];
  }>();
  private readonly errorSubject = new Subject<string>();

  // Observables
  readonly chunk$ = this.chunkSubject.asObservable();
  readonly complete$ = this.completeSubject.asObservable();
  readonly error$ = this.errorSubject.asObservable();

  constructor(private authService: AuthService) {}

  /**
   * Connect to WebSocket
   */
  connect(): void {
    if (this.socket?.readyState === WebSocket.OPEN) {
      return;
    }

    this._connectionState.set('connecting');

    try {
      const token = this.authService.getIdToken();
      if (!token) {
        console.error('[WebSocket] No auth token available');
        this._connectionState.set('error');
        return;
      }
      
      const wsUrl = `${environment.ws.endpoint}?token=${token}`;
      this.socket = new WebSocket(wsUrl);
      this.setupEventHandlers();
    } catch (error) {
      console.error('[WebSocket] Connection failed:', error);
      this._connectionState.set('error');
      this.scheduleReconnect();
    }
  }

  /**
   * Disconnect WebSocket
   */
  disconnect(): void {
    this.clearHeartbeat();
    if (this.socket) {
      this.socket.close(1000, 'Client disconnect');
      this.socket = null;
    }
    this._connectionState.set('disconnected');
    this.reconnectAttempts = 0;
  }

  /**
   * Send query via WebSocket (for streaming response)
   * Query validation: 1-500 characters
   */
  sendQuery(query: string, sessionId: string, persona?: string): boolean {
    // Validate query length
    if (!query || query.trim().length < VALIDATION.QUERY_MIN_LENGTH) {
      this.errorSubject.next('Query cannot be empty');
      return false;
    }

    if (query.length > VALIDATION.QUERY_MAX_LENGTH) {
      this.errorSubject.next(`Query exceeds maximum length of ${VALIDATION.QUERY_MAX_LENGTH} characters`);
      return false;
    }

    // Validate session ID
    if (!sessionId || sessionId.trim().length === 0) {
      this.errorSubject.next('Session ID is required');
      return false;
    }

    if (this.socket?.readyState !== WebSocket.OPEN) {
      this.errorSubject.next('WebSocket not connected. Please wait for connection.');
      return false;
    }

    const message = {
      type: 'query',
      query: query.trim(),
      session_id: sessionId,
      persona
    };

    this.socket.send(JSON.stringify(message));
    return true;
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    this.disconnect();
  }

  // ==========================================================================
  // PRIVATE METHODS
  // ==========================================================================

  private setupEventHandlers(): void {
    if (!this.socket) return;

    this.socket.onopen = () => {
      console.log('[WebSocket] Connected');
      this._connectionState.set('connected');
      this.reconnectAttempts = 0;
      this.startHeartbeat();
    };

    this.socket.onmessage = (event) => {
      this.handleMessage(event);
    };

    this.socket.onclose = (event) => {
      console.log('[WebSocket] Closed:', event.code, event.reason);
      this._connectionState.set('disconnected');
      this.clearHeartbeat();
      
      if (event.code !== 1000) {
        this.scheduleReconnect();
      }
    };

    this.socket.onerror = (error) => {
      console.error('[WebSocket] Error:', error);
      this._connectionState.set('error');
    };
  }

  private handleMessage(event: MessageEvent): void {
    try {
      const message: WebSocketMessage = JSON.parse(event.data);

      switch (message.type) {
        // ---- Canonical frontend types ----
        case 'connection_ack':
        // Backend Lambda 09 sends 'ack' instead of 'connection_ack'
        case 'ack':
          console.log('[WebSocket] Connection acknowledged');
          break;

        case 'answer_chunk':
        // Backend Lambda 09 sends 'chunk' with field 'content' (not 'delta')
        case 'chunk': {
          const chunkText = message.delta ?? message.content ?? '';
          if (chunkText) {
            this.chunkSubject.next(chunkText);
          }
          break;
        }

        // Backend Lambda 09 sends 'final' as the last streaming chunk
        case 'final': {
          const finalText = message.content ?? message.answer ?? '';
          if (finalText) {
            this.chunkSubject.next(finalText);
          }
          break;
        }

        case 'answer_complete':
        // Backend Lambda 09 sends 'complete' (sources in both 'sources' and 'citations')
        case 'complete':
          this.completeSubject.next({
            answer: message.answer ?? '',
            confidence: typeof message.confidence === 'number' ? message.confidence : 0,
            grounded: message.grounded ?? true,
            sources: message.sources ?? message.citations ?? [],
            follow_up_suggestions: message.follow_up_suggestions ?? []
          });
          break;

        case 'sources':
          // Sources can come separately (no-op — handled in 'complete')
          break;

        case 'error':
          this.errorSubject.next(message.error ?? 'Unknown error');
          break;

        case 'pong':
          // Heartbeat response
          break;
      }
    } catch (error) {
      console.error('[WebSocket] Failed to parse message:', error);
    }
  }

  private startHeartbeat(): void {
    this.clearHeartbeat();
    this.heartbeatInterval = setInterval(() => {
      if (this.socket?.readyState === WebSocket.OPEN) {
        this.socket.send(JSON.stringify({ type: 'ping' }));
      }
    }, 30000);
  }

  private clearHeartbeat(): void {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      console.error('[WebSocket] Max reconnect attempts reached');
      this._connectionState.set('error');
      return;
    }

    this.reconnectAttempts++;
    const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
    
    console.log(`[WebSocket] Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`);
    
    timer(delay).pipe(takeUntil(this.destroy$)).subscribe(() => {
      this.connect();
    });
  }
}
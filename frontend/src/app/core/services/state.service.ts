/**
 * MECH Avatar - State Service
 * Simple state management using Angular signals
 * Persists conversations per-user in localStorage
 */
import { Injectable, signal, computed } from '@angular/core';
import { 
  ChatMessage, 
  Persona, 
  RecentConversation,
  ConnectionState 
} from '@app/core/models';

// localStorage keys (suffixed with userId at runtime)
const STORAGE_KEYS = {
  recentConversations: 'mech_recent_conversations',
  conversationMessages: 'mech_conv_msgs_',  // + sessionId
} as const;

@Injectable({
  providedIn: 'root'
})
export class StateService {
  // Session
  private readonly _sessionId = signal<string>(this.generateSessionId());
  readonly sessionId = this._sessionId.asReadonly();

  // Messages
  private readonly _messages = signal<ChatMessage[]>([]);
  readonly messages = this._messages.asReadonly();

  // Persona
  private readonly _selectedPersona = signal<Persona | null>(null);
  readonly selectedPersona = this._selectedPersona.asReadonly();

  // Recent Conversations
  private readonly _recentConversations = signal<RecentConversation[]>([]);
  readonly recentConversations = this._recentConversations.asReadonly();

  // UI State
  private readonly _isLoading = signal<boolean>(false);
  readonly isLoading = this._isLoading.asReadonly();

  private readonly _currentStreamingMessage = signal<string>('');
  readonly currentStreamingMessage = this._currentStreamingMessage.asReadonly();

  // Current user id for scoping localStorage
  private _userId = '';

  // Computed
  readonly hasMessages = computed(() => this._messages().length > 0);
  readonly messageCount = computed(() => this._messages().length);

  // ==========================================================================
  // INITIALIZATION
  // ==========================================================================

  /**
   * Initialize state for a specific user — call after login
   */
  initForUser(userId: string): void {
    this._userId = userId;
    this._recentConversations.set(this.loadRecentFromStorage());
  }

  // ==========================================================================
  // SESSION MANAGEMENT
  // ==========================================================================

  /**
   * Start a new chat — saves the current conversation first
   */
  newSession(): void {
    // Save current conversation before clearing
    this.saveCurrentToRecent();

    // Start fresh
    this._sessionId.set(this.generateSessionId());
    this._messages.set([]);
    this._currentStreamingMessage.set('');
  }

  // ==========================================================================
  // MESSAGE MANAGEMENT
  // ==========================================================================

  /**
   * Add user message
   */
  addUserMessage(query: string): void {
    const message: ChatMessage = {
      id: this.generateMessageId(),
      role: 'user',
      content: query,
      timestamp: new Date().toISOString()
    };
    this._messages.update(msgs => [...msgs, message]);
    this.persistCurrentMessages();
  }

  /**
   * Add assistant message
   */
  addAssistantMessage(
    content: string, 
    confidence: number,
    grounded: boolean,
    sources: ChatMessage['sources'],
    followUpSuggestions?: string[]
  ): void {
    const message: ChatMessage = {
      id: this.generateMessageId(),
      role: 'assistant',
      content,
      confidence,
      grounded,
      sources,
      followUpSuggestions,
      timestamp: new Date().toISOString()
    };
    this._messages.update(msgs => [...msgs, message]);
    this.persistCurrentMessages();
  }

  /**
   * Update current streaming content
   */
  appendStreamingContent(chunk: string): void {
    this._currentStreamingMessage.update(content => content + chunk);
  }

  /**
   * Clear streaming content
   */
  clearStreamingContent(): void {
    this._currentStreamingMessage.set('');
  }

  /**
   * Clear all messages
   */
  clearMessages(): void {
    this._messages.set([]);
    this._currentStreamingMessage.set('');
  }

  // ==========================================================================
  // PERSONA MANAGEMENT
  // ==========================================================================

  /**
   * Set selected persona
   */
  setPersona(persona: Persona | null): void {
    this._selectedPersona.set(persona);
  }

  // ==========================================================================
  // LOADING STATE
  // ==========================================================================

  /**
   * Set loading state
   */
  setLoading(loading: boolean): void {
    this._isLoading.set(loading);
  }

  // ==========================================================================
  // RECENT CONVERSATIONS
  // ==========================================================================

  /**
   * Save the current conversation to the recent list and persist messages
   */
  private saveCurrentToRecent(): void {
    const messages = this._messages();
    if (messages.length === 0) return;

    const firstUserMessage = messages.find(m => m.role === 'user');
    if (!firstUserMessage) return;

    const sessionId = this._sessionId();
    const conversation: RecentConversation = {
      id: sessionId,
      title: this.truncate(firstUserMessage.content, 50),
      timestamp: new Date().toISOString(),
      messageCount: messages.length
    };

    this._recentConversations.update(convs => {
      const filtered = convs.filter(c => c.id !== conversation.id);
      return [conversation, ...filtered].slice(0, 20);
    });

    // Persist to localStorage
    this.persistCurrentMessages();
    this.saveRecentToStorage();
  }

  /**
   * Load a previous conversation by ID
   */
  loadConversation(conversationId: string): void {
    // Save current conversation first (if it has messages)
    this.saveCurrentToRecent();

    this._sessionId.set(conversationId);
    this._currentStreamingMessage.set('');

    // Restore messages from localStorage
    const stored = this.loadMessagesFromStorage(conversationId);
    this._messages.set(stored);
  }

  /**
   * Set messages (for loading from API)
   */
  setMessages(messages: ChatMessage[]): void {
    this._messages.set(messages);
    this.persistCurrentMessages();
  }

  // ==========================================================================
  // LOCAL STORAGE PERSISTENCE
  // ==========================================================================

  private storageKey(key: string): string {
    return this._userId ? `${key}_${this._userId}` : key;
  }

  private persistCurrentMessages(): void {
    const sessionId = this._sessionId();
    const messages = this._messages();
    try {
      localStorage.setItem(
        this.storageKey(STORAGE_KEYS.conversationMessages + sessionId),
        JSON.stringify(messages)
      );
    } catch { /* quota exceeded — silently ignore */ }
  }

  private loadMessagesFromStorage(sessionId: string): ChatMessage[] {
    try {
      const raw = localStorage.getItem(
        this.storageKey(STORAGE_KEYS.conversationMessages + sessionId)
      );
      return raw ? JSON.parse(raw) : [];
    } catch {
      return [];
    }
  }

  private saveRecentToStorage(): void {
    try {
      localStorage.setItem(
        this.storageKey(STORAGE_KEYS.recentConversations),
        JSON.stringify(this._recentConversations())
      );
    } catch { /* quota exceeded — silently ignore */ }
  }

  private loadRecentFromStorage(): RecentConversation[] {
    try {
      const raw = localStorage.getItem(
        this.storageKey(STORAGE_KEYS.recentConversations)
      );
      return raw ? JSON.parse(raw) : [];
    } catch {
      return [];
    }
  }

  // ==========================================================================
  // UTILS
  // ==========================================================================

  private generateSessionId(): string {
    return crypto.randomUUID();
  }

  private generateMessageId(): string {
    return `msg_${crypto.randomUUID()}`;
  }

  private truncate(text: string, maxLength: number): string {
    if (text.length <= maxLength) return text;
    return text.substring(0, maxLength - 3) + '...';
  }
}

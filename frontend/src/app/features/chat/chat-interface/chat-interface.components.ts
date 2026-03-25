import { Component, inject, signal, ViewChild, ElementRef, OnInit, OnDestroy, AfterViewChecked, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Subject } from 'rxjs';
import { firstValueFrom } from 'rxjs';
import { StateService } from '@app/core/services/state.service';
import { ApiService } from '@app/core/services/api.service';
import { AuthService } from '@app/core/services/auth.service';
import { PERSONAS, VALIDATION, Persona } from '@app/core/models';
import { MessageComponent } from '../message/message.component';

/**
 * Chat Interface Component
 * Main chat interface following MECH Avatar spec
 */
@Component({
  selector: 'app-chat-interface',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MessageComponent
  ],
  template: `
    <div class="flex flex-col h-full bg-white">
      <!-- Mobile Header (visible on small screens) -->
      <div class="md:hidden flex items-center justify-between px-4 py-3 bg-white border-b border-gray-100">
        <div class="flex items-center gap-2">
          <span class="text-lg font-bold text-bmo-blue">MECH Avatar</span>
        </div>
        <!-- Persona Display (Read-only) -->
        @if (state.selectedPersona(); as persona) {
          <div class="flex items-center gap-2 px-3 py-1.5 bg-gray-50 rounded-lg">
            <div 
              class="w-5 h-5 rounded flex items-center justify-center"
              [class]="getPersonaIconBg(persona)"
            >
              <svg class="w-3 h-3 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"/>
              </svg>
            </div>
            <span class="text-xs font-medium text-gray-700">{{ persona.name }}</span>
          </div>
        }
      </div>

      <!-- Messages Area -->
      <div 
        #messagesContainer
        class="flex-1 overflow-y-auto px-3 sm:px-4 md:px-8 py-4 sm:py-6"
      >
        @if (state.messages().length === 0 && !state.currentStreamingMessage()) {
          <!-- Welcome Screen / Dashboard -->
          <div class="max-w-4xl mx-auto">
            <!-- Welcome Hero -->
            <div class="text-center mb-8 mt-8">
              <h2 class="text-xl sm:text-2xl md:text-2xl font-semibold text-gray-800 mb-2">
                Hello, {{ userName() }}!
              </h2>
              <p class="text-gray-500 text-lg">What can I help you with today?</p>
            </div>

            <!-- Quick Prompts Section -->
            <div class="mb-4">
              <h3 class="text-sm font-semibold text-gray-500 uppercase tracking-wide mb-3 px-1">Try asking</h3>
              <div class="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-2 sm:gap-3">
                @for (action of currentQuickActions(); track action.label) {
                  <button
                    (click)="setInput(action.prompt)"
                    class="prompt-card p-4 text-left rounded-xl bg-white border border-gray-100 hover:border-bmo-blue hover:shadow-md transition-all group"
                  >
                    <div class="flex items-start gap-3">
                      <div class="w-8 h-8 rounded-lg bg-blue-50 flex items-center justify-center flex-shrink-0 group-hover:bg-blue-100 transition-colors">
                        <span class="text-lg">{{ action.icon }}</span>
                      </div>
                      <div class="flex-1">
                        <p class="font-medium text-gray-800 text-sm mb-1">{{ action.label }}</p>
                        <p class="text-xs text-gray-500 line-clamp-2">{{ action.description }}</p>
                      </div>
                    </div>
                  </button>
                }
              </div>
            </div>
          </div>
        } @else {
          <!-- Message List -->
          <div class="max-w-3xl mx-auto space-y-4">
            @for (message of state.messages(); track message.id) {
              <app-message [message]="message" (followUp)="setInput($event)" />
            }
            
            <!-- Streaming Message -->
            @if (state.currentStreamingMessage()) {
              <div class="flex gap-3">
                <div class="w-10 h-10 rounded-xl bg-bmo-blue flex-shrink-0 flex items-center justify-center shadow-sm">
                  <svg class="w-5 h-5 text-white" fill="currentColor" viewBox="0 0 24 24">
                    <circle cx="9" cy="9" r="1.5"/>
                    <circle cx="15" cy="9" r="1.5"/>
                    <path d="M12 16c-1.48 0-2.75-.81-3.45-2h6.9c-.7 1.19-1.97 2-3.45 2z"/>
                  </svg>
                </div>
                <div class="flex-1 max-w-[85%]">
                  <div class="bg-white rounded-2xl rounded-tl-sm px-4 py-3 shadow-sm border border-gray-100">
                    <p class="text-gray-800 whitespace-pre-wrap">{{ state.currentStreamingMessage() }}</p>
                    <span class="inline-block w-2 h-5 bg-bmo-blue animate-pulse ml-1 rounded"></span>
                  </div>
                </div>
              </div>
            }
            
            <!-- Scroll anchor -->
            <div #scrollAnchor></div>
          </div>
        }
      </div>

      <!-- Loading Indicator -->
      @if (state.isLoading() && !state.currentStreamingMessage()) {
        <div class="px-6 py-3 bg-white border-t border-gray-100">
          <div class="max-w-3xl mx-auto flex items-center gap-3">
            <div class="loading-dots flex gap-1">
              <span class="w-2 h-2 bg-bmo-blue rounded-full animate-bounce" style="animation-delay: 0ms"></span>
              <span class="w-2 h-2 bg-bmo-blue rounded-full animate-bounce" style="animation-delay: 150ms"></span>
              <span class="w-2 h-2 bg-bmo-blue rounded-full animate-bounce" style="animation-delay: 300ms"></span>
            </div>
            <span class="text-sm text-gray-500">MECH Avatar is thinking...</span>
          </div>
        </div>
      }

      <!-- Input Area -->
      <div class="bg-white border-t border-gray-100 px-3 sm:px-4 py-3 sm:py-4">
        <div class="max-w-3xl mx-auto">
          <form (submit)="sendMessage($event)" class="flex gap-3 items-end">
            <!-- Text Input -->
            <div class="flex-1 relative">
              <input
                #textInput
                type="text"
                [ngModel]="inputText()"
                (ngModelChange)="inputText.set($event)"
                name="message"
                (keydown)="onKeyDown($event)"
                placeholder="What would you like to know?"
                class="w-full px-4 sm:px-5 py-3 sm:py-3.5 border border-gray-200 rounded-xl sm:rounded-2xl focus:outline-none focus:ring-2 focus:ring-bmo-blue focus:border-transparent bg-gray-50 hover:bg-gray-100 focus:bg-white transition-colors text-sm sm:text-base"
                [disabled]="state.isLoading()"
                [maxLength]="VALIDATION.QUERY_MAX_LENGTH"
              />
              <div class="absolute right-4 top-1/2 -translate-y-1/2 text-xs text-gray-400">
                {{ inputText().length }}/{{ VALIDATION.QUERY_MAX_LENGTH }}
              </div>
            </div>
            
            <!-- Send Button -->
            <button
              type="submit"
              [disabled]="!canSend()"
              class="send-button p-3 sm:p-4 rounded-xl sm:rounded-2xl transition-all shadow-sm hover:shadow-md flex-shrink-0"
              [class.opacity-50]="!canSend()"
              [class.cursor-not-allowed]="!canSend()"
            >
              @if (state.isLoading()) {
                <div class="loading-spinner-white"></div>
              } @else {
                <svg class="w-5 h-5 text-white" fill="currentColor" viewBox="0 0 24 24">
                  <path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z"/>
                </svg>
              }
            </button>
          </form>
        </div>
      </div>
    </div>
  `,
  styles: [`
    :host {
      display: flex;
      flex-direction: column;
      height: 100%;
    }
    
    /* BMO Brand Colors (Official Palette) */
    .chat-header {
      background-color: #0079C1;
    }
    
    .from-bmo-blue { --tw-gradient-from: #0079C1; }
    .to-bmo-blue-dark { --tw-gradient-to: #005587; }
    .bg-bmo-blue { background-color: #0079C1; }
    .text-bmo-blue { color: #0079C1; }
    .hover\\:border-bmo-blue:hover { border-color: #0079C1; }
    .hover\\:text-bmo-blue:hover { color: #0079C1; }
    .focus\\:ring-bmo-blue:focus { --tw-ring-color: #0079C1; }
    
    /* BMO Brand Icon Backgrounds */
    .icon-bg-blue { background-color: #0079C1; }
    .icon-bg-purple { background-color: #4c6cb3; }
    .icon-bg-orange { background-color: #ffc72c; }
    .icon-bg-green { background-color: #00843d; }
    .icon-bg-red { background-color: #ED1C24; }
    .icon-bg-yellow { background-color: #00a9e0; }
    
    .loading-spinner-white {
      width: 20px;
      height: 20px;
      border: 2px solid rgba(255, 255, 255, 0.3);
      border-top-color: white;
      border-radius: 50%;
      animation: spin 0.8s linear infinite;
    }
    
    @keyframes spin {
      to { transform: rotate(360deg); }
    }
    
    .prompt-card:hover {
      transform: translateY(-2px);
    }
    
    .persona-mini-card:hover {
      transform: translateY(-1px);
    }
    
    .send-button {
      background-color: #0079C1;
    }
    
    .send-button:hover:not(:disabled) {
      background-color: #005587;
    }
    
    .line-clamp-2 {
      display: -webkit-box;
      -webkit-line-clamp: 2;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }
    
    .gradient-text {
      color: #0079C1;
    }
  `]
})
export class ChatInterfaceComponent implements OnInit, OnDestroy, AfterViewChecked {
  readonly state = inject(StateService);
  private readonly api = inject(ApiService);
  private readonly auth = inject(AuthService);
  
  @ViewChild('messagesContainer') messagesContainer!: ElementRef<HTMLDivElement>;
  @ViewChild('scrollAnchor') scrollAnchor!: ElementRef<HTMLDivElement>;
  @ViewChild('textInput') textInput!: ElementRef<HTMLInputElement>;

  private destroy$ = new Subject<void>();
  private shouldScrollToBottom = false;

  // Use signal for reactive canSend computation
  inputText = signal('');
  
  // Constants from spec
  readonly VALIDATION = VALIDATION;
  readonly personas = PERSONAS;

  // Quick actions per persona
  private readonly personaQuickActions: Record<string, Array<{label: string; description: string; prompt: string; icon: string}>> = {
    'new-joiner': [
      { label: 'Getting Started', description: 'Learn the basics of MECH system and tools', prompt: 'What is TSS testing tool usage and common errors?', icon: '🚀' },
      { label: 'Onboarding Guide', description: 'Step-by-step guide for new team members', prompt: 'What are the first steps for getting started with MECH?', icon: '📖' },
      { label: 'Key Concepts', description: 'Understand core MECH terminology', prompt: 'Explain the key concepts and terminology used in MECH', icon: '💡' }
    ],
    'designer': [
      { label: 'Credit Calculations', description: 'Technical explanations for business logic', prompt: 'How do credit interest calculations work?', icon: '📊' },
      { label: 'System Changes', description: 'Track changes across releases', prompt: 'What changes were made in the latest release?', icon: '🔍' },
      { label: 'Technical Specs', description: 'Detailed technical specifications', prompt: 'Where can I find technical specifications for account processing?', icon: '📋' }
    ],
    'qa-tester': [
      { label: 'Transaction Formats', description: 'Field details and validation rules', prompt: 'What are the inquiry transaction formats and field details?', icon: '📝' },
      { label: 'Test Coverage', description: 'Review past test coverage', prompt: 'Show me test coverage for account balance inquiries', icon: '✅' },
      { label: 'Test Data', description: 'Generate test data scenarios', prompt: 'What test data is needed for payment processing tests?', icon: '🧪' }
    ],
    'developer': [
      { label: 'Code Changes', description: 'Track why and when changes were made', prompt: 'Find why and when changes were made for MECH transaction log data requests', icon: '💻' },
      { label: 'API Documentation', description: 'Endpoint specifications and usage', prompt: 'Show me the API documentation for account services', icon: '🔌' },
      { label: 'Architecture', description: 'System architecture and dependencies', prompt: 'Explain the system architecture for payment processing', icon: '🏗️' }
    ],
    'tech-support': [
      { label: 'Troubleshooting', description: 'Resolve production issues quickly', prompt: 'What are challenges with offline file recoveries and MECH posting?', icon: '🔧' },
      { label: 'Past Solutions', description: 'Find historical resolution steps', prompt: 'Show me past solutions for posting failures', icon: '📚' },
      { label: 'Dependencies', description: 'Analyze system dependencies', prompt: 'What are the dependencies for the batch processing system?', icon: '🔗' }
    ],
    'business-support': [
      { label: 'Business Rules', description: 'Policy and operational guidelines', prompt: 'Explain why customer financial items appear in a specific report (e.g., holds)', icon: '📑' },
      { label: 'Recent Updates', description: 'Latest policy changes', prompt: 'What are the latest updates on account hold policies?', icon: '📢' },
      { label: 'Common Queries', description: 'Frequently asked questions', prompt: 'What are the most common customer questions about account holds?', icon: '❓' }
    ]
  };

  // Default quick actions
  private readonly defaultQuickActions = [
    { label: 'Getting Started', description: 'Learn the basics of MECH Avatar', prompt: 'What are the first steps for getting started with MECH?', icon: '🚀' },
    { label: 'Documentation', description: 'Find relevant documentation', prompt: 'Where can I find technical documentation?', icon: '📚' },
    { label: 'Best Practices', description: 'Development guidelines and standards', prompt: 'What are the best practices for development?', icon: '✨' }
  ];

  // Computed: Quick actions based on selected persona
  currentQuickActions = computed(() => {
    const persona = this.state.selectedPersona();
    if (persona && this.personaQuickActions[persona.id]) {
      return this.personaQuickActions[persona.id];
    }
    return this.defaultQuickActions;
  });

  // Computed values
  personaLabel = computed((): string => {
    const persona = this.state.selectedPersona();
    return persona?.label || 'Select a persona';
  });

  selectedPersonaId = computed((): string => {
    return this.state.selectedPersona()?.id || PERSONAS[0].id;
  });

  userName = computed((): string => {
    const user = this.auth.user();
    return user?.name || user?.username || 'Name';
  });

  canSend = computed(() => {
    const text = this.inputText().trim();
    return text.length >= VALIDATION.QUERY_MIN_LENGTH 
           && text.length <= VALIDATION.QUERY_MAX_LENGTH
           && !this.state.isLoading();
  });

  ngOnInit(): void {
    // Set default persona
    if (!this.state.selectedPersona()) {
      this.state.setPersona(PERSONAS[0]);
    }
  }

  ngAfterViewChecked(): void {
    if (this.shouldScrollToBottom) {
      this.scrollToBottom();
      this.shouldScrollToBottom = false;
    }
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  /**
   * Get icon background class for persona
   */
  getPersonaIconBg(persona: Persona): string {
    const colorMap: Record<string, string> = {
      'blue': 'icon-bg-blue',
      'purple': 'icon-bg-purple',
      'orange': 'icon-bg-orange',
      'green': 'icon-bg-green',
      'red': 'icon-bg-red',
      'yellow': 'icon-bg-yellow'
    };
    return colorMap[persona.color] || 'icon-bg-blue';
  }

  /**
   * Handle persona change (legacy dropdown)
   */
  onPersonaChange(event: Event): void {
    const select = event.target as HTMLSelectElement;
    const persona = PERSONAS.find(p => p.id === select.value);
    if (persona) {
      this.state.setPersona(persona);
    }
  }

  /**
   * Start a new chat session
   */
  startNewChat(): void {
    this.state.newSession();
    this.inputText.set('');
  }

  /**
   * Send a message
   */
  async sendMessage(event: Event): Promise<void> {
    event.preventDefault();
    
    const query = this.inputText().trim();
    if (query.length < VALIDATION.QUERY_MIN_LENGTH) return;
    if (query.length > VALIDATION.QUERY_MAX_LENGTH) return;
    if (this.state.isLoading()) return;

    // Add user message
    this.state.addUserMessage(query);

    // Clear input
    this.inputText.set('');
    this.shouldScrollToBottom = true;
    this.state.setLoading(true);

    // Send via REST API
    try {
      const response = await firstValueFrom(this.api.sendQuery({
        query,
        session_id: this.state.sessionId(),
        persona: this.state.selectedPersona()?.apiPersona
      }));

      // Handle response
      if (response) {
        this.state.addAssistantMessage(
          response.answer || 'No response received.',
          response.confidence ?? 0,
          response.grounded ?? false,
          response.sources ?? [],
          response.follow_up_suggestions ?? []
        );
      }
      this.state.setLoading(false);
      this.shouldScrollToBottom = true;
    } catch (error: any) {
      console.error('Send query failed:', error);
      const errorMessage = error?.message || 'Sorry, an error occurred while processing your request. Please try again.';
      this.state.addAssistantMessage(
        errorMessage,
        0,
        false,
        []
      );
      this.state.setLoading(false);
    }
  }

  /**
   * Handle keyboard events
   */
  onKeyDown(event: KeyboardEvent): void {
    if (event.key === 'Enter') {
      event.preventDefault();
      if (this.canSend()) {
        this.sendMessage(event);
      }
    }
  }

  /**
   * Set input text (for quick actions/prompts)
   */
  setInput(text: string): void {
    this.inputText.set(text);
    setTimeout(() => {
      this.textInput?.nativeElement?.focus();
    }, 0);
  }

  /**
   * Scroll to bottom of messages
   */
  private scrollToBottom(): void {
    this.scrollAnchor?.nativeElement?.scrollIntoView({ behavior: 'smooth' });
  }
}
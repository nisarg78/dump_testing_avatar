import { Component, Input, Output, EventEmitter } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ChatMessage } from '@app/core/models';

/**
 * Message Component
 * Renders a single chat message with sources and follow-up suggestions
 * Following MECH Avatar spec
 */
@Component({
  selector: 'app-message',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div 
      class="flex gap-3 message-enter"
      [class.flex-row-reverse]="message.role === 'user'"
    >
      <!-- Avatar -->
      @if (message.role === 'assistant') {
        <div class="w-10 h-10 rounded-full bmo-avatar-bg flex items-center justify-center flex-shrink-0">
          <svg class="w-5 h-5 text-white" fill="currentColor" viewBox="0 0 24 24">
            <circle cx="9" cy="9" r="1.5"/>
            <circle cx="15" cy="9" r="1.5"/>
            <path d="M12 16c-1.48 0-2.75-.81-3.45-2h6.9c-.7 1.19-1.97 2-3.45 2z"/>
          </svg>
        </div>
      }

      <!-- Message Content -->
      <div 
        class="flex-1 max-w-[95%]"
        [class.text-right]="message.role === 'user'"
      >
        <!-- Content Bubble -->
        <div 
          class="rounded-2xl px-4 py-3 inline-block"
          [class]="message.role === 'user' 
            ? 'user-bubble text-white rounded-tr-sm' 
            : 'assistant-bubble text-gray-800 rounded-tl-sm'"
        >
          @if (message.role === 'assistant') {
            <div class="prose prose-sm max-w-none" [innerHTML]="formatContent(message.content)"></div>
            
            <!-- Sources (collapsed) -->
            @if (message.sources && message.sources.length > 0) {
              <div class="mt-3 pt-3 border-t border-gray-300">
                <p class="text-xs font-medium text-gray-500 mb-2">Sources:</p>
                <div class="space-y-2">
                  @for (source of message.sources.slice(0, 2); track source.chunk_id) {
                    <div class="bg-white/60 rounded-lg p-2 text-xs">
                      <span class="font-medium text-gray-700">{{ source.document_name ?? source.file_name }}</span>
                    </div>
                  }
                </div>
              </div>
            }
            
            <!-- Follow-up suggestions -->
            @if (message.followUpSuggestions && message.followUpSuggestions.length > 0) {
              <div class="mt-3 pt-3 border-t border-gray-300">
                <div class="flex flex-wrap gap-2">
                  @for (suggestion of message.followUpSuggestions; track suggestion) {
                    <button
                      (click)="followUp.emit(suggestion)"
                      class="text-xs px-3 py-1.5 bg-white text-bmo-blue rounded-full hover:bg-gray-50 hover:shadow-sm transition-colors border border-bmo-blue"
                    >
                      {{ suggestion }}
                    </button>
                  }
                </div>
              </div>
            }
          } @else {
            <p class="whitespace-pre-wrap text-left">{{ message.content }}</p>
          }
        </div>

        <!-- Timestamp and read receipt -->
        <div 
          class="flex items-center gap-1 mt-1 text-xs text-gray-400"
          [class.justify-end]="message.role === 'user'"
        >
          <span>{{ formatTime(message.timestamp) }}</span>
          @if (message.role === 'user') {
            <!-- Double checkmark for read receipt -->
            <svg class="w-4 h-4 bmo-blue-text" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7M5 13l4 4L19 7"/>
            </svg>
          }
        </div>
      </div>
    </div>
  `,
  styles: [`
    /* BMO Brand Colors (Official Palette) */
    .bmo-avatar-bg {
      background-color: #0079C1;
    }
    
    .bmo-blue-text {
      color: #0079C1;
    }
    
    .text-bmo-blue {
      color: #0079C1;
    }
    
    .border-bmo-blue {
      border-color: #0079C1;
    }
    
    .hover\:bg-bmo-blue:hover {
      background-color: #0079C1;
    }
    
    .user-bubble {
      background-color: #0079C1;
    }
    
    .assistant-bubble {
      background-color: #f0f7fc;
      border: 1px solid #e0ecf5;
    }
    
    .message-enter {
      animation: fadeSlideIn 0.3s ease-out;
    }
    
    @keyframes fadeSlideIn {
      from {
        opacity: 0;
        transform: translateY(10px);
      }
      to {
        opacity: 1;
        transform: translateY(0);
      }
    }
    
    .line-clamp-2 {
      display: -webkit-box;
      -webkit-line-clamp: 2;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }
  `]
})
export class MessageComponent {
  @Input({ required: true }) message!: ChatMessage;
  @Output() followUp = new EventEmitter<string>();

  formatTime(timestamp?: string): string {
    if (!timestamp) return '';
    const date = new Date(timestamp);
    return date.toLocaleTimeString('en-US', { 
      hour: '2-digit', 
      minute: '2-digit' 
    });
  }

  formatContent(content: string): string {
    // Basic markdown-like formatting
    return content
      .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
      .replace(/\*(.*?)\*/g, '<em>$1</em>')
      .replace(/`([^`]+)`/g, '<code class="bg-gray-100 px-1 rounded">$1</code>')
      .replace(/\n/g, '<br>');
  }
}
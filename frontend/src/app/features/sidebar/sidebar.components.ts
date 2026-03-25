import { Component, inject, signal, ViewChild } from '@angular/core';
import { CommonModule } from '@angular/common';
import { StateService } from '@app/core/services/state.service';
import { AuthService } from '@app/core/services/auth.service';
import { PERSONAS, Persona, RecentConversation } from '@app/core/models';
import { PersonaSelectorComponent } from '../persona-selector/persona-selector.component';

/**
 * Sidebar Component
 * Navigation, recent chats, persona selection, and user info
 * Following MECH Avatar spec
 */
@Component({
  selector: 'app-sidebar',
  standalone: true,
  imports: [CommonModule, PersonaSelectorComponent],
  template: `
    <aside 
      class="sidebar-container bg-white border-r border-gray-200 transition-all duration-300 flex flex-col h-full"
      [class.w-80]="isOpen()"
      [class.w-16]="!isOpen()"
    >
      <!-- Header -->
      <div class="p-4 border-b border-gray-100 flex items-center justify-between">
        @if (isOpen()) {
          <span class="text-lg font-bold text-bmo-blue tracking-wide">MECH Avatar</span>
        }
        <button 
          (click)="toggleSidebar()"
          class="p-2 hover:bg-gray-100 rounded-lg transition-colors"
        >
          <svg 
            class="w-5 h-5 text-gray-600 transition-transform"
            [class.rotate-180]="!isOpen()"
            fill="none" stroke="currentColor" viewBox="0 0 24 24"
          >
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7"/>
          </svg>
        </button>
      </div>

      <!-- New Chat Button -->
      <div class="p-3">
        <button
          (click)="startNewChat()"
          class="new-chat-btn w-full flex items-center justify-center gap-2 px-4 py-3 text-white rounded-xl transition-all shadow-md hover:shadow-lg"
        >
          <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4"/>
          </svg>
          @if (isOpen()) {
            <span class="font-semibold">New Chat</span>
          }
        </button>
      </div>

      @if (isOpen()) {
        <!-- Persona Display (Read-only) -->
        <div class="px-3 pb-3">
          <div
            class="w-full persona-card rounded-xl p-3 border border-gray-200 bg-white shadow-sm"
          >
            @if (state.selectedPersona(); as persona) {
              <div class="flex items-center gap-3">
                <div 
                  class="w-10 h-10 rounded-lg flex items-center justify-center flex-shrink-0"
                  [class]="getPersonaIconBg(persona)"
                >
                  <ng-container [ngSwitch]="persona.icon">
                    <svg *ngSwitchCase="'user-plus'" class="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18 9v3m0 0v3m0-3h3m-3 0h-3m-2-5a4 4 0 11-8 0 4 4 0 018 0zM3 20a6 6 0 0112 0v1H3v-1z"/>
                    </svg>
                    <svg *ngSwitchCase="'palette'" class="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 21a4 4 0 01-4-4V5a2 2 0 012-2h4a2 2 0 012 2v12a4 4 0 01-4 4zm0 0h12a2 2 0 002-2v-4a2 2 0 00-2-2h-2.343M11 7.343l1.657-1.657a2 2 0 012.828 0l2.829 2.829a2 2 0 010 2.828l-8.486 8.485M7 17h.01"/>
                    </svg>
                    <svg *ngSwitchCase="'clipboard-check'" class="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4"/>
                    </svg>
                    <svg *ngSwitchCase="'code'" class="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4"/>
                    </svg>
                    <svg *ngSwitchCase="'server'" class="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01"/>
                    </svg>
                    <svg *ngSwitchCase="'briefcase'" class="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 13.255A23.931 23.931 0 0112 15c-3.183 0-6.22-.62-9-1.745M16 6V4a2 2 0 00-2-2h-4a2 2 0 00-2 2v2m4 6h.01M5 20h14a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"/>
                    </svg>
                    <svg *ngSwitchDefault class="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"/>
                    </svg>
                  </ng-container>
                </div>
                <div class="flex-1 text-left min-w-0 overflow-hidden">
                  <p class="text-sm font-semibold text-gray-800 truncate">{{ persona.name }}</p>
                  <p class="text-xs text-gray-500 truncate">{{ persona.description }}</p>
                </div>
              </div>
            }
          </div>
        </div>

        <!-- Divider -->
        <div class="px-4 py-2">
          <div class="h-px bg-gray-100"></div>
        </div>

        <!-- Recent Chats -->
        <div class="flex-1 overflow-y-auto px-3">
          <h3 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2 px-1">Recent Conversations</h3>
          
          @if (state.recentConversations().length === 0) {
            <div class="text-center py-8">
              <div class="w-12 h-12 rounded-full bg-gray-100 flex items-center justify-center mx-auto mb-3">
                <svg class="w-6 h-6 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z"/>
                </svg>
              </div>
              <p class="text-sm text-gray-400">No recent conversations</p>
              <p class="text-xs text-gray-300 mt-1">Start a new chat to begin</p>
            </div>
          } @else {
            <div class="space-y-1">
              @for (conv of state.recentConversations(); track conv.id) {
                <div
                  class="conversation-item p-3 rounded-lg cursor-pointer transition-all"
                  [class]="conv.id === state.sessionId() 
                    ? 'bg-blue-50 border-l-2 border-bmo-blue' 
                    : 'hover:bg-gray-50'"
                  (click)="loadConversation(conv)"
                >
                  <p class="text-sm font-medium text-gray-800 truncate">{{ conv.title }}</p>
                  <p class="text-xs text-gray-400 mt-1">
                    {{ formatDate(conv.timestamp) }} • {{ conv.messageCount }} messages
                  </p>
                </div>
              }
            </div>
          }
        </div>
      }

    </aside>

    <!-- Persona Selector Modal -->
    <app-persona-selector 
      #personaSelector
      (personaSelected)="onPersonaSelected($event)"
    />
  `,
  styles: [`
    .sidebar-container {
      box-shadow: 2px 0 8px rgba(0, 0, 0, 0.03);
    }
    
    .gradient-text {
      color: #0079C1;
    }
    
    .new-chat-btn {
      background-color: #0079C1;
    }
    
    .new-chat-btn:hover {
      background-color: #005587;
    }
    
    .from-bmo-blue { --tw-gradient-from: #0079C1; }
    .to-bmo-blue-dark { --tw-gradient-to: #005587; }
    .bg-bmo-blue { background-color: #0079C1; }
    .border-bmo-blue { border-color: #0079C1; }
    .hover\\:border-bmo-blue:hover { border-color: #0079C1; }
    .text-bmo-blue { color: #0079C1; }
    .hover\\:text-bmo-blue:hover { color: #0079C1; }
    .hover\\:bg-bmo-blue:hover { background-color: #0079C1; }
    .hover\\:text-white:hover { color: white; }
    
    /* BMO Brand Persona icon backgrounds */
    .icon-bg-blue { background-color: #0079C1; }
    .icon-bg-purple { background-color: #4c6cb3; }
    .icon-bg-orange { background-color: #ffc72c; }
    .icon-bg-green { background-color: #00843d; }
    .icon-bg-red { background-color: #ED1C24; }
    .icon-bg-yellow { background-color: #00a9e0; }
    
    .persona-card:hover {
      background-color: rgba(0, 120, 193, 0.04);
    }
    
    .conversation-item {
      border-left: 2px solid transparent;
    }
  `]
})
export class SidebarComponent {
  readonly state = inject(StateService);
  readonly auth = inject(AuthService);

  @ViewChild('personaSelector') personaSelector!: PersonaSelectorComponent;

  readonly isOpen = signal(true);

  toggleSidebar(): void {
    this.isOpen.update(v => !v);
  }

  startNewChat(): void {
    this.state.newSession();
  }

  openPersonaSelector(): void {
    this.personaSelector.openModal();
  }

  onPersonaSelected(persona: Persona): void {
    // Persona is already set by the component
    console.log('Persona selected:', persona.name);
  }

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

  loadConversation(conv: RecentConversation): void {
    this.state.loadConversation(conv.id);
    // In a real app, load messages via API
  }

  formatDate(timestamp: string): string {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    
    if (diffMins < 1) return 'Just now';
    if (diffMins < 60) return `${diffMins}m ago`;
    
    const diffHours = Math.floor(diffMins / 60);
    if (diffHours < 24) return `${diffHours}h ago`;
    
    const diffDays = Math.floor(diffHours / 24);
    if (diffDays < 7) return `${diffDays}d ago`;
    
    return date.toLocaleDateString();
  }

  getUserInitials(): string {
    const user = this.auth.user();
    const name = user?.name || user?.username || 'U';
    return name.split(' ').map((n: string) => n[0]).join('').toUpperCase().slice(0, 2);
  }

  login(): void {
    this.auth.login();
  }

  logout(): void {
    this.auth.logout();
  }
}

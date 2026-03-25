import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { StateService } from '@app/core/services/state.service';
import { SidebarComponent } from '@app/features/sidebar/sidebar.component';
import { ChatInterfaceComponent } from '@app/features/chat/chat-interface/chat-interface.component';

/**
 * Shell Component
 * Main authenticated layout with sidebar and chat interface
 */
@Component({
  selector: 'app-shell',
  standalone: true,
  imports: [
    CommonModule,
    SidebarComponent,
    ChatInterfaceComponent
  ],
  template: `
    <div class="flex flex-1 overflow-hidden h-full">
      <!-- Sidebar -->
      <app-sidebar class="hidden md:flex flex-shrink-0" />
      
      <!-- Main Content -->
      <main class="flex-1 flex flex-col overflow-hidden min-w-0">
        <app-chat-interface />
      </main>
    </div>
  `,
  styles: [`
    :host {
      display: flex;
      flex: 1;
      overflow: hidden;
    }
  `]
})
export class ShellComponent {
  readonly state = inject(StateService);
}
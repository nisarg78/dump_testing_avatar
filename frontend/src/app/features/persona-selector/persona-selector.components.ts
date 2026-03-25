import { Component, inject, signal, Output, EventEmitter } from '@angular/core';
import { CommonModule } from '@angular/common';
import { StateService } from '@app/core/services/state.service';
import { PERSONAS, Persona } from '@app/core/models';

/**
 * Persona Selector Component
 * Displays 6 persona cards for user selection
 * Based on MECH Avatar handoff specification
 */
@Component({
  selector: 'app-persona-selector',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div class="persona-selector">
      <!-- Modal Overlay -->
      @if (isModalOpen()) {
        <div 
          class="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4"
          (click)="closeModal()"
        >
          <div 
            class="bg-white rounded-2xl shadow-2xl max-w-5xl w-full overflow-hidden max-h-[95vh] flex flex-col"
            (click)="$event.stopPropagation()"
          >
            <!-- Header -->
            <div class="bg-bmo-blue px-4 sm:px-6 py-3 sm:py-4 flex-shrink-0">
              <h2 class="text-xl font-bold text-white">Select Your Persona</h2>
              <p class="text-blue-100 text-sm mt-1">
                Choose a persona that best matches your role for tailored responses
              </p>
            </div>

            <!-- Persona Cards Grid -->
            <div class="p-3 sm:p-5 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3 sm:gap-4 overflow-y-auto flex-1">
              @for (persona of personas; track persona.id) {
                <div 
                  class="persona-card group cursor-pointer rounded-xl border-2 p-4 transition-colors duration-200 flex flex-col relative"
                  [class]="getCardClasses(persona)"
                  (click)="selectPersona(persona)"
                >
                  <!-- Check mark for selected (absolute positioned) -->
                  @if (isSelected(persona)) {
                    <div class="absolute top-3 right-3 w-6 h-6 rounded-full bg-green-500 flex items-center justify-center">
                      <svg class="w-4 h-4 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/>
                      </svg>
                    </div>
                  }
                  
                  <!-- Icon & Header -->
                  <div class="flex items-start gap-3 mb-2 flex-shrink-0">
                    <div 
                      class="w-11 h-11 rounded-xl flex items-center justify-center flex-shrink-0 transition-colors"
                      [class]="getIconBgClass(persona)"
                    >
                      <ng-container [ngSwitch]="persona.icon">
                        <!-- User Plus - New Joiners -->
                        <svg *ngSwitchCase="'user-plus'" class="w-6 h-6 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18 9v3m0 0v3m0-3h3m-3 0h-3m-2-5a4 4 0 11-8 0 4 4 0 018 0zM3 20a6 6 0 0112 0v1H3v-1z"/>
                        </svg>
                        <!-- Palette - Designers -->
                        <svg *ngSwitchCase="'palette'" class="w-6 h-6 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 21a4 4 0 01-4-4V5a2 2 0 012-2h4a2 2 0 012 2v12a4 4 0 01-4 4zm0 0h12a2 2 0 002-2v-4a2 2 0 00-2-2h-2.343M11 7.343l1.657-1.657a2 2 0 012.828 0l2.829 2.829a2 2 0 010 2.828l-8.486 8.485M7 17h.01"/>
                        </svg>
                        <!-- Clipboard Check - QA Testers -->
                        <svg *ngSwitchCase="'clipboard-check'" class="w-6 h-6 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4"/>
                        </svg>
                        <!-- Code - Developers -->
                        <svg *ngSwitchCase="'code'" class="w-6 h-6 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4"/>
                        </svg>
                        <!-- Server - Tech Support -->
                        <svg *ngSwitchCase="'server'" class="w-6 h-6 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01"/>
                        </svg>
                        <!-- Briefcase - Business Support -->
                        <svg *ngSwitchCase="'briefcase'" class="w-6 h-6 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 13.255A23.931 23.931 0 0112 15c-3.183 0-6.22-.62-9-1.745M16 6V4a2 2 0 00-2-2h-4a2 2 0 00-2 2v2m4 6h.01M5 20h14a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"/>
                        </svg>
                        <!-- Default -->
                        <svg *ngSwitchDefault class="w-6 h-6 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"/>
                        </svg>
                      </ng-container>
                    </div>
                    <div class="flex-1 min-w-0 pr-6">
                      <h3 class="font-semibold text-gray-900 group-hover:text-bmo-blue transition-colors">
                        {{ persona.name }}
                      </h3>
                      <p class="text-xs text-gray-500 mt-0.5">{{ persona.description }}</p>
                    </div>
                  </div>

                  <!-- Benefits Preview -->
                  <div class="space-y-1 mt-auto">
                    @for (benefit of persona.benefits.slice(0, 2); track $index) {
                      <div class="flex items-center gap-2 text-xs text-gray-600">
                        <svg class="w-3.5 h-3.5 text-green-500 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
                          <path fill-rule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clip-rule="evenodd"/>
                        </svg>
                        <span class="line-clamp-1">{{ benefit }}</span>
                      </div>
                    }
                  </div>
                </div>
              }
            </div>

            <!-- Footer -->
            <div class="px-5 py-3 bg-gray-50 border-t flex justify-end gap-3">
              <button 
                (click)="closeModal()"
                class="px-4 py-2 text-gray-600 hover:text-gray-800 font-medium transition-colors"
              >
                Cancel
              </button>
              <button 
                (click)="confirmSelection()"
                class="px-6 py-2 bg-bmo-blue text-white rounded-lg hover:bg-bmo-blue-dark font-medium transition-colors"
                [disabled]="!tempSelectedPersona()"
              >
                Continue
              </button>
            </div>
          </div>
        </div>
      }
    </div>
  `,
  styles: [`
    .bg-bmo-blue { background-color: #0079C1; }
    .bg-bmo-blue-dark { background-color: #005587; }
    .from-bmo-blue { --tw-gradient-from: #0079C1; }
    .to-bmo-blue-dark { --tw-gradient-to: #005587; }
    .text-bmo-blue { color: #0079C1; }
    .hover\\:bg-bmo-blue-dark:hover { background-color: #005587; }
    .hover\\:text-bmo-blue:hover { color: #0079C1; }
    
    .persona-card {
      background: white;
    }
    
    .persona-card:hover {
      box-shadow: 0 4px 15px rgba(0, 120, 193, 0.12);
    }
    
    .persona-card.selected {
      border-color: #0079C1;
      background-color: #eff6ff;
    }

    .line-clamp-1 {
      display: -webkit-box;
      -webkit-line-clamp: 1;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }

    .line-clamp-2 {
      display: -webkit-box;
      -webkit-line-clamp: 2;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }
    
    /* Icon background colors */
    /* BMO Brand Icon Backgrounds */
    .icon-bg-blue { background-color: #0079C1; }
    .icon-bg-purple { background-color: #4c6cb3; }
    .icon-bg-orange { background-color: #ffc72c; }
    .icon-bg-green { background-color: #00843d; }
    .icon-bg-red { background-color: #ED1C24; }
    .icon-bg-yellow { background-color: #00a9e0; }
  `]
})
export class PersonaSelectorComponent {
  readonly state = inject(StateService);
  readonly personas = PERSONAS;
  
  @Output() personaSelected = new EventEmitter<Persona>();
  @Output() modalClosed = new EventEmitter<void>();
  
  isModalOpen = signal(false);
  tempSelectedPersona = signal<Persona | null>(null);

  openModal(): void {
    this.tempSelectedPersona.set(this.state.selectedPersona());
    this.isModalOpen.set(true);
  }

  closeModal(): void {
    this.isModalOpen.set(false);
    this.modalClosed.emit();
  }

  selectPersona(persona: Persona): void {
    this.tempSelectedPersona.set(persona);
  }

  confirmSelection(): void {
    const persona = this.tempSelectedPersona();
    if (persona) {
      this.state.setPersona(persona);
      this.personaSelected.emit(persona);
    }
    this.closeModal();
  }

  isSelected(persona: Persona): boolean {
    return this.tempSelectedPersona()?.id === persona.id;
  }

  getCardClasses(persona: Persona): string {
    const base = 'border-gray-200 hover:border-bmo-blue';
    return this.isSelected(persona) ? 'selected border-bmo-blue' : base;
  }

  getIconBgClass(persona: Persona): string {
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
}

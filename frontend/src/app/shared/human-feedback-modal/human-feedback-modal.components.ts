import { Component, Input, Output, EventEmitter } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { HumanFeedbackRequest, DownloadOption } from '@app/core/models';

/**
 * Human Feedback Modal Component
 * Shows when agent requires human input
 */
@Component({
  selector: 'app-human-feedback-modal',
  standalone: true,
  imports: [CommonModule, FormsModule],
  template: `
    @if (feedback) {
      <div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
        <div class="bg-white rounded-xl max-w-2xl w-full max-h-[90vh] overflow-hidden shadow-2xl">
          <!-- Header -->
          <div class="bg-primary-500 px-6 py-4">
            <div class="flex items-center gap-3">
              <div class="w-10 h-10 rounded-full bg-white/20 flex items-center justify-center">
                <svg class="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" 
                        d="M8.228 9c.549-1.165 2.03-2 3.772-2 2.21 0 4 1.343 4 3 0 1.4-1.278 2.575-3.006 2.907-.542.104-.994.54-.994 1.093m0 3h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>
                </svg>
              </div>
              <div>
                <h2 class="text-lg font-semibold text-white">{{ feedback.phaseDisplay }}</h2>
                <p class="text-sm text-white/80">Your input is needed</p>
              </div>
            </div>
          </div>

          <!-- Content -->
          <div class="p-6 overflow-y-auto max-h-[60vh]">
            <!-- Summary -->
            <div class="mb-6">
              <h3 class="text-sm font-medium text-gray-500 uppercase mb-2">Summary</h3>
              <p class="text-gray-700">{{ feedback.summary }}</p>
            </div>

            <!-- Downloads -->
            @if (feedback.availableDownloads && feedback.availableDownloads.length > 0) {
              <div class="mb-6">
                <h3 class="text-sm font-medium text-gray-500 uppercase mb-2">Available Downloads</h3>
                <div class="flex flex-wrap gap-2">
                  @for (download of feedback.availableDownloads; track download.type) {
                    <button
                      (click)="onDownload(download)"
                      class="flex items-center gap-2 px-3 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg text-sm transition-colors"
                    >
                      <svg class="w-4 h-4 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" 
                              d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"/>
                      </svg>
                      {{ download.label }}
                    </button>
                  }
                </div>
              </div>
            }

            <!-- Options -->
            <div class="mb-6">
              <h3 class="text-sm font-medium text-gray-500 uppercase mb-3">Choose an option</h3>
              <div class="space-y-2">
                @for (option of feedback.options; track option.value) {
                  <button
                    (click)="selectedOption = option.value"
                    class="w-full flex items-start gap-3 p-4 rounded-lg border-2 transition-all text-left"
                    [class]="selectedOption === option.value 
                      ? 'border-primary-500 bg-primary-50' 
                      : 'border-gray-200 hover:border-primary-300'"
                  >
                    <div 
                      class="w-5 h-5 rounded-full border-2 flex items-center justify-center flex-shrink-0 mt-0.5"
                      [class]="selectedOption === option.value ? 'border-primary-500' : 'border-gray-300'"
                    >
                      @if (selectedOption === option.value) {
                        <div class="w-3 h-3 rounded-full bg-primary-500"></div>
                      }
                    </div>
                    <div>
                      <p class="font-medium text-gray-800">{{ option.label }}</p>
                      <p class="text-sm text-gray-500">{{ option.description }}</p>
                    </div>
                  </button>
                }
              </div>
            </div>

            <!-- User Input -->
            <div>
              <h3 class="text-sm font-medium text-gray-500 uppercase mb-2">Additional Comments (Optional)</h3>
              <textarea
                [(ngModel)]="userInput"
                rows="3"
                placeholder="Add any specific instructions or feedback..."
                class="input-base resize-none"
              ></textarea>
            </div>
          </div>

          <!-- Footer -->
          <div class="border-t border-gray-200 px-6 py-4 flex justify-end gap-3">
            <button
              (click)="onCancel()"
              class="btn-outline"
            >
              Cancel
            </button>
            <button
              (click)="onSubmit()"
              [disabled]="!selectedOption"
              class="btn-primary"
              [class.opacity-50]="!selectedOption"
            >
              Submit Feedback
            </button>
          </div>
        </div>
      </div>
    }
  `
})
export class HumanFeedbackModalComponent {
  @Input() feedback: HumanFeedbackRequest | null = null;
  @Output() submit = new EventEmitter<{ action: string; userInput: string }>();
  @Output() cancel = new EventEmitter<void>();
  @Output() download = new EventEmitter<DownloadOption>();

  selectedOption = '';
  userInput = '';

  onSubmit(): void {
    if (this.selectedOption) {
      this.submit.emit({
        action: this.selectedOption,
        userInput: this.userInput
      });
      this.reset();
    }
  }

  onCancel(): void {
    this.cancel.emit();
    this.reset();
  }

  onDownload(download: DownloadOption): void {
    this.download.emit(download);
  }

  private reset(): void {
    this.selectedOption = '';
    this.userInput = '';
  }
}

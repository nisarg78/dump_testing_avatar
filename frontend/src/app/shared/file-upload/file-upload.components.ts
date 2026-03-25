import { Component, Input, Output, EventEmitter, signal } from '@angular/core';
import { CommonModule } from '@angular/common';

/**
 * File Upload Component
 * Reusable drag-and-drop file upload
 */
@Component({
  selector: 'app-file-upload',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div
      (dragover)="onDragOver($event)"
      (drop)="onDrop($event)"
      (dragleave)="isDragActive.set(false)"
      (click)="fileInput.click()"
      class="border-2 border-dashed rounded-xl p-6 text-center cursor-pointer transition-all"
      [class]="isDragActive() ? 'border-primary-400 bg-primary-50' : 'border-gray-300 hover:border-primary-300 hover:bg-gray-50'"
    >
      <input
        #fileInput
        type="file"
        [multiple]="multiple"
        (change)="onFilesSelected($event)"
        class="hidden"
        [accept]="acceptedTypes"
      />
      
      <svg class="w-10 h-10 text-gray-400 mx-auto mb-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" 
              d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12"/>
      </svg>
      
      <p class="text-gray-600 mb-1">
        @if (isDragActive()) {
          Drop files here...
        } @else {
          {{ label || 'Drag and drop files here, or click to select' }}
        }
      </p>
      
      @if (hint) {
        <p class="text-sm text-gray-400">{{ hint }}</p>
      }
    </div>
  `
})
export class FileUploadComponent {
  @Input() multiple = true;
  @Input() acceptedTypes = '.pdf,.docx,.txt,.md,.json,.csv,.xlsx,.yaml,.yml';
  @Input() label?: string;
  @Input() hint?: string;
  @Output() filesSelected = new EventEmitter<File[]>();

  isDragActive = signal(false);

  onDragOver(event: DragEvent): void {
    event.preventDefault();
    this.isDragActive.set(true);
  }

  onDrop(event: DragEvent): void {
    event.preventDefault();
    this.isDragActive.set(false);
    
    const files = event.dataTransfer?.files;
    if (files && files.length > 0) {
      this.filesSelected.emit(Array.from(files));
    }
  }

  onFilesSelected(event: Event): void {
    const input = event.target as HTMLInputElement;
    if (input.files && input.files.length > 0) {
      this.filesSelected.emit(Array.from(input.files));
      input.value = '';
    }
  }
}

This paste expires in <1 hour. Public IP access. Share whatever you see with others in seconds with Context.Terms of ServiceReport this
import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Router } from '@angular/router';

/**
 * 404 Not Found Component
 * Professional BMO-branded error page for missing routes
 */
@Component({
  selector: 'app-not-found',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div class="min-h-screen bg-gradient-to-br from-gray-50 to-gray-100 flex items-center justify-center p-4">
      <div class="max-w-md w-full text-center">
        <!-- Error Icon -->
        <div class="mb-8">
          <div class="w-24 h-24 mx-auto bg-bmo-blue/10 rounded-full flex items-center justify-center mb-4">
            <svg class="w-12 h-12 text-bmo-blue" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" 
                    d="M9.172 16.172a4 4 0 015.656 0M9 10h.01M15 10h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>
            </svg>
          </div>
          
          <!-- Error Code -->
          <h1 class="text-8xl font-bold text-gray-200 select-none">404</h1>
        </div>

        <!-- Error Message -->
        <h2 class="text-2xl font-semibold text-gray-800 mb-3">
          Page Not Found
        </h2>
        <p class="text-gray-500 mb-8 leading-relaxed">
          The page you're looking for doesn't exist or has been moved. 
          Let's get you back on track.
        </p>

        <!-- Actions -->
        <div class="flex flex-col sm:flex-row gap-3 justify-center">
          <button
            (click)="goHome()"
            class="px-6 py-3 bg-bmo-blue text-white font-medium rounded-lg hover:bg-bmo-blue-dark transition-colors shadow-sm"
          >
            <span class="flex items-center justify-center gap-2">
              <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" 
                      d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6"/>
              </svg>
              Go to Home
            </span>
          </button>
          
          <button
            (click)="goBack()"
            class="px-6 py-3 bg-white text-gray-700 font-medium rounded-lg border border-gray-200 hover:bg-gray-50 transition-colors"
          >
            <span class="flex items-center justify-center gap-2">
              <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" 
                      d="M10 19l-7-7m0 0l7-7m-7 7h18"/>
              </svg>
              Go Back
            </span>
          </button>
        </div>

        <!-- Help Link -->
        <p class="mt-8 text-sm text-gray-400">
          Need help? 
          <a href="mailto:support@bmo.com" class="text-bmo-blue hover:underline">
            Contact Support
          </a>
        </p>
      </div>
    </div>
  `,
  styles: [`
    .bg-bmo-blue { background-color: #0079C1; }
    .bg-bmo-blue-dark { background-color: #005587; }
    .text-bmo-blue { color: #0079C1; }
    .hover\\:bg-bmo-blue-dark:hover { background-color: #005587; }
    .bg-bmo-blue\\/10 { background-color: rgba(0, 121, 193, 0.1); }
  `]
})
export class NotFoundComponent {
  private readonly router = inject(Router);

  goHome(): void {
    this.router.navigate(['/']);
  }

  goBack(): void {
    window.history.back();
  }
}
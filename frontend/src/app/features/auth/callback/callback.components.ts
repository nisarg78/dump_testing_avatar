import { Component, inject, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Router } from '@angular/router';
// AuthService, ActivatedRoute not used — OAuth2/Cognito callback is disabled

/**
 * OAuth Callback Component
 * OAuth2 / Cognito callback is currently disabled — using DynamoDB auth via Lambda 08/10.
 * This component just redirects to /login.
 */
@Component({
  selector: 'app-callback',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div class="min-h-screen bg-gray-50 flex items-center justify-center">
      <div class="text-center">
        <div class="loading-spinner w-12 h-12 mx-auto mb-4"></div>
        <h1 class="text-xl font-semibold text-gray-800 mb-2">Redirecting...</h1>
        <p class="text-gray-500">Please wait</p>
      </div>
    </div>
  `
})
export class CallbackComponent implements OnInit {
  private readonly router = inject(Router);

  // Unused — kept to avoid breaking imports elsewhere
  error = '';

  ngOnInit(): void {
    // OAuth2/Cognito callback is no longer used.
    // Redirect to login page where DynamoDB auth happens.
    this.router.navigate(['/login']);
  }

  retry(): void {
    this.router.navigate(['/login']);
  }
}
import { Component, inject, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterOutlet, Router } from '@angular/router';
import { AuthService } from '@app/core/services/auth.service';

/**
 * Root Application Component
 * Contains navigation bar and router outlet
 */
@Component({
  selector: 'app-root',
  standalone: true,
  imports: [
    CommonModule,
    RouterOutlet
  ],
  template: `
    <div class="flex flex-col h-screen bg-gray-50 overflow-hidden">
      <!-- Top Navigation Bar -->
      <nav class="flex items-center justify-between px-6 py-3 bg-bmo-blue text-white flex-shrink-0">
        <!-- Left: BMO Logo -->
        <div class="flex items-center gap-3 cursor-pointer flex-1" (click)="goHome()">
          <img 
            src="assets/bmo-original.png" 
            alt="BMO Logo" 
            class="h-8 w-auto object-contain"
          />
        </div>
        
        <!-- Center: MECH Avatar -->
        <div class="flex items-center justify-center flex-1 gap-2">
          <img 
            src="assets/mech-avatar-logo.png" 
            alt="MECH Avatar Logo" 
            class="h-8 w-8 object-contain"
          />
          <span class="text-sm font-semibold tracking-wide">MECH Avatar</span>
        </div>
        
        <!-- Right: User & Auth -->
        <div class="flex items-center gap-3 justify-end flex-1">
          @if (auth.isAuthenticated()) {
            <!-- User Info -->
            <div class="flex items-center gap-2">
              <div class="w-8 h-8 rounded-full bg-white/20 flex items-center justify-center">
                <span class="text-white font-semibold text-xs">
                  {{ getUserInitials() }}
                </span>
              </div>
              <div class="hidden sm:block text-right">
                <p class="text-sm font-medium leading-tight">
                  {{ auth.user()?.name || auth.user()?.username || 'User' }}
                </p>
                <p class="text-xs text-white/70 leading-tight">
                  {{ auth.user()?.email }}
                </p>
              </div>
            </div>
            <!-- Sign Out Button -->
            <button 
              (click)="logout()"
              class="flex items-center gap-2 px-4 py-1.5 text-sm font-medium border border-white/70 rounded-lg hover:bg-white hover:text-bmo-blue transition-colors"
            >
              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1"/>
              </svg>
              <span class="hidden sm:inline">Sign Out</span>
            </button>
          } @else {
            <!-- Sign In Button -->
            <button 
              (click)="goToLogin()"
              class="flex items-center gap-2 px-4 py-1.5 text-sm font-medium bg-white text-bmo-blue rounded-lg hover:bg-white/90 transition-colors"
            >
              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 16l-4-4m0 0l4-4m-4 4h14m-5 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h7a3 3 0 013 3v1"/>
              </svg>
              <span>Sign In</span>
            </button>
          }
        </div>
      </nav>

      <!-- Router Outlet (Main Content) -->
      <div class="flex flex-1 overflow-hidden">
        <router-outlet />
      </div>

      <!-- Loading Overlay -->
      @if (auth.isLoading()) {
        <div class="fixed inset-0 bg-white/80 flex items-center justify-center z-50">
          <div class="text-center">
            <div class="loading-spinner w-12 h-12 mb-4 mx-auto"></div>
            <p class="text-gray-600">Loading...</p>
          </div>
        </div>
      }
    </div>
  `,
  styles: [`
    :host {
      display: block;
      height: 100vh;
    }
    
    .loading-spinner {
      border: 3px solid #e5e7eb;
      border-top-color: #0078C1;
      border-radius: 50%;
      animation: spin 0.8s linear infinite;
    }
    
    @keyframes spin {
      to { transform: rotate(360deg); }
    }
  `]
})
export class AppComponent implements OnInit {
  readonly auth = inject(AuthService);
  private readonly router = inject(Router);

  ngOnInit(): void {
    // Handle OAuth callback if present
    const urlParams = new URLSearchParams(window.location.search);
    const code = urlParams.get('code');
    
    if (code) {
      this.auth.handleCallback(code).subscribe({
        next: () => {
          // Clear URL params after successful auth
          window.history.replaceState({}, '', window.location.pathname);
        },
        error: (err) => {
          console.error('Auth callback failed:', err);
        }
      });
    }
  }

  getUserInitials(): string {
    const user = this.auth.user();
    const name = user?.name || user?.username || 'U';
    return name.split(' ').map((n: string) => n[0]).join('').toUpperCase().slice(0, 2);
  }

  goHome(): void {
    this.router.navigate(['/']);
  }

  goToLogin(): void {
    this.router.navigate(['/login']);
  }

  logout(): void {
    this.auth.logout();
  }
}

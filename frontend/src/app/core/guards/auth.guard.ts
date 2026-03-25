import { Injectable, inject } from '@angular/core';
import { CanActivate, Router, UrlTree } from '@angular/router';
import { Observable } from 'rxjs';
import { AuthService } from '../services/auth.service';

/**
 * Auth Guard
 * Protects routes that require authentication
 */
@Injectable({
  providedIn: 'root'
})
export class AuthGuard implements CanActivate {
  private readonly authService = inject(AuthService);
  private readonly router = inject(Router);

  canActivate(): Observable<boolean | UrlTree> | Promise<boolean | UrlTree> | boolean | UrlTree {
    // Check if user is authenticated
    if (this.authService.isAuthenticated()) {
      return true;
    }
    
    // Redirect to login page
    return this.router.createUrlTree(['/login']);
  }
}

/**
 * Admin Guard
 * Protects routes that require admin role
 */
@Injectable({
  providedIn: 'root'
})
export class AdminGuard implements CanActivate {
  private readonly authService = inject(AuthService);
  private readonly router = inject(Router);

  canActivate(): Observable<boolean | UrlTree> | Promise<boolean | UrlTree> | boolean | UrlTree {
    if (this.authService.isAuthenticated() && this.authService.isAdmin()) {
      return true;
    }

    // Redirect to home or unauthorized
    return this.router.createUrlTree(['/']);
  }
}

/**
 * Public Guard
 * Redirects authenticated users away from public pages (login)
 */
@Injectable({
  providedIn: 'root'
})
export class PublicGuard implements CanActivate {
  private readonly authService = inject(AuthService);
  private readonly router = inject(Router);

  canActivate(): Observable<boolean | UrlTree> | Promise<boolean | UrlTree> | boolean | UrlTree {
    if (!this.authService.isAuthenticated()) {
      return true;
    }

    // Redirect to home
    return this.router.createUrlTree(['/']);
  }
}

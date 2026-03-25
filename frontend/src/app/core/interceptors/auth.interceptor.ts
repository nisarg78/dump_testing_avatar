import { Injectable, inject } from '@angular/core';
import {
  HttpInterceptor,
  HttpRequest,
  HttpHandler,
  HttpEvent,
  HttpErrorResponse,
  HTTP_INTERCEPTORS
} from '@angular/common/http';
import { Observable, throwError, BehaviorSubject } from 'rxjs';
import { catchError, filter, take, switchMap, finalize } from 'rxjs/operators';
import { AuthService } from '@app/core/services/auth.service';
import { AuthTokens } from '@app/core/models';
import { Provider } from '@angular/core';

/**
 * Auth Interceptor
 * Adds JWT token to outgoing requests and handles token refresh
 */
@Injectable()
export class AuthInterceptor implements HttpInterceptor {
  private readonly authService = inject(AuthService);
  private isRefreshing = false;
  private refreshTokenSubject = new BehaviorSubject<string | null>(null);

  intercept(request: HttpRequest<unknown>, next: HttpHandler): Observable<HttpEvent<unknown>> {
    // Skip auth for certain endpoints
    if (this.shouldSkipAuth(request.url)) {
      return next.handle(request);
    }

    // Add token to request
    const token = this.authService.getIdToken();
    if (token) {
      request = this.addToken(request, token);
    }

    return next.handle(request).pipe(
      catchError((error: HttpErrorResponse) => {
        if (error.status === 401) {
          return this.handle401Error(request, next);
        }
        return throwError(() => error);
      })
    );
  }

  private addToken(request: HttpRequest<unknown>, token: string): HttpRequest<unknown> {
    return request.clone({
      setHeaders: {
        Authorization: `Bearer ${token}`
      }
    });
  }

  private shouldSkipAuth(url: string): boolean {
    const skipUrls = ['/oauth2/token', '/health', '/public'];
    return skipUrls.some(skip => url.includes(skip));
  }

  private handle401Error(request: HttpRequest<unknown>, next: HttpHandler): Observable<HttpEvent<unknown>> {
    if (!this.isRefreshing) {
      this.isRefreshing = true;
      this.refreshTokenSubject.next(null);

      return this.authService.refreshTokens().pipe(
        switchMap((tokens: AuthTokens) => {
          this.refreshTokenSubject.next(tokens.idToken);
          return next.handle(this.addToken(request, tokens.idToken));
        }),
        catchError((error: Error) => {
          // Refresh failed, redirect to login
          this.authService.logout();
          return throwError(() => error);
        }),
        finalize(() => {
          this.isRefreshing = false;
        })
      );
    }

    // Wait for token refresh to complete
    return this.refreshTokenSubject.pipe(
      filter((token: string | null) => token !== null),
      take(1),
      switchMap((token: string | null) => next.handle(this.addToken(request, token!)))
    );
  }
}

/**
 * Provider for Auth Interceptor
 */
export const authInterceptorProvider: Provider = {
  provide: HTTP_INTERCEPTORS,
  useClass: AuthInterceptor,
  multi: true
};

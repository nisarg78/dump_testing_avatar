import { Injectable, ErrorHandler, inject } from '@angular/core';
import { HttpErrorResponse } from '@angular/common/http';
import { Router } from '@angular/router';
import { environment } from '@environments/environment';

/**
 * Global Error Handler
 * Catches unhandled errors application-wide
 * Production: Logs to monitoring service
 * Development: Logs to console with full details
 */
@Injectable()
export class GlobalErrorHandler implements ErrorHandler {
  private readonly router = inject(Router);

  handleError(error: Error | HttpErrorResponse): void {
    let errorMessage: string;
    let errorType: string;

    if (error instanceof HttpErrorResponse) {
      // Server-side error
      errorType = 'HTTP Error';
      errorMessage = this.getHttpErrorMessage(error);
      this.handleHttpError(error);
    } else {
      // Client-side error
      errorType = 'Client Error';
      errorMessage = error.message || 'Unknown error occurred';
      this.handleClientError(error);
    }

    // Always log in development
    if (!environment.production) {
      console.group(`🚨 ${errorType}`);
      console.error('Message:', errorMessage);
      console.error('Error:', error);
      if (error instanceof Error && error.stack) {
        console.error('Stack:', error.stack);
      }
      console.groupEnd();
    }

    // In production, send to monitoring service
    if (environment.production) {
      this.logToMonitoringService(errorType, errorMessage, error);
    }
  }

  private getHttpErrorMessage(error: HttpErrorResponse): string {
    switch (error.status) {
      case 0:
        return 'Unable to connect to server. Please check your internet connection.';
      case 400:
        return error.error?.message || 'Bad request. Please check your input.';
      case 401:
        return 'Session expired. Please log in again.';
      case 403:
        return 'Access denied. You don\'t have permission.';
      case 404:
        return 'Resource not found.';
      case 429:
        return 'Too many requests. Please try again later.';
      case 500:
        return 'Server error. Please try again later.';
      case 502:
      case 503:
      case 504:
        return 'Service temporarily unavailable. Please try again.';
      default:
        return `Error ${error.status}: ${error.statusText}`;
    }
  }

  private handleHttpError(error: HttpErrorResponse): void {
    // Handle specific HTTP errors
    if (error.status === 401) {
      // Unauthorized - redirect to login
      this.router.navigate(['/login']);
    } else if (error.status === 503) {
      // Service unavailable - could show maintenance page
      console.warn('[ErrorHandler] Service unavailable - 503');
    }
  }

  private handleClientError(error: Error): void {
    // Handle specific client-side errors
    if (error.name === 'ChunkLoadError') {
      // Lazy loading failed - likely a deployment update
      console.warn('[ErrorHandler] Chunk load error - reloading page');
      window.location.reload();
    }
  }

  private logToMonitoringService(
    errorType: string,
    message: string,
    error: Error | HttpErrorResponse
  ): void {
    // TODO: Integrate with monitoring service (e.g., Sentry, DataDog, CloudWatch)
    // Example:
    // this.monitoringService.logError({
    //   type: errorType,
    //   message,
    //   stack: error instanceof Error ? error.stack : undefined,
    //   timestamp: new Date().toISOString(),
    //   url: window.location.href,
    //   userAgent: navigator.userAgent
    // });

    // For now, just log a sanitized version
    console.error(`[Production Error] ${errorType}: ${message}`);
  }
}

/**
 * Error handler provider for app.config.ts
 */
export const errorHandlerProvider = {
  provide: ErrorHandler,
  useClass: GlobalErrorHandler
};
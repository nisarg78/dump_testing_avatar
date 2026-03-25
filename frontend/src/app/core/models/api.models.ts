/**
 * API Response wrappers and utility types
 */

/**
 * Re-define ApiError interface locally to avoid circular dependency
 * This matches the definition in index.ts
 */
export interface ApiErrorType {
  statusCode: number;
  error: string;
  message: string;
  details?: Record<string, unknown>;
  retry_after_seconds?: number;
}

/**
 * Generic API response wrapper
 */
export interface ApiResponse<T> {
  data: T;
  success: boolean;
  message?: string;
  timestamp: string;
}

/**
 * Paginated response wrapper
 */
export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  pageSize: number;
  totalPages: number;
  hasNext: boolean;
  hasPrevious: boolean;
}

/**
 * Health check response
 */
export interface HealthCheckResponse {
  status: 'healthy' | 'degraded' | 'unhealthy';
  version: string;
  timestamp: string;
  services: {
    database: boolean;
    vectorStore: boolean;
    llm: boolean;
    websocket: boolean;
  };
}

/**
 * Rate limit information
 */
export interface RateLimitInfo {
  limit: number;
  remaining: number;
  resetTime: number;
}

/**
 * Type guard for API errors
 */
export function isApiError(error: unknown): error is ApiErrorType {
  return (
    typeof error === 'object' &&
    error !== null &&
    'statusCode' in error &&
    'message' in error
  );
}

/**
 * HTTP status codes enum
 */
export enum HttpStatusCode {
  OK = 200,
  CREATED = 201,
  NO_CONTENT = 204,
  BAD_REQUEST = 400,
  UNAUTHORIZED = 401,
  FORBIDDEN = 403,
  NOT_FOUND = 404,
  TOO_MANY_REQUESTS = 429,
  INTERNAL_SERVER_ERROR = 500,
  BAD_GATEWAY = 502,
  SERVICE_UNAVAILABLE = 503
}

/**
 * Error messages for common scenarios
 */
export const ERROR_MESSAGES: Record<number, string> = {
  [HttpStatusCode.BAD_REQUEST]: 'Invalid request. Please check your input.',
  [HttpStatusCode.UNAUTHORIZED]: 'Authentication required. Please log in.',
  [HttpStatusCode.FORBIDDEN]: 'Access denied. You don\'t have permission.',
  [HttpStatusCode.NOT_FOUND]: 'Resource not found.',
  [HttpStatusCode.TOO_MANY_REQUESTS]: 'Rate limit exceeded. Please try again later.',
  [HttpStatusCode.INTERNAL_SERVER_ERROR]: 'Server error. Please try again.',
  [HttpStatusCode.SERVICE_UNAVAILABLE]: 'Service temporarily unavailable.'
};

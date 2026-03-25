import { Injectable, inject, signal, computed, OnDestroy } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Router } from '@angular/router';
import { BehaviorSubject, Observable, Subject, from, of, throwError } from 'rxjs';
import { map, tap, catchError, takeUntil, switchMap } from 'rxjs/operators';
import { environment } from '@environments/environment';
import {
  User,
  AuthTokens,
  JwtPayload,
  Persona,
  PersonaType,
  UserGroup
} from '../models';
import { StateService } from './state.service';

// AWS Cognito imports would be:
// import { CognitoUserPool, CognitoUser, AuthenticationDetails, CognitoUserSession } from 'amazon-cognito-identity-js';

/** Registered user record stored in localStorage */
interface StoredCredential {
  email: string;
  passwordHash: string;   // SHA-256 hex digest
  name: string;
  persona: PersonaType;
  createdAt: string;
}

/**
 * Authentication Service
 * Handles email/password authentication backed by DynamoDB (Lambda 08/10).
 * Azure AD / Cognito is currently disabled — commented code preserved for future re-enable.
 */
@Injectable({
  providedIn: 'root'
})
export class AuthService implements OnDestroy {
  private readonly router = inject(Router);
  private readonly http = inject(HttpClient);
  private readonly stateService = inject(StateService);
  private readonly destroy$ = new Subject<void>();
  
  // Token storage keys
  private readonly TOKEN_KEY = 'auth_tokens';
  private readonly USER_KEY = 'current_user';
  private readonly CREDENTIALS_KEY = 'mech_registered_users';

  // Signals for reactive state
  private readonly _user = signal<User | null>(null);
  private readonly _isAuthenticated = signal<boolean>(false);
  private readonly _isLoading = signal<boolean>(true);

  // Public readonly signals
  readonly user = this._user.asReadonly();
  readonly isAuthenticated = this._isAuthenticated.asReadonly();
  readonly isLoading = this._isLoading.asReadonly();

  // Computed signals
  readonly userPersona = computed(() => this._user()?.persona || 'developer');
  readonly userGroups = computed(() => this._user()?.groups || []);
  readonly isAdmin = computed(() => this.userGroups().includes('admin'));

  constructor() {
    this.initializeAuth();
  }

  /**
   * Initialize authentication state from storage
   */
  private initializeAuth(): void {
    try {
      const storedTokens = this.getStoredTokens();
      const storedUser = this.getStoredUser();

      if (storedTokens && storedUser && this.isTokenValid(storedTokens.idToken)) {
        this._user.set(storedUser);
        this._isAuthenticated.set(true);
        this.stateService.initForUser(storedUser.id);
      } else {
        this.clearAuth();
      }
    } catch (error) {
      console.error('[Auth] Initialization error:', error);
      this.clearAuth();
    } finally {
      this._isLoading.set(false);
    }
  }

  /**
   * Navigate to the login page.
   * (Cognito/Azure AD redirect is disabled — using DynamoDB auth instead.)
   */
  login(): void {
    this.router.navigate(['/login']);

    // --- Cognito/Azure AD redirect (disabled) ---
    // if (!environment.production) { this.devModeLogin(); return; }
    // const { cognito } = environment;
    // const authUrl = new URL(`https://${cognito.domain}/oauth2/authorize`);
    // authUrl.searchParams.set('client_id', cognito.clientId);
    // authUrl.searchParams.set('response_type', cognito.responseType);
    // authUrl.searchParams.set('scope', cognito.scope.join(' '));
    // authUrl.searchParams.set('redirect_uri', cognito.redirectSignIn);
    // window.location.href = authUrl.toString();
  }

  /**
   * Dev mode login - creates mock user without Cognito
   */
  private devModeLogin(): void {
    const mockUser: User = {
      id: 'dev-user-001',
      email: 'developer@bmo.com',
      name: 'Dev User',
      username: 'devuser',
      groups: ['user', 'developer'],
      persona: 'developer',
      isAuthenticated: true
    };

    const mockToken = this.generateMockToken(mockUser);
    const mockTokens: AuthTokens = {
      idToken: mockToken,
      accessToken: mockToken,
      refreshToken: 'mock-refresh-token',
      expiresIn: 28800,
      tokenType: 'Bearer'
    };

    this.storeTokens(mockTokens);
    this.storeUser(mockUser);
    this._user.set(mockUser);
    this._isAuthenticated.set(true);
    this.stateService.initForUser(mockUser.id);
    
    console.log('[Auth] Dev mode login successful');
    
    // Navigate to main page
    this.router.navigate(['/']);
  }

  /**
   * Sign in with email + password via the DynamoDB auth backend.
   * On success, stores tokens/user and navigates to "/".
   */
  loginWithCredentials(email: string, password: string): Observable<User> {
    this._isLoading.set(true);
    const url = `${environment.api.endpoint}/auth/login`;

    type LoginResponse = { user: { id: string; email: string; name: string; persona: string }; token: string; expires_in: number };

    return this.http.post<LoginResponse>(url, { email, password }).pipe(
      map((r: LoginResponse) => {
        const tokens: AuthTokens = {
          idToken:      r.token,
          accessToken:  r.token,
          refreshToken: '',
          expiresIn:    r.expires_in,
          tokenType:    'Bearer'
        };
        const user: User = {
          id:              r.user.id,
          email:           r.user.email,
          name:            r.user.name,
          username:        r.user.email,
          groups:          ['user'],
          persona:         (r.user.persona as PersonaType) || 'developer',
          isAuthenticated: true
        };
        this.storeTokens(tokens);
        this.storeUser(user);
        this._user.set(user);
        this._isAuthenticated.set(true);
        this.stateService.initForUser(user.id);
        return user;
      }),
      tap(() => {
        this._isLoading.set(false);
        this.router.navigate(['/']);
      }),
      catchError((err: unknown) => {
        this._isLoading.set(false);
        return throwError(() => err);
      })
    );
  }

  /**
   * Create a new account via the DynamoDB auth backend.
   * On success, stores tokens/user and navigates to "/".
   */
  register(name: string, email: string, password: string, persona: string): Observable<User> {
    this._isLoading.set(true);
    const url = `${environment.api.endpoint}/auth/register`;

    type RegisterResponse = { user: { id: string; email: string; name: string; persona: string }; token: string; expires_in: number };

    return this.http.post<RegisterResponse>(url, { name, email, password, persona }).pipe(
      map((r: RegisterResponse) => {
        const tokens: AuthTokens = {
          idToken:      r.token,
          accessToken:  r.token,
          refreshToken: '',
          expiresIn:    r.expires_in,
          tokenType:    'Bearer'
        };
        const user: User = {
          id:              r.user.id,
          email:           r.user.email,
          name:            r.user.name,
          username:        r.user.email,
          groups:          ['user'],
          persona:         (r.user.persona as PersonaType) || 'developer',
          isAuthenticated: true
        };
        this.storeTokens(tokens);
        this.storeUser(user);
        this._user.set(user);
        this._isAuthenticated.set(true);
        this.stateService.initForUser(user.id);
        return user;
      }),
      tap(() => {
        this._isLoading.set(false);
        this.router.navigate(['/']);
      }),
      catchError((err: unknown) => {
        this._isLoading.set(false);
        return throwError(() => err);
      })
    );
  }

  /**
   * Register locally — stores credentials in localStorage and signs the user in.
   * Used as dev fallback when backend is unreachable.
   */
  localRegister(name: string, email: string, password: string, persona?: Persona): { success: boolean; error?: string } {
    const normalizedEmail = email.trim().toLowerCase();

    // Check if email is already registered
    const existing = this.getStoredCredential(normalizedEmail);
    if (existing) {
      return { success: false, error: 'An account with this email already exists. Please sign in.' };
    }

    const passwordHash = this.hashPassword(password);
    const cred: StoredCredential = {
      email: normalizedEmail,
      passwordHash,
      name: name.trim(),
      persona: persona?.apiPersona || 'developer',
      createdAt: new Date().toISOString()
    };
    this.saveCredential(cred);

    // Derive a deterministic user ID from email so history persists
    const userId = this.deriveUserId(normalizedEmail);
    const user: User = {
      id: userId,
      email: normalizedEmail,
      name: cred.name,
      username: normalizedEmail,
      groups: ['user'],
      persona: cred.persona,
      isAuthenticated: true
    };

    const token = this.generateMockToken(user);
    const tokens: AuthTokens = {
      idToken: token,
      accessToken: token,
      refreshToken: '',
      expiresIn: 28800,
      tokenType: 'Bearer'
    };

    this.storeTokens(tokens);
    this.storeUser(user);
    this._user.set(user);
    this._isAuthenticated.set(true);
    this.stateService.initForUser(userId);

    console.log('[Auth] Local register successful:', user.name, 'as', user.persona);
    this.router.navigate(['/']);
    return { success: true };
  }

  /**
   * Login locally — validates credentials from localStorage.
   * Used as dev fallback when backend is unreachable.
   */
  localLogin(email: string, password: string): { success: boolean; error?: string } {
    const normalizedEmail = email.trim().toLowerCase();

    const cred = this.getStoredCredential(normalizedEmail);
    if (!cred) {
      return { success: false, error: 'No account found with this email. Please sign up first.' };
    }

    const passwordHash = this.hashPassword(password);
    if (cred.passwordHash !== passwordHash) {
      return { success: false, error: 'Incorrect password. Please try again.' };
    }

    const userId = this.deriveUserId(normalizedEmail);
    const user: User = {
      id: userId,
      email: normalizedEmail,
      name: cred.name,
      username: normalizedEmail,
      groups: ['user'],
      persona: cred.persona,
      isAuthenticated: true
    };

    const token = this.generateMockToken(user);
    const tokens: AuthTokens = {
      idToken: token,
      accessToken: token,
      refreshToken: '',
      expiresIn: 28800,
      tokenType: 'Bearer'
    };

    this.storeTokens(tokens);
    this.storeUser(user);
    this._user.set(user);
    this._isAuthenticated.set(true);
    this.stateService.initForUser(userId);

    console.log('[Auth] Local login successful:', user.name, 'as', user.persona);
    this.router.navigate(['/']);
    return { success: true };
  }

  /**
   * OAuth callback handler — Cognito/Azure AD is disabled.
   * The /callback route now simply redirects here, then on to /login.
   */
  handleCallback(_code: string): Observable<User> {
    this._isLoading.set(false);
    this.router.navigate(['/login']);
    return throwError(() => new Error('OAuth2 callback is disabled — using DynamoDB auth'));

    // --- Cognito OAuth2 code exchange (disabled) ---
    // return this.exchangeCodeForTokens(code).pipe(
    //   switchMap((tokens: AuthTokens) => { ... }),
    //   catchError(...),
    //   tap(() => this._isLoading.set(false))
    // );
  }

  /**
   * Logout — clear local auth state and navigate to login.
   * (Cognito/Azure AD logout redirect is disabled.)
   */
  logout(): void {
    this.clearAuth();
    console.log('[Auth] Logout — navigating to /login');
    this.router.navigate(['/login']);

    // --- Cognito logout redirect (disabled) ---
    // const { cognito } = environment;
    // const logoutUrl = new URL(`https://${cognito.domain}/logout`);
    // logoutUrl.searchParams.set('client_id', cognito.clientId);
    // logoutUrl.searchParams.set('logout_uri', cognito.redirectSignOut);
    // window.location.href = logoutUrl.toString();
  }

  /**
   * Get current access token
   */
  getAccessToken(): string | null {
    const tokens = this.getStoredTokens();
    if (tokens && this.isTokenValid(tokens.accessToken)) {
      return tokens.accessToken;
    }
    return null;
  }

  /**
   * Get current ID token for API calls
   */
  getIdToken(): string | null {
    const tokens = this.getStoredTokens();
    if (tokens && this.isTokenValid(tokens.idToken)) {
      return tokens.idToken;
    }
    return null;
  }

  /**
   * Refresh the JWT token via POST /auth/refresh.
   */
  refreshTokens(): Observable<AuthTokens> {
    const tokens = this.getStoredTokens();
    if (!tokens?.idToken) {
      return throwError(() => new Error('No token available to refresh'));
    }

    const url = `${environment.api.endpoint}/auth/refresh`;
    const headers = new HttpHeaders({ Authorization: `Bearer ${tokens.idToken}` });

    type RefreshResponse = { token: string; expires_in: number };

    return this.http.post<RefreshResponse>(url, {}, { headers }).pipe(
      map((r: RefreshResponse) => ({
        idToken:      r.token,
        accessToken:  r.token,
        refreshToken: '',
        expiresIn:    r.expires_in,
        tokenType:    'Bearer'
      })),
      tap((newTokens: AuthTokens) => {
        this.storeTokens(newTokens);
        const user = this.parseUserFromToken(newTokens.idToken);
        this._user.set(user);
        this.storeUser(user);
      }),
      catchError((error: Error) => {
        console.error('[Auth] Token refresh failed:', error);
        this.clearAuth();
        return throwError(() => error);
      })
    );
  }

  /**
   * Check if user has required role
   */
  hasRole(role: UserGroup): boolean {
    return this.userGroups().includes(role);
  }

  /**
   * Check if user has any of the required roles
   */
  hasAnyRole(roles: UserGroup[]): boolean {
    return roles.some(role => this.hasRole(role));
  }

  /**
   * Update user persona
   */
  setPersona(persona: PersonaType): void {
    const currentUser = this._user();
    if (currentUser) {
      const updatedUser = { ...currentUser, persona };
      this._user.set(updatedUser);
      this.storeUser(updatedUser);
    }
  }

  // ==========================================================================
  // PRIVATE METHODS
  // ==========================================================================

  // --- Cognito code exchange (disabled — kept for future re-enable) ----------
  // private exchangeCodeForTokens(code: string): Observable<AuthTokens> { ... }
  // private performTokenRefresh(refreshToken: string): Observable<AuthTokens> { ... }
  // --------------------------------------------------------------------------

  private parseUserFromToken(idToken: string): User {
    try {
      const payload = this.decodeToken(idToken);
      // Our HS256 JWT payload: { sub, email, name, persona, iat, exp }
      return {
        id:              payload.sub,
        email:           payload.email,
        name:            (payload as any)['name'] || payload.email,
        username:        payload.email,
        groups:          ['user'],
        persona:         ((payload as any)['persona'] as PersonaType) || 'developer',
        isAuthenticated: true
      };
    } catch (error) {
      console.error('[Auth] Failed to parse token:', error);
      throw error;
    }
  }

  private decodeToken(token: string): JwtPayload {
    try {
      const parts = token.split('.');
      if (parts.length !== 3) {
        throw new Error('Invalid token format');
      }
      const payload = JSON.parse(atob(parts[1]));
      return payload as JwtPayload;
    } catch (error) {
      throw new Error('Failed to decode token');
    }
  }

  private isTokenValid(token: string): boolean {
    try {
      const payload = this.decodeToken(token);
      const now = Math.floor(Date.now() / 1000);
      return payload.exp > now;
    } catch {
      return false;
    }
  }

  private generateMockToken(user?: User): string {
    // Generate a mock JWT matching our HS256 format (for dev-mode fallback only)
    const header  = { alg: 'HS256', typ: 'JWT' };
    const payload = {
      sub:     user?.id || 'mock-user-id',
      email:   user?.email || 'developer@bmo.com',
      name:    user?.name || 'Dev User',
      persona: user?.persona || 'developer',
      iat:     Math.floor(Date.now() / 1000),
      exp:     Math.floor(Date.now() / 1000) + 28800   // 8 hours
    };
    // Signature is intentionally fake — this token is for local dev only
    return `${btoa(JSON.stringify(header))}.${btoa(JSON.stringify(payload))}.dev-mock-signature`;
  }

  private storeTokens(tokens: AuthTokens): void {
    localStorage.setItem(this.TOKEN_KEY, JSON.stringify(tokens));
  }

  private getStoredTokens(): AuthTokens | null {
    const stored = localStorage.getItem(this.TOKEN_KEY);
    return stored ? JSON.parse(stored) : null;
  }

  private storeUser(user: User): void {
    localStorage.setItem(this.USER_KEY, JSON.stringify(user));
  }

  private getStoredUser(): User | null {
    const stored = localStorage.getItem(this.USER_KEY);
    return stored ? JSON.parse(stored) : null;
  }

  private clearAuth(): void {
    localStorage.removeItem(this.TOKEN_KEY);
    localStorage.removeItem(this.USER_KEY);
    this._user.set(null);
    this._isAuthenticated.set(false);
  }

  // ==========================================================================
  // LOCAL CREDENTIAL STORE (localStorage-based, per-email)
  // ==========================================================================

  private getAllCredentials(): Record<string, StoredCredential> {
    try {
      const raw = localStorage.getItem(this.CREDENTIALS_KEY);
      return raw ? JSON.parse(raw) : {};
    } catch {
      return {};
    }
  }

  private getStoredCredential(email: string): StoredCredential | null {
    return this.getAllCredentials()[email] || null;
  }

  private saveCredential(cred: StoredCredential): void {
    const all = this.getAllCredentials();
    all[cred.email] = cred;
    localStorage.setItem(this.CREDENTIALS_KEY, JSON.stringify(all));
  }

  /**
   * Deterministic user ID from email — same email always gets the same ID,
   * so chat history and conversations persist across sessions.
   */
  private deriveUserId(email: string): string {
    // Simple deterministic hash: use a stable string derived from the email
    let hash = 0;
    for (let i = 0; i < email.length; i++) {
      const char = email.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash; // Convert to 32-bit integer
    }
    return `user-${Math.abs(hash).toString(36)}-${email.replace(/[^a-z0-9]/g, '')}`;
  }

  /**
   * Simple synchronous password hash (SHA-256 via Web Crypto is async,
   * so we use a fast deterministic JS hash for local-only dev storage).
   * NOT suitable for production — backend should use bcrypt/argon2.
   */
  private hashPassword(password: string): string {
    let h1 = 0xdeadbeef;
    let h2 = 0x41c6ce57;
    for (let i = 0; i < password.length; i++) {
      const ch = password.charCodeAt(i);
      h1 = Math.imul(h1 ^ ch, 2654435761);
      h2 = Math.imul(h2 ^ ch, 1597334677);
    }
    h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507);
    h1 ^= Math.imul(h2 ^ (h2 >>> 13), 3266489909);
    h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507);
    h2 ^= Math.imul(h1 ^ (h1 >>> 13), 3266489909);
    return (4294967296 * (2097151 & h2) + (h1 >>> 0)).toString(36);
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

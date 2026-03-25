import { Component, inject, signal, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { AuthService } from '@app/core/services/auth.service';
import { PERSONAS, PersonaConfig, VALIDATION, Persona } from '@app/core/models';
import { environment } from '@environments/environment';

/**
 * Login/Signup Component
 * BMO Enterprise Login UI with persona selection
 * Professional banking-grade authentication experience
 */
@Component({ 
  selector: 'app-login',
  standalone: true,
  imports: [CommonModule, FormsModule],
  template: `
    <div class="login-page">
      <!-- Main Content - Scrollable -->
      <main class="main-content">
        <div class="auth-container">
          <!-- Auth Forms Panel -->
          <div class="auth-panel">
            <div class="auth-card">
              <!-- Tab Navigation -->
              <div class="auth-tabs">
                <button 
                  class="auth-tab"
                  [class.active]="activeTab() === 'login'"
                  (click)="setActiveTab('login')"
                >
                  Sign In
                </button>
                <button 
                  class="auth-tab"
                  [class.active]="activeTab() === 'signup'"
                  (click)="setActiveTab('signup')"
                >
                  Create Account
                </button>
              </div>

              <!-- Login Form -->
              @if (activeTab() === 'login') {
                <div class="auth-form-container">
                  <div class="welcome-text">
                    <h2>Welcome back</h2>
                    <p>Sign in to continue to MECH Avatar</p>
                  </div>

                  <form (ngSubmit)="onSubmit()" class="auth-form">
                    <!-- Login ID -->
                    <div class="form-group">
                      <label for="loginId" class="form-label">
                        Email Address
                        <span class="required">*</span>
                      </label>
                      <div class="input-wrapper">
                        <svg class="input-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"/>
                        </svg>
                        <input
                          type="email"
                          id="loginId"
                          name="loginId"
                          [(ngModel)]="loginId"
                          class="form-input"
                          placeholder="Enter your email address"
                          autocomplete="email"
                        />
                      </div>
                      @if (loginIdError()) {
                        <span class="form-error">{{ loginIdError() }}</span>
                      }
                    </div>

                    <!-- Password -->
                    <div class="form-group">
                      <label for="password" class="form-label">
                        Password
                        <span class="required">*</span>
                      </label>
                      <div class="input-wrapper">
                        <svg class="input-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"/>
                        </svg>
                        <input
                          [type]="showPassword() ? 'text' : 'password'"
                          id="password"
                          name="password"
                          [(ngModel)]="password"
                          class="form-input"
                          placeholder="Enter your password"
                          autocomplete="current-password"
                        />
                        <button
                          type="button"
                          class="password-toggle"
                          (click)="togglePasswordVisibility()"
                        >
                          @if (showPassword()) {
                            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.875 18.825A10.05 10.05 0 0112 19c-4.478 0-8.268-2.943-9.543-7a9.97 9.97 0 011.563-3.029m5.858.908a3 3 0 114.243 4.243M9.878 9.878l4.242 4.242M9.88 9.88l-3.29-3.29m7.532 7.532l3.29 3.29M3 3l3.59 3.59m0 0A9.953 9.953 0 0112 5c4.478 0 8.268 2.943 9.543 7a10.025 10.025 0 01-4.132 5.411m0 0L21 21"/>
                            </svg>
                          } @else {
                            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/>
                              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z"/>
                            </svg>
                          }
                        </button>
                      </div>
                      @if (passwordError()) {
                        <span class="form-error">{{ passwordError() }}</span>
                      }
                    </div>

                    <!-- Remember Me & Forgot Password -->
                    <div class="form-extras">
                      <label class="checkbox-label">
                        <input type="checkbox" [(ngModel)]="rememberMe" name="rememberMe" class="checkbox" />
                        <span>Remember me</span>
                      </label>
                      <a href="#" class="forgot-link">Forgot password?</a>
                    </div>

                    <!-- Sign In Button -->
                    <button
                      type="submit"
                      class="submit-btn"
                      [disabled]="isLoading()"
                    >
                      @if (isLoading()) {
                        <svg class="animate-spin h-5 w-5" fill="none" viewBox="0 0 24 24">
                          <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                          <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                        </svg>
                        <span>Signing in...</span>
                      } @else {
                        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 16l-4-4m0 0l4-4m-4 4h14m-5 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h7a3 3 0 013 3v1"/>
                        </svg>
                        <span>Sign In</span>
                      }
                    </button>
                  </form>

                  <!-- SSO Option (disabled — not currently using Azure AD / Cognito) -->
                  <!-- Uncomment and configure Azure AD / Cognito to re-enable SSO -->
                  <!--
                  <div class="sso-section">
                    <div class="sso-divider"><span>or continue with</span></div>
                    <button class="sso-btn" (click)="loginWithSSO()">
                      <span>BMO Single Sign-On</span>
                    </button>
                  </div>
                  -->

                  <!-- Error Message -->
                  @if (errorMessage()) {
                    <div class="error-alert">
                      <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>
                      </svg>
                      <span>{{ errorMessage() }}</span>
                    </div>
                  }
                </div>
              }

              <!-- Signup Form -->
              @if (activeTab() === 'signup') {
                <div class="auth-form-container">
                  <div class="welcome-text">
                    <h2>Create your account</h2>
                    <p>Join MECH Avatar for intelligent documentation assistance</p>
                  </div>

                  <!-- Signup Steps -->
                  <div class="signup-steps">
                    <div class="step" [class.active]="signupStep() === 1" [class.completed]="signupStep() > 1">
                      <div class="step-number">
                        @if (signupStep() > 1) {
                          <svg class="w-4 h-4" fill="currentColor" viewBox="0 0 20 20">
                            <path fill-rule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clip-rule="evenodd"/>
                          </svg>
                        } @else {
                          1
                        }
                      </div>
                      <span class="step-label">Account</span>
                    </div>
                    <div class="step-connector" [class.active]="signupStep() > 1"></div>
                    <div class="step" [class.active]="signupStep() === 2">
                      <div class="step-number">2</div>
                      <span class="step-label">Persona</span>
                    </div>
                  </div>

                  <!-- Step 1: Account Details -->
                  @if (signupStep() === 1) {
                    <form (ngSubmit)="nextSignupStep()" class="auth-form">
                      <div class="form-group">
                        <label for="signupName" class="form-label">
                          Full Name <span class="required">*</span>
                        </label>
                        <div class="input-wrapper">
                          <svg class="input-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"/>
                          </svg>
                          <input
                            type="text"
                            id="signupName"
                            name="signupName"
                            [(ngModel)]="signupName"
                            class="form-input"
                            placeholder="Enter your full name"
                          />
                        </div>
                      </div>

                      <div class="form-group">
                        <label for="signupEmail" class="form-label">
                          Email Address <span class="required">*</span>
                        </label>
                        <div class="input-wrapper">
                          <svg class="input-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"/>
                          </svg>
                          <input
                            type="email"
                            id="signupEmail"
                            name="signupEmail"
                            [(ngModel)]="signupEmail"
                            class="form-input"
                            placeholder="yourname@bmo.com"
                          />
                        </div>
                      </div>

                      <div class="form-group">
                        <label for="signupPassword" class="form-label">
                          Password <span class="required">*</span>
                        </label>
                        <div class="input-wrapper">
                          <svg class="input-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"/>
                          </svg>
                          <input
                            [type]="showSignupPassword() ? 'text' : 'password'"
                            id="signupPassword"
                            name="signupPassword"
                            [(ngModel)]="signupPassword"
                            class="form-input"
                            placeholder="Create a strong password"
                          />
                          <button
                            type="button"
                            class="password-toggle"
                            (click)="toggleSignupPasswordVisibility()"
                          >
                            @if (showSignupPassword()) {
                              <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.875 18.825A10.05 10.05 0 0112 19c-4.478 0-8.268-2.943-9.543-7a9.97 9.97 0 011.563-3.029m5.858.908a3 3 0 114.243 4.243M9.878 9.878l4.242 4.242M9.88 9.88l-3.29-3.29m7.532 7.532l3.29 3.29M3 3l3.59 3.59m0 0A9.953 9.953 0 0112 5c4.478 0 8.268 2.943 9.543 7a10.025 10.025 0 01-4.132 5.411m0 0L21 21"/>
                              </svg>
                            } @else {
                              <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/>
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z"/>
                              </svg>
                            }
                          </button>
                        </div>
                        <!-- Password Requirements -->
                        <div class="password-requirements">
                          <div class="req" [class.met]="signupPassword.length >= 8">
                            <svg class="w-3 h-3" fill="currentColor" viewBox="0 0 20 20">
                              <path fill-rule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clip-rule="evenodd"/>
                            </svg>
                            <span>At least 8 characters</span>
                          </div>
                          <div class="req" [class.met]="hasUppercase()">
                            <svg class="w-3 h-3" fill="currentColor" viewBox="0 0 20 20">
                              <path fill-rule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clip-rule="evenodd"/>
                            </svg>
                            <span>One uppercase letter</span>
                          </div>
                          <div class="req" [class.met]="hasNumber()">
                            <svg class="w-3 h-3" fill="currentColor" viewBox="0 0 20 20">
                              <path fill-rule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clip-rule="evenodd"/>
                            </svg>
                            <span>One number</span>
                          </div>
                        </div>
                      </div>

                      <button
                        type="submit"
                        class="submit-btn"
                        [disabled]="!isSignupStep1Valid()"
                      >
                        <span>Continue to Persona Selection</span>
                        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 7l5 5m0 0l-5 5m5-5H6"/>
                        </svg>
                      </button>
                    </form>
                  }

                  <!-- Step 2: Persona Selection -->
                  @if (signupStep() === 2) {
                    <div class="persona-selection">
                      <p class="persona-intro">Choose a persona that best matches your role. This helps us tailor responses to your needs.</p>
                      
                      <div class="persona-grid">
                        @for (persona of personas; track persona.id) {
                          <div 
                            class="persona-card"
                            [class.selected]="selectedPersona?.id === persona.id"
                            (click)="selectPersona(persona)"
                          >
                            <!-- Selection Indicator -->
                            @if (selectedPersona?.id === persona.id) {
                              <div class="selected-badge">
                                <svg class="w-3 h-3" fill="currentColor" viewBox="0 0 20 20">
                                  <path fill-rule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clip-rule="evenodd"/>
                                </svg>
                              </div>
                            }

                            <div class="persona-icon" [style.background-color]="getPersonaColor(persona)">
                              <ng-container [ngSwitch]="persona.icon">
                                <svg *ngSwitchCase="'user-plus'" class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18 9v3m0 0v3m0-3h3m-3 0h-3m-2-5a4 4 0 11-8 0 4 4 0 018 0zM3 20a6 6 0 0112 0v1H3v-1z"/>
                                </svg>
                                <svg *ngSwitchCase="'palette'" class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 21a4 4 0 01-4-4V5a2 2 0 012-2h4a2 2 0 012 2v12a4 4 0 01-4 4zm0 0h12a2 2 0 002-2v-4a2 2 0 00-2-2h-2.343M11 7.343l1.657-1.657a2 2 0 012.828 0l2.829 2.829a2 2 0 010 2.828l-8.486 8.485M7 17h.01"/>
                                </svg>
                                <svg *ngSwitchCase="'clipboard-check'" class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4"/>
                                </svg>
                                <svg *ngSwitchCase="'code'" class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4"/>
                                </svg>
                                <svg *ngSwitchCase="'server'" class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01"/>
                                </svg>
                                <svg *ngSwitchCase="'briefcase'" class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 13.255A23.931 23.931 0 0112 15c-3.183 0-6.22-.62-9-1.745M16 6V4a2 2 0 00-2-2h-4a2 2 0 00-2 2v2m4 6h.01M5 20h14a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"/>
                                </svg>
                                <svg *ngSwitchDefault class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"/>
                                </svg>
                              </ng-container>
                            </div>
                            <div class="persona-info">
                              <h4>{{ persona.name }}</h4>
                              <p>{{ persona.description }}</p>
                            </div>
                          </div>
                        }
                      </div>

                      <!-- Selected Persona Details -->
                      @if (selectedPersona) {
                        <div class="selected-persona-details">
                          <h4>{{ selectedPersona.name }} Benefits:</h4>
                          <ul>
                            @for (benefit of selectedPersona.benefits; track $index) {
                              <li>
                                <svg class="w-4 h-4 text-green-500" fill="currentColor" viewBox="0 0 20 20">
                                  <path fill-rule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clip-rule="evenodd"/>
                                </svg>
                                {{ benefit }}
                              </li>
                            }
                          </ul>
                        </div>
                      }

                      <!-- Action Buttons -->
                      <div class="signup-actions">
                        <button class="back-btn" (click)="prevSignupStep()">
                          <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 17l-5-5m0 0l5-5m-5 5h12"/>
                          </svg>
                          <span>Back</span>
                        </button>
                        <button 
                          class="submit-btn flex-1"
                          [disabled]="!selectedPersona || isLoading()"
                          (click)="completeSignup()"
                        >
                          @if (isLoading()) {
                            <svg class="animate-spin h-5 w-5" fill="none" viewBox="0 0 24 24">
                              <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                              <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                            </svg>
                            <span>Creating account...</span>
                          } @else {
                            <span>Create Account</span>
                            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"/>
                            </svg>
                          }
                        </button>
                      </div>
                    </div>
                  }

                  <!-- Error Message -->
                  @if (errorMessage()) {
                    <div class="error-alert">
                      <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>
                      </svg>
                      <span>{{ errorMessage() }}</span>
                    </div>
                  }
                </div>
              }
            </div>

            <!-- Footer Links -->
            <div class="auth-footer">
              <p>By signing in, you agree to BMO's <a href="#">Terms of Service</a> and <a href="#">Privacy Policy</a></p>
            </div>
          </div>
        </div>
      </main>
    </div>
  `,
  styles: [`
    :host {
      display: block;
      width: 100%;
      height: 100%;
      overflow: auto;
    }

    /* Page Layout */
    .login-page {
      min-height: 100%;
      display: flex;
      flex-direction: column;
      background: linear-gradient(135deg, #f5f7fa 0%, #e4e8ec 100%);
      overflow-y: auto;
    }

    /* Main Content */
    .main-content {
      flex: 1;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 2rem;
      min-height: min-content;
    }

    .auth-container {
      display: flex;
      justify-content: center;
      max-width: 500px;
      width: 100%;
      background: white;
      border-radius: 16px;
      box-shadow: 0 20px 60px rgba(0, 0, 0, 0.1);
      overflow: hidden;
    }

    /* Auth Panel */
    .auth-panel {
      width: 100%;
      padding: 2rem;
      display: flex;
      flex-direction: column;
    }

    .auth-card {
      flex: 1;
      display: flex;
      flex-direction: column;
    }

    /* Auth Tabs */
    .auth-tabs {
      display: flex;
      border-bottom: 2px solid #e5e7eb;
      margin-bottom: 1.5rem;
    }

    .auth-tab {
      flex: 1;
      padding: 0.875rem 1rem;
      background: none;
      border: none;
      font-size: 0.95rem;
      font-weight: 500;
      color: #6b7280;
      cursor: pointer;
      position: relative;
      transition: color 0.2s;
    }

    .auth-tab.active {
      color: #0079C1;
    }

    .auth-tab.active::after {
      content: '';
      position: absolute;
      bottom: -2px;
      left: 0;
      right: 0;
      height: 2px;
      background: #0079C1;
    }

    .auth-tab:hover:not(.active) {
      color: #374151;
    }

    /* Auth Form Container */
    .auth-form-container {
      flex: 1;
      display: flex;
      flex-direction: column;
    }

    .welcome-text {
      margin-bottom: 1.5rem;
    }

    .welcome-text h2 {
      font-size: 1.375rem;
      font-weight: 600;
      color: #111827;
      margin-bottom: 0.25rem;
    }

    .welcome-text p {
      color: #6b7280;
      font-size: 0.9rem;
    }

    /* Auth Form */
    .auth-form {
      display: flex;
      flex-direction: column;
      gap: 1.25rem;
    }

    .form-group {
      display: flex;
      flex-direction: column;
      gap: 0.5rem;
    }

    .form-label {
      font-size: 0.875rem;
      font-weight: 500;
      color: #374151;
    }

    .required {
      color: #ef4444;
    }

    .input-wrapper {
      position: relative;
      display: flex;
      align-items: center;
    }

    .input-icon {
      position: absolute;
      left: 0.875rem;
      width: 18px;
      height: 18px;
      color: #9ca3af;
      pointer-events: none;
    }

    .form-input {
      width: 100%;
      padding: 0.75rem 0.875rem 0.75rem 2.75rem;
      border: 1.5px solid #e5e7eb;
      border-radius: 10px;
      font-size: 0.95rem;
      transition: all 0.2s;
      background: #fafbfc;
    }

    .form-input:focus {
      outline: none;
      border-color: #0079C1;
      box-shadow: 0 0 0 3px rgba(0, 121, 193, 0.1);
      background: white;
    }

    .form-input::placeholder {
      color: #9ca3af;
    }

    .password-toggle {
      position: absolute;
      right: 0.875rem;
      background: none;
      border: none;
      color: #6b7280;
      cursor: pointer;
      padding: 0.25rem;
      display: flex;
      transition: color 0.2s;
    }

    .password-toggle:hover {
      color: #374151;
    }

    .form-error {
      color: #ef4444;
      font-size: 0.8rem;
    }

    /* Form Extras */
    .form-extras {
      display: flex;
      justify-content: space-between;
      align-items: center;
    }

    .checkbox-label {
      display: flex;
      align-items: center;
      gap: 0.5rem;
      cursor: pointer;
      font-size: 0.875rem;
      color: #4b5563;
    }

    .checkbox {
      width: 16px;
      height: 16px;
      accent-color: #0079C1;
    }

    .forgot-link {
      color: #0079C1;
      font-size: 0.875rem;
      text-decoration: none;
      font-weight: 500;
      transition: color 0.2s;
    }

    .forgot-link:hover {
      color: #005587;
    }

    /* Submit Button */
    .submit-btn {
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 0.5rem;
      width: 100%;
      padding: 0.875rem 1.5rem;
      background: linear-gradient(135deg, #0079C1 0%, #005587 100%);
      color: white;
      font-size: 1rem;
      font-weight: 600;
      border: none;
      border-radius: 10px;
      cursor: pointer;
      transition: all 0.2s;
      box-shadow: 0 4px 12px rgba(0, 121, 193, 0.3);
    }

    .submit-btn:hover:not(:disabled) {
      transform: translateY(-1px);
      box-shadow: 0 6px 16px rgba(0, 121, 193, 0.4);
    }

    .submit-btn:disabled {
      opacity: 0.6;
      cursor: not-allowed;
      transform: none;
    }

    /* SSO Section */
    .sso-section {
      margin-top: 1.5rem;
    }

    .sso-divider {
      display: flex;
      align-items: center;
      gap: 1rem;
      margin-bottom: 1rem;
    }

    .sso-divider::before,
    .sso-divider::after {
      content: '';
      flex: 1;
      height: 1px;
      background: #e5e7eb;
    }

    .sso-divider span {
      color: #9ca3af;
      font-size: 0.8rem;
      white-space: nowrap;
    }

    .sso-btn {
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 0.75rem;
      width: 100%;
      padding: 0.75rem 1rem;
      background: white;
      border: 1.5px solid #e5e7eb;
      border-radius: 10px;
      font-size: 0.95rem;
      font-weight: 500;
      color: #374151;
      cursor: pointer;
      transition: all 0.2s;
    }

    .sso-btn:hover {
      background: #f9fafb;
      border-color: #d1d5db;
    }

    /* Error Alert */
    .error-alert {
      display: flex;
      align-items: center;
      gap: 0.75rem;
      padding: 0.875rem 1rem;
      background: #fef2f2;
      border: 1px solid #fecaca;
      border-radius: 10px;
      margin-top: 1rem;
    }

    .error-alert svg {
      color: #ef4444;
      flex-shrink: 0;
    }

    .error-alert span {
      color: #dc2626;
      font-size: 0.875rem;
    }

    /* Signup Steps */
    .signup-steps {
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 0;
      margin-bottom: 1.5rem;
      padding: 0 1rem;
    }

    .step {
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 0.5rem;
    }

    .step-number {
      width: 32px;
      height: 32px;
      border-radius: 50%;
      background: #e5e7eb;
      color: #6b7280;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 0.875rem;
      font-weight: 600;
      transition: all 0.3s;
    }

    .step.active .step-number {
      background: #0079C1;
      color: white;
    }

    .step.completed .step-number {
      background: #10b981;
      color: white;
    }

    .step-label {
      font-size: 0.75rem;
      color: #6b7280;
      font-weight: 500;
    }

    .step.active .step-label {
      color: #0079C1;
    }

    .step-connector {
      width: 80px;
      height: 2px;
      background: #e5e7eb;
      margin: 0 0.5rem;
      margin-bottom: 1.5rem;
      transition: background 0.3s;
    }

    .step-connector.active {
      background: #10b981;
    }

    /* Password Requirements */
    .password-requirements {
      display: flex;
      flex-wrap: wrap;
      gap: 0.5rem;
      margin-top: 0.5rem;
    }

    .req {
      display: flex;
      align-items: center;
      gap: 0.25rem;
      font-size: 0.75rem;
      color: #9ca3af;
    }

    .req svg {
      color: #d1d5db;
    }

    .req.met {
      color: #10b981;
    }

    .req.met svg {
      color: #10b981;
    }

    /* Persona Selection */
    .persona-selection {
      display: flex;
      flex-direction: column;
      gap: 1rem;
    }

    .persona-intro {
      color: #6b7280;
      font-size: 0.9rem;
      line-height: 1.5;
    }

    .persona-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 0.75rem;
    }

    .persona-card {
      display: flex;
      align-items: flex-start;
      gap: 0.75rem;
      padding: 1rem 1rem;
      min-height: 80px;
      border: 2px solid #e5e7eb;
      border-radius: 12px;
      cursor: pointer;
      transition: all 0.2s;
      position: relative;
    }

    .persona-card:hover {
      border-color: #bfdbfe;
      background: #f8fafc;
    }

    .persona-card.selected {
      border-color: #0079C1;
      background: #eff6ff;
    }

    .selected-badge {
      position: absolute;
      top: 0.5rem;
      right: 0.5rem;
      width: 18px;
      height: 18px;
      background: #10b981;
      border-radius: 50%;
      display: flex;
      align-items: center;
      justify-content: center;
      color: white;
    }

    .persona-icon {
      width: 36px;
      height: 36px;
      border-radius: 10px;
      display: flex;
      align-items: center;
      justify-content: center;
      color: white;
      flex-shrink: 0;
    }

    .persona-info {
      flex: 1;
      min-width: 0;
    }

    .persona-info h4 {
      font-size: 0.85rem;
      font-weight: 600;
      color: #111827;
      margin-bottom: 0.125rem;
    }

    .persona-info p {
      font-size: 0.7rem;
      color: #6b7280;
      line-height: 1.3;
      display: -webkit-box;
      -webkit-line-clamp: 2;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }

    /* Selected Persona Details */
    .selected-persona-details {
      background: #f0fdf4;
      border: 1px solid #bbf7d0;
      border-radius: 10px;
      padding: 1rem;
      margin-top: 0.5rem;
    }

    .selected-persona-details h4 {
      font-size: 0.85rem;
      font-weight: 600;
      color: #166534;
      margin-bottom: 0.5rem;
    }

    .selected-persona-details ul {
      display: flex;
      flex-direction: column;
      gap: 0.25rem;
    }

    .selected-persona-details li {
      display: flex;
      align-items: center;
      gap: 0.5rem;
      font-size: 0.8rem;
      color: #15803d;
    }

    /* Signup Actions */
    .signup-actions {
      display: flex;
      gap: 0.75rem;
      margin-top: 1rem;
    }

    .back-btn {
      display: flex;
      align-items: center;
      gap: 0.5rem;
      padding: 0.75rem 1rem;
      background: white;
      border: 1.5px solid #e5e7eb;
      border-radius: 10px;
      font-size: 0.95rem;
      font-weight: 500;
      color: #374151;
      cursor: pointer;
      transition: all 0.2s;
    }

    .back-btn:hover {
      background: #f9fafb;
      border-color: #d1d5db;
    }

    /* Auth Footer */
    .auth-footer {
      margin-top: auto;
      padding-top: 1rem;
      text-align: center;
    }

    .auth-footer p {
      font-size: 0.75rem;
      color: #9ca3af;
    }

    .auth-footer a {
      color: #0079C1;
      text-decoration: none;
    }

    .auth-footer a:hover {
      text-decoration: underline;
    }

    /* Responsive */
    @media (max-width: 768px) {
      .main-content {
        padding: 1.5rem;
        align-items: flex-start;
        padding-top: 2rem;
      }

      .auth-container {
        max-width: 100%;
      }

      .auth-panel {
        padding: 1.5rem;
      }

      .persona-grid {
        grid-template-columns: 1fr;
      }

      .auth-tabs {
        margin-bottom: 1rem;
      }

      .welcome-text {
        margin-bottom: 1rem;
      }

      .welcome-text h2 {
        font-size: 1.25rem;
      }

      .signup-actions {
        flex-direction: column;
      }

      .back-btn {
        justify-content: center;
      }
    }

    @media (max-width: 480px) {
      .main-content {
        padding: 1rem;
        padding-top: 1.5rem;
      }

      .auth-panel {
        padding: 1.25rem;
      }

      .auth-container {
        border-radius: 12px;
      }

      .form-input {
        font-size: 16px; /* Prevent zoom on iOS */
      }

      .form-extras {
        flex-direction: column;
        gap: 0.75rem;
        align-items: flex-start;
      }

      .step-connector {
        width: 40px;
      }
    }

    /* Tablet landscape and desktop */
    @media (min-width: 769px) {
      .main-content {
        padding: 3rem;
      }

      .auth-container {
        max-width: 520px;
      }
    }
  `]
})
export class LoginComponent {
  private readonly auth = inject(AuthService);

  // Tab state
  activeTab = signal<'login' | 'signup'>('login');
  signupStep = signal(1);

  // Login form fields
  loginId = '';
  password = '';
  rememberMe = false;

  // Signup form fields
  signupName = '';
  signupEmail = '';
  signupPassword = '';
  selectedPersona: Persona | null = null;

  // UI state
  showPassword = signal(false);
  showSignupPassword = signal(false);
  isLoading = signal(false);
  errorMessage = signal('');
  loginIdError = signal('');
  passwordError = signal('');

  // Personas from spec (6 official personas)
  personas = PERSONAS;

  // Mock credentials removed — authentication is now handled by DynamoDB backend (Lambda 08/10).
  // Previously used for local demo only.

  // Tab navigation
  setActiveTab(tab: 'login' | 'signup'): void {
    this.activeTab.set(tab);
    this.errorMessage.set('');
    this.signupStep.set(1);
  }

  // Password visibility toggles
  togglePasswordVisibility(): void {
    this.showPassword.update(v => !v);
  }

  toggleSignupPasswordVisibility(): void {
    this.showSignupPassword.update(v => !v);
  }

  // Password validation helpers
  hasUppercase(): boolean {
    return /[A-Z]/.test(this.signupPassword);
  }

  hasNumber(): boolean {
    return /[0-9]/.test(this.signupPassword);
  }

  // Signup step 1 validation
  isSignupStep1Valid(): boolean {
    return this.signupName.trim().length > 0 &&
           this.signupEmail.includes('@') &&
           this.signupPassword.length >= 8 &&
           this.hasUppercase() &&
           this.hasNumber();
  }

  // Signup navigation
  nextSignupStep(): void {
    if (this.isSignupStep1Valid()) {
      this.signupStep.set(2);
    }
  }

  prevSignupStep(): void {
    this.signupStep.set(1);
  }

  // Persona selection
  selectPersona(persona: Persona): void {
    this.selectedPersona = persona;
  }

  getPersonaColor(persona: Persona): string {
    const colorMap: Record<string, string> = {
      blue: '#0079C1',
      purple: '#6366f1',
      orange: '#f59e0b',
      green: '#10b981',
      red: '#ef4444',
      yellow: '#00a9e0'
    };
    return colorMap[persona.color] || '#0079C1';
  }

  // Complete signup — calls DynamoDB auth backend, falls back to mock in dev
  completeSignup(): void {
    if (!this.selectedPersona) {
      this.errorMessage.set('Please select a persona');
      return;
    }

    this.isLoading.set(true);
    this.errorMessage.set('');

    this.auth.register(
      this.signupName.trim(),
      this.signupEmail.trim().toLowerCase(),
      this.signupPassword,
      this.selectedPersona.apiPersona
    ).subscribe({
      next: () => { /* AuthService navigates to / on success */ },
      error: () => {
        // Backend unreachable — fall back to local auth in dev mode
        if (!environment.production) {
          console.warn('[Auth] Backend unreachable, using local auth');
          const result = this.auth.localRegister(
            this.signupName.trim(),
            this.signupEmail.trim().toLowerCase(),
            this.signupPassword,
            this.selectedPersona!
          );
          if (!result.success) {
            this.errorMessage.set(result.error || 'Registration failed.');
          }
          this.isLoading.set(false);
        } else {
          this.errorMessage.set('Failed to create account. Please try again.');
          this.isLoading.set(false);
        }
      }
    });
  }

  // Login with SSO (disabled — not currently using Azure AD / Cognito)
  loginWithSSO(): void {
    // SSO is disabled. Uncomment and configure Azure AD / Cognito to re-enable.
    // this.auth.login();
    this.errorMessage.set('Single Sign-On is not currently enabled.');
  }

  // Login form validation
  validateLoginForm(): boolean {
    let isValid = true;
    this.loginIdError.set('');
    this.passwordError.set('');

    if (!this.loginId.trim() || !this.loginId.includes('@')) {
      this.loginIdError.set('A valid email address is required');
      isValid = false;
    }

    if (!this.password) {
      this.passwordError.set('Password is required');
      isValid = false;
    }

    return isValid;
  }

  // Login form submission — calls DynamoDB auth backend, falls back to mock in dev
  onSubmit(): void {
    this.errorMessage.set('');

    if (!this.validateLoginForm()) {
      return;
    }

    this.isLoading.set(true);

    this.auth.loginWithCredentials(this.loginId.trim().toLowerCase(), this.password).subscribe({
      next: () => { /* AuthService navigates to / on success */ },
      error: () => {
        // Backend unreachable — fall back to local auth in dev mode
        if (!environment.production) {
          console.warn('[Auth] Backend unreachable, using local auth');
          const result = this.auth.localLogin(
            this.loginId.trim().toLowerCase(),
            this.password
          );
          if (!result.success) {
            this.errorMessage.set(result.error || 'Login failed.');
          }
          this.isLoading.set(false);
        } else {
          this.errorMessage.set('Invalid email or password. Please try again.');
          this.isLoading.set(false);
        }
      }
    });
  }
}

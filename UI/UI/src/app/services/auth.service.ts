import { Injectable, signal, computed, inject } from '@angular/core';
import { Router } from '@angular/router';
import { Observable, tap, catchError, throwError } from 'rxjs';
import { ApiService } from './api.service';
import { AuthUser, LoginRequest, LoginResponse, UserRole } from '../models';

const TOKEN_KEY      = 'wap_token';
const REFRESH_KEY    = 'wap_refresh';
const USER_KEY       = 'wap_user';
const SESSION_ID_KEY = 'wap_session_id';

// We use sessionStorage (not localStorage) so closing the browser tab/window
// clears the session and the next visit lands on /login. Requested by the
// team for demos — they don't want the app silently auto-resuming into the
// dashboard from a stale token.
//
// ⚠️ sessionStorage alone is NOT enough in Chrome: the "Continue where you
// left off" startup setting and tab-restore (Cmd+Shift+T) both preserve
// sessionStorage across a full browser close. To truly force re-login on
// every fresh browser launch we pair sessionStorage with window.name:
//
//   - on login we mint a random session id, writing it to BOTH sessionStorage
//     AND window.name.
//   - on app boot we compare the two. If sessionStorage has a user but
//     window.name is empty or mismatched, the tab was restored / freshly
//     opened — we clear auth and the guard bounces to /login.
//
// window.name is a per-tab scratch string that Chrome does NOT repopulate on
// session restore, so a mismatch is a reliable "this is a fresh tab" signal.

@Injectable({ providedIn: 'root' })
export class AuthService {
  private api    = inject(ApiService);
  private router = inject(Router);

  // ── State ──────────────────────────────────────────────────────────
  private _user = signal<AuthUser | null>(this.loadUser());
  readonly user    = this._user.asReadonly();
  readonly isLoggedIn = computed(() => this._user() !== null);
  // With the 'admin' user role retired, admin privileges belong exclusively
  // to super_admin. isAdmin and isSuperAdmin are now equivalent — we keep
  // both names so existing call-sites (guards, templates) still work.
  readonly isAdmin    = computed(() => this._user()?.role === 'super_admin');
  readonly isSuperAdmin = computed(() => this._user()?.role === 'super_admin');

  // ── Public API ─────────────────────────────────────────────────────
  login(req: LoginRequest): Observable<LoginResponse> {
    return this.api.post<LoginResponse>('/auth/login', req).pipe(
      tap(res => this.persist(res)),
      catchError(err => throwError(() => err))
    );
  }

  /**
   * Request a password reset. Backend generates a temporary 12-char password
   * and returns it inline — no email is sent. The caller (login page) shows
   * the temp password in a modal with a Copy button.
   */
  forgotPassword(email: string): Observable<{ email: string; temp_password: string; message: string }> {
    return this.api.post<{ email: string; temp_password: string; message: string }>(
      '/auth/forgot-password',
      { email }
    );
  }

  logout(): void {
    // Best-effort server-side revoke
    const token = sessionStorage.getItem(TOKEN_KEY);
    if (token) {
      this.api.post('/auth/logout', {}).subscribe({ error: () => {} });
    }
    this.clear();
    this.router.navigate(['/login']);
  }

  /** Call on app start — validates stored token against server */
  validateSession(): Observable<AuthUser> {
    return this.api.get<AuthUser>('/auth/me').pipe(
      tap(user => {
        this._user.set(user);
        sessionStorage.setItem(USER_KEY, JSON.stringify(user));
      }),
      catchError(err => {
        this.clear();
        return throwError(() => err);
      })
    );
  }

  /** Swap old token for a new one */
  refreshToken(): Observable<LoginResponse> {
    const refreshToken = sessionStorage.getItem(REFRESH_KEY) ?? '';
    return this.api.post<LoginResponse>('/auth/refresh', { refreshToken }).pipe(
      tap(res => this.persist(res))
    );
  }

  getToken(): string | null {
    return sessionStorage.getItem(TOKEN_KEY);
  }

  hasRole(role: UserRole): boolean {
    return this._user()?.role === role;
  }

  hasClientAccess(clientId: string): boolean {
    const access = this._user()?.clientAccess ?? [];
    return access.includes('*') || access.includes(clientId);
  }

  /**
   * Get the current user's active client_id.
   *
   * Returns an empty string if no valid client is resolvable. Never falls
   * back to another tenant's ID — that would silently leak cross-tenant
   * data. Callers should treat '' as "no client selected" and let the
   * backend reject the request (or redirect to /login / client picker).
   *
   * - No user logged in                    → '' (caller should redirect)
   * - User has exactly 1 client            → that client
   * - User has multiple clients            → selected client (sessionStorage)
   *                                          or first in list
   * - Super admin with wildcard '*'        → selected client (sessionStorage)
   *                                          or '' (must pick via selector)
   */
  getClientId(): string {
    const user = this._user();
    if (!user) return '';

    const access = user.clientAccess ?? [];
    if (access.length === 0) return '';

    const selected = sessionStorage.getItem('wap_selected_client') ?? '';

    // Super admin: requires an explicit selection — no default tenant.
    if (access.includes('*')) {
      return selected;
    }

    // Regular user with multiple clients: honor their selection if it's in scope.
    if (access.length > 1 && selected && access.includes(selected)) {
      return selected;
    }

    return access[0];
  }

  /** Set the active client for users with access to more than one (or '*'). */
  setClientId(clientId: string): void {
    const access = this._user()?.clientAccess ?? [];
    if (!access.includes('*') && !access.includes(clientId)) {
      throw new Error(`User does not have access to ${clientId}`);
    }
    sessionStorage.setItem('wap_selected_client', clientId);
  }

  /** Get the client name for display (from user metadata or fallback) */
  getClientName(): string {
    const id = this.getClientId();
    if (!id) return '—';
    // Map of known client names — this will come from backend later
    const names: Record<string, string> = {
      'CLT-001': 'Walmart Inc.',
      'CLT-002': 'Costco Wholesale',
    };
    return names[id] ?? `Client ${id}`;
  }

  // ── Helpers ────────────────────────────────────────────────────────
  private persist(res: LoginResponse): void {
    // Mint a per-tab session id and stamp BOTH storages. window.name is the
    // "tab is still alive" marker that Chrome does not restore on reopen,
    // so on next boot we can tell active-tab from restored-tab apart.
    const sessionId = this.newSessionId();
    sessionStorage.setItem(TOKEN_KEY,      res.token);
    sessionStorage.setItem(REFRESH_KEY,    res.refreshToken);
    sessionStorage.setItem(USER_KEY,       JSON.stringify(res.user));
    sessionStorage.setItem(SESSION_ID_KEY, sessionId);
    window.name = sessionId;
    this._user.set(res.user);
  }

  private clear(): void {
    sessionStorage.removeItem(TOKEN_KEY);
    sessionStorage.removeItem(REFRESH_KEY);
    sessionStorage.removeItem(USER_KEY);
    sessionStorage.removeItem(SESSION_ID_KEY);
    sessionStorage.removeItem('wap_selected_client');
    window.name = '';
    this._user.set(null);
  }

  private loadUser(): AuthUser | null {
    // One-time cleanup: if a previous build left tokens in localStorage,
    // wipe them so we don't auto-resume from stale credentials. Safe to
    // remove this block after one or two deploys.
    if (localStorage.getItem(TOKEN_KEY) || localStorage.getItem(USER_KEY)) {
      localStorage.removeItem(TOKEN_KEY);
      localStorage.removeItem(REFRESH_KEY);
      localStorage.removeItem(USER_KEY);
      localStorage.removeItem('wap_selected_client');
    }

    // Fresh-browser-launch guard: if sessionStorage has a session id (because
    // Chrome restored it) but window.name doesn't match, this tab was NOT
    // the one that logged in — force a clean re-login instead of silently
    // resuming into the dashboard.
    const storedSessionId = sessionStorage.getItem(SESSION_ID_KEY);
    if (storedSessionId && window.name !== storedSessionId) {
      sessionStorage.removeItem(TOKEN_KEY);
      sessionStorage.removeItem(REFRESH_KEY);
      sessionStorage.removeItem(USER_KEY);
      sessionStorage.removeItem(SESSION_ID_KEY);
      sessionStorage.removeItem('wap_selected_client');
      return null;
    }

    try {
      const raw = sessionStorage.getItem(USER_KEY);
      return raw ? JSON.parse(raw) : null;
    } catch { return null; }
  }

  private newSessionId(): string {
    // crypto.randomUUID is available in all modern browsers; fall back to a
    // Math.random string just in case the app is opened in a very old one.
    try {
      return crypto.randomUUID();
    } catch {
      return 'wap-' + Math.random().toString(36).slice(2) + Date.now().toString(36);
    }
  }
}

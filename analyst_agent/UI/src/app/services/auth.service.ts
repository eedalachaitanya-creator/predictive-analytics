import { Injectable, signal, computed, inject } from '@angular/core';
import { Router } from '@angular/router';
import { Observable, tap, catchError, throwError } from 'rxjs';
import { ApiService } from './api.service';
import { AuthUser, LoginRequest, LoginResponse, UserRole } from '../models';

const TOKEN_KEY   = 'wap_token';
const REFRESH_KEY = 'wap_refresh';
const USER_KEY    = 'wap_user';

@Injectable({ providedIn: 'root' })
export class AuthService {
  private api    = inject(ApiService);
  private router = inject(Router);

  // ── State ──────────────────────────────────────────────────────────
  private _user = signal<AuthUser | null>(this.loadUser());
  readonly user    = this._user.asReadonly();
  readonly isLoggedIn = computed(() => this._user() !== null);
  readonly isAdmin    = computed(() =>
    ['super_admin', 'admin'].includes(this._user()?.role ?? '')
  );
  readonly isSuperAdmin = computed(() => this._user()?.role === 'super_admin');

  // ── Public API ─────────────────────────────────────────────────────
  login(req: LoginRequest): Observable<LoginResponse> {
    return this.api.post<LoginResponse>('/auth/login', req).pipe(
      tap(res => this.persist(res)),
      catchError(err => throwError(() => err))
    );
  }

  logout(): void {
    // Best-effort server-side revoke
    const token = localStorage.getItem(TOKEN_KEY);
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
        localStorage.setItem(USER_KEY, JSON.stringify(user));
      }),
      catchError(err => {
        this.clear();
        return throwError(() => err);
      })
    );
  }

  /** Swap old token for a new one */
  refreshToken(): Observable<LoginResponse> {
    const refreshToken = localStorage.getItem(REFRESH_KEY) ?? '';
    return this.api.post<LoginResponse>('/auth/refresh', { refreshToken }).pipe(
      tap(res => this.persist(res))
    );
  }

  getToken(): string | null {
    return localStorage.getItem(TOKEN_KEY);
  }

  hasRole(role: UserRole): boolean {
    return this._user()?.role === role;
  }

  hasClientAccess(clientId: string): boolean {
    const access = this._user()?.clientAccess ?? [];
    return access.includes('*') || access.includes(clientId);
  }

  // ── Helpers ────────────────────────────────────────────────────────
  private persist(res: LoginResponse): void {
    localStorage.setItem(TOKEN_KEY,   res.token);
    localStorage.setItem(REFRESH_KEY, res.refreshToken);
    localStorage.setItem(USER_KEY,    JSON.stringify(res.user));
    this._user.set(res.user);
  }

  private clear(): void {
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(REFRESH_KEY);
    localStorage.removeItem(USER_KEY);
    this._user.set(null);
  }

  private loadUser(): AuthUser | null {
    try {
      const raw = localStorage.getItem(USER_KEY);
      return raw ? JSON.parse(raw) : null;
    } catch { return null; }
  }
}

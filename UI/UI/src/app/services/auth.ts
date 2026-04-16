import { Injectable, signal } from '@angular/core';

export type UserRole = 'admin' | 'client';
export interface AuthUser { email: string; role: UserRole; name: string; }

@Injectable({ providedIn: 'root' })
export class AuthService {
  private _user = signal<AuthUser | null>(null);
  readonly user = this._user.asReadonly();

  login(email: string, role: UserRole): void {
    this._user.set({ email, role, name: role === 'admin' ? 'Admin' : 'Walmart Ops' });
  }

  logout(): void {
    this._user.set(null);
  }

  isLoggedIn(): boolean {
    return this._user() !== null;
  }

  isAdmin(): boolean {
    return this._user()?.role === 'admin';
  }
}

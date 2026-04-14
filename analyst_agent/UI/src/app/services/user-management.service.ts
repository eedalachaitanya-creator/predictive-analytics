import { Injectable, inject, signal } from '@angular/core';
import { Observable, tap } from 'rxjs';
import { ApiService } from './api.service';
import { AppUser, CreateUserRequest } from '../models';

@Injectable({ providedIn: 'root' })
export class UserManagementService {
  private api = inject(ApiService);

  readonly users   = signal<AppUser[]>([]);
  readonly loading = signal(false);
  readonly error   = signal<string | null>(null);

  loadUsers(): Observable<AppUser[]> {
    this.loading.set(true);
    return this.api.get<AppUser[]>('/users').pipe(
      tap({
        next:  users => { this.users.set(users); this.loading.set(false); },
        error: e     => { this.error.set(e.message); this.loading.set(false); }
      })
    );
  }

  createUser(req: CreateUserRequest): Observable<AppUser> {
    return this.api.post<AppUser>('/users', req).pipe(
      tap(user => this.users.update(list => [...list, user]))
    );
  }

  updateUser(id: string, changes: Partial<AppUser>): Observable<AppUser> {
    return this.api.put<AppUser>(`/users/${id}`, changes).pipe(
      tap(updated => this.users.update(list => list.map(u => u.id === id ? updated : u)))
    );
  }

  deleteUser(id: string): Observable<void> {
    return this.api.delete<void>(`/users/${id}`).pipe(
      tap(() => this.users.update(list => list.filter(u => u.id !== id)))
    );
  }

  toggleStatus(id: string, status: 'active' | 'inactive'): Observable<AppUser> {
    return this.updateUser(id, { status });
  }
}

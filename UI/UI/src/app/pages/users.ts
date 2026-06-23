import { Component, OnInit, inject, signal, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { UserManagementService } from '../services/user-management.service';
import { AuthService } from '../services/auth.service';
import { ApiService } from '../services/api.service';
import { AppUser, UserRole, CreateUserRequest } from '../models';

@Component({
  selector: 'app-users',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './users.html',
  styleUrls: ['./users.scss']
})
export class UsersComponent implements OnInit {
  svc = inject(UserManagementService);
  private auth = inject(AuthService);
  private api = inject(ApiService);

  // The currently signed-in user — used to forbid changing your OWN status
  // (a super admin who deactivates themselves is locked out at next login).
  private currentUserId = this.auth.user()?.id ?? null;
  toggleError = signal('');

  isSelf(u: AppUser): boolean { return !!this.currentUserId && u.id === this.currentUserId; }

  // The "+ Add New User" modal has been retired — new users are created by
  // the "+ Add New Client" flow on the Clients page (which provisions a
  // client_config row AND its first user in one transaction). This page is
  // now a read-only roster + status toggle.
  //
  // The per-row hard-delete (DELETE FROM users) was removed 2026-04-25
  // because it created drift between Users and Clients pages: a deleted
  // user vanished here but the corresponding client_config row stayed on
  // the Clients page, with no way to restore the (gone) user via the
  // Clients-page reactivate flow. Tenant offboarding now happens on the
  // Clients page via soft-delete (is_active flag); the user account
  // follows the client's active state through auth_router's login gate.
  // The backend DELETE /users/{id} endpoint still exists for one-off
  // admin scripts but no UI calls it anymore.

  // Stats
  superAdmins = computed(() => this.svc.users().filter(u => u.role === 'super_admin').length);
  clientUsers = computed(() => this.svc.users().filter(u => u.role === 'client_user').length);

  // ── Add-user modal ─────────────────────────────────────────────────
  // Restores the ability to add MORE logins to a client (each client was
  // provisioned with exactly one login at onboarding). Backend: POST /users.
  clients = signal<{ client_id: string; client_name: string }[]>([]);
  addOpen     = signal(false);
  addName     = signal('');
  addEmail    = signal('');
  addPassword = signal('');
  addRole     = signal<UserRole>('client_user');
  addClient   = signal('');
  addSaving   = signal(false);
  addError    = signal('');

  ngOnInit() {
    this.svc.loadUsers().subscribe({ error: () => {} });
    // client list for the access dropdown (active clients only)
    this.api.get<any[]>('/clients').subscribe({
      next: (cs) => this.clients.set(
        (cs || []).map(c => ({ client_id: c.client_id, client_name: c.client_name }))),
      error: () => {},
    });
  }

  openAdd() {
    this.addName.set(''); this.addEmail.set(''); this.addPassword.set('');
    this.addRole.set('client_user'); this.addClient.set('');
    this.addError.set(''); this.addSaving.set(false);
    this.addOpen.set(true);
  }

  closeAdd() { this.addOpen.set(false); }

  submitAdd() {
    const name = this.addName().trim();
    const email = this.addEmail().trim();
    const password = this.addPassword();
    const role = this.addRole();
    if (!name || !email || !password) {
      this.addError.set('Name, email and a temporary password are required.'); return;
    }
    if (role === 'client_user' && !this.addClient()) {
      this.addError.set('Select the client this login belongs to.'); return;
    }
    const clientAccess = role === 'super_admin' ? ['*'] : [this.addClient()];
    const req: CreateUserRequest = { name, email, password, role, clientAccess };
    this.addSaving.set(true); this.addError.set('');
    this.svc.createUser(req).subscribe({
      next: () => { this.addSaving.set(false); this.closeAdd(); },
      error: (e) => {
        this.addSaving.set(false);
        this.addError.set(e?.error?.detail ?? e?.message ?? 'Could not create the user.');
      },
    });
  }

  toggleStatus(u: AppUser) {
    // Never let an admin deactivate their own account (backend also blocks it,
    // but stop the click here so it can't even be attempted).
    if (this.isSelf(u)) {
      this.toggleError.set('You cannot change your own account status.');
      return;
    }
    this.toggleError.set('');
    const next = u.status === 'active' ? 'inactive' : 'active';
    this.svc.toggleStatus(u.id, next).subscribe({
      error: (e) => this.toggleError.set(
        e?.error?.detail ?? e?.message ?? `Could not update ${u.email}.`),
    });
  }

  roleColor(r: string) {
    if (r === 'super_admin') return 'purple';
    return 'gray';
  }

  statusDot(s: string) {
    if (s === 'active')   return 'green';
    if (s === 'inactive') return 'yellow';
    return 'red';
  }

  accessLabel(u: AppUser): string {
    if (u.clientAccess.includes('*')) return 'All Clients';
    return u.clientAccess.join(', ') || '—';
  }

  roleLabel(r: UserRole): string {
    const map: Record<string,string> = { super_admin:'Super Admin', client_user:'Client User' };
    return map[r] ?? r;
  }
  formatLogin(iso: string | null | undefined): string {
    if (!iso) return '—';
    try {
      return new Date(iso).toLocaleString('en-GB', {
        day: '2-digit', month: 'short', year: 'numeric',
        hour: '2-digit', minute: '2-digit', hour12: false
      });
    } catch {
      return iso;
    }
  }
}

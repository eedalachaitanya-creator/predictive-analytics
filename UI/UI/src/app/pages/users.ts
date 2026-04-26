import { Component, OnInit, inject, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { UserManagementService } from '../services/user-management.service';
import { AppUser, UserRole } from '../models';

@Component({
  selector: 'app-users',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './users.html',
  styleUrls: ['./users.scss']
})
export class UsersComponent implements OnInit {
  svc = inject(UserManagementService);

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

  ngOnInit() {
    this.svc.loadUsers().subscribe({ error: () => {} });
  }

  toggleStatus(u: AppUser) {
    const next = u.status === 'active' ? 'inactive' : 'active';
    this.svc.toggleStatus(u.id, next).subscribe({ error: () => {} });
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
}

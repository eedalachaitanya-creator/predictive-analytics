import { Component, OnInit, signal, inject, computed } from '@angular/core';
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
  // client_config row AND its first user in one transaction). This page
  // now only shows the roster + lets a super admin toggle status or delete.
  // deleteConfirmId drives the two-click delete pattern below.
  deleteConfirmId = signal<string | null>(null);

  // Stats
  superAdmins = computed(() => this.svc.users().filter(u => u.role === 'super_admin').length);
  clientUsers = computed(() => this.svc.users().filter(u => u.role === 'client_user').length);

  ngOnInit() {
    this.svc.loadUsers().subscribe({ error: () => {} });
  }

  confirmDelete(id: string) { this.deleteConfirmId.set(id); }
  cancelDelete()             { this.deleteConfirmId.set(null); }

  deleteUser(id: string) {
    this.svc.deleteUser(id).subscribe({
      next: () => this.deleteConfirmId.set(null),
      error: e  => console.error(e)
    });
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

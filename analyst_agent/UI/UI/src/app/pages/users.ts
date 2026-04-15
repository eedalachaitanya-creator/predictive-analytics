import { Component, OnInit, signal, inject, computed } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { UserManagementService } from '../services/user-management.service';
import { ApiService } from '../services/api.service';
import { AppUser, CreateUserRequest, UserRole } from '../models';

@Component({
  selector: 'app-users',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './users.html',
  styleUrls: ['./users.scss']
})
export class UsersComponent implements OnInit {
  svc = inject(UserManagementService);
  private api = inject(ApiService);

  // Add User modal
  showModal  = signal(false);
  saving     = signal(false);
  modalError = signal('');
  deleteConfirmId = signal<string | null>(null);

  // Form fields
  form = signal<CreateUserRequest>({
    name: '', email: '', password: '', role: 'client_user', clientAccess: []
  });

  clientOptions = signal<{id: string; name: string}[]>([]);

  roles: { value: UserRole; label: string }[] = [
    { value: 'super_admin', label: 'Super Admin' },
    { value: 'admin',       label: 'Admin' },
    { value: 'client_user', label: 'Client User' },
    { value: 'viewer',      label: 'Viewer' },
  ];

  perms = [
    { perm:'Add / remove clients',     sa:true,  a:false, cu:false, v:false },
    { perm:'Manage all users',         sa:true,  a:true,  cu:false, v:false },
    { perm:'Upload data files',        sa:true,  a:true,  cu:true,  v:false },
    { perm:'Run pipeline',             sa:true,  a:true,  cu:true,  v:false },
    { perm:'View dashboard & reports', sa:true,  a:true,  cu:true,  v:true  },
    { perm:'Edit Settings & Config',   sa:true,  a:true,  cu:true,  v:false },
    { perm:'Download reports',         sa:true,  a:true,  cu:true,  v:false },
    { perm:'View audit log',           sa:true,  a:true,  cu:false, v:false },
    { perm:'System configuration',     sa:true,  a:false, cu:false, v:false },
  ];

  // Stats
  superAdmins = computed(() => this.svc.users().filter(u => u.role === 'super_admin').length);
  admins      = computed(() => this.svc.users().filter(u => u.role === 'admin').length);
  clientUsers = computed(() => this.svc.users().filter(u => u.role === 'client_user').length);

  ngOnInit() {
    this.svc.loadUsers().subscribe({ error: () => {} });
    // Load real client list from database for the dropdown
    this.api.get<{client_id: string; client_name: string}[]>('/clients').subscribe({
      next: (clients) => {
        this.clientOptions.set(clients.map(c => ({ id: c.client_id, name: c.client_name })));
      },
      error: () => {}
    });
  }

  openModal() {
    this.form.set({ name:'', email:'', password:'', role:'client_user', clientAccess:[] });
    this.modalError.set('');
    this.showModal.set(true);
  }

  closeModal() { this.showModal.set(false); }

  updateField(field: keyof CreateUserRequest, value: unknown) {
    this.form.update(f => ({ ...f, [field]: value }));
  }

  toggleClientAccess(id: string) {
    this.form.update(f => {
      const access = f.clientAccess.includes(id)
        ? f.clientAccess.filter(c => c !== id)
        : [...f.clientAccess, id];
      return { ...f, clientAccess: access };
    });
  }

  saveUser() {
    const f = this.form();
    if (!f.name || !f.email || !f.password) {
      this.modalError.set('Name, email and password are required.');
      return;
    }
    this.saving.set(true);
    this.modalError.set('');
    this.svc.createUser(f).subscribe({
      next: () => { this.saving.set(false); this.showModal.set(false); },
      error: e  => { this.saving.set(false); this.modalError.set(e.message ?? 'Failed to create user.'); }
    });
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
    if (r === 'admin')       return 'blue';
    if (r === 'viewer')      return 'cyan';
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
    const map: Record<string,string> = { super_admin:'Super Admin', admin:'Admin', client_user:'Client User', viewer:'Viewer' };
    return map[r] ?? r;
  }
}

import { Component, signal } from '@angular/core';
import { Router } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { AuthService } from '../services/auth.service';
import { environment } from '../../environments/environment';

@Component({
  selector: 'app-login',
  standalone: true,
  imports: [FormsModule, CommonModule],
  templateUrl: './login.html',
  styleUrls: ['./login.scss']
})
export class LoginComponent {
  role     = signal<'admin' | 'client'>('admin');
  email    = signal('admin@walmart.com');
  // In mock mode pre-fill password so users can click Sign In immediately
  password = signal(environment.useMocks ? 'demo1234' : '');
  loading  = signal(false);
  error    = signal('');
  isMock   = environment.useMocks;

  constructor(private auth: AuthService, private router: Router) {}

  selectRole(r: 'admin' | 'client') {
    this.role.set(r);
    this.email.set(r === 'admin' ? 'admin@walmart.com' : 'ops@walmart.com');
    if (this.isMock) this.password.set('demo1234');
  }

  login() {
    this.error.set('');
    if (!this.email().trim()) {
      this.error.set('Please enter your email address.');
      return;
    }
    if (!this.password().trim()) {
      this.error.set('Please enter your password.');
      return;
    }

    this.loading.set(true);

    this.auth.login({ email: this.email(), password: this.password() }).subscribe({
      next: () => {
        this.loading.set(false);
        this.router.navigate(['/app/upload']);
      },
      error: (err) => {
        this.loading.set(false);
        this.error.set(err?.error?.message ?? err?.message ?? 'Login failed. Please check your credentials.');
      }
    });
  }
}

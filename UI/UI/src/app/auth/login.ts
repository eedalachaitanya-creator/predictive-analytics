import { Component, signal } from '@angular/core';
import { Router, RouterLink } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { AuthService } from '../services/auth.service';
import { environment } from '../../environments/environment';

@Component({
  selector: 'app-login',
  standalone: true,
  imports: [FormsModule, CommonModule, RouterLink],
  templateUrl: './login.html',
  styleUrls: ['./login.scss']
})
export class LoginComponent {
  role     = signal<'admin' | 'client'>('admin');
  email    = signal('');
  password = signal(environment.useMocks ? 'demo1234' : '');
  loading  = signal(false);
  error    = signal('');
  isMock   = environment.useMocks;

  constructor(private auth: AuthService, private router: Router) {}

  selectRole(r: 'admin' | 'client') {
    this.role.set(r);
    // Clear fields so user types their own credentials
    this.email.set('');
    this.password.set(this.isMock ? 'demo1234' : '');
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

    this.auth.login({ email: this.email(), password: this.password(), loginRole: this.role() }).subscribe({
      next: () => {
        this.loading.set(false);
        // Admins go to admin console, clients go to upload page
        if (this.auth.isAdmin()) {
          this.router.navigate(['/app/clients']);
        } else {
          this.router.navigate(['/app/upload']);
        }
      },
      error: (err) => {
        this.loading.set(false);
        this.error.set(err?.error?.message ?? err?.message ?? 'Login failed. Please check your credentials.');
      }
    });
  }
}

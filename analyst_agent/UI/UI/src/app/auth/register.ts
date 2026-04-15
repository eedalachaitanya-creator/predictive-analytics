import { Component, signal, inject } from '@angular/core';
import { Router, RouterLink } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { ApiService } from '../services/api.service';

@Component({
  selector: 'app-register',
  standalone: true,
  imports: [FormsModule, CommonModule, RouterLink],
  templateUrl: './register.html',
  styleUrls: ['./login.scss']
})
export class RegisterComponent {
  private api = inject(ApiService);
  private router = inject(Router);

  // Form fields
  companyName = signal('');
  companyCode = signal('');
  contactName = signal('');
  contactEmail = signal('');
  password = signal('');
  confirmPassword = signal('');

  // State
  loading = signal(false);
  error = signal('');
  success = signal(false);
  newClientId = signal('');

  register() {
    this.error.set('');

    // Validation
    if (!this.companyName().trim()) {
      this.error.set('Please enter your company name.');
      return;
    }
    if (!this.companyCode().trim()) {
      this.error.set('Please enter a short company code (e.g., COSTCO, TARGET).');
      return;
    }
    if (this.companyCode().length > 10) {
      this.error.set('Company code must be 10 characters or less.');
      return;
    }
    if (!this.contactName().trim()) {
      this.error.set('Please enter your full name.');
      return;
    }
    if (!this.contactEmail().trim()) {
      this.error.set('Please enter your email address.');
      return;
    }
    if (!this.password().trim()) {
      this.error.set('Please create a password.');
      return;
    }
    if (this.password().length < 6) {
      this.error.set('Password must be at least 6 characters.');
      return;
    }
    if (this.password() !== this.confirmPassword()) {
      this.error.set('Passwords do not match.');
      return;
    }

    this.loading.set(true);

    const body = {
      client_name: this.companyName(),
      client_code: this.companyCode().toUpperCase(),
      contact_name: this.contactName(),
      contact_email: this.contactEmail(),
      password: this.password(),
    };

    this.api.post<any>('/clients/self-register', body).subscribe({
      next: (res) => {
        this.loading.set(false);
        this.success.set(true);
        this.newClientId.set(res.client_id);
      },
      error: (err) => {
        this.loading.set(false);
        this.error.set(
          err?.error?.detail ?? err?.message ?? 'Registration failed. Please try again.'
        );
      }
    });
  }
}

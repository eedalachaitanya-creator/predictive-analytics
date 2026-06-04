import { Component, signal, computed, inject } from '@angular/core';
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
  private api    = inject(ApiService);
  private router = inject(Router);

  companyName     = signal('');
  companyCode     = signal('');
  contactName     = signal('');
  contactEmail    = signal('');
  password        = signal('');
  confirmPassword = signal('');

  loading      = signal(false);
  error        = signal('');
  success      = signal(false);
  newClientId  = signal('');
  showPassword = signal(false);
  showConfirm  = signal(false);

  emailTouched   = signal(false);
  passTouched    = signal(false);
  confirmTouched = signal(false);

  private readonly EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/;

  rules = computed(() => {
    const p = this.password();
    return {
      minLength:  p.length >= 8,
      hasUpper:   /[A-Z]/.test(p),
      hasLower:   /[a-z]/.test(p),
      hasNumber:  /\d/.test(p),
      hasSpecial: /[^A-Za-z0-9]/.test(p),
    };
  });

  allRulesPass = computed(() => Object.values(this.rules()).every(Boolean));

  emailError = computed(() => {
    if (!this.emailTouched()) return '';
    const v = this.contactEmail().trim();
    if (!v) return 'Email address is required.';
    if (!this.EMAIL_RE.test(v)) return 'Please enter a valid email address.';
    return '';
  });

  passError = computed(() => {
    if (!this.passTouched()) return '';
    if (!this.password()) return 'Password is required.';
    if (!this.allRulesPass()) return 'Password does not meet all requirements below.';
    return '';
  });

  confirmError = computed(() => {
    if (!this.confirmTouched()) return '';
    if (!this.confirmPassword()) return 'Please confirm your password.';
    if (this.password() !== this.confirmPassword()) return 'Passwords do not match.';
    return '';
  });

  register() {
    this.error.set('');
    this.emailTouched.set(true);
    this.passTouched.set(true);
    this.confirmTouched.set(true);

    if (!this.companyName().trim()) {
      this.error.set('Please enter your company name.'); return;
    }
    if (!this.companyCode().trim()) {
      this.error.set('Please enter a company code (e.g. COSTCO, TARGET).'); return;
    }
    if (this.companyCode().length > 10) {
      this.error.set('Company code must be 10 characters or less.'); return;
    }
    if (!this.contactName().trim()) {
      this.error.set('Please enter your full name.'); return;
    }
    if (!this.contactEmail().trim()) {
      this.error.set('Please enter your email address.'); return;
    }
    if (!this.EMAIL_RE.test(this.contactEmail().trim())) {
      this.error.set('Please enter a valid email address (e.g. user@company.com).'); return;
    }
    if (!this.password()) {
      this.error.set('Please create a password.'); return;
    }
    if (!this.allRulesPass()) {
      this.error.set('Your password does not meet all the requirements. Please check the rules below.'); return;
    }
    if (this.password() !== this.confirmPassword()) {
      this.error.set('Passwords do not match.'); return;
    }

    this.loading.set(true);

    const body = {
      client_name:   this.companyName(),
      client_code:   this.companyCode().toUpperCase(),
      contact_name:  this.contactName(),
      contact_email: this.contactEmail().trim(),
      password:      this.password(),
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
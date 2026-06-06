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

  // Field values
  companyName     = signal('');
  companyCode     = signal('');
  contactName     = signal('');
  contactEmail    = signal('');
  password        = signal('');
  confirmPassword = signal('');

  // UI state
  loading      = signal(false);
  error        = signal('');
  success      = signal(false);
  newClientId  = signal('');
  showPassword = signal(false);
  showConfirm  = signal(false);

  // Touched flags — one per field
  companyNameTouched = signal(false);
  companyCodeTouched = signal(false);
  contactNameTouched = signal(false);
  emailTouched       = signal(false);
  passTouched        = signal(false);
  confirmTouched     = signal(false);

  private readonly EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]{3,}$/;
  private readonly CODE_RE  = /^[A-Za-z0-9]+$/;

  // Password rules
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

  // Inline errors — only show after field is touched
  companyNameError = computed(() => {
    if (!this.companyNameTouched()) return '';
    if (!this.companyName().trim()) return 'Company name is required.';
    return '';
  });

  companyCodeError = computed(() => {
    if (!this.companyCodeTouched()) return '';
    const code = this.companyCode().trim();
    if (!code) return 'Company code is required (e.g. COSTCO, TARGET).';
    if (code.length > 10) return 'Company code must be 10 characters or less.';
    if (!this.CODE_RE.test(code)) return 'Letters and numbers only — no spaces or special characters.';
    return '';
  });

  contactNameError = computed(() => {
    if (!this.contactNameTouched()) return '';
    if (!this.contactName().trim()) return 'Full name is required.';
    return '';
  });

  emailError = computed(() => {
    if (!this.emailTouched()) return '';
    const v = this.contactEmail().trim();
    if (!v) return 'Email address is required.';
    if (!this.EMAIL_RE.test(v)) return 'Please enter a valid email (e.g. john@costco.com).';
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

  // Check if form is fully valid
  private formValid(): boolean {
    return (
      !!this.companyName().trim() &&
      !!this.companyCode().trim() &&
      this.companyCode().length <= 10 &&
      this.CODE_RE.test(this.companyCode()) &&
      !!this.contactName().trim() &&
      !!this.contactEmail().trim() &&
      this.EMAIL_RE.test(this.contactEmail().trim()) &&
      !!this.password() &&
      this.allRulesPass() &&
      this.password() === this.confirmPassword()
    );
  }

  register() {
    // Touch all fields so all inline errors appear at once
    this.companyNameTouched.set(true);
    this.companyCodeTouched.set(true);
    this.contactNameTouched.set(true);
    this.emailTouched.set(true);
    this.passTouched.set(true);
    this.confirmTouched.set(true);
    this.error.set('');

    if (!this.formValid()) {
      // Scroll to the first visible error (browser handles it via red border)
      return;
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
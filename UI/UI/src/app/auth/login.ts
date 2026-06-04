import { Component, signal, computed } from '@angular/core';
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
  role         = signal<'super_admin' | 'client'>('super_admin');
  email        = signal('');
  password     = signal(environment.useMocks ? 'demo1234' : '');
  loading      = signal(false);
  error        = signal('');
  isMock       = environment.useMocks;
  showPassword = signal(false);

  emailTouched    = signal(false);
  passwordTouched = signal(false);

  forgotOpen    = signal(false);
  forgotEmail   = signal('');
  forgotLoading = signal(false);
  forgotError   = signal('');
  forgotResult  = signal<{ email: string; temp_password: string; message: string } | null>(null);
  copyDone      = signal(false);

  constructor(private auth: AuthService, private router: Router) {}

  private readonly EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/;

  emailError = computed(() => {
    if (!this.emailTouched()) return '';
    const v = this.email().trim();
    if (!v) return 'Email address is required.';
    if (!this.EMAIL_RE.test(v)) return 'Please enter a valid email address.';
    return '';
  });

  passwordError = computed(() => {
    if (!this.passwordTouched()) return '';
    const v = this.password();
    if (!v.trim()) return 'Password is required.';
    if (v.length < 6) return 'Password must be at least 6 characters.';
    return '';
  });

  selectRole(r: 'super_admin' | 'client') {
    this.role.set(r);
    this.email.set('');
    this.password.set(this.isMock ? 'demo1234' : '');
    this.showPassword.set(false);
    this.error.set('');
    this.emailTouched.set(false);
    this.passwordTouched.set(false);
  }

  login() {
    this.emailTouched.set(true);
    this.passwordTouched.set(true);
    this.error.set('');

    const emailVal = this.email().trim();
    const passVal  = this.password();

    if (!emailVal) {
      this.error.set('Please enter your email address.');
      return;
    }
    if (!this.EMAIL_RE.test(emailVal)) {
      this.error.set('Please enter a valid email address (e.g. user@company.com).');
      return;
    }
    if (!passVal.trim()) {
      this.error.set('Please enter your password.');
      return;
    }
    if (passVal.length < 6) {
      this.error.set('Password must be at least 6 characters.');
      return;
    }

    this.loading.set(true);

    this.auth.login({ email: emailVal, password: passVal, loginRole: this.role() }).subscribe({
      next: () => {
        this.loading.set(false);
        if (this.auth.isSuperAdmin()) {
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

  openForgot() {
    this.forgotEmail.set(this.email().trim());
    this.forgotError.set('');
    this.forgotResult.set(null);
    this.forgotLoading.set(false);
    this.copyDone.set(false);
    this.forgotOpen.set(true);
  }

  closeForgot() {
    this.forgotOpen.set(false);
    this.forgotEmail.set('');
    this.forgotError.set('');
    this.forgotResult.set(null);
    this.forgotLoading.set(false);
    this.copyDone.set(false);
  }

  submitForgot() {
    const email = this.forgotEmail().trim();
    if (!email) {
      this.forgotError.set('Please enter your email address.');
      return;
    }
    if (!this.EMAIL_RE.test(email)) {
      this.forgotError.set('Please enter a valid email address (e.g. user@company.com).');
      return;
    }
    this.forgotError.set('');
    this.forgotLoading.set(true);

    this.auth.forgotPassword(email).subscribe({
      next: (res) => {
        this.forgotLoading.set(false);
        this.forgotResult.set(res);
      },
      error: (err) => {
        this.forgotLoading.set(false);
        this.forgotError.set(
          err?.error?.detail ?? err?.error?.message ?? err?.message ?? 'Could not reset password. Please try again.'
        );
      }
    });
  }

  async copyTempPassword() {
    const pw = this.forgotResult()?.temp_password;
    if (!pw) return;
    try {
      await navigator.clipboard.writeText(pw);
      this.copyDone.set(true);
      setTimeout(() => this.copyDone.set(false), 2000);
    } catch {}
  }
}
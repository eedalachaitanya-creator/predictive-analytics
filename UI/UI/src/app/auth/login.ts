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

  // ── Forgot-password modal state ────────────────────────────────────
  // forgotOpen:    controls whether the modal is rendered
  // forgotEmail:   the email typed in the modal input
  // forgotLoading: true while the API call is in flight
  // forgotError:   error message to show inside the modal
  // forgotResult:  success payload from the backend (email + temp_password
  //                + message); null until the reset succeeds.
  forgotOpen    = signal(false);
  forgotEmail   = signal('');
  forgotLoading = signal(false);
  forgotError   = signal('');
  forgotResult  = signal<{ email: string; temp_password: string; message: string } | null>(null);
  copyDone      = signal(false);

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

  // ── Forgot-password flow ─────────────────────────────────────────────
  openForgot() {
    // Pre-fill the modal's email input with whatever they typed into the
    // login form, so they don't have to type it twice.
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
    this.forgotError.set('');
    this.forgotLoading.set(true);

    this.auth.forgotPassword(email).subscribe({
      next: (res) => {
        this.forgotLoading.set(false);
        this.forgotResult.set(res);
      },
      error: (err) => {
        this.forgotLoading.set(false);
        // FastAPI returns error detail under err.error.detail; fall back to
        // other common shapes so we never show an empty "undefined" message.
        this.forgotError.set(
          err?.error?.detail ??
          err?.error?.message ??
          err?.message ??
          'Could not reset password. Please try again.'
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
      // Revert the "Copied!" label back to "Copy" after 2 seconds so a
      // second click still gives visual feedback.
      setTimeout(() => this.copyDone.set(false), 2000);
    } catch {
      // Clipboard API can fail in non-secure contexts (http:// pages) — the
      // temp password is still visible + selectable, so we just no-op here.
    }
  }
}

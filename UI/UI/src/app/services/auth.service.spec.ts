import { TestBed } from '@angular/core/testing';
import { effect } from '@angular/core';
import { provideRouter } from '@angular/router';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting, HttpTestingController } from '@angular/common/http/testing';
import { AuthService } from './auth.service';
import { environment } from '../../environments/environment';
import { LoginResponse } from '../models';

/**
 * The active tenant must be a REACTIVE signal, not just a value read out of
 * sessionStorage on demand. A super_admin can switch the active client via the
 * dropdown mid-session (setClientId) without logging out; dependent state — most
 * importantly the Agent Chat transcript — has to be able to observe that change
 * and reset, so one tenant's chat can never linger after a switch to another.
 */
describe('AuthService.activeClient — reactive active tenant', () => {
  let auth: AuthService;
  let http: HttpTestingController;

  beforeEach(() => {
    sessionStorage.clear();
    TestBed.configureTestingModule({
      providers: [AuthService, provideRouter([]), provideHttpClient(), provideHttpClientTesting()],
    });
    auth = TestBed.inject(AuthService);
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    http.verify();
    sessionStorage.clear();
  });

  function loginSuperAdmin(): void {
    auth.login({ email: 'admin@example.com', password: 'pw' }).subscribe();
    const res: LoginResponse = {
      token: 'tok',
      refreshToken: 'ref',
      user: {
        id: 'admin-1', email: 'admin@example.com', name: 'Admin',
        role: 'super_admin', clientAccess: ['*'], token: 'tok',
      },
    };
    http.expectOne(`${environment.apiUrl}/auth/login`).flush(res);
  }

  it('updates reactively when a super_admin switches the active client', () => {
    loginSuperAdmin();

    const seen: string[] = [];
    TestBed.runInInjectionContext(() => {
      effect(() => seen.push(auth.activeClient()));
    });
    TestBed.tick();                  // baseline: '' (super_admin, no selection yet)
    auth.setClientId('CLT-001');
    TestBed.tick();
    auth.setClientId('CLT-002');
    TestBed.tick();

    expect(seen).toEqual(['', 'CLT-001', 'CLT-002']);
    // getClientId stays consistent with the reactive signal (single source of truth).
    expect(auth.getClientId()).toBe('CLT-002');
  });
});

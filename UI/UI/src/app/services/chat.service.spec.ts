import { TestBed } from '@angular/core/testing';
import { signal } from '@angular/core';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { ChatService } from './chat.service';
import { AuthService } from './auth.service';
import { AuthUser } from '../models';

/**
 * Security regression: a teammate logged in as CLT-001, used the Agent Chat,
 * logged out, then logged in as CLT-002 — and CLT-001's conversation was still
 * on screen. ChatService is a providedIn:'root' singleton, so its `messages`
 * and `conversationId` signals outlive the session that produced them. When the
 * authenticated identity changes — logout, login as a different user, OR a
 * super_admin / multi-client user switching the active tenant in-session — the
 * in-memory transcript MUST be dropped so one tenant's chat can never render
 * under another tenant's session.
 *
 * The backend is already correctly tenant-scoped (chat_router._require_client_access
 * + WHERE client_id = :cid), so this leak is purely client-side stale state.
 */
function makeUser(id: string, clientAccess: string[]): AuthUser {
  return {
    id,
    email: `${id}@example.com`,
    name: id,
    role: clientAccess.includes('*') ? 'super_admin' : 'client_user',
    clientAccess,
    token: 'tok-' + id,
  };
}

describe('ChatService — tenant isolation across logout/login + client switch', () => {
  let svc: ChatService;
  let userSig: ReturnType<typeof signal<AuthUser | null>>;
  let clientSig: ReturnType<typeof signal<string>>;

  beforeEach(() => {
    userSig = signal<AuthUser | null>(makeUser('user-clt001', ['CLT-001']));
    clientSig = signal<string>('CLT-001');
    const fakeAuth = {
      user: userSig,
      activeClient: clientSig,
      getClientId: () => clientSig(),
    };

    TestBed.configureTestingModule({
      providers: [
        ChatService,
        { provide: AuthService, useValue: fakeAuth },
        provideHttpClient(),
        provideHttpClientTesting(),
      ],
    });
    svc = TestBed.inject(ChatService);
    TestBed.tick(); // flush the constructor effect's first (baseline) run
  });

  it('drops the previous tenant transcript when a different user logs in', () => {
    svc.conversationId.set('conv-clt001');
    svc.messages.set([{ role: 'user', content: 'Show CLT-001 high-risk Platinum customers' }]);

    // Logout, then log in as a CLT-002 user (no page reload — SPA navigation).
    userSig.set(null);
    clientSig.set('');
    TestBed.tick();
    userSig.set(makeUser('user-clt002', ['CLT-002']));
    clientSig.set('CLT-002');
    TestBed.tick();

    expect(svc.messages()).toEqual([]);
    expect(svc.conversationId()).toBeNull();
    expect(svc.error()).toBeNull();
  });

  it('drops the transcript when a super_admin switches the active client (same user)', () => {
    // A super_admin stays logged in but switches tenant via the dropdown.
    userSig.set(makeUser('admin-1', ['*']));
    clientSig.set('CLT-001');
    TestBed.tick();

    svc.conversationId.set('conv-clt001');
    svc.messages.set([{ role: 'assistant', content: 'CLT-001 churn summary' }]);

    clientSig.set('CLT-002');   // switch active client — user.id is unchanged
    TestBed.tick();

    expect(svc.messages()).toEqual([]);
    expect(svc.conversationId()).toBeNull();
  });

  it('does NOT wipe the transcript while the same identity stays logged in', () => {
    svc.conversationId.set('conv-clt001');
    svc.messages.set([{ role: 'user', content: 'keep me' }]);

    // Unrelated re-render churn with the same user + same client — must persist.
    TestBed.tick();

    expect(svc.messages()).toEqual([{ role: 'user', content: 'keep me' }]);
    expect(svc.conversationId()).toBe('conv-clt001');
  });
});

import { Injectable, inject, signal, effect } from '@angular/core';
import { Observable, tap, catchError, of } from 'rxjs';
import { ApiService } from './api.service';
import { AuthService } from './auth.service';

export interface ChatMessage {
  id?: number;
  role: 'user' | 'assistant';
  content: string;
  timestamp?: string;
}

export interface ChatResponse {
  answer: string;
  conversationId: string;
  timestamp: string;
  messages: ChatMessage[];
}

export interface ChatSuggestion {
  text: string;
  icon: string;
}

@Injectable({ providedIn: 'root' })
export class ChatService {
  private api = inject(ApiService);
  private auth = inject(AuthService);

  /** Current conversation ID */
  conversationId = signal<string | null>(null);

  /** All messages in current conversation */
  messages = signal<ChatMessage[]>([]);

  /** Loading state */
  loading = signal(false);

  /** Error message */
  error = signal<string | null>(null);

  /** Suggested questions */
  suggestions = signal<ChatSuggestion[]>([]);

  // Identity (user id + active tenant) the current transcript belongs to.
  // ChatService is a providedIn:'root' singleton, so its `messages`/
  // `conversationId` signals outlive a logout/login within the same SPA session
  // (no page reload). When the authenticated identity changes — logout, login
  // as a different user, OR a super_admin / multi-client user switching the
  // active tenant in-session — we drop the in-memory transcript so one tenant's
  // chat can never render under another tenant's session. The backend is
  // already tenant-scoped; this closes the matching client-side leak.
  private _identity: string | null = null;

  constructor() {
    effect(() => {
      const identity = `${this.auth.user()?.id ?? ''}::${this.auth.activeClient()}`;
      if (identity === this._identity) return;
      this._identity = identity;
      this.messages.set([]);
      this.conversationId.set(null);
      this.error.set(null);
    });
  }

  /** Send a question to the agent */
  ask(question: string): void {
    this.loading.set(true);
    this.error.set(null);

    // Optimistically add user message to UI
    const userMsg: ChatMessage = {
      role: 'user',
      content: question,
      timestamp: new Date().toISOString(),
    };
    this.messages.update(msgs => [...msgs, userMsg]);

    const body = {
      question,
      conversationId: this.conversationId(),
      clientId: this.auth.getClientId(),
    };

    this.api.post<ChatResponse>('/chat/ask', body).pipe(
      catchError(err => {
        // A 4xx carries a meaningful, user-facing message from the backend
        // (e.g. the firewall's "looks like a prompt-injection attempt…" block).
        // Surface it verbatim so the user sees WHY, instead of a generic error.
        // 5xx / network failures stay generic — those aren't actionable by the user.
        const clientError =
          err?.status >= 400 && err?.status < 500 &&
          typeof err?.message === 'string' && err.message.length > 0;
        const assistantText = clientError
          ? err.message
          : 'Sorry, I encountered an error processing your question. Please try again.';
        this.error.set(clientError ? err.message : 'Failed to get a response. Please try again.');
        this.loading.set(false);
        // Add error message as assistant
        this.messages.update(msgs => [...msgs, {
          role: 'assistant' as const,
          content: assistantText,
          timestamp: new Date().toISOString(),
        }]);
        return of(null);
      })
    ).subscribe(res => {
      if (res) {
        this.conversationId.set(res.conversationId);
        // Replace optimistic messages with server truth
        this.messages.set(res.messages);
      }
      this.loading.set(false);
    });
  }

  /** Load conversation history */
  loadHistory(conversationId: string): void {
    this.api.get<{ conversationId: string; messages: ChatMessage[] }>(
      `/chat/history?clientId=${this.auth.getClientId()}&conversationId=${conversationId}`
    ).subscribe(res => {
      this.conversationId.set(conversationId);
      this.messages.set(res.messages);
    });
  }

  /** Clear current conversation */
  clearConversation(): void {
    const convId = this.conversationId();
    if (convId) {
      this.api.post(`/chat/clear?clientId=${this.auth.getClientId()}&conversationId=${convId}`, {}).subscribe();
    }
    this.conversationId.set(null);
    this.messages.set([]);
    this.error.set(null);
  }

  /** Start a new conversation */
  newConversation(): void {
    this.conversationId.set(null);
    this.messages.set([]);
    this.error.set(null);
  }

  /** Load suggestions */
  loadSuggestions(): void {
    this.api.get<{ suggestions: ChatSuggestion[] }>('/chat/suggestions').pipe(
      catchError(() => of({ suggestions: [] }))
    ).subscribe(res => {
      this.suggestions.set(res.suggestions);
    });
  }
}

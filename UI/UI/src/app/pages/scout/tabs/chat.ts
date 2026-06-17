import { Component, inject, signal, ViewChild, ElementRef, AfterViewChecked, ViewEncapsulation } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';
import { marked } from 'marked';
import DOMPurify from 'dompurify';
import { ScoutService, AgentChatResponse, AgentSessionDeleteResponse } from '../../../services/scout.service';

interface ChatMessage {
  role: 'user' | 'assistant';
  content: string;
  timestamp: Date;
}

@Component({
  selector: 'scout-chat',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './chat.html',
  styleUrls: ['./chat.scss'],
  // View encapsulation OFF: Markdown is rendered into the DOM via [innerHTML],
  // and Angular's default emulated encapsulation attaches scoped attribute
  // selectors (_ngcontent-xxx) that our raw <ul>/<li>/<p> elements don't
  // inherit. The result: chat.scss rules for .md-content ul li never reach
  // the rendered content, and global styles on <p>/<li> win — producing huge
  // vertical gaps between bullets. Turning encapsulation off lets our scoped
  // selectors (all prefixed with .msg-bot .md-content) apply correctly.
  // Safe because every selector in chat.scss starts with a chat-specific
  // class — nothing leaks out to other components.
  encapsulation: ViewEncapsulation.None,
})
export class ScoutChatTab implements AfterViewChecked {
  private svc = inject(ScoutService);
  private sanitizer = inject(DomSanitizer);

  // Per-browser-session ID — stable across tab switches within one visit so
  // the backend agent keeps conversation memory. Regenerated only on page reload
  // or when the user clicks "New Chat".
  sessionId = signal(this.generateSessionId());

  messages = signal<ChatMessage[]>([]);
  input    = signal('');
  sending  = signal(false);
  error    = signal('');

  @ViewChild('messagesEnd') messagesEnd?: ElementRef<HTMLDivElement>;
  private shouldScroll = false;

  constructor() {
    // Configure marked for safe, sensible defaults:
    // - gfm: GitHub-flavored markdown (tables, strikethrough, etc.)
    // - breaks: single newline → <br> (matches how the agent writes replies)
    marked.setOptions({ gfm: true, breaks: true });

    // FIX: Custom renderer so all links open in a new tab.
    // Without this, "View on Amazon" / "View on Flipkart" links navigate
    // the user away from the app (same-tab). target="_blank" keeps the app
    // open. rel="noopener noreferrer" is a security best-practice: it
    // prevents the opened page from accessing window.opener.
    const renderer = new marked.Renderer();
    renderer.link = ({ href, title, text }: { href: string; title?: string | null; text: string }) => {
      const titleAttr = title ? ` title="${title}"` : '';
      return `<a href="${href}"${titleAttr} target="_blank" rel="noopener noreferrer">${text}</a>`;
    };
    marked.use({ renderer });
  }

  ngAfterViewChecked() {
    if (this.shouldScroll) {
      this.messagesEnd?.nativeElement.scrollIntoView({ behavior: 'smooth' });
      this.shouldScroll = false;
    }
  }

  send() {
    const text = this.input().trim();
    if (!text || this.sending()) return;

    // Optimistic append — user sees their own message immediately
    const userMsg: ChatMessage = { role: 'user', content: text, timestamp: new Date() };
    this.messages.set([...this.messages(), userMsg]);
    this.input.set('');
    this.error.set('');
    this.sending.set(true);
    this.shouldScroll = true;

    // FIX: typed as AgentChatResponse instead of `any` to resolve TS2571
    this.svc.agentChat(text, this.sessionId()).subscribe({
      next: (res: AgentChatResponse) => {
        const reply: ChatMessage = {
          role: 'assistant',
          content: res.response || '(no response)',
          timestamp: new Date(),
        };
        this.messages.set([...this.messages(), reply]);
        this.sending.set(false);
        this.shouldScroll = true;
      },
      error: (err: any) => {
        this.error.set(err.message || 'Chat request failed');
        this.sending.set(false);
      }
    });
  }

  onKeydown(e: KeyboardEvent) {
    // Enter sends; Shift+Enter inserts newline (standard chat UX)
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      this.send();
    }
  }

  // FIX: corrected indentation — was 1-space (outside class body in TS's view),
  // causing `this` to have no type context → TS2571 "Object is of type unknown".
  newChat() {
    // No confirmation — New Chat is intentional and recoverable (user can
    // always retype their question). Avoids the browser's ugly native
    // confirm() dialog that doesn't match our app style.
    this.svc.agentDeleteSession(this.sessionId()).subscribe({
      next: (_res: AgentSessionDeleteResponse) => {},
      error: () => {}
    });
    this.messages.set([]);
    this.sessionId.set(this.generateSessionId());
    this.error.set('');
  }

  /**
   * Quick-suggestion chips on the empty state should fire and forget —
   * no extra click on Send. We set the input to the suggestion text and
   * immediately invoke send(), which reads from this.input() and then
   * clears it. The user perceives one action (click → message sent),
   * not two (click → text fills → click Send).
   *
   * Note: send() is a no-op if already sending() or if the text is empty,
   * so rapid clicks during a pending request are safely ignored.
   */
  suggestQuery(q: string) {
    this.input.set(q);
    this.send();
  }

  private generateSessionId(): string {
    // Matches format used by backend: any string works, but we use a
    // readable timestamped ID so it's easy to correlate with logs.
    return 'ui-' + Date.now() + '-' + Math.random().toString(36).slice(2, 8);
  }

  // Convenience getters for template
  hasMessages(): boolean { return this.messages().length > 0; }

  formatTime(d: Date): string {
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  }

  /**
   * Convert the assistant's markdown response to safe, rendered HTML.
   *
   * Pipeline: raw markdown → marked (HTML) → DOMPurify (sanitized HTML)
   *   → DomSanitizer.bypassSecurityTrustHtml (Angular-safe)
   *
   * Why DOMPurify: marked will happily render <script>, <iframe>, event
   * handlers, javascript: URLs, etc. Even though we only render agent
   * output (not user input), the agent could echo user text or get
   * prompt-injected. DOMPurify strips anything that can execute code.
   *
   * Why bypassSecurityTrustHtml: Angular's default [innerHTML] sanitizer
   * strips SOME valid markdown features (like `target="_blank"` on links).
   * Since we've already sanitized through DOMPurify, telling Angular to
   * trust this specific string is the right escape hatch.
   *
   * Only called for assistant messages. User messages render as plain text
   * (they typed it, they see it back literally — no markdown interpretation).
   */
  renderMarkdown(content: string): SafeHtml {
    const rawHtml = marked.parse(content, { async: false }) as string;
    const clean = DOMPurify.sanitize(rawHtml, {
      ADD_ATTR: ['target'], // allow target="_blank" for product links
    });
    return this.sanitizer.bypassSecurityTrustHtml(clean);
  }
}
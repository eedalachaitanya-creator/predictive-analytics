import { Component, computed, inject, signal, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ScoutService, Website } from '../../../services/scout.service';

@Component({
  selector: 'scout-platforms',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './platforms.html',
  styleUrls: ['./platforms.scss']
})
export class ScoutPlatformsTab implements OnInit, OnDestroy {
  private svc = inject(ScoutService);

  websites = computed(() => this.svc.websites());
  loading   = signal(true);
  adding    = signal(false);
  cancelling = signal(false);
  newName   = signal('');
  currentRequestId = signal<string | null>(null);
  error     = signal('');
  success   = signal('');
  private readonly FLASH_MS = 4000;
  private flashTimer: any = null;
  editingIdx = signal<number | null>(null);
  editUrl   = signal('');

  confirmDelete = signal<Website | null>(null);
  deleting      = signal(false);

  ngOnInit() {
    this.svc.refreshPlatforms().subscribe({
      next: () => this.loading.set(false),
      error: () => this.loading.set(false),
    });
  }

  private canonicalizeName(raw: string): string {
    let s = raw.trim().toLowerCase();
    s = s.replace(/^https?:\/\//, '');
    s = s.replace(/^www\./, '');
    s = s.split('/')[0].split('?')[0].split('#')[0];
    s = s.replace(/\.(com\.au|co\.uk|co\.in|com|net|org|in|io|co|store|shop|app)$/, '');
    return s.trim();
  }

  addWebsite() {
    const name = this.newName().trim();
    if (!name || this.adding()) return;

    const canonical = this.canonicalizeName(name);
    const duplicate = this.websites().find(
      site => this.canonicalizeName(site.name) === canonical
    );
    if (duplicate) {
      this.error.set(
        `'${duplicate.name}' is already added. ` +
        `('${name}' resolves to the same platform.)`
      );
      return;
    }

    const requestId = typeof crypto.randomUUID === 'function'
      ? crypto.randomUUID()
      : 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
          const r = Math.random() * 16 | 0;
          return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
        });
    this.currentRequestId.set(requestId);

    this.adding.set(true);
    this.error.set('');
    this.success.set('');

    this.svc.addWebsite(name, requestId).subscribe({
      next: res => {
        if (res?.status === 'cancelled') {
          this.adding.set(false);
          this.cancelling.set(false);
          this.currentRequestId.set(null);
          return;
        }
        const site = res.data!;
        this.flashSuccess(`Added "${site.name}" — search URL: ${site.search_url}`);
        this.newName.set('');
        this.adding.set(false);
        this.cancelling.set(false);
        this.currentRequestId.set(null);
      },
      error: err => {
        if (this.flashTimer !== null) {
          clearTimeout(this.flashTimer);
          this.flashTimer = null;
        }
        this.success.set('');
        this.error.set(err.message || 'Failed to add website');
        this.adding.set(false);
        this.cancelling.set(false);
        this.currentRequestId.set(null);
      }
    });
  }

  cancelAdd() {
    const requestId = this.currentRequestId();
    if (!requestId || this.cancelling()) return;
    this.cancelling.set(true);
    this.svc.cancelAddWebsite(requestId).subscribe({
      error: err => console.warn('[platforms] cancel POST failed:', err),
    });
  }

  toggleActive(site: Website) {
    const action = site.active
      ? this.svc.deactivateWebsite(site)
      : this.svc.reactivateWebsite(site.name);
    action.subscribe({
      error: err => this.flashError(err.message || 'Toggle failed')
    });
  }

  startEdit(i: number) {
    this.editingIdx.set(i);
    this.editUrl.set(this.websites()[i].search_url);
  }

  cancelEdit() {
    this.editingIdx.set(null);
    this.editUrl.set('');
  }

  // PA-056 FIX: validate URL is non-empty before saving.
  // Previously sent raw editUrl() with no guard — empty string went to DB.
  saveEdit(site: Website) {
    const url = this.editUrl().trim();
    if (!url) {
      this.flashError('Search URL cannot be empty.');
      return;
    }
    if (!/^https?:\/\//i.test(url)) {
      this.flashError('Search URL must start with http:// or https://');
      return;
    }
    if (!url.includes('{query}')) {
      this.flashError('Search URL must contain the {query} placeholder (e.g. ?q={query})');
      return;
    }
    this.svc.updateWebsite({ name: site.name, search_url: url }).subscribe({
      next: () => {
        this.editingIdx.set(null);
        this.flashSuccess(`Updated search URL for "${site.name}"`);
      },
      error: err => this.flashError(err.message || 'Update failed')
    });
  }

  askDeleteWebsite(site: Website) {
    this.confirmDelete.set(site);
    this.clearMessages();
  }

  cancelDelete() {
    this.confirmDelete.set(null);
  }

  confirmDeleteWebsite() {
    const site = this.confirmDelete();
    if (!site || this.deleting()) return;

    this.deleting.set(true);
    this.svc.deleteWebsite(site.name).subscribe({
      next: (res: any) => {
        const counts = res?.deleted_counts || {};
        const detail = [
          counts.price_history   ? `${counts.price_history} price records`   : null,
          counts.price_alerts    ? `${counts.price_alerts} alerts`           : null,
          counts.product_results ? `${counts.product_results} scrape results`: null,
        ].filter(Boolean).join(', ');

        this.flashSuccess(
          detail
            ? `Deleted "${site.name}" — purged ${detail}`
            : `Deleted "${site.name}"`
        );
        this.confirmDelete.set(null);
        this.deleting.set(false);
      },
      error: err => {
        this.flashError(err.message || 'Delete failed');
        this.deleting.set(false);
        this.confirmDelete.set(null);
      }
    });
  }

  onAddKeydown(e: KeyboardEvent) {
    if (e.key === 'Enter') this.addWebsite();
  }

  clearMessages() {
    if (this.flashTimer !== null) {
      clearTimeout(this.flashTimer);
      this.flashTimer = null;
    }
    this.error.set('');
    this.success.set('');
  }

  private flashSuccess(message: string) {
    this.error.set('');
    this.success.set(message);
    this.scheduleClear();
  }

  private flashError(message: string) {
    this.success.set('');
    this.error.set(message);
    this.scheduleClear();
  }

  private scheduleClear() {
    if (this.flashTimer !== null) {
      clearTimeout(this.flashTimer);
    }
    this.flashTimer = setTimeout(() => {
      this.error.set('');
      this.success.set('');
      this.flashTimer = null;
    }, this.FLASH_MS);
  }

  ngOnDestroy() {
    if (this.flashTimer !== null) {
      clearTimeout(this.flashTimer);
      this.flashTimer = null;
    }
  }
}
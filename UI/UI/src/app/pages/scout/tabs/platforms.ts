import { Component, inject, signal, OnInit } from '@angular/core';
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
export class ScoutPlatformsTab implements OnInit {
  private svc = inject(ScoutService);

  websites  = signal<Website[]>([]);
  loading   = signal(true);
  adding    = signal(false);
  newName   = signal('');
  error     = signal('');
  success   = signal('');
  editingIdx = signal<number | null>(null);
  editUrl   = signal('');

  // Delete confirmation modal state
  confirmDelete = signal<Website | null>(null);
  deleting      = signal(false);

  ngOnInit() {
    this.loadWebsites();
  }

  loadWebsites() {
    this.loading.set(true);
    this.svc.loadWebsites().subscribe({
      next: res => {
        this.websites.set(res.data || []);
        this.loading.set(false);
      },
      error: () => this.loading.set(false)
    });
  }

  addWebsite() {
    const name = this.newName().trim();
    if (!name || this.adding()) return;

    this.adding.set(true);
    this.error.set('');
    this.success.set('');

    this.svc.addWebsite(name).subscribe({
      next: res => {
        this.success.set(`Added "${res.data.name}" — search URL: ${res.data.search_url}`);
        this.newName.set('');
        this.adding.set(false);
        this.loadWebsites();
      },
      error: err => {
        this.error.set(err.message || 'Failed to add website');
        this.adding.set(false);
      }
    });
  }

  toggleActive(site: Website) {
  const action = site.active
    ? this.svc.deactivateWebsite(site)           // pass full site object
    : this.svc.reactivateWebsite(site.name);

  action.subscribe({
    next: () => this.loadWebsites(),
    error: err => this.error.set(err.message || 'Toggle failed')
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

  saveEdit(site: Website) {
    this.svc.updateWebsite({ name: site.name, search_url: this.editUrl() }).subscribe({
      next: () => {
        this.editingIdx.set(null);
        this.success.set(`Updated search URL for "${site.name}"`);
        this.loadWebsites();
      },
      error: err => this.error.set(err.message || 'Update failed')
    });
  }

  // Opens the confirmation modal (called by the trash icon)
  askDeleteWebsite(site: Website) {
    this.confirmDelete.set(site);
    this.clearMessages();
  }

  // Cancels the modal
  cancelDelete() {
    this.confirmDelete.set(null);
  }

  // Actually performs the delete after user confirms in the modal
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

        this.success.set(
          detail
            ? `Deleted "${site.name}" — purged ${detail}`
            : `Deleted "${site.name}"`
        );
        this.confirmDelete.set(null);
        this.deleting.set(false);
        this.loadWebsites();
      },
      error: err => {
        this.error.set(err.message || 'Delete failed');
        this.deleting.set(false);
        this.confirmDelete.set(null);
      }
    });
  }

  onAddKeydown(e: KeyboardEvent) {
    if (e.key === 'Enter') this.addWebsite();
  }

  clearMessages() {
    this.error.set('');
    this.success.set('');
  }
}
import { Component, inject, signal, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ScoutService, Listing, SearchResult } from '../../../services/scout.service';

@Component({
  selector: 'scout-search',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './search.html',
  styleUrls: ['./search.scss']
})
export class ScoutSearchTab implements OnInit {
  private svc = inject(ScoutService);

  mode       = signal<'single' | 'bulk'>('single');
  query      = signal('');
  platforms  = signal<string[]>([]);
  selected   = signal<Set<string>>(new Set());
  searching  = signal(false);
  results    = signal<SearchResult[]>([]);
  error      = signal('');
  expandedRow = signal<number | null>(null);
  bulkFile     = signal<File | null>(null);
  bulkDragover = signal(false);

  ngOnInit() {
    this.svc.getActivePlatforms().subscribe({
      next: (res: any) => {
        this.platforms.set(res.platforms);
        this.selected.set(new Set(res.platforms));
      }
    });
  }

  togglePlatform(name: string) {
    const s = new Set(this.selected());
    s.has(name) ? s.delete(name) : s.add(name);
    this.selected.set(s);
  }
  selectAll() { this.selected.set(new Set(this.platforms())); }
  selectNone() { this.selected.set(new Set()); }

  search() {
    const q = this.query().trim();
    if (!q || this.searching()) return;
    this.searching.set(true); this.error.set(''); this.results.set([]); this.expandedRow.set(null);
    this.svc.searchProducts(q, [...this.selected()]).subscribe({
      next: (res: any) => { this.results.set(res.products); this.searching.set(false); },
      error: (err: any) => { this.error.set(err.message || 'Search failed'); this.searching.set(false); }
    });
  }

  onKeydown(e: KeyboardEvent) { if (e.key === 'Enter') this.search(); }

  onFileSelect(e: Event) { const f = (e.target as HTMLInputElement).files; if (f?.length) this.bulkFile.set(f[0]); }
  onDragOver(e: DragEvent) { e.preventDefault(); this.bulkDragover.set(true); }
  onDragLeave() { this.bulkDragover.set(false); }
  onDrop(e: DragEvent) { e.preventDefault(); this.bulkDragover.set(false); if (e.dataTransfer?.files.length) this.bulkFile.set(e.dataTransfer.files[0]); }
  clearFile() { this.bulkFile.set(null); }

  searchBulk() {
    const file = this.bulkFile();
    if (!file || this.searching()) return;
    this.searching.set(true); this.error.set(''); this.results.set([]);
    this.svc.uploadBulk(file, [...this.selected()]).subscribe({
      next: (res: any) => { this.results.set(res.products); this.searching.set(false); },
      error: (err: any) => { this.error.set(err.message || 'Bulk search failed'); this.searching.set(false); }
    });
  }

  toggleExpand(i: number) { this.expandedRow.set(this.expandedRow() === i ? null : i); }

  bestPrice(listings: Listing[]): Listing | null {
    const valid = listings.filter(l => l.price.value > 0);
    return valid.length ? valid.reduce((m, l) => l.price.value < m.price.value ? l : m) : null;
  }

  isBest(l: Listing, all: Listing[]): boolean {
    const b = this.bestPrice(all);
    return !!b && l.price.value === b.price.value && l.price.value > 0;
  }

  fmt(p: any): string {
    if (!p || !p.value || p.value <= 0) return '—';
    const symbols: Record<string, string> = {
      INR: '₹', USD: '$', EUR: '€', GBP: '£', JPY: '¥',
      AUD: 'A$', CAD: 'C$', SGD: 'S$', AED: 'AED ',
    };
    return (symbols[p.currency] || p.currency + ' ') + p.value.toLocaleString();
  }

  statusIcon(s?: string): string { return s === 'found' ? '✅' : s === 'not_found' ? '❌' : '⏳'; }
  specKeys(specs: Record<string, string>): string[] { return specs ? Object.keys(specs).slice(0, 12) : []; }
}
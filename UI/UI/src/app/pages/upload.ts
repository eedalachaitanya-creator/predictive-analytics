import { Component, OnInit, signal, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { UploadService } from '../services/upload.service';
import { AuthService } from '../services/auth.service';
import { MatchReport, MasterType, SourceOption, UploadPreview } from '../models';

interface MasterDef {
  key: MasterType;
  label: string;
  icon: string;
  formats: string[];
  columns: string;
  required: boolean;
  accept: string;
}

@Component({
  selector: 'app-upload',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './upload.html',
  styleUrls: ['./upload.scss']
})
export class UploadComponent implements OnInit {
  uploadSvc = inject(UploadService);
  private auth = inject(AuthService);
  private clientId = this.auth.getClientId();

  masters: { group: string; desc: string; icon: string; items: MasterDef[] }[] = [
    {
      group: 'Transaction Data', desc: 'Core transactional records — required for all analytics', icon: '🔄',
      items: [
        { key:'customer',  label:'Customer Master',    icon:'👤', formats:['.xlsx','.csv'], columns:'client_id · customer_id · email · name · phone · reg_date · device · email_opt_in', required:true,  accept:'.xlsx,.xls,.csv' },
        { key:'order',     label:'Order Master',       icon:'📦', formats:['.xlsx','.csv'], columns:'client_id · order_id · customer_id · order_date · status · value_usd · discount',   required:true,  accept:'.xlsx,.xls,.csv' },
        { key:'line_items',label:'Line Items Master',  icon:'🛍️', formats:['.xlsx','.csv'], columns:'client_id · line_item_id · order_id · customer_id · product_id · qty · price · status', required:true, accept:'.xlsx,.xls,.csv' },
      ]
    },
    {
      group: 'Product Data', desc: 'Product catalogue, pricing tiers and vendor-product mapping', icon: '📦',
      items: [
        { key:'product',   label:'Product Master',      icon:'📋', formats:['.xlsx','.csv'], columns:'product_id · sku · name · category_id · sub_cat_id · brand_id · price_id · active', required:true, accept:'.xlsx,.xls,.csv' },
        { key:'price',     label:'Product Price Master', icon:'💲', formats:['.xlsx','.csv'], columns:'product_price_id · product_id · qty_range_label · qty_min · qty_max · unit_price_usd', required:true, accept:'.xlsx,.xls,.csv' },
        { key:'vendor_map',label:'Product-Vendor Mapping',icon:'🔗',formats:['.xlsx','.csv'], columns:'pv_id · product_id · brand_id · vendor_id', required:true, accept:'.xlsx,.xls,.csv' },
      ]
    },
    {
      group: 'Category Hierarchy', desc: '3-level category tree: Category → Sub-Category → Sub-Sub-Category', icon: '📂',
      items: [
        { key:'category',       label:'Category Master',        icon:'📂', formats:['.xlsx','.csv'], columns:'category_id · category_name', required:true, accept:'.xlsx,.xls,.csv' },
        { key:'sub_category',   label:'Sub-Category Master',    icon:'📁', formats:['.xlsx','.csv'], columns:'sub_category_id · sub_category_name · category_id', required:true, accept:'.xlsx,.xls,.csv' },
        { key:'sub_sub_category',label:'Sub-Sub-Category Master',icon:'📄',formats:['.xlsx','.csv'], columns:'sub_sub_category_id · name · sub_category_id · category_id', required:true, accept:'.xlsx,.xls,.csv' },
      ]
    },
    {
      group: 'Brand & Vendor Masters', desc: 'Brand registry and supplier information', icon: '🏷️',
      items: [
        { key:'brand',  label:'Brand Master',  icon:'🏷️', formats:['.xlsx','.csv'], columns:'brand_id · brand_name · vendor_id · active · not_available · category_hint', required:true, accept:'.xlsx,.xls,.csv' },
        { key:'vendor', label:'Vendor Master', icon:'🏭', formats:['.xlsx','.csv'], columns:'vendor_id · vendor_name · description · contact_no · address · email', required:true, accept:'.xlsx,.xls,.csv' },
      ]
    },
    {
      group: 'Customer Feedback', desc: 'Reviews, ratings, and support tickets — key churn signals', icon: '💬',
      items: [
        { key:'customer_reviews',  label:'Customer Reviews',  icon:'⭐', formats:['.xlsx','.csv'], columns:'review_id · customer_id · product_id · rating · review_text · sentiment', required:false, accept:'.xlsx,.xls,.csv' },
        { key:'support_tickets',   label:'Support Tickets',   icon:'🎫', formats:['.xlsx','.csv'], columns:'ticket_id · customer_id · ticket_type · priority · status · channel', required:false, accept:'.xlsx,.xls,.csv' },
      ]
    },
    {
      group: 'Engagement', desc: 'Login history — one row per login, used for recent-login churn signals', icon: '🔑',
      items: [
        { key:'login_event', label:'Login Events', icon:'🔑', formats:['.xlsx','.csv'], columns:'login_id · customer_id · login_at · login_channel', required:false, accept:'.xlsx,.xls,.csv' },
      ]
    }
  ];

  ngOnInit() {
    // Load any previously uploaded files for this session
    this.uploadSvc.loadUploads(this.clientId).subscribe({ error: () => {} });
    // Check if this client already has a pending batch waiting to commit
    this.refreshBatch();
    // Load source registry for the source-aware tiles
    this.uploadSvc.loadSources().subscribe({
      next: r => this.sources.set(r.sources),
      error: () => {},
    });
    // Which masters are ALREADY committed for this client — lets a required
    // master count as satisfied without re-uploading it (incremental top-ups).
    this.uploadSvc.loadDataStatus(this.clientId).subscribe({ error: () => {} });
    // "Your integrations" — feedback data volume by source + connector status.
    this.uploadSvc.loadIntegrationsSummary(this.clientId).subscribe({ error: () => {} });
  }

  /** Re-fetch pending batch info from the backend. Called after uploads,
   *  commits, or discards so the Review panel stays in sync. */
  refreshBatch() {
    this.uploadSvc.getBatchInfo(this.clientId).subscribe({ error: () => {} });
  }

  /** Commit the pending batch. On success reload the upload list so the
   *  UI reflects the now-empty staging area. */
  commitBatch() {
    this.uploadSvc.commit(this.clientId).subscribe({
      next: () => {
        // After commit, staging is empty — reload to confirm and refresh batch info
        this.uploadSvc.loadUploads(this.clientId).subscribe({ error: () => {} });
        // Newly-committed masters are now "satisfied" for any follow-up batch.
        this.uploadSvc.loadDataStatus(this.clientId).subscribe({ error: () => {} });
        // Refresh the per-source integration counts (tickets/reviews changed).
        this.uploadSvc.loadIntegrationsSummary(this.clientId).subscribe({ error: () => {} });
      },
      error: (err) => console.error('Commit failed:', err.message ?? err),
    });
  }

  // ── Source selection state (tickets + reviews only) ────────────────
  sources    = signal<SourceOption[]>([]);
  sourceSel  = signal<Record<string, string>>({ support_tickets: 'jira', customer_reviews: 'jira' });
  sourceName = signal<Record<string, string>>({});
  lastMatch  = signal<Record<string, MatchReport | null>>({});

  readonly SOURCE_KEYS = ['support_tickets', 'customer_reviews'];
  hasSource(key: string) { return this.SOURCE_KEYS.includes(key); }
  isOther(key: string)   { return (this.sourceSel()[key] ?? '') === 'other'; }
  setSource(key: string, v: string)     { this.sourceSel.update(m => ({ ...m, [key]: v })); }
  setSourceName(key: string, v: string) { this.sourceName.update(m => ({ ...m, [key]: v })); }

  // ── Discard-confirm modal state ─────────────────────────────────────
  // discardConfirmOpen toggles the in-app modal that asks the user to
  // confirm before wiping all staged files. We dropped the native
  // window.confirm() because it renders the browser-chrome dialog with
  // a "localhost:4200 says" header that looks like a system error and
  // breaks the visual flow of the app.
  discardConfirmOpen = signal(false);

  /** Open the discard-confirm modal. (Doesn't call the API yet.) */
  discardBatch() {
    this.discardConfirmOpen.set(true);
  }

  /** User clicked Cancel in the modal — just close it, no API call. */
  cancelDiscard() {
    this.discardConfirmOpen.set(false);
  }

  /** User clicked Confirm in the modal — fire the discard API call,
   *  reload, and close the modal regardless of success/failure. */
  confirmDiscard() {
    this.discardConfirmOpen.set(false);
    this.uploadSvc.discard(this.clientId).subscribe({
      next: () => {
        this.uploadSvc.loadUploads(this.clientId).subscribe({ error: () => {} });
      },
      error: (err) => console.error('Discard failed:', err.message ?? err),
    });
  }

  // ── Preview modal ───────────────────────────────────────────────────
  // Lets the user see the first rows of a staged file before saving.
  previewOpen    = signal(false);
  previewLoading = signal(false);
  previewError   = signal('');
  previewData    = signal<UploadPreview | null>(null);

  openPreview(key: MasterType) {
    this.previewOpen.set(true);
    this.previewLoading.set(true);
    this.previewError.set('');
    this.previewData.set(null);
    this.uploadSvc.preview(this.clientId, key).subscribe({
      next: (data) => { this.previewData.set(data); this.previewLoading.set(false); },
      error: (err) => {
        this.previewLoading.set(false);
        this.previewError.set(err?.message ?? 'Could not load the preview.');
      },
    });
  }

  /** Like openPreview, but for SAVED (committed) data — used by the post-commit
   *  "your data has been saved" banner so the client can see what was actually
   *  saved. Reuses the same preview modal; only the data source differs. */
  openSavedPreview(key: string) {
    this.previewOpen.set(true);
    this.previewLoading.set(true);
    this.previewError.set('');
    this.previewData.set(null);
    this.uploadSvc.savedPreview(this.clientId, key as MasterType).subscribe({
      next: (data) => { this.previewData.set(data); this.previewLoading.set(false); },
      error: (err) => {
        this.previewLoading.set(false);
        this.previewError.set(err?.message ?? 'Could not load the preview.');
      },
    });
  }

  closePreview() {
    this.previewOpen.set(false);
    this.previewData.set(null);
    this.previewError.set('');
  }

  /** Humanize a raw DB column name for display in the Preview grid headers,
      e.g. customer_id -> 'Customer ID', order_value_usd -> 'Order Value USD'.
      Mirrors clients.formatColumnName so no table in the app shows raw snake_case. */
  formatColumnName(col: string): string {
    if (!col) return col;
    const acronyms = new Set(['id', 'usd', 'rfm', 'ltv', 'sku', 'api', 'url', 'csv', 'db', 'pv']);
    return col
      .split('_')
      .map(w => !w ? w
        : acronyms.has(w.toLowerCase()) ? w.toUpperCase()
        : w.charAt(0).toUpperCase() + w.slice(1))
      .join(' ');
  }

  onFileSelected(event: Event, key: MasterType) {
    const input = event.target as HTMLInputElement;
    const file = input.files?.[0];
    if (!file) return;
    // Reset input so same file can be re-selected
    input.value = '';
    this.doUpload(key, file);
  }

  onDrop(event: DragEvent, key: MasterType) {
    event.preventDefault();
    const file = event.dataTransfer?.files[0];
    if (file) this.doUpload(key, file);
  }

  onDragOver(event: DragEvent) { event.preventDefault(); }

  private doUpload(key: MasterType, file: File) {
    // Starting a fresh upload — hide any stale "committed" success banner
    this.uploadSvc.dismissCommitResult();
    const src  = this.hasSource(key) ? this.sourceSel()[key] : undefined;
    const name = this.isOther(key)   ? this.sourceName()[key] : undefined;
    this.uploadSvc.upload(this.clientId, key, file, src, name).subscribe({
      next: res => {
        this.refreshBatch();  // refresh batch panel so new file shows up
        if (res.matchReport) this.lastMatch.update(m => ({ ...m, [key]: res.matchReport! }));
      },
      error: (err) => console.error('Upload failed:', err.message)
    });
  }

  /** Template helper: convert the backend's rowsCommitted dict into a list
   *  of { key, value } pairs the @for block can iterate. Angular templates
   *  can't iterate objects directly, so the conversion lives here. */
  commitResultEntries(): { key: string; value: number }[] {
    const res = this.uploadSvc.lastCommitResult();
    if (!res?.rowsCommitted) return [];
    return Object.entries(res.rowsCommitted).map(([key, value]) => ({ key, value }));
  }

  /** Look up the friendly display name + icon for a raw masterType key
   *  (e.g. 'customer_reviews' → '⭐ Customer Reviews'). Used in the Pending
   *  Batch and Commit Result panels so clients see human-readable labels
   *  instead of snake_case DB-ish keys. Falls back to Title-Casing the key
   *  with a generic 📄 icon if the key isn't in the masters config. */
  masterDisplay(key: string): { icon: string; label: string } {
    for (const grp of this.masters) {
      const hit = grp.items.find(i => i.key === key);
      if (hit) return { icon: hit.icon, label: hit.label.replace(/ Master$/, '') };
    }
    return {
      icon: '📄',
      label: key.split('_').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' '),
    };
  }

  remove(key: MasterType) {
    this.uploadSvc.removeUpload(this.clientId, key).subscribe({
      next: () => this.refreshBatch(),  // batch totals need to shrink after a remove
      error: () => {},
    });
  }

  getInfo(key: MasterType) { return this.uploadSvc.getUpload(key); }
  isUploading(key: MasterType) { return this.uploadSvc.isUploading(key); }
  isUploaded(key: MasterType) { return this.uploadSvc.isUploaded(key); }
  uploadedCount() { return this.uploadSvc.uploadedCount(); }

  /** Friendly labels of required masters that still haven't been uploaded.
   *  Drives the "still missing: X, Y, Z" hint above the Save button so
   *  clients see exactly what they need to upload — instead of the
   *  backend silently committing a partial batch (old behavior) or
   *  failing with a cryptic FK violation (the only signal we used to
   *  give for missing parents). Walks the same `masters` config used
   *  to render the tiles so labels stay in sync automatically. */
  missingRequiredLabels(): string[] {
    const missing: string[] = [];
    for (const grp of this.masters) {
      for (const item of grp.items) {
        // "missing" only if required AND not staged in this batch AND not
        // already committed for the client (incremental top-ups don't re-need it).
        if (item.required && !this.uploadSvc.isUploaded(item.key) && !this.uploadSvc.isCommitted(item.key)) {
          missing.push(item.label.replace(/ Master$/, ''));
        }
      }
    }
    return missing;
  }

  /** Total count of required masters (the 2 feedback files are optional).
   *  Kept as a method so the hint reads from a single source of truth
   *  rather than a hardcoded number in the template. */
  requiredCount(): number {
    let n = 0;
    for (const grp of this.masters) {
      for (const item of grp.items) if (item.required) n++;
    }
    return n;
  }

  /** How many of the required masters are already uploaded. */
  requiredUploadedCount(): number {
    return this.requiredCount() - this.missingRequiredLabels().length;
  }

  formatSize(bytes: number): string {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(2) + ' MB';
  }
}

import { Component, OnInit, signal, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { UploadService } from '../services/upload.service';
import { AuthService } from '../services/auth.service';
import { MasterType } from '../models';

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
  imports: [CommonModule],
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
    }
  ];

  ngOnInit() {
    // Load any previously uploaded files for this session
    this.uploadSvc.loadUploads(this.clientId).subscribe({ error: () => {} });
    // Check if this client already has a pending batch waiting to commit
    this.refreshBatch();
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
      },
      error: (err) => console.error('Commit failed:', err.message ?? err),
    });
  }

  /** Discard the pending batch. Confirm first to avoid accidental data loss. */
  discardBatch() {
    if (!confirm('Discard all staged files? This cannot be undone.')) return;
    this.uploadSvc.discard(this.clientId).subscribe({
      next: () => {
        this.uploadSvc.loadUploads(this.clientId).subscribe({ error: () => {} });
      },
      error: (err) => console.error('Discard failed:', err.message ?? err),
    });
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
    this.uploadSvc.upload(this.clientId, key, file).subscribe({
      next: () => this.refreshBatch(),  // refresh batch panel so new file shows up
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
  formatSize(bytes: number): string {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(2) + ' MB';
  }
}

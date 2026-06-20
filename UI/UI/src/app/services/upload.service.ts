import { Injectable, inject, signal } from '@angular/core';
import { Observable, tap } from 'rxjs';
import { ApiService } from './api.service';
import { environment } from '../../environments/environment';
import {
  BatchInfoResponse,
  CommitResponse,
  DiscardResponse,
  MasterType,
  PendingBatch,
  UploadedFile,
  UploadPreview,
  UploadResponse,
  UploadStatus,
} from '../models';

@Injectable({ providedIn: 'root' })
export class UploadService {
  private api = inject(ApiService);

  /** Live map of per-master upload state */
  readonly uploads = signal<Record<MasterType, UploadedFile | null>>({
    customer: null, order: null, line_items: null,
    product: null, price: null, vendor_map: null,
    category: null, sub_category: null, sub_sub_category: null,
    brand: null, vendor: null,
    customer_reviews: null, support_tickets: null,
    login_event: null
  });

  /** The currently pending batch for the active client (null if none). */
  readonly pendingBatch = signal<PendingBatch | null>(null);

  /** Transient state flags so the UI can show spinners / disable buttons. */
  readonly committing = signal<boolean>(false);
  readonly discarding = signal<boolean>(false);
  readonly lastCommitError = signal<string | null>(null);

  /** Result of the most recent successful commit (null until first commit
   *  completes, or after dismissCommitResult() is called). The UI reads
   *  this to render a "✓ Committed N rows" banner. */
  readonly lastCommitResult = signal<CommitResponse | null>(null);

  upload(clientId: string, masterType: MasterType, file: File): Observable<UploadResponse> {
    // Immediately reflect uploading state
    this.setStatus(masterType, 'uploading', file.name, file.size);

    const fd = new FormData();
    fd.append('file', file);
    fd.append('clientId', clientId);
    fd.append('masterType', masterType);

    return this.api.upload<UploadResponse>(`/uploads/${masterType}`, fd).pipe(
      tap({
        next: res => this.setSuccess(masterType, res),
        error: err => this.setError(masterType, err?.error?.detail ?? err?.message ?? 'Upload failed. Please try again.')  // PA_012 fix: show backend error message
      })
    );
  }

  /** Load previously uploaded files for a client/session */
  loadUploads(clientId: string): Observable<UploadedFile[]> {
    return this.api.get<UploadedFile[]>(`/uploads?clientId=${clientId}`).pipe(
      tap(files => {
        // Rebuild from scratch so the map MIRRORS the backend's pending batch:
        // files the server no longer reports (e.g. a batch discarded on logout)
        // must be cleared, not left stale. GET /uploads is the source of truth.
        // (UploadService is a root singleton — without this, a logout→login in
        // the same SPA session would keep showing discarded uploads.)
        const map = this.emptyUploads();
        files.forEach(f => { map[f.masterType] = f; });
        this.uploads.set(map);
      })
    );
  }

  /** First-N-rows preview of a staged master file, so the user can see what
   *  they uploaded before saving. Backed by GET /uploads/preview. */
  preview(clientId: string, masterType: MasterType): Observable<UploadPreview> {
    return this.api.get<UploadPreview>(
      `/uploads/preview?clientId=${clientId}&masterType=${masterType}`);
  }

  /** Preview of SAVED (committed) master data, so after committing the user can
   *  see what was saved. Reads the real table (not staging), same shape as
   *  preview(). Backed by GET /uploads/saved-preview. */
  savedPreview(clientId: string, masterType: MasterType): Observable<UploadPreview> {
    return this.api.get<UploadPreview>(
      `/uploads/saved-preview?clientId=${clientId}&masterType=${masterType}`);
  }

  removeUpload(clientId: string, masterType: MasterType): Observable<void> {
    return this.api.delete<void>(`/uploads/${masterType}?clientId=${clientId}`).pipe(
      tap(() => {
        const map = { ...this.uploads() };
        map[masterType] = null;
        this.uploads.set(map);
      })
    );
  }

  getUpload(masterType: MasterType): UploadedFile | null {
    return this.uploads()[masterType];
  }

  isUploaded(masterType: MasterType): boolean {
    return this.uploads()[masterType]?.status === 'success';
  }

  isUploading(masterType: MasterType): boolean {
    return this.uploads()[masterType]?.status === 'uploading';
  }

  allRequiredUploaded(): boolean {
    const required: MasterType[] = ['customer','order','line_items','product','price','vendor_map','category','sub_category','sub_sub_category','brand','vendor'];
    return required.every(m => this.isUploaded(m));
  }

  uploadedCount(): number {
    return Object.values(this.uploads()).filter(u => u?.status === 'success').length;
  }

  /** Absolute URL of the downloadable sample CSV template for a master type.
   *  Backed by GET /api/v1/uploads/sample/{masterType}, which generates the
   *  template (the exact columns in order + example rows) from the backend's
   *  canonical schema — so it always matches what the upload validator expects.
   *  Rendered as an <a [href] download> on each upload tile. */
  sampleUrl(masterType: MasterType): string {
    return `${environment.apiUrl}/uploads/sample/${masterType}`;
  }

  // ── Batch lifecycle ──────────────────────────────────────────
  // These endpoints map to upload_router.py:
  //   GET  /uploads/batch     → get_pending_batch
  //   POST /uploads/commit    → commit_batch
  //   POST /uploads/discard   → discard_batch

  /**
   * Fetch the current pending batch for a client (if any). Updates the
   * pendingBatch signal so any component can react.
   */
  getBatchInfo(clientId: string): Observable<BatchInfoResponse> {
    return this.api.get<BatchInfoResponse>(`/uploads/batch?clientId=${clientId}`).pipe(
      tap(res => this.pendingBatch.set(res.pendingBatch))
    );
  }

  /**
   * Commit the pending batch. On success clears the staged upload map and
   * nulls pendingBatch. On failure captures the error for UI display.
   */
  commit(clientId: string): Observable<CommitResponse> {
    this.committing.set(true);
    this.lastCommitError.set(null);
    this.lastCommitResult.set(null);   // clear any prior success banner
    return this.api.post<CommitResponse>(`/uploads/commit?clientId=${clientId}`, {}).pipe(
      tap({
        next: (res) => {
          this.committing.set(false);
          this.pendingBatch.set(null);
          // Staged uploads no longer exist — reset local map so the UI
          // doesn't keep showing "uploaded" rows from the committed batch.
          this.clearUploads();
          // Surface the full response so the UI can render per-master row counts.
          this.lastCommitResult.set(res);
        },
        error: err => {
          this.committing.set(false);
          const msg = err?.error?.detail ?? err?.message ?? 'Commit failed. Please try again.';  // PA_012 fix: show backend error message
          this.lastCommitError.set(msg);
        },
      })
    );
  }

  /** Clear the success banner — called when the user dismisses it or
   *  starts a new upload. */
  dismissCommitResult(): void {
    this.lastCommitResult.set(null);
    this.lastCommitError.set(null);
  }

  /**
   * Discard the pending batch. Clears staged upload map and nulls
   * pendingBatch on success.
   */
  discard(clientId: string): Observable<DiscardResponse> {
    this.discarding.set(true);
    return this.api.post<DiscardResponse>(`/uploads/discard?clientId=${clientId}`, {}).pipe(
      tap({
        next: () => {
          this.discarding.set(false);
          this.pendingBatch.set(null);
          this.clearUploads();
        },
        error: () => {
          this.discarding.set(false);
        },
      })
    );
  }

  /** A fresh all-null upload map (every master type → null). */
  private emptyUploads(): Record<MasterType, UploadedFile | null> {
    return {
      customer: null, order: null, line_items: null,
      product: null, price: null, vendor_map: null,
      category: null, sub_category: null, sub_sub_category: null,
      brand: null, vendor: null,
      customer_reviews: null, support_tickets: null,
      login_event: null,
    };
  }

  /** Reset the staged upload map back to all-null (used after commit/discard). */
  private clearUploads(): void {
    this.uploads.set(this.emptyUploads());
  }

  private setStatus(type: MasterType, status: UploadStatus, fileName: string, size: number): void {
    this.uploads.update(map => ({
      ...map,
      [type]: { masterType: type, fileName, fileSize: size, rowCount: 0, uploadedAt: new Date().toISOString(), status }
    }));
  }

  private setSuccess(type: MasterType, res: UploadResponse): void {
    this.uploads.update(map => ({
      ...map,
      [type]: { masterType: type, fileName: res.fileName, fileSize: map[type]?.fileSize ?? 0, rowCount: res.rowCount, uploadedAt: res.uploadedAt, status: 'success' }
    }));
  }

  private setError(type: MasterType, msg: string): void {
    this.uploads.update(map => ({
      ...map,
      [type]: { ...map[type]!, status: 'error', errorMessage: msg }
    }));
  }
}
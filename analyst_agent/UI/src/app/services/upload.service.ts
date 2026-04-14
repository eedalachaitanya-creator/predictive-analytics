import { Injectable, inject, signal } from '@angular/core';
import { Observable, tap } from 'rxjs';
import { ApiService } from './api.service';
import { MasterType, UploadedFile, UploadResponse, UploadStatus } from '../models';

@Injectable({ providedIn: 'root' })
export class UploadService {
  private api = inject(ApiService);

  /** Live map of per-master upload state */
  readonly uploads = signal<Record<MasterType, UploadedFile | null>>({
    customer: null, order: null, line_items: null,
    product: null, price: null, vendor_map: null,
    category: null, sub_category: null, sub_sub_category: null,
    brand: null, vendor: null
  });

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
        error: err => this.setError(masterType, err.message)
      })
    );
  }

  /** Load previously uploaded files for a client/session */
  loadUploads(clientId: string): Observable<UploadedFile[]> {
    return this.api.get<UploadedFile[]>(`/uploads?clientId=${clientId}`).pipe(
      tap(files => {
        const map = { ...this.uploads() };
        files.forEach(f => { map[f.masterType] = f; });
        this.uploads.set(map);
      })
    );
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

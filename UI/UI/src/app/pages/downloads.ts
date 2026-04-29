import { Component, inject, signal, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { environment } from '../../environments/environment';
import { AuthService } from '../services/auth.service';

interface DownloadFile {
  filename: string;
  title: string;
  icon: string;
  desc: string;
  category: string;
  size: string;
  sizeBytes: number;
  lastModified: string;
  ready: boolean;
}

@Component({
  selector: 'app-downloads',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './downloads.html',
  styleUrls: ['./downloads.scss'],
})
export class DownloadsComponent implements OnInit {
  private http = inject(HttpClient);
  private auth = inject(AuthService);
  private base = environment.apiUrl;

  files = signal<DownloadFile[]>([]);
  loading = signal(true);
  error = signal<string | null>(null);
  lastRun = signal<string | null>(null);
  downloading = signal<string | null>(null);

  ngOnInit() {
    this.loadFiles();
  }

  loadFiles() {
    this.loading.set(true);
    this.error.set(null);
    this.http
      .get<{ files: DownloadFile[]; lastPipelineRun: string | null }>(
        `${this.base}/downloads?clientId=${this.auth.getClientId()}`
      )
      .subscribe({
        next: (res) => {
          this.files.set(res.files);
          this.lastRun.set(res.lastPipelineRun);
          this.loading.set(false);
        },
        error: (err) => {
          this.error.set('Could not load download list. Is the backend running?');
          this.loading.set(false);
        },
      });
  }

  // Audit fix 2026-04-29: download via HttpClient → blob, NOT a raw
  // <a href> click. Top-level browser navigation does NOT carry the
  // SPA's Authorization header, so the previous implementation broke
  // when /api/v1/downloads went behind router-level auth (every link
  // returned 401 "Authorization required"). HttpClient routes through
  // the auth interceptor, which attaches the Bearer token, then we
  // materialise the response as a Blob and trigger the download
  // through a synthesized object URL. No token-in-URL, no backend
  // changes, works with HttpOnly cookies if we ever switch.
  private blobDownload(url: string, downloadName: string, cleanupKey: string) {
    this.downloading.set(cleanupKey);
    this.http
      .get(url, { responseType: 'blob' })
      .subscribe({
        next: (blob: Blob) => {
          const objectUrl = URL.createObjectURL(blob);
          const a = document.createElement('a');
          a.href = objectUrl;
          a.download = downloadName;
          document.body.appendChild(a);
          a.click();
          document.body.removeChild(a);
          // Free the object URL after the click — browsers keep the
          // blob alive otherwise (memory leak across many downloads).
          URL.revokeObjectURL(objectUrl);
          this.downloading.set(null);
        },
        error: (err) => {
          this.error.set(`Download failed: ${err.status === 401 ? 'session expired — please log in again' : err.message || 'unknown error'}`);
          this.downloading.set(null);
        },
      });
  }

  download(filename: string) {
    const cid = this.auth.getClientId();
    this.blobDownload(
      `${this.base}/downloads/${filename}?clientId=${cid}`,
      filename,
      filename,
    );
  }

  downloadZip() {
    const cid = this.auth.getClientId();
    this.blobDownload(
      `${this.base}/downloads/zip/all?clientId=${cid}`,
      'CRP_ML_Reports.zip',
      'zip',
    );
  }

  formatDate(iso: string): string {
    if (!iso) return '';
    const d = new Date(iso);
    return d.toLocaleDateString('en-US', {
      year: 'numeric', month: 'short', day: 'numeric',
      hour: '2-digit', minute: '2-digit',
    });
  }
}

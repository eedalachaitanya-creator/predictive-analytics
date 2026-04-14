import { Component, inject, signal, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { environment } from '../../environments/environment';

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
        `${this.base}/downloads?clientId=${environment.clientId}`
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

  download(filename: string) {
    this.downloading.set(filename);
    const a = document.createElement('a');
    a.href = `${this.base}/downloads/${filename}`;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    // Reset after a short delay to allow browser to start the download
    setTimeout(() => this.downloading.set(null), 1500);
  }

  downloadZip() {
    this.downloading.set('zip');
    const a = document.createElement('a');
    a.href = `${this.base}/downloads/zip/all`;
    a.download = 'CRP_ML_Reports.zip';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(() => this.downloading.set(null), 2000);
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

import { Component, OnInit, signal, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ApiService } from '../services/api.service';
import { environment } from '../../environments/environment';

interface ValidationFile {
  n: number;
  masterType: string;
  name: string;
  group: string;
  rows: number;
  cols: number;
  missing: number;
  missingDetails: { column: string; nullCount: number }[];
  dup: number;
  dateErrors: number;
  status: string;
}

interface ValidationSummary {
  totalMasters: number;
  uploaded: number;
  passed: number;
  warnings: number;
  errors: number;
}

interface ColumnDetail {
  col: string;
  type: string;
  nonNull: string;
  unique: number;
  sample: string;
  req: boolean;
  nullCount: number;
  status: string;
}

interface ValidationDetailResponse {
  masterType: string;
  label: string;
  totalRows: number;
  columns: ColumnDetail[];
}

@Component({
  selector: 'app-validation',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './validation.html',
  styleUrls: ['./validation.scss'],
})
export class ValidationComponent implements OnInit {
  private api = inject(ApiService);
  private clientId = environment.clientId;

  // ── Page-level state ────────────────────────────────────────────
  loading     = signal(false);
  error       = signal<string | null>(null);

  summary     = signal<ValidationSummary | null>(null);
  files       = signal<ValidationFile[]>([]);

  // ── Column detail state ─────────────────────────────────────────
  selectedFile     = signal<ValidationFile | null>(null);
  detailLoading    = signal(false);
  detailLabel      = signal('');
  detailTotalRows  = signal(0);
  cols             = signal<ColumnDetail[]>([]);

  // ── Warnings list (computed from files with issues) ─────────────
  warnings = signal<string[]>([]);

  ngOnInit() {
    this.runValidation();
  }

  runValidation() {
    this.loading.set(true);
    this.error.set(null);
    this.selectedFile.set(null);
    this.cols.set([]);

    this.api.get<{ summary: ValidationSummary; files: ValidationFile[] }>(
      `/validation?clientId=${this.clientId}`
    ).subscribe({
      next: (res) => {
        this.summary.set(res.summary);
        this.files.set(res.files);
        this.loading.set(false);

        // Build warning messages from missingDetails
        const warns: string[] = [];
        for (const f of res.files) {
          if (f.missingDetails && f.missingDetails.length > 0) {
            for (const d of f.missingDetails) {
              warns.push(
                `${d.nullCount} rows in ${f.name} have missing ${d.column}`
              );
            }
          }
          if (f.dup > 0) {
            warns.push(
              `${f.dup} duplicate primary keys found in ${f.name}`
            );
          }
          if (f.dateErrors > 0) {
            warns.push(
              `${f.dateErrors} date column issues in ${f.name}`
            );
          }
        }
        this.warnings.set(warns);

        // Auto-select first file for column detail
        if (res.files.length > 0) {
          this.selectFile(res.files[0]);
        }
      },
      error: (e) => {
        this.error.set(e.message || 'Failed to load validation data');
        this.loading.set(false);
      },
    });
  }

  selectFile(file: ValidationFile) {
    this.selectedFile.set(file);
    this.detailLoading.set(true);
    this.cols.set([]);

    this.api.get<ValidationDetailResponse>(
      `/validation/${file.masterType}?clientId=${this.clientId}`
    ).subscribe({
      next: (res) => {
        this.detailLabel.set(res.label);
        this.detailTotalRows.set(res.totalRows);
        this.cols.set(res.columns);
        this.detailLoading.set(false);
      },
      error: () => {
        this.detailLoading.set(false);
      },
    });
  }
}

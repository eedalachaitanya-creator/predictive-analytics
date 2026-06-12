import { Component, OnInit, computed, signal, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ApiService } from '../services/api.service';
import { AuthService } from '../services/auth.service';

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
  empty?: boolean;   // true when the master has zero rows (no upload yet)
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

/**
 * Humanize a raw DB column name for display — customer_id -> 'Customer ID',
 * order_value_usd -> 'Order Value USD'. Mirrors clients/upload formatColumnName
 * so no table in the app shows raw snake_case. Exported for unit tests.
 */
export function humanizeColumnName(col: string): string {
  if (!col) return col;
  const acronyms = new Set(['id', 'usd', 'rfm', 'ltv', 'sku', 'api', 'url', 'csv', 'db', 'pv']);
  return col
    .split('_')
    .map(w => !w ? w
      : acronyms.has(w.toLowerCase()) ? w.toUpperCase()
      : w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
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
  private auth = inject(AuthService);
  private clientId = this.auth.getClientId();

  /** Template helper — humanize a snake_case column name for display. */
  formatColumnName = humanizeColumnName;

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

  // ── Status filter — drives which rows the summary table shows ───
  // The four stat tiles at the top act as filter tabs: clicking them
  // narrows the Data Validation Summary to tables in that state.
  //   'all'   → every uploaded table
  //   'ok'    → only tables that passed cleanly
  //   'warn'  → only tables with warnings (nulls in required cols)
  //   'error' → only tables with errors (duplicate primary keys)
  statusFilter = signal<'all' | 'ok' | 'warn' | 'error'>('all');

  visibleFiles = computed<ValidationFile[]>(() => {
    const f = this.statusFilter();
    const all = this.files();
    return f === 'all' ? all : all.filter(x => x.status === f);
  });

  setStatusFilter(f: 'all' | 'ok' | 'warn' | 'error') {
    this.statusFilter.set(f);
    // Keep the column-detail panel in sync with the filter: if the
    // currently-selected file is no longer visible, slide to the first
    // file in the new view (or clear if the view is empty).
    const sel = this.selectedFile();
    const visible = this.visibleFiles();
    if (sel && !visible.some(v => v.masterType === sel.masterType)) {
      if (visible.length > 0) this.selectFile(visible[0]);
      else this.selectedFile.set(null);
    }
  }

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
                `${d.nullCount} rows in ${f.name} have missing ${humanizeColumnName(d.column)}`
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

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

/** The View popup's payload — the master table's ACTUAL rows (generic
 *  columns/rows table shape, server-paginated, like the clients data-viewer). */
interface TableRows {
  masterType: string;
  label: string;
  table: string;
  columns: string[];
  rows: Record<string, unknown>[];
  total: number;
  offset: number;
  limit: number;
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

  // ── Data-viewer modal state ─────────────────────────────────────
  // detailFile != null ⇒ the popup is open. It shows the table's ACTUAL rows,
  // server-paginated (100/page) like the clients / dashboard data-viewers.
  detailFile    = signal<ValidationFile | null>(null);
  detailLoading = signal(false);
  detailLabel   = signal('');
  viewData      = signal<TableRows | null>(null);
  viewError     = signal('');
  viewOffset    = signal(0);
  readonly viewLimit = 100;

  viewHasPrev = computed(() => this.viewOffset() > 0);
  viewHasNext = computed(() => {
    const d = this.viewData();
    return !!d && (this.viewOffset() + this.viewLimit) < d.total;
  });

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
  }

  ngOnInit() {
    this.runValidation();
  }

  runValidation() {
    this.loading.set(true);
    this.error.set(null);
    this.closeDetail();

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
      },
      error: (e) => {
        this.error.set(e.message || 'Failed to load validation data');
        this.loading.set(false);
      },
    });
  }

  /** Open the popup for a table and load its ACTUAL rows (page 1). */
  openDetail(file: ValidationFile) {
    this.detailFile.set(file);
    this.detailLabel.set(file.name);
    this.viewOffset.set(0);
    this.viewData.set(null);
    this.viewError.set('');
    this.fetchRows();
  }

  private fetchRows() {
    const f = this.detailFile();
    if (!f) return;
    this.detailLoading.set(true);
    this.viewError.set('');
    this.api.get<TableRows>(
      `/validation/${f.masterType}/rows?clientId=${this.clientId}&limit=${this.viewLimit}&offset=${this.viewOffset()}`
    ).subscribe({
      next: (d) => { this.viewData.set(d); this.detailLoading.set(false); },
      error: (err) => {
        this.detailLoading.set(false);
        this.viewError.set(err?.error?.detail ?? err?.message ?? 'Could not load data.');
      },
    });
  }

  closeDetail() {
    this.detailFile.set(null);
    this.viewData.set(null);
    this.viewError.set('');
    this.viewOffset.set(0);
    this.detailLoading.set(false);
  }

  nextPage() {
    if (!this.viewHasNext()) return;
    this.viewOffset.set(this.viewOffset() + this.viewLimit);
    this.fetchRows();
  }

  prevPage() {
    if (!this.viewHasPrev()) return;
    this.viewOffset.set(Math.max(0, this.viewOffset() - this.viewLimit));
    this.fetchRows();
  }

  /** "start–end of total" label for the modal footer. */
  paginationLabel(): string {
    const d = this.viewData();
    if (!d || d.total === 0) return 'No rows';
    const start = d.offset + 1;
    const end = Math.min(d.offset + d.rows.length, d.total);
    return `${start}–${end} of ${d.total.toLocaleString('en-US')}`;
  }

  /** Display one cell value: dates → readable, ints → grouped, null → —. */
  renderCell(value: unknown): string {
    if (value === null || value === undefined) return '—';
    if (typeof value === 'boolean') return value ? '✓' : '—';
    if (typeof value === 'number') {
      return Number.isInteger(value) ? value.toLocaleString('en-US') : String(Number(value.toFixed(4)));
    }
    if (typeof value === 'string') {
      if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(value)) {
        const dt = new Date(value);
        return isNaN(dt.getTime()) ? value
          : dt.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
      }
      return value;
    }
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
  }
}

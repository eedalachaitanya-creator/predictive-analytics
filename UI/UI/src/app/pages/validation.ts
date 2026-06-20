import { Component, OnInit, computed, signal, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ApiService } from '../services/api.service';
import { AuthService } from '../services/auth.service';
import { paginate } from './paginate';

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

  // ── Column-detail modal state ───────────────────────────────────
  // detailFile != null ⇒ the popup is open for that table. The column rows
  // are paginated client-side (paginate()) so the modal matches the paging
  // UX used elsewhere in the app, rather than rendering inline below the
  // table.
  detailFile       = signal<ValidationFile | null>(null);
  detailLoading    = signal(false);
  detailLabel      = signal('');
  detailTotalRows  = signal(0);
  cols             = signal<ColumnDetail[]>([]);

  detailPage           = signal(1);
  readonly detailPageSize = 10;

  // The current page of column rows + the resolved page/total-page count.
  pagedCols = computed(() => paginate(this.cols(), this.detailPage(), this.detailPageSize));

  detailHasPrev = computed(() => this.pagedCols().page > 1);
  detailHasNext = computed(() => this.pagedCols().page < this.pagedCols().totalPages);

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

  /** Open the column-detail popup for a table and load its columns. */
  openDetail(file: ValidationFile) {
    this.detailFile.set(file);
    this.detailLabel.set(file.name);
    this.detailTotalRows.set(file.rows);
    this.detailPage.set(1);
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

  closeDetail() {
    this.detailFile.set(null);
    this.cols.set([]);
    this.detailPage.set(1);
    this.detailLoading.set(false);
  }

  detailNextPage() {
    if (this.detailHasNext()) this.detailPage.set(this.pagedCols().page + 1);
  }

  detailPrevPage() {
    if (this.detailHasPrev()) this.detailPage.set(this.pagedCols().page - 1);
  }

  /** "start–end of total-columns" label for the modal footer. */
  detailPaginationLabel(): string {
    const total = this.cols().length;
    if (total === 0) return 'No columns';
    const p = this.pagedCols();
    const start = (p.page - 1) * this.detailPageSize + 1;
    const end = Math.min(start + p.slice.length - 1, total);
    return `${start}–${end} of ${total} columns`;
  }
}

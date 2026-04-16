import { Injectable, inject, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, catchError, throwError } from 'rxjs';

// ── Scout API base — runs separately from the main app backend ──────
const SCOUT_API = 'http://localhost:8000';

// ── Models ──────────────────────────────────────────────────────────

export interface ScoutPrice {
  value: number;
  currency: string;
  raw: string | null;
}

export interface ProductDetails {
  manufacturer: string;
  marketed_by: string;
  country_of_origin?: string;
  description: string;
  specifications: Record<string, string>;
  availability: string;
  ingredients?: string;
}

export interface Listing {
  platform: string;
  title: string;
  price: ScoutPrice;
  url: string;
  availability: string;
  product_details: ProductDetails;
  last_updated: string;
}

export interface SearchResult {
  name: string;
  listings: Listing[];
  platform_status?: Record<string, string>;
  entities?: EntityGroup[];
}

export interface SearchResponse {
  status: string;
  products: SearchResult[];
}

export interface Website {
  name: string;
  base_url: string;
  search_url: string;
  encoding: string;
  active: boolean;
}

export interface PriceAlert {
  id: number;
  product_name: string;
  platform: string;
  old_price: number | null;
  new_price: number;
  change_pct: number | null;
  created_at: string;
}

export interface PricePoint {
  price: number;
  currency: string;
  scraped_at: string;
  platform: string;
}

export interface EntityGroup {
  entity_id: string;
  listings: Listing[];
}

export interface CompareResult {
  query: string;
  entities: EntityGroup[];
  unmatched: Listing[];
}

export interface MonitorResult {
  status: string;
  products_checked: number;
  alerts_generated: number;
}

// ── Service ─────────────────────────────────────────────────────────

@Injectable({ providedIn: 'root' })
export class ScoutService {
  private http = inject(HttpClient);

  // Reactive state
  websites    = signal<Website[]>([]);
  searching   = signal(false);
  monitoring  = signal(false);

  // ── Search ──────────────────────────────────────────────────────

  searchProducts(name: string, platforms: string[] = [], forceRefresh = false): Observable<SearchResponse> {
    return this.http.post<SearchResponse>(`${SCOUT_API}/search/products`, {
      name, platforms, force_refresh: forceRefresh
    }).pipe(catchError(this.handleError));
  }

  searchBulk(names: string[], platforms: string[] = []): Observable<SearchResponse> {
    return this.http.post<SearchResponse>(`${SCOUT_API}/search/bulk`, {
      names, platforms
    }).pipe(catchError(this.handleError));
  }

  uploadBulk(file: File, platforms: string[] = []): Observable<SearchResponse> {
    const formData = new FormData();
    formData.append('file', file);
    if (platforms.length) {
      formData.append('platforms', JSON.stringify(platforms));
    }
    return this.http.post<SearchResponse>(`${SCOUT_API}/upload/file`, formData)
      .pipe(catchError(this.handleError));
  }

  // ── Compare ─────────────────────────────────────────────────────

  compareProducts(query: string): Observable<CompareResult> {
    return this.http.get<CompareResult>(`${SCOUT_API}/compare/${encodeURIComponent(query)}`)
      .pipe(catchError(this.handleError));
  }

  // ── Price History & Alerts ──────────────────────────────────────

  getPriceHistory(query: string): Observable<{ product: string; history: PricePoint[] }> {
    return this.http.get<{ product: string; history: PricePoint[] }>(
      `${SCOUT_API}/price-history/${encodeURIComponent(query)}`
    ).pipe(catchError(this.handleError));
  }

  getAlerts(unreadOnly = false): Observable<{ alerts: PriceAlert[] }> {
    const q = unreadOnly ? '?unread=true' : '';
    return this.http.get<{ alerts: PriceAlert[] }>(`${SCOUT_API}/alerts${q}`)
      .pipe(catchError(this.handleError));
  }

  // ── Monitor ─────────────────────────────────────────────────────

  runMonitor(): Observable<MonitorResult> {
    return this.http.post<MonitorResult>(`${SCOUT_API}/monitor/run`, {})
      .pipe(catchError(this.handleError));
  }

  // ── Websites / Platforms ────────────────────────────────────────

  loadWebsites(): Observable<{ data: Website[] }> {
    return this.http.get<{ data: Website[] }>(`${SCOUT_API}/websites/all`)
      .pipe(catchError(this.handleError));
  }

  getActivePlatforms(): Observable<{ platforms: string[] }> {
    return this.http.get<{ platforms: string[] }>(`${SCOUT_API}/websites`)
      .pipe(catchError(this.handleError));
  }

  addWebsite(name: string): Observable<{ data: Website }> {
    return this.http.post<{ data: Website }>(`${SCOUT_API}/websites`, { name })
      .pipe(catchError(this.handleError));
  }

  updateWebsite(payload: Partial<Website> & { name: string }): Observable<{ data: Website }> {
    return this.http.put<{ data: Website }>(`${SCOUT_API}/websites`, payload)
      .pipe(catchError(this.handleError));
  }

  deactivateWebsite(name: string): Observable<any> {
    return this.http.post(`${SCOUT_API}/websites/${encodeURIComponent(name)}/deactivate`, {})
      .pipe(catchError(this.handleError));
  }

  reactivateWebsite(name: string): Observable<any> {
    return this.http.post(`${SCOUT_API}/websites/${encodeURIComponent(name)}/reactivate`, {})
      .pipe(catchError(this.handleError));
  }

  deleteWebsite(name: string): Observable<any> {
    return this.http.delete(`${SCOUT_API}/websites/${encodeURIComponent(name)}`)
      .pipe(catchError(this.handleError));
  }

  // ── All Products ────────────────────────────────────────────────

  getAllProducts(): Observable<{ data: any[]; platforms: string[] }> {
    return this.http.get<{ data: any[]; platforms: string[] }>(`${SCOUT_API}/products`)
      .pipe(catchError(this.handleError));
  }

  // ── Error Handler ───────────────────────────────────────────────

  private handleError(err: any): Observable<never> {
    const msg = err?.error?.detail ?? err?.error?.message ?? err?.message ?? 'Scout API error';
    console.error('[Scout]', err.status, msg);
    return throwError(() => ({ status: err.status, message: msg }));
  }
}
import { Injectable, inject, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, catchError, tap, throwError } from 'rxjs';

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
  change_amount: number | null;
  change_percent: number | null;
  direction: string;
  url: string;
  detected_at: string;
  acknowledged: boolean;
}

export interface PricePoint {
  price: number;
  currency: string;
  scraped_at: string;
  platform: string;
}

export interface ComparePlatformEntry {
  platform: string;
  price: number;
  url: string;
}

export interface ComparableEntity {
  entity_id: string;
  product: string;
  brand: string;
  variant: string;
  cheapest: { platform: string; price: number } | null;
  price_spread: { min: number; max: number; diff_percent: number; savings: number } | null;
  platforms: ComparePlatformEntry[];
}

export interface SinglePlatformEntity {
  entity_id: string;
  product: string;
  platform: string;
  price: { platform: string; price: number } | null;
}

export interface CompareResult {
  query: string;
  comparable: ComparableEntity[];
  single_platform: SinglePlatformEntity[];
  summary: { total_entities: number; cross_platform: number; single_platform: number; best_savings: number };
}

export interface MonitorResult {
  status: string;
  products_checked: number;
  alerts_generated: number;
}

// ── Chat agent models ───────────────────────────────────────────────

export interface AgentChatResponse {
  session_id: string;
  message: string;
  response: string;
}

export interface AgentSessionDeleteResponse {
  status: string;
  session_id: string;
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
   
  /** Re-fetch the websites list from the server and update the signal. */
  refreshPlatforms(): Observable<{ data: Website[] }> {
    const obs = this.http.get<{ data: Website[] }>(`${SCOUT_API}/websites/all`)
      .pipe(catchError(this.handleError));
    obs.subscribe({
      next: res => this.websites.set(res.data || []),
      // On error we leave the existing signal as-is. Components handle errors
      // at the call site via the observable they subscribe to themselves.
      error: () => {},
    });
    return obs;
  }

  /**
   * Extract just the active platform names from the current websites signal.
   * Used by Search and Monitor components for their platform toggle UIs.
   */
  activePlatformNames(): string[] {
    return this.websites().filter(w => w.active).map(w => w.name);
  }


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
      formData.append('platforms', platforms.join(','));
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

  getPriceHistory(query: string): Observable<{ product_name: string; platforms: Record<string, PricePoint[]>; total_points: number }> {
    return this.http.get<{ product_name: string; platforms: Record<string, PricePoint[]>; total_points: number }>(
      `${SCOUT_API}/price-history/${encodeURIComponent(query)}`
    ).pipe(catchError(this.handleError));
  }

    getAlerts(
      opts: { unreadOnly?: boolean; limit?: number; offset?: number } = {}
    ): Observable<{ unread_count: number; total: number; alerts: PriceAlert[] }> {
    const params: string[] = [];
    if (opts.unreadOnly) params.push('unacknowledged_only=true');
    if (opts.limit != null)  params.push(`limit=${opts.limit}`);
    if (opts.offset != null) params.push(`offset=${opts.offset}`);
    const q = params.length ? `?${params.join('&')}` : '';
    return this.http.get<{ unread_count: number; total: number; alerts: PriceAlert[] }>(
      `${SCOUT_API}/alerts${q}`
    ).pipe(catchError(this.handleError));
  }


  // ── Monitor ─────────────────────────────────────────────────────

  runMonitor(): Observable<MonitorResult> {
    return this.http.post<MonitorResult>(`${SCOUT_API}/price-monitor/run`, {})
      .pipe(catchError(this.handleError));
  }

  // ── Websites / Platforms ────────────────────────────────────────
  
  /**
 * @deprecated kept for any legacy caller; prefer refreshPlatforms() which
 * returns the same data AND updates the shared signal.
 */
  loadWebsites(): Observable<{ data: Website[] }> {
    return this.refreshPlatforms();
  }

  getActivePlatforms(): Observable<{ platforms: string[] }> {
    return this.http.get<{ platforms: string[] }>(`${SCOUT_API}/websites`)
      .pipe(catchError(this.handleError));
  }

  addWebsite(name: string): Observable<{ data: Website }> {
    return this.http.post<{ data: Website }>(`${SCOUT_API}/websites`, { name }).pipe(
      catchError(this.handleError),
      // Refresh the shared signal on success so all components see the new
      // platform appear immediately.
      tap(() => this.refreshPlatforms()),
    );
  }

  updateWebsite(payload: Partial<Website> & { name: string }): Observable<{ data: Website }> {
    return this.http.put<{ data: Website }>(`${SCOUT_API}/websites`, payload).pipe(
      catchError(this.handleError),
      tap(() => this.refreshPlatforms()),
    );
  }

  deactivateWebsite(site: Website): Observable<any> {
    return this.http.put(`${SCOUT_API}/websites`, {
      name: site.name,
      search_url: site.search_url,
      base_url: site.base_url,
      active: false,
    }).pipe(
      catchError(this.handleError),
      tap(() => this.refreshPlatforms()),
    );
  }

  reactivateWebsite(name: string): Observable<any> {
    return this.http.post(`${SCOUT_API}/websites/${encodeURIComponent(name)}/reactivate`, {}).pipe(
      catchError(this.handleError),
      tap(() => this.refreshPlatforms()),
    );
  }

  deleteWebsite(name: string): Observable<any> {
    return this.http.delete(`${SCOUT_API}/websites/${encodeURIComponent(name)}`).pipe(
      catchError(this.handleError),
      tap(() => this.refreshPlatforms()),
    );
  }

  // ── All Products ────────────────────────────────────────────────

  getAllProducts(
    opts: { limit?: number; offset?: number } = {}
    ): Observable<{ data: any[]; platforms: string[]; total: number }> {
      const params: string[] = [];
      if (opts.limit != null)  params.push(`limit=${opts.limit}`);
      if (opts.offset != null) params.push(`offset=${opts.offset}`);
      const q = params.length ? `?${params.join('&')}` : '';
      return this.http.get<{ data: any[]; platforms: string[]; total: number }>(
        `${SCOUT_API}/products${q}`
      ).pipe(catchError(this.handleError));
    }

  // ── Chat Agent ──────────────────────────────────────────────────
  //
  // LangGraph ReAct agent mounted at /agent/* on the same Scout backend.
  // Maintains per-session conversation memory keyed by session_id.
  // The UI generates its own session_id on mount (see chat.ts) and reuses
  // it across sends in the same chat. "New Chat" creates a fresh session.

  agentChat(message: string, sessionId: string): Observable<AgentChatResponse> {
    return this.http.post<AgentChatResponse>(`${SCOUT_API}/agent/chat`, {
      message, session_id: sessionId
    }).pipe(catchError(this.handleError));
  }

  agentDeleteSession(sessionId: string): Observable<AgentSessionDeleteResponse> {
    return this.http.delete<AgentSessionDeleteResponse>(
      `${SCOUT_API}/agent/session/${encodeURIComponent(sessionId)}`
    ).pipe(catchError(this.handleError));
  }

  // ── Error Handler ───────────────────────────────────────────────

  private handleError(err: any): Observable<never> {
    const msg = err?.error?.detail ?? err?.error?.message ?? err?.message ?? 'Scout API error';
    console.error('[Scout]', err.status, msg);
    return throwError(() => ({ status: err.status, message: msg }));
  }
}
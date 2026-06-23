import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable, throwError, of } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { environment } from '../../environments/environment';

const BASE = environment.apiUrl.replace('/api/v1', '');

function headers(): HttpHeaders {
  const token = sessionStorage.getItem('wap_token');
  return new HttpHeaders({
    'Content-Type': 'application/json',
    ...(token ? { Authorization: `Bearer ${token}` } : {})
  });
}

// ── Request / Response models ────────────────────────────────────────────────

export interface ScoutPrice    { value: number; currency: string; }
export interface ScoutSource   { type: string; confidence: number; }
export interface ScoutListing  { platform: string; price: ScoutPrice; availability: string; source: ScoutSource; }
export interface ScoutProduct  { name: string; listings: ScoutListing[]; }
export interface ScoutOutput   { status: string; products: ScoutProduct[]; }

export interface ChurnScore {
  client_id:            string;
  customer_id:          string;
  churn_probability:    number;
  risk_level:           string;
  customer_tier:        string;
  total_spend_usd:      number;
  total_orders:         number;
  avg_order_value_usd:  number;
  avg_rating:           number;
  days_since_last_order:number;
  is_high_value:        number;
  rfm_total_score:      number;
}

export interface ChurnBatch { total_customers: number; scores: ChurnScore[]; }

export interface StrategistRequest {
  scout_output:      ScoutOutput;
  our_costs:         Record<string, number>;
  client_id:         string;
  churn_batch?:      ChurnBatch;
  target_margin_pct?: number;
  min_margin_pct?:    number;
  undercut_pct?:      number;
  currency?:          string;
  skip_churn?:        boolean;
  client_priority?:   string | null;
  customer_segment?:  string | null;
}

export interface ChurnContext {
  customer_id:       string;
  churn_probability: number;
  risk_level:        string;
  customer_tier:     string;
  discount_applied:  number;
  discount_reason:   string;
}

export interface PlatformPrice {
  platform:     string;
  price:        number;
  availability?: string;
  confidence?:   number;
  url?:          string;
}

export interface PricingRecommendation {
  product_name:        string;
  strategy:            string;
  suggested_price:     number;
  pre_retention_price: number;
  floor_price:         number;
  target_price:        number;
  our_cost:            number;
  raw_cogs?:           number;
  competitor_min:      number;
  competitor_avg:      number;
  competitor_max:      number;
  competitor_median?:  number;
  platform_breakdown?: PlatformPrice[];
  margin_percent:      number;
  market_trend:        string;
  confidence:          string;
  flag:                string;
  reasoning:           string;
  churn_context?:      ChurnContext | null;
}

export interface StrategistResponse {
  run_id:            string;
  client_id:         string;
  status:            string;
  recommendations:   PricingRecommendation[];
  retention_count:   number;
  total_products:    number;
  elapsed_seconds:   number;
  currency:          string;   // ISO code of all prices in this response
}

export interface MarketTrend   { product_name: string; trend: string; }
export interface SampleRequest { scout_output: ScoutOutput; client_id: string; }

export interface PriceContext {
  context_id:           number;
  customer_id:          string;
  client_id:            string;
  product_name:         string;
  strategy:             string;
  suggested_price:      number;
  pre_retention_price:  number;
  discount_pct_applied: number;
  churn_probability:    number;
  risk_tier:            string;
  run_id:               string;
  created_at:           string;
}

@Injectable({ providedIn: 'root' })
export class StrategistService {
  private http = inject(HttpClient);

  recommend(req: StrategistRequest): Observable<StrategistResponse> {
    return this.http.post<StrategistResponse>(`${BASE}/api/strategist/recommend`, req, { headers: headers() })
      .pipe(catchError(e => throwError(() => e)));
  }

  getSampleRequest(clientId: string): Observable<SampleRequest> {
    return this.http.get<SampleRequest>(`${BASE}/api/strategist/sample-request?client_id=${clientId}`, { headers: headers() })
      .pipe(catchError(e => throwError(() => e)));
  }

  getMarketTrend(productName: string): Observable<MarketTrend> {
    return this.http.get<MarketTrend>(`${BASE}/api/strategist/market-trend/${encodeURIComponent(productName)}`, { headers: headers() })
      .pipe(catchError(e => throwError(() => e)));
  }

  validateChurn(batch: ChurnBatch): Observable<any> {
    return this.http.post<any>(`${BASE}/api/strategist/ingest-churn`, batch, { headers: headers() })
      .pipe(catchError(e => throwError(() => e)));
  }

  getPriceContexts(clientId: string): Observable<any> {
    return this.http.get<any>(`${BASE}/api/db/price-contexts?client_id=${clientId}`, { headers: headers() })
      .pipe(catchError(e => throwError(() => e)));
  }

  saveCosts(clientId: string, costs: Record<string, number>): Observable<any> {
    return this.http.post<any>(`${BASE}/api/db/product-costs`, { client_id: clientId, costs }, { headers: headers() })
      .pipe(catchError(e => throwError(() => e)));
  }

  getCosts(clientId: string): Observable<any> {
    return this.http.get<any>(`${BASE}/api/db/product-costs?client_id=${clientId}`, { headers: headers() })
      .pipe(catchError(e => throwError(() => e)));
  }

  getPipelineStats(): Observable<any> {
    return this.http.get<any>(`${BASE}/api/strategist/costs`, { headers: headers() })
      .pipe(catchError(e => throwError(() => e)));
  }

  searchProducts(clientId: string, q: string): Observable<{ count: number; products: { name: string; sku: string; saved_cost: number }[] }> {
    const qPart = q ? `?q=${encodeURIComponent(q)}&limit=10&client_id=${encodeURIComponent(clientId)}` : `?limit=20&client_id=${encodeURIComponent(clientId)}`;
    return this.http.get<any>(`${BASE}/api/db/scout-products${qPart}`, { headers: headers() })
      .pipe(catchError(() => of({ count: 0, products: [] })));
  }

  getClientConfig(clientId: string): Observable<any> {
    return this.http.get<any>(`${BASE}/api/db/client-config/${clientId}`, { headers: headers() })
      .pipe(catchError(() => of({ currency: 'INR' })));
  }

  getDefaults(): Observable<Record<string, number>> {
    return this.http.get<Record<string, number>>(`${BASE}/api/strategist/defaults`, { headers: headers() })
      .pipe(catchError(() => of({})));
  }

  getPriceHistoryProducts(q: string): Observable<any> {
    return this.http.get<any>(
      `${BASE}/api/db/price-history-products?q=${encodeURIComponent(q)}&limit=10`,
      { headers: headers() }
    ).pipe(catchError(e => throwError(() => e)));
  }

  /**
   * PA-046 fix: fetch product names scoped to this client_id from the
   * `products` table in Analyst DB. Returns empty list for new clients
   * who haven't uploaded data yet — never leaks global demo products.
   * Used by Market Trends "Your products" chip section.
   */
  getClientProducts(clientId: string, limit = 20): Observable<{ count: number; products: string[] }> {
    return this.http.get<{ count: number; products: string[] }>(
      `${BASE}/api/db/client-products?client_id=${encodeURIComponent(clientId)}&limit=${limit}`,
      { headers: headers() }
    ).pipe(catchError(() => of({ count: 0, products: [] })));
  }
}
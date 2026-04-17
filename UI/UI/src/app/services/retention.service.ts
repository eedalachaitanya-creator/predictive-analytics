import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable, throwError } from 'rxjs';
import { catchError } from 'rxjs/operators';

const BASE = 'http://localhost:8000';

function headers(): HttpHeaders {
  const token = localStorage.getItem('wap_token');
  return new HttpHeaders({
    'Content-Type': 'application/json',
    ...(token ? { Authorization: `Bearer ${token}` } : {})
  });
}

export interface RetentionRequest {
  client_id: string;
  dry_run:   boolean;
  min_risk?: 'HIGH' | 'MEDIUM';
}

export interface Intervention {
  intervention_id:    number;
  client_id:          string;
  customer_id:        string;
  created_at:         string;
  churn_probability:  number;
  risk_tier:          string;
  offer_type:         string;
  discount_pct:       number;
  offer_message:      string;
  channel:            string;
  customer_ltv_usd:   number;
  max_allowed_discount: number;
  guardrail_passed:   boolean;
  escalated_to_human: boolean;
  offer_status:       string;
  outcome_recorded_at:string | null;
  revenue_recovered:  number | null;
  langfuse_trace_id:  string | null;
  agent_cost_usd:     number | null;
}

export interface RetentionSummary {
  client_id:              string;
  total_interventions:    number;
  high_risk_count:        number;
  medium_risk_count:      number;
  escalated_count:        number;
  accepted_count:         number;
  declined_count:         number;
  no_response_count:      number;
  conversion_rate_pct:    number;
  total_revenue_recovered:number;
  avg_discount_pct:       number;
}

export interface RetentionResponse {
  run_id:        string;
  client_id:     string;
  dry_run:       boolean;
  summary:       any;
  interventions: Intervention[];
}

export interface OutcomeRequest {
  intervention_id:  number;
  offer_status:     'accepted' | 'declined' | 'no_response' | 'bounced';
  revenue_recovered?: number;
}

@Injectable({ providedIn: 'root' })
export class RetentionService {
  private http = inject(HttpClient);

  run(req: RetentionRequest): Observable<RetentionResponse> {
    return this.http.post<RetentionResponse>(`${BASE}/api/retention/run`, req, { headers: headers() })
      .pipe(catchError(e => throwError(() => e)));
  }

  getEscalations(clientId: string): Observable<any> {
    return this.http.get<any>(`${BASE}/api/retention/escalations?client_id=${clientId}`, { headers: headers() })
      .pipe(catchError(e => throwError(() => e)));
  }

  getSummary(clientId: string): Observable<RetentionSummary> {
    return this.http.get<RetentionSummary>(`${BASE}/api/retention/summary/${clientId}`, { headers: headers() })
      .pipe(catchError(e => throwError(() => e)));
  }

  recordOutcome(interventionId: number, body: OutcomeRequest): Observable<any> {
    return this.http.patch<any>(`${BASE}/api/retention/${interventionId}/outcome`, body, { headers: headers() })
      .pipe(catchError(e => throwError(() => e)));
  }

  getInterventions(clientId: string): Observable<any> {
    return this.http.get<any>(`${BASE}/api/db/interventions?client_id=${clientId}`, { headers: headers() })
      .pipe(catchError(e => throwError(() => e)));
  }
}
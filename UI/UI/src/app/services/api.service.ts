import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpHeaders, HttpErrorResponse } from '@angular/common/http';
import { Observable, throwError } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { environment } from '../../environments/environment';

export interface ApiResponse<T> {
  data: T;
  message?: string;
  success: boolean;
}

@Injectable({ providedIn: 'root' })
export class ApiService {
  private http = inject(HttpClient);
  private base = environment.apiUrl;

  private headers(): HttpHeaders {
    const token = localStorage.getItem('wap_token');
    return new HttpHeaders({
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {})
    });
  }

  get<T>(path: string): Observable<T> {
    return this.http.get<T>(`${this.base}${path}`, { headers: this.headers() })
      .pipe(catchError(this.handleError));
  }

  post<T>(path: string, body: unknown): Observable<T> {
    return this.http.post<T>(`${this.base}${path}`, body, { headers: this.headers() })
      .pipe(catchError(this.handleError));
  }

  put<T>(path: string, body: unknown): Observable<T> {
    return this.http.put<T>(`${this.base}${path}`, body, { headers: this.headers() })
      .pipe(catchError(this.handleError));
  }

  delete<T>(path: string): Observable<T> {
    return this.http.delete<T>(`${this.base}${path}`, { headers: this.headers() })
      .pipe(catchError(this.handleError));
  }

  /** Multipart upload — no Content-Type header so browser sets boundary */
  upload<T>(path: string, formData: FormData): Observable<T> {
    const token = localStorage.getItem('wap_token');
    const headers = new HttpHeaders(token ? { Authorization: `Bearer ${token}` } : {});
    return this.http.post<T>(`${this.base}${path}`, formData, { headers })
      .pipe(catchError(this.handleError));
  }

  private handleError(err: HttpErrorResponse): Observable<never> {
    // FastAPI raises HTTPException as {"detail": "..."} — try that first.
    // For validation errors the detail may itself be a list/dict; stringify it.
    // Legacy endpoints might use {"message": "..."}, so fall back to that,
    // then Angular's generic err.message, then a static string.
    const body = err.error;
    let msg: string;
    if (body && typeof body === 'object') {
      const detail = (body as any).detail;
      if (typeof detail === 'string') {
        msg = detail;
      } else if (detail && typeof detail === 'object') {
        // e.g. FK violation: { message: "...", violations: [...] }
        msg = detail.message
          ? `${detail.message} ${JSON.stringify(detail.violations ?? detail)}`
          : JSON.stringify(detail);
      } else if (typeof (body as any).message === 'string') {
        msg = (body as any).message;
      } else {
        msg = err.message ?? 'Unknown error';
      }
    } else if (typeof body === 'string' && body.length > 0) {
      msg = body;
    } else {
      msg = err.message ?? 'Unknown error';
    }
    console.error('[API]', err.status, msg, body);
    return throwError(() => ({ status: err.status, message: msg }));
  }
}

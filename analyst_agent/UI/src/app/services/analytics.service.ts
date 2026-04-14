import { Injectable, inject, signal } from '@angular/core';
import { Observable, tap } from 'rxjs';
import { ApiService } from './api.service';
import { AnalyticsData } from '../models';

@Injectable({ providedIn: 'root' })
export class AnalyticsService {
  private api = inject(ApiService);

  readonly data    = signal<AnalyticsData | null>(null);
  readonly loading = signal(false);
  readonly error   = signal<string | null>(null);

  load(): Observable<AnalyticsData> {
    this.loading.set(true);
    this.error.set(null);
    return this.api.get<AnalyticsData>('/analytics').pipe(
      tap({
        next:  d => { this.data.set(d); this.loading.set(false); },
        error: e => { this.error.set(e.message); this.loading.set(false); }
      })
    );
  }
}

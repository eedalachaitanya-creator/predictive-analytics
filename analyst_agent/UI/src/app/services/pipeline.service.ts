import { Injectable, inject, signal } from '@angular/core';
import { Observable, interval, switchMap, takeWhile, tap, startWith } from 'rxjs';
import { ApiService } from './api.service';
import { PipelineRunRequest, PipelineRunResponse, PipelineStage } from '../models';

@Injectable({ providedIn: 'root' })
export class PipelineService {
  private api = inject(ApiService);

  readonly currentJob   = signal<PipelineRunResponse | null>(null);
  readonly polling      = signal(false);

  /** Trigger a new pipeline run, then poll until complete */
  run(req: PipelineRunRequest): Observable<PipelineRunResponse> {
    return this.api.post<PipelineRunResponse>('/pipeline/run', req).pipe(
      tap(job => {
        this.currentJob.set(job);
        this.polling.set(true);
      })
    );
  }

  /** Poll job status every 2 seconds until done/failed */
  pollJob(jobId: string): Observable<PipelineRunResponse> {
    return interval(2000).pipe(
      startWith(0),
      switchMap(() => this.api.get<PipelineRunResponse>(`/pipeline/status/${jobId}`)),
      tap(job => this.currentJob.set(job)),
      takeWhile(job => job.status === 'running' || job.status === 'queued', true)
    );
  }

  getLastRun(clientId: string): Observable<PipelineRunResponse> {
    return this.api.get<PipelineRunResponse>(`/pipeline/last-run?clientId=${clientId}`).pipe(
      tap(job => this.currentJob.set(job))
    );
  }

  isRunning(): boolean {
    const s = this.currentJob()?.status;
    return s === 'running' || s === 'queued';
  }

  stageIcon(stage: PipelineStage): string {
    if (stage.status === 'done')    return '✅';
    if (stage.status === 'running') return '⏳';
    if (stage.status === 'error')   return '❌';
    return '—';
  }
}

import { Injectable, inject, signal } from '@angular/core';
import { Observable, interval, switchMap, takeWhile, tap, startWith, catchError, of, EMPTY } from 'rxjs';
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

  /**
   * One resilient status poll. Tries the live in-memory job status; if that
   * request fails — a 404 after the backend `--reload`ed and dropped the job, or
   * a transient network blip — it falls back to the PERSISTED last-run for the
   * client, which survives an in-memory reset. Without a clientId there is
   * nothing to fall back to, so the original error is left to propagate.
   *
   * This is the seam that stops a single dropped poll from freezing the
   * progress bar (it used to error the whole stream and stop polling).
   */
  pollOnce(jobId: string, clientId?: string | null): Observable<PipelineRunResponse> {
    const status$ = this.api.get<PipelineRunResponse>(`/pipeline/status/${jobId}`);
    if (!clientId) return status$;
    return status$.pipe(
      catchError(() => this.api.get<PipelineRunResponse>(`/pipeline/last-run?clientId=${clientId}`))
    );
  }

  /**
   * Poll job status every 2 seconds until done/failed. A failed tick can NEVER
   * freeze the UI: pollOnce recovers via last-run, and if even that fails we
   * re-emit the last known job and keep polling, recovering on a later tick.
   * The stream ends only when a poll reports a terminal status (complete/failed),
   * which takeWhile emits inclusively so the bar snaps to its final value.
   */
  pollJob(jobId: string, clientId?: string | null): Observable<PipelineRunResponse> {
    return interval(2000).pipe(
      startWith(0),
      switchMap(() => this.pollOnce(jobId, clientId).pipe(
        catchError(() => {
          const last = this.currentJob();
          return last ? of(last) : EMPTY;
        })
      )),
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

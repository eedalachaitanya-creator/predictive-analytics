import { TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { PipelineService } from './pipeline.service';
import { PipelineRunResponse } from '../models';

const API = 'http://localhost:8000/api/v1';

function job(over: Partial<PipelineRunResponse> = {}): PipelineRunResponse {
  return {
    jobId: 'job-1', status: 'running', progress: 58, stages: [],
    startedAt: '2026-06-04T00:00:00Z', ...over,
  };
}

/**
 * Regression guard for the "progress bar frozen at 58%" bug.
 *
 * The bar reads pipelineSvc.currentJob().progress, fed by pollJob → switchMap →
 * GET /pipeline/status/{jobId}. A single failed poll (a 404 after the backend
 * --reloaded and dropped its in-memory job, or any transient blip) used to error
 * the whole RxJS stream, stopping polling and freezing currentJob at its last
 * value. pollOnce makes one poll RESILIENT: on failure it falls back to the
 * persisted /last-run, which survives an in-memory reset.
 */
describe('PipelineService.pollOnce — a dropped status poll must not freeze the bar', () => {
  let svc: PipelineService;
  let http: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [PipelineService, provideHttpClient(), provideHttpClientTesting()],
    });
    svc = TestBed.inject(PipelineService);
    http = TestBed.inject(HttpTestingController);
  });
  afterEach(() => http.verify());

  it('falls back to /last-run when the live status poll 404s after a backend reload', () => {
    let got: PipelineRunResponse | undefined;
    let errored = false;
    svc.pollOnce('job-1', 'CLT-007').subscribe({ next: j => (got = j), error: () => (errored = true) });

    // In-memory job vanished when uvicorn --reload restarted → 404
    http.expectOne(`${API}/pipeline/status/job-1`).flush('gone', { status: 404, statusText: 'Not Found' });
    // ...so the poll consults the persisted last-run, which still holds the finished job
    http.expectOne(`${API}/pipeline/last-run?clientId=CLT-007`).flush(job({ status: 'complete', progress: 100 }));

    expect(errored).toBe(false);          // stream survived — bar is NOT frozen
    expect(got?.status).toBe('complete'); // and surfaced the real finished state
    expect(got?.progress).toBe(100);
  });

  it('does NOT hit /last-run when the status poll succeeds (no wasteful double-fetch)', () => {
    let got: PipelineRunResponse | undefined;
    svc.pollOnce('job-1', 'CLT-007').subscribe(j => (got = j));
    http.expectOne(`${API}/pipeline/status/job-1`).flush(job({ progress: 60 }));
    http.expectNone(`${API}/pipeline/last-run?clientId=CLT-007`);
    expect(got?.progress).toBe(60);
  });

  it('without a clientId there is nothing to fall back to → error propagates (back-compat)', () => {
    let errored = false;
    svc.pollOnce('job-1').subscribe({ next: () => {}, error: () => (errored = true) });
    http.expectOne(`${API}/pipeline/status/job-1`).flush('boom', { status: 500, statusText: 'Server Error' });
    http.expectNone(`${API}/pipeline/last-run?clientId=undefined`);
    expect(errored).toBe(true);
  });
});

import { Component, OnInit, OnDestroy, signal, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Subscription } from 'rxjs';
import { PipelineService } from '../services/pipeline.service';
import { UploadService } from '../services/upload.service';
import { MessagesService } from '../services/messages.service';
import { PipelineRunRequest, PipelineStage } from '../models';
import { AuthService } from '../services/auth.service';

@Component({
  selector: 'app-run',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './run.html',
  styleUrls: ['./run.scss']
})
export class RunComponent implements OnInit, OnDestroy {
  pipelineSvc = inject(PipelineService);
  uploadSvc   = inject(UploadService);
  msgSvc      = inject(MessagesService);
  private auth = inject(AuthService);

  predMode    = signal<'churn' | 'retention' | 'segmentation' | 'full'>('full');
  error       = signal<string | null>(null);
  private sub?: Subscription;

  private clientId = this.auth.getClientId();

  ngOnInit() {
    // Load last run details on mount
    this.pipelineSvc.getLastRun(this.clientId).subscribe({ error: () => {} });
  }

  ngOnDestroy() { this.sub?.unsubscribe(); }

  get job() { return this.pipelineSvc.currentJob(); }
  get running() { return this.pipelineSvc.isRunning(); }
  get progress() { return this.job?.progress ?? 0; }

  checklist = [
    { icon: '✅', label: 'Transaction masters uploaded',       detail: 'Customer · Order · Line Items',       ok: true },
    { icon: '✅', label: 'Product masters uploaded',           detail: 'Product · Price · Vendor-Map',        ok: true },
    { icon: '✅', label: 'Hierarchy masters uploaded',         detail: 'Category · Sub-Cat · Sub-Sub-Cat',    ok: true },
    { icon: '✅', label: 'Brand & Vendor masters uploaded',    detail: 'Brand · Vendor',                      ok: true },
    { icon: '⚠️', label: 'Validation passed',                  detail: '9 OK · 1 warning · 0 errors',        ok: false },
    { icon: '✅', label: 'Settings & Config rules saved',      detail: 'Churn: 90d · Threshold: Quartile',    ok: true },
    { icon: '✅', label: 'Tier thresholds confirmed',          detail: 'Method: Quartile · 4 bands set',      ok: true },
  ];

  run() {
    this.error.set(null);
    const req: PipelineRunRequest = { clientId: this.clientId, mode: this.predMode() };

    this.pipelineSvc.run(req).subscribe({
      next: job => {
        // Start polling
        this.sub = this.pipelineSvc.pollJob(job.jobId).subscribe({
          error: e => this.error.set(e.message)
        });
      },
      error: e => this.error.set(e.message ?? 'Failed to start pipeline.')
    });
  }

  stageIcon(s: PipelineStage): string { return this.pipelineSvc.stageIcon(s); }

  stageClass(s: PipelineStage): string {
    if (s.status === 'done')    return 'blue';
    if (s.status === 'running') return 'yellow';
    if (s.status === 'error')   return 'red';
    return 'gray';
  }

  // ── Manual outreach generation ──────────────────────────────
  outreachResult = signal<string | null>(null);

  generateOutreach() {
    this.outreachResult.set(null);
    this.msgSvc.generateOutreach({
      clientId: this.clientId,
      saveToDb: true,
    }).subscribe({
      next: (res) => this.outreachResult.set(res.message || `Generated ${res.total} emails`),
      error: (e) => this.outreachResult.set('Failed: ' + (e.message || 'Unknown error')),
    });
  }
}

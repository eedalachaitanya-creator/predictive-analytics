import { TestBed } from '@angular/core/testing';
import { provideRouter } from '@angular/router';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { SidebarComponent } from './sidebar';

/**
 * The Analyst Agent must NOT expose "Outreach Emails" — outreach (message
 * templates / personalised emails) is the Retention Agent's job, and the
 * Analyst-side nav item pointed at a dead, commented-out route (/app/outreach).
 * These tests pin the corrected Analyst nav so the broken item can't creep back.
 */
describe('SidebarComponent — Analyst Agent nav excludes outreach', () => {
  let cmp: SidebarComponent;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [provideRouter([]), provideHttpClient(), provideHttpClientTesting()],
    });
    cmp = TestBed.createComponent(SidebarComponent).componentInstance;
  });

  it('does NOT list an Outreach Emails item (Retention Agent owns outreach)', () => {
    const labels = cmp.analystGroup.children.map(c => c.label);
    const paths = cmp.analystGroup.children.map(c => c.path);
    expect(labels).not.toContain('Outreach Emails');
    expect(paths).not.toContain('/app/outreach');
  });

  it('does not treat /app/outreach as an Analyst route prefix', () => {
    expect(cmp.analystGroup.pathPrefixes).not.toContain('/app/outreach');
  });

  it('keeps exactly the remaining Analyst items, in order (no over-removal)', () => {
    const paths = cmp.analystGroup.children.map(c => c.path);
    expect(paths).toEqual([
      '/app/upload', '/app/validation', '/app/settings', '/app/dashboard',
      '/app/churn-scores', '/app/downloads', '/app/chat', '/app/cost-tracking',
    ]);
  });
});

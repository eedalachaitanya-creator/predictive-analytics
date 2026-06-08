import { TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { UploadService } from './upload.service';
import { environment } from '../../environments/environment';
import { MasterType } from '../models';

/**
 * Each upload tile offers a "Download sample CSV" link so clients see the exact
 * columns (and order) we expect. The link points at the backend route
 * GET /api/v1/uploads/sample/{masterType}, which generates the template from the
 * same canonical column map the validator uses. This guards that the frontend
 * builds that URL with the raw master key (so it stays in sync with the route).
 */
describe('UploadService.sampleUrl — per-tile sample CSV template link', () => {
  let svc: UploadService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [UploadService, provideHttpClient(), provideHttpClientTesting()],
    });
    svc = TestBed.inject(UploadService);
  });

  it('points at the backend sample-template route for a master type', () => {
    expect(svc.sampleUrl('customer')).toBe(`${environment.apiUrl}/uploads/sample/customer`);
  });

  it('preserves snake_case master keys verbatim in the path', () => {
    expect(svc.sampleUrl('vendor_map')).toBe(`${environment.apiUrl}/uploads/sample/vendor_map`);
    expect(svc.sampleUrl('sub_sub_category')).toBe(`${environment.apiUrl}/uploads/sample/sub_sub_category`);
  });

  it('produces a /uploads/sample/{key} URL for every master type', () => {
    const keys: MasterType[] = [
      'customer', 'order', 'line_items', 'product', 'price', 'vendor_map',
      'category', 'sub_category', 'sub_sub_category', 'brand', 'vendor',
      'customer_reviews', 'support_tickets',
    ];
    for (const k of keys) {
      expect(svc.sampleUrl(k)).toBe(`${environment.apiUrl}/uploads/sample/${k}`);
    }
  });
});

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


/**
 * Regression: GET /uploads is the source of truth for the pending batch.
 * loadUploads must REBUILD the map from the response so files the backend no
 * longer reports (e.g. a batch discarded on logout) are cleared — not merged
 * into a stale in-memory map. The UploadService is a root singleton, so without
 * this a logout→login (SPA, no reload) leaves discarded uploads showing.
 */
import { HttpTestingController } from '@angular/common/http/testing';
import { UploadedFile } from '../models';

describe('UploadService.loadUploads — mirrors backend state, no stale entries', () => {
  let svc: UploadService;
  let http: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [UploadService, provideHttpClient(), provideHttpClientTesting()],
    });
    svc = TestBed.inject(UploadService);
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => http.verify());

  function seedStaleCustomer() {
    const stale: UploadedFile = {
      masterType: 'customer', fileName: 'customer_master.csv',
      fileSize: 0, rowCount: 100, uploadedAt: '', status: 'success',
    };
    svc.uploads.set({ ...svc.uploads(), customer: stale });
  }

  it('clears a stale staged file when the backend reports no pending batch', () => {
    seedStaleCustomer();
    svc.loadUploads('CLT-001').subscribe();
    http.expectOne(`${environment.apiUrl}/uploads?clientId=CLT-001`).flush([]); // discarded → empty
    expect(svc.uploads().customer).toBeNull();
  });

  it('rebuilds the map from the response, dropping files no longer present', () => {
    seedStaleCustomer();
    svc.loadUploads('CLT-001').subscribe();
    const order: UploadedFile = {
      masterType: 'order', fileName: 'order.csv',
      fileSize: 0, rowCount: 5, uploadedAt: '', status: 'success',
    };
    http.expectOne(`${environment.apiUrl}/uploads?clientId=CLT-001`).flush([order]);
    expect(svc.uploads().order?.fileName).toBe('order.csv');
    expect(svc.uploads().customer).toBeNull();
  });
});


describe('UploadService.preview — staged file preview', () => {
  let svc: UploadService;
  let http: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [UploadService, provideHttpClient(), provideHttpClientTesting()],
    });
    svc = TestBed.inject(UploadService);
    http = TestBed.inject(HttpTestingController);
  });
  afterEach(() => http.verify());

  it('calls GET /uploads/preview with clientId + masterType and returns rows', () => {
    let result: any;
    svc.preview('CLT-001', 'customer').subscribe(r => (result = r));
    const req = http.expectOne(
      `${environment.apiUrl}/uploads/preview?clientId=CLT-001&masterType=customer`);
    expect(req.request.method).toBe('GET');
    req.flush({
      masterType: 'customer', fileName: 'customer_master.csv',
      columns: ['customer_id', 'customer_name'],
      rows: [['CUST-00001', 'John Doe']], shownRows: 1, totalRows: 100,
    });
    expect(result.columns).toEqual(['customer_id', 'customer_name']);
    expect(result.totalRows).toBe(100);
    expect(result.rows[0][0]).toBe('CUST-00001');
  });
});


/**
 * After a client commits an upload, the success banner lets them preview what was
 * SAVED. That reads the committed table (not staging), via GET /uploads/saved-preview,
 * and returns the same UploadPreview shape so the existing preview modal renders it.
 */
describe('UploadService.savedPreview — committed data preview', () => {
  let svc: UploadService;
  let http: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [UploadService, provideHttpClient(), provideHttpClientTesting()],
    });
    svc = TestBed.inject(UploadService);
    http = TestBed.inject(HttpTestingController);
  });
  afterEach(() => http.verify());

  it('calls GET /uploads/saved-preview with clientId + masterType and returns rows', () => {
    let result: any;
    svc.savedPreview('CLT-001', 'customer').subscribe(r => (result = r));
    const req = http.expectOne(
      `${environment.apiUrl}/uploads/saved-preview?clientId=CLT-001&masterType=customer`);
    expect(req.request.method).toBe('GET');
    req.flush({
      masterType: 'customer', fileName: 'customer_master.csv',
      columns: ['customer_id', 'customer_name'],
      rows: [['CUST-00001', 'John Doe']], shownRows: 1, totalRows: 700,
    });
    expect(result.columns).toEqual(['customer_id', 'customer_name']);
    expect(result.totalRows).toBe(700);
    expect(result.rows[0][0]).toBe('CUST-00001');
  });
});

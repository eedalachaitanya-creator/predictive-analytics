import { TestBed } from '@angular/core/testing';
import { signal } from '@angular/core';
import { of } from 'rxjs';

import { ClientsComponent } from './clients';
import { ApiService } from '../services/api.service';
import { AuthService } from '../services/auth.service';

/**
 * Bug fix: clicking "View" on a client used to swap an INLINE detail panel
 * below the table, so the super-admin got no clear signal the click worked.
 * "View" now opens a MODAL popup for that client. These tests pin the modal
 * toggle so it can't silently regress to the inline panel.
 */
describe('ClientsComponent — View opens a client-detail modal', () => {
  let cmp: ClientsComponent;

  const client = {
    client_id: 'CLT-001', client_name: 'Walmart Inc.', client_code: 'WMT',
    created_at: '2026-03-30T00:00:00Z', is_active: true, deactivated_at: null,
    address: null, city: null, state_province: null, postal_code: null,
    country: null, contact_email: null, company_phone: null,
  };
  const overview = {
    client_id: 'CLT-001', client_name: 'Walmart Inc.',
    uploaded: [], generated: [], totals: { uploaded_rows: 0, generated_rows: 0 },
  };

  beforeEach(() => {
    const apiStub = { get: () => of(overview), post: () => of({}), delete: () => of({}) };
    const authStub = { isSuperAdmin: signal(true) };

    TestBed.configureTestingModule({
      providers: [
        { provide: ApiService, useValue: apiStub },
        { provide: AuthService, useValue: authStub },
      ],
    });
    cmp = TestBed.createComponent(ClientsComponent).componentInstance;
  });

  it('starts with the detail modal closed', () => {
    expect(cmp.showClientModal()).toBe(false);
  });

  it('opens the modal AND selects the client when View is clicked', () => {
    cmp.viewClient(client);
    expect(cmp.showClientModal()).toBe(true);
    expect(cmp.selected()?.client_id).toBe('CLT-001');
  });

  it('loads that client\'s overview when opened (data lives in the popup)', () => {
    cmp.viewClient(client);
    expect(cmp.overview()?.client_id).toBe('CLT-001');
  });

  it('closes the modal — and any nested data viewer — on close', () => {
    cmp.viewClient(client);
    cmp.openDataView('customers', 'Customers');   // a nested data-viewer modal is open
    cmp.closeClientModal();
    expect(cmp.showClientModal()).toBe(false);
    expect(cmp.viewTable()).toBeNull();           // nested viewer closed too (no orphan modal)
  });
});


/**
 * Feature: the "Add New Client" form now captures full ORGANIZATION DETAILS +
 * an ADMINISTRATOR ACCOUNT (mirroring the English-Proficiency onboarding) and
 * NO company code (client_id is the sole identifier). These tests pin the form
 * shape + validation.
 */
describe('ClientsComponent — Add Client form (org details + admin account)', () => {
  let cmp: ClientsComponent;

  const FIELDS: Record<string, string> = {
    organization_name: 'Acme Retail Group', address: '123 Market St', city: 'Dallas',
    state_province: 'TX', postal_code: '75201', country: 'United States',
    company_contact_email: 'ops@acme.com', company_phone: '12145550100',
    admin_name: 'Jane Smith', admin_phone: '12145550199',
    admin_email: 'jane.admin@acme.com', password: 'Str0ng!Pass',
  };

  function fillValid() {
    for (const [k, v] of Object.entries(FIELDS)) cmp.updateAddField(k as never, v);
  }

  beforeEach(() => {
    const apiStub = { get: () => of({}), post: () => of({}), delete: () => of({}) };
    const authStub = { isSuperAdmin: signal(true) };
    TestBed.configureTestingModule({
      providers: [
        { provide: ApiService, useValue: apiStub },
        { provide: AuthService, useValue: authStub },
      ],
    });
    cmp = TestBed.createComponent(ClientsComponent).componentInstance;
    cmp.openAddForm();
  });

  it('captures organization + admin fields and NOT a company code', () => {
    const keys = Object.keys(cmp.addForm());
    for (const k of Object.keys(FIELDS)) expect(keys).toContain(k);
    expect(keys).not.toContain('client_code');
  });

  it('is invalid until every required field is filled', () => {
    expect(cmp.addFormValid()).toBe(false);
    fillValid();
    expect(cmp.addFormValid()).toBe(true);
  });

  it('rejects a malformed admin login email', () => {
    fillValid();
    cmp.updateAddField('admin_email' as never, 'nope');
    cmp.touchAdd('admin_email' as never);
    expect(cmp.addErrors()['admin_email']).toBeTruthy();
    expect(cmp.addFormValid()).toBe(false);
  });

  it('rejects a non-numeric phone', () => {
    fillValid();
    cmp.updateAddField('company_phone' as never, 'call-me');
    cmp.touchAdd('company_phone' as never);
    expect(cmp.addErrors()['company_phone']).toBeTruthy();
    expect(cmp.addFormValid()).toBe(false);
  });

  it('rejects a phone with more than 12 digits', () => {
    fillValid();
    cmp.updateAddField('company_phone' as never, '1231243342543234534');  // 19 digits
    cmp.touchAdd('company_phone' as never);
    expect(cmp.addErrors()['company_phone']).toBeTruthy();
    expect(cmp.addFormValid()).toBe(false);
  });

  it('accepts a 10-to-12 digit phone', () => {
    fillValid();
    cmp.updateAddField('admin_phone' as never, '919876543210');  // 12 digits
    cmp.touchAdd('admin_phone' as never);
    expect(cmp.addErrors()['admin_phone']).toBeFalsy();
  });

  it('shows no errors until a field is touched', () => {
    expect(Object.keys(cmp.addErrors()).length).toBe(0);   // pristine after openAddForm
  });

  it('exposes a country list including United States', () => {
    expect(cmp.countries.length).toBeGreaterThan(50);
    expect(cmp.countries).toContain('United States');
  });
});


/**
 * The Client Management table now shows organization details (Address / Email /
 * Phone) like the reference "Organizations" page. addressLines() turns the flat
 * org columns into the multi-line address block (street / city-state-zip /
 * country), tolerating the NULL columns of pre-onboarding tenants.
 */
describe('ClientsComponent — org-detail address rendering', () => {
  let cmp: ClientsComponent;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        { provide: ApiService, useValue: { get: () => of([]), post: () => of({}), delete: () => of({}) } },
        { provide: AuthService, useValue: { isSuperAdmin: signal(true) } },
      ],
    });
    cmp = TestBed.createComponent(ClientsComponent).componentInstance;
  });

  const row = (over: Record<string, unknown> = {}) => ({
    client_id: 'CLT-099', client_name: 'Acme', client_code: 'CLT-099',
    created_at: null, is_active: true, deactivated_at: null,
    address: '123 Market St', city: 'Dallas', state_province: 'TX',
    postal_code: '75201', country: 'United States',
    contact_email: 'ops@acme.com', company_phone: '+1 (214) 555-0100',
    ...over,
  });

  it('formats a full address into street / city-state-zip / country lines', () => {
    expect(cmp.addressLines(row() as never))
      .toEqual(['123 Market St', 'Dallas, TX 75201', 'United States']);
  });

  it('omits missing parts (city only, no street/state/zip)', () => {
    expect(cmp.addressLines(row({ address: null, state_province: null, postal_code: null }) as never))
      .toEqual(['Dallas', 'United States']);
  });

  it('returns no lines for a pre-onboarding tenant with no org data', () => {
    expect(cmp.addressLines(row({
      address: null, city: null, state_province: null, postal_code: null, country: null,
    }) as never)).toEqual([]);
  });
});

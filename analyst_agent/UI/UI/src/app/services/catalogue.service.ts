import { Injectable, inject, signal } from '@angular/core';
import { Observable, tap, forkJoin } from 'rxjs';
import { ApiService } from './api.service';
import { Category, SubCategory, SubSubCategory, Brand, Vendor } from '../models';

/**
 * CatalogueService
 * Covers: categories, sub_categories, sub_sub_categories, brands, vendors.
 * All of these are reference / master data loaded once at startup.
 */
@Injectable({ providedIn: 'root' })
export class CatalogueService {
  private api = inject(ApiService);

  readonly categories    = signal<Category[]>([]);
  readonly subCategories = signal<SubCategory[]>([]);
  readonly subSubCats    = signal<SubSubCategory[]>([]);
  readonly brands        = signal<Brand[]>([]);
  readonly vendors       = signal<Vendor[]>([]);
  readonly loading       = signal(false);
  readonly error         = signal<string | null>(null);

  /** Load all catalogue reference data in one shot */
  loadAll(): Observable<[Category[], SubCategory[], SubSubCategory[], Brand[], Vendor[]]> {
    this.loading.set(true);
    this.error.set(null);
    return forkJoin([
      this.api.get<Category[]>('/catalogue/categories'),
      this.api.get<SubCategory[]>('/catalogue/sub-categories'),
      this.api.get<SubSubCategory[]>('/catalogue/sub-sub-categories'),
      this.api.get<Brand[]>('/catalogue/brands'),
      this.api.get<Vendor[]>('/catalogue/vendors'),
    ]).pipe(
      tap({
        next: ([cats, subCats, subSubCats, brands, vendors]) => {
          this.categories.set(cats);
          this.subCategories.set(subCats);
          this.subSubCats.set(subSubCats);
          this.brands.set(brands);
          this.vendors.set(vendors);
          this.loading.set(false);
        },
        error: e => { this.error.set(e.message); this.loading.set(false); }
      })
    );
  }

  /** Convenience lookups — derived from signals, no HTTP call */
  getCategoryById(id: number): Category | undefined {
    return this.categories().find(c => c.category_id === id);
  }

  getSubCategoryById(id: number): SubCategory | undefined {
    return this.subCategories().find(s => s.sub_category_id === id);
  }

  getBrandById(id: number): Brand | undefined {
    return this.brands().find(b => b.brand_id === id);
  }

  getVendorById(id: number): Vendor | undefined {
    return this.vendors().find(v => v.vendor_id === id);
  }

  /** Active brands only */
  activeBrands(): Brand[] {
    return this.brands().filter(b => b.active === 1 && b.not_available === 0);
  }

  // ── Future methods to add ──────────────────────────────────
  // getBrandsByCategory(categoryHint): Brand[] { ... }
  // getSubCatsByCategory(categoryId): SubCategory[] { ... }
  // addBrand(brand): Observable<Brand> { ... }
  // addVendor(vendor): Observable<Vendor> { ... }
}

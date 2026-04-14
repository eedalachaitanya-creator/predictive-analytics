import { Injectable, inject, signal } from '@angular/core';
import { Observable, tap } from 'rxjs';
import { ApiService } from './api.service';
import { Product, ProductPrice, ProductVendorMapping } from '../models';

@Injectable({ providedIn: 'root' })
export class ProductsService {
  private api = inject(ApiService);

  readonly products = signal<Product[]>([]);
  readonly loading  = signal(false);
  readonly error    = signal<string | null>(null);

  /** Load all products (optionally filtered by category) */
  load(page = 1, pageSize = 100, categoryId?: number): Observable<{ data: Product[]; total: number; pages: number }> {
    this.loading.set(true);
    this.error.set(null);
    const cat = categoryId != null ? `&categoryId=${categoryId}` : '';
    return this.api.get<{ data: Product[]; total: number; pages: number }>(
      `/products?page=${page}&pageSize=${pageSize}${cat}`
    ).pipe(
      tap({
        next:  r => { this.products.set(r.data); this.loading.set(false); },
        error: e => { this.error.set(e.message);  this.loading.set(false); }
      })
    );
  }

  /** Get pricing tiers for a product */
  getPrices(productId: number): Observable<ProductPrice[]> {
    return this.api.get<ProductPrice[]>(`/products/${productId}/prices`);
  }

  /** Get vendor mapping for a product */
  getVendorMapping(productId: number): Observable<ProductVendorMapping> {
    return this.api.get<ProductVendorMapping>(`/products/${productId}/vendor-mapping`);
  }

  // ── Future methods to add ──────────────────────────────────
  // search(query): Observable<Product[]> { ... }
  // getByBrand(brandId): Observable<Product[]> { ... }
  // getTopSellers(clientId, limit): Observable<Product[]> { ... }
  // updateActiveStatus(productId, active): Observable<Product> { ... }
}

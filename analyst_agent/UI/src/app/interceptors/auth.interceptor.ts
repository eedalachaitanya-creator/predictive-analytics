import { HttpInterceptorFn, HttpErrorResponse } from '@angular/common/http';
import { inject } from '@angular/core';
import { Router } from '@angular/router';
import { catchError, throwError } from 'rxjs';
import { environment } from '../../environments/environment';

const TOKEN_KEY   = 'wap_token';
const REFRESH_KEY = 'wap_refresh';
const USER_KEY    = 'wap_user';

export const authInterceptor: HttpInterceptorFn = (req, next) => {
  // In mock mode the mockInterceptor already handled the request — this won't fire
  // But if it does reach here (e.g. non-API requests), just pass through
  if (environment.useMocks) return next(req);

  const router = inject(Router);
  const isAuthEndpoint = req.url.includes('/auth/login') || req.url.includes('/auth/refresh');
  const token = localStorage.getItem(TOKEN_KEY);

  const authReq = (!isAuthEndpoint && token)
    ? req.clone({ setHeaders: { Authorization: `Bearer ${token}` } })
    : req;

  return next(authReq).pipe(
    catchError((err: HttpErrorResponse) => {
      if (err.status === 401 && !isAuthEndpoint) {
        localStorage.removeItem(TOKEN_KEY);
        localStorage.removeItem(REFRESH_KEY);
        localStorage.removeItem(USER_KEY);
        router.navigate(['/login']);
      }
      return throwError(() => err);
    })
  );
};

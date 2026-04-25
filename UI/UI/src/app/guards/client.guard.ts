import { inject } from '@angular/core';
import { CanActivateFn, Router } from '@angular/router';
import { AuthService } from '../services/auth.service';

/**
 * Blocks super admins from accessing client-portal pages.
 * If a super admin tries to go to /app/upload, they get redirected to /app/clients.
 */
export const clientGuard: CanActivateFn = () => {
  const auth   = inject(AuthService);
  const router = inject(Router);
  if (!auth.isSuperAdmin()) return true;  // client_user can access
  return router.createUrlTree(['/app/clients']);  // super admins get redirected
};

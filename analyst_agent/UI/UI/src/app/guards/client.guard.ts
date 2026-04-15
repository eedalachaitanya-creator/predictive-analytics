import { inject } from '@angular/core';
import { CanActivateFn, Router } from '@angular/router';
import { AuthService } from '../services/auth.service';

/**
 * Blocks admin users from accessing client portal pages.
 * If an admin tries to go to /app/upload, they get redirected to /app/clients.
 */
export const clientGuard: CanActivateFn = () => {
  const auth   = inject(AuthService);
  const router = inject(Router);
  if (!auth.isAdmin()) return true;  // client_user can access
  return router.createUrlTree(['/app/clients']);  // admins get redirected
};

import {Injectable, inject} from '@angular/core';
import {ActivatedRouteSnapshot, CanActivate, Router, RouterStateSnapshot, UrlTree} from '@angular/router';
import {Observable} from 'rxjs';
import {UserService} from '@geoengine/common';

@Injectable({
    providedIn: 'root',
})
export class LogInGuard implements CanActivate {
    private router = inject(Router);
    private userService = inject(UserService);

    canActivate(
        _route: ActivatedRouteSnapshot,
        state: RouterStateSnapshot,
    ): Observable<boolean | UrlTree> | Promise<boolean | UrlTree> | boolean | UrlTree {
        const loggedInOrRedirect = this.userService.isLoggedIn().then((loggedIn) => {
            if (loggedIn) {
                return true;
            }
            return this.router.createUrlTree(['/signin'], {queryParams: {returnUrl: state.url}});
        });
        return loggedInOrRedirect;
    }
}

import {Injectable, inject} from '@angular/core';
import {ActivatedRouteSnapshot, RouterStateSnapshot, UrlTree, Router} from '@angular/router';

import {Observable, map, skipWhile} from 'rxjs';
import {UserService} from '../../user/user.service';

@Injectable({
    providedIn: 'root',
})
export class BackendAvailableGuard {
    private readonly userService = inject(UserService);
    private router = inject(Router);

    canActivate(
        _route: ActivatedRouteSnapshot,
        _state: RouterStateSnapshot,
    ): Observable<boolean | UrlTree> | Promise<boolean | UrlTree> | boolean | UrlTree {
        const loggedInOrRedirect = this.userService.getBackendStatus().pipe(
            skipWhile((status) => status.initial === true),
            map((status) => {
                if (status.available) {
                    return true;
                } else {
                    return this.router.parseUrl('/backend-status');
                }
            }),
        );
        return loggedInOrRedirect;
    }
}

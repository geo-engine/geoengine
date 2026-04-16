import {Injectable, inject} from '@angular/core';
import {ActivatedRouteSnapshot, RouterStateSnapshot, UrlTree} from '@angular/router';
import {Observable} from 'rxjs';
import {CommonConfig} from '../../config.service';

@Injectable({
    providedIn: 'root',
})
export class CanRegisterGuard {
    private readonly config = inject(CommonConfig);

    canActivate(
        _route: ActivatedRouteSnapshot,
        _state: RouterStateSnapshot,
    ): Observable<boolean | UrlTree> | Promise<boolean | UrlTree> | boolean | UrlTree {
        return this.config.USER.REGISTRATION_AVAILABLE;
    }
}

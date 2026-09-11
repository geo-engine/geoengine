import {ChangeDetectionStrategy, Component, inject, input, resource} from '@angular/core';
import {User} from '../user.model';
import {Router} from '@angular/router';
import {UserService} from '@geoengine/common';
import {SidenavHeaderComponent} from '../../sidenav/sidenav-header/sidenav-header.component';
import {MatCard, MatCardHeader, MatCardTitle, MatCardContent} from '@angular/material/card';
import {IfLoggedInDirective} from '../../util/directives/if-logged-in.directive';
import {MatButton} from '@angular/material/button';
import {IfGuestDirective} from '../../util/directives/if-guest.directive';
import {UserSessionComponent} from '../user-session/user-session.component';
import {QuotaInfoComponent} from '../quota/quota-info/quota-info.component';
import {RolesComponent} from '../roles/roles.component';
import {firstValueFrom} from 'rxjs';

@Component({
    selector: 'geoengine-oidc',
    templateUrl: 'oidc.component.html',
    styleUrls: ['./oidc.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        SidenavHeaderComponent,
        MatCard,
        MatCardHeader,
        MatCardTitle,
        MatCardContent,
        IfLoggedInDirective,
        MatButton,
        IfGuestDirective,
        UserSessionComponent,
        QuotaInfoComponent,
        RolesComponent,
    ],
})
export class OidcComponent {
    private readonly userService = inject(UserService);
    private readonly router = inject(Router);

    /** Whether the additional account information cards are shown. */
    readonly showSession = input(true);
    readonly showQuota = input(true);
    readonly showRoles = input(true);
    /** Whether login should redirect directly to the OIDC provider. */
    readonly directLogin = input(false);

    readonly user = resource({
        defaultValue: undefined,
        loader: async (): Promise<User | undefined> => {
            const session = await firstValueFrom(this.userService.getSessionStream());
            if (!session.user || session.user.isGuest) {
                return undefined;
            }
            return session.user;
        },
    });

    async login(): Promise<void> {
        if (this.directLogin()) {
            const oidcRequest = await this.userService.oidcInit(this.router.url);
            window.location.href = oidcRequest.url;
            return;
        }

        // Redirect to /signin with returnUrl pointing back here
        // LoginComponent will handle OIDC init and callback
        await this.router.navigate(['/signin'], {
            queryParams: {returnUrl: this.router.url},
        });
    }

    async logout(): Promise<void> {
        try {
            const session = await this.userService.guestLogin();
            if (!session.user || session.user.isGuest) {
                this.user.set(undefined);
            } else {
                this.user.set(session.user);
            }
        } catch {
            // guest login failed -> reload the application
            // we navigate to a dummy url first in order to ensure that the guards (logged in) are executed again
            const url = this.router.url;
            await this.router.navigate(['/dummy']);
            await this.router.navigate([url]);
        }
    }
}

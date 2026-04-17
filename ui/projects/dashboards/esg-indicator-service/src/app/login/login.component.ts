import {ChangeDetectionStrategy, Component, OnInit, inject, signal} from '@angular/core';
import {User, CoreModule} from '@geoengine/core';
import {Router} from '@angular/router';
import {MatButtonModule} from '@angular/material/button';
import {MatCardModule} from '@angular/material/card';
import {MatGridListModule} from '@angular/material/grid-list';
import {MatIconModule} from '@angular/material/icon';
import {MatMenuModule} from '@angular/material/menu';
import {NotificationService, UserService} from '@geoengine/common';
import {FormField, required, form} from '@angular/forms/signals';
import {firstValueFrom} from 'rxjs';
import {HttpErrorResponse} from '@angular/common/http';

enum FormStatus {
    LoggedOut,
    LoggedIn,
    Loading,
    Oidc,
}

@Component({
    selector: 'geoengine-login',
    templateUrl: './login.component.html',
    styleUrls: ['./login.component.scss'],
    imports: [CoreModule, FormField, MatGridListModule, MatMenuModule, MatIconModule, MatButtonModule, MatCardModule, FormField],
    changeDetection: ChangeDetectionStrategy.OnPush,
})
export class LoginComponent implements OnInit {
    private readonly userService = inject(UserService);
    private readonly notificationService = inject(NotificationService);
    private readonly router = inject(Router);

    readonly FormStatus = FormStatus;

    readonly formStatus = signal<FormStatus>(FormStatus.Loading);

    readonly loginModel = signal({
        email: '',
        password: '',
    });
    readonly loginForm = form(this.loginModel, (schemaPath) => {
        required(schemaPath.email);
        required(schemaPath.password);
    });

    readonly user = signal<User | undefined>(undefined);
    readonly invalidCredentials = signal<boolean>(false);

    private oidcUrl = '';

    ngOnInit(): void {
        void this.onInit();
    }

    async onInit(): Promise<void> {
        const restoreRoute = 'dashboard';

        // check if OIDC login is enabled
        try {
            const idr = await this.userService.oidcInit(restoreRoute);
            this.oidcUrl = idr.url;
            this.formStatus.set(FormStatus.Oidc);
        } catch {
            // OIDC login failed show local login
            const session = await firstValueFrom(this.userService.getSessionOrUndefinedStream());

            if (!session?.user || session.user.isGuest) {
                this.formStatus.set(FormStatus.LoggedOut);
            } else {
                this.user.set(session.user);
                this.formStatus.set(FormStatus.LoggedIn);
            }
        }
    }

    oidcLogin(): void {
        this.formStatus.set(FormStatus.Loading);
        window.location.href = this.oidcUrl;
    }

    async login(): Promise<void> {
        this.formStatus.set(FormStatus.Loading);

        try {
            const session = await this.userService.login({
                email: this.loginModel().email,
                password: this.loginModel().password,
            });

            this.user.set(session.user);
            this.invalidCredentials.set(false);
            this.formStatus.set(FormStatus.LoggedIn);

            await this.redirectToMainView();
        } catch {
            // on error
            this.invalidCredentials.set(true);
            this.loginForm.password().value.set('');
            this.formStatus.set(FormStatus.LoggedOut);
        }
    }

    async logout(): Promise<void> {
        this.formStatus.set(FormStatus.LoggedOut);

        // we log out by trying to perform a guest login
        // if this fails, we will get logged out
        try {
            await this.userService.guestLogin();
            this.loginForm.password().value.set('');
        } catch (error) {
            if (
                error instanceof HttpErrorResponse &&
                error.error &&
                typeof error.error === 'object' &&
                'error' in error.error &&
                (error.error as {error?: string}).error !== 'AnonymousAccessDisabled'
            ) {
                this.notificationService.error(`The backend is currently unavailable (${error.message})`);
            }
        }
    }

    async redirectToMainView(): Promise<void> {
        await this.router.navigate(['dashboard']);
    }
}

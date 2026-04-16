import {ChangeDetectionStrategy, Component, input, OnInit, inject, signal} from '@angular/core';
import {Router, RouterLink, ActivatedRoute} from '@angular/router';
import {CommonConfig} from '../config.service';
import {UserService} from '../user/user.service';
import {geoengineValidators} from '../util/form.validators';
import {User} from '../user/user.model';
import {NotificationService} from '../notification.service';
import {
    FxLayoutDirective,
    FxLayoutAlignDirective,
    FxLayoutGapDirective,
    FxFlexDirective,
} from '../util/directives/flexbox-legacy.directive';
import {MatCard, MatCardHeader, MatCardSubtitle, MatCardContent, MatCardActions} from '@angular/material/card';
import {MatButton} from '@angular/material/button';
import {MatFormField, MatInput} from '@angular/material/input';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {MatIcon} from '@angular/material/icon';
import {MatTooltip} from '@angular/material/tooltip';
import {email, required, form, FormField} from '@angular/forms/signals';
import {FormsModule} from '@angular/forms';
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
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        FormsModule,
        FxLayoutDirective,
        FxLayoutAlignDirective,
        MatCard,
        MatCardHeader,
        MatCardSubtitle,
        MatCardContent,
        MatCardActions,
        MatButton,
        MatFormField,
        MatInput,
        MatProgressSpinner,
        FxLayoutGapDirective,
        FxFlexDirective,
        RouterLink,
        MatIcon,
        MatTooltip,
        FormField,
    ],
})
export class LoginComponent implements OnInit {
    readonly config = inject(CommonConfig);
    private readonly userService = inject(UserService);
    private readonly notificationService = inject(NotificationService);
    private readonly router = inject(Router);
    private readonly route = inject(ActivatedRoute);

    readonly FormStatus = FormStatus;
    readonly defaultRedirect = input('/map');

    readonly formStatus = signal<FormStatus>(FormStatus.Loading);
    readonly canRegister = this.config.USER.REGISTRATION_AVAILABLE;
    readonly canGuestLogin = this.config.USER.AUTO_GUEST_LOGIN;

    readonly loginModel = signal({
        email: '',
        password: '',
    });
    readonly loginForm = form(this.loginModel, (schemaPath) => {
        required(schemaPath.email);
        required(schemaPath.password);
        email(schemaPath.email);

        geoengineValidators.keyword2(schemaPath.email, [this.config.USER.GUEST.NAME]);
    });

    readonly user = signal<User | undefined>(undefined);
    readonly invalidCredentials = signal<boolean>(false);

    private oidcUrl = '';

    readonly loginRedirect = (): string => {
        // Priority: explicit returnUrl query param -> route data.loginRedirect -> default input
        const queryRedirect = this.route.snapshot.queryParamMap.get('returnUrl');
        if (queryRedirect) {
            return queryRedirect;
        }

        const dataRedirect = this.route.snapshot.data['loginRedirect'];
        if (dataRedirect) {
            return dataRedirect as string;
        }

        const inputDefaultRedirect = this.defaultRedirect();
        if (inputDefaultRedirect) {
            return inputDefaultRedirect;
        }

        // if there is no redirect set, redirect to the application root.
        return '/';
    };

    ngOnInit(): void {
        void this.onInit();
    }

    async onInit(): Promise<void> {
        // check if OIDC login is enabled
        try {
            // resolve the route to restore after login
            const oidcRestoreRoute = this.loginRedirect();
            const idr = await this.userService.oidcInit(oidcRestoreRoute);
            this.oidcUrl = idr.url;
            this.formStatus.set(FormStatus.Oidc);

            // Auto-redirect to OIDC if local login is disabled
            if (!this.config.USER.LOCAL_LOGIN_AVAILABLE) {
                this.oidcLogin();
            }
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

    async onSubmit(event: Event): Promise<void> {
        event.preventDefault();
        await this.login();
    }

    oidcLogin(): void {
        this.formStatus.set(FormStatus.Oidc);
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
        const target = this.loginRedirect();
        await this.router.navigateByUrl(target, {replaceUrl: true});
    }
}

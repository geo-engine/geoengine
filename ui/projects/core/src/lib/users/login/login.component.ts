import {ChangeDetectionStrategy, Component, OnInit, inject, signal} from '@angular/core';
import {CoreConfig} from '../../config.service';
import {User} from '../user.model';
import {geoengineValidators, NotificationService, UserService, FxLayoutDirective, FxFlexDirective} from '@geoengine/common';
import {SidenavHeaderComponent} from '../../sidenav/sidenav-header/sidenav-header.component';
import {MatFormField, MatInput} from '@angular/material/input';
import {MatButton} from '@angular/material/button';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {HttpErrorResponse} from '@angular/common/http';
import {required, form, FormField} from '@angular/forms/signals';
import {firstValueFrom} from 'rxjs';

enum FormStatus {
    LoggedOut,
    LoggedIn,
    Loading,
}

@Component({
    selector: 'geoengine-login',
    templateUrl: './login.component.html',
    styleUrls: ['./login.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [FormField, FxFlexDirective, FxLayoutDirective, MatButton, MatFormField, MatInput, MatProgressSpinner, SidenavHeaderComponent],
})
export class LoginComponent implements OnInit {
    private readonly config = inject(CoreConfig);
    private readonly userService = inject(UserService);
    private readonly notificationService = inject(NotificationService);

    readonly FormStatus = FormStatus;

    readonly formStatus = signal<FormStatus>(FormStatus.Loading);

    readonly loginModel = signal({
        email: '',
        password: '',
    });
    readonly loginForm = form(this.loginModel, (schemaPath) => {
        required(schemaPath.email);
        required(schemaPath.password);

        geoengineValidators.keyword2(schemaPath.email, [this.config.USER.GUEST.NAME]);
    });

    readonly user = signal<User | undefined>(undefined);
    readonly invalidCredentials = signal<boolean>(false);

    ngOnInit(): void {
        void this.onInit();
    }

    async onInit(): Promise<void> {
        const session = await firstValueFrom(this.userService.getSessionStream());

        if (!session.user || session.user.isGuest) {
            this.formStatus.set(FormStatus.LoggedOut);
        } else {
            this.user.set(session.user);
            this.formStatus.set(FormStatus.LoggedIn);
        }
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
}

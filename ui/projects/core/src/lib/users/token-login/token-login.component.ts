import {ChangeDetectionStrategy, Component, OnInit, inject, input, signal} from '@angular/core';
import {first} from 'rxjs/operators';
import {Router} from '@angular/router';
import {UUID} from '../../backend/backend.model';
import {UserService, FxLayoutDirective, FxLayoutAlignDirective, FxLayoutGapDirective, FxFlexDirective} from '@geoengine/common';
import {NgClass} from '@angular/common';
import {MatCard, MatCardContent, MatCardActions} from '@angular/material/card';
import {MatFormField, MatInput} from '@angular/material/input';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {MatButton} from '@angular/material/button';
import {form, FormField, required} from '@angular/forms/signals';

enum FormStatus {
    LoggedOut,
    LoggedIn,
    Loading,
}

@Component({
    selector: 'geoengine-token-login',
    templateUrl: './token-login.component.html',
    styleUrls: ['./token-login.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        FormField,
        FxLayoutDirective,
        FxLayoutAlignDirective,
        NgClass,
        MatCard,
        MatCardContent,
        MatFormField,
        MatInput,
        MatProgressSpinner,
        MatCardActions,
        FxLayoutGapDirective,
        MatButton,
        FxFlexDirective,
    ],
})
export class TokenLoginComponent implements OnInit {
    private readonly userService = inject(UserService);
    private readonly router = inject(Router);

    readonly routeTo = input<Array<string>>();
    readonly invalidTokenText = input('Invalid token');
    readonly color = input<'primary' | 'accent'>('primary');

    readonly FormStatus = FormStatus;

    readonly formStatus = signal<FormStatus>(FormStatus.LoggedOut);

    readonly loginModel = signal({
        sessionToken: '',
    });
    readonly loginForm = form(this.loginModel, (schemaPath) => {
        required(schemaPath.sessionToken, {message: 'Session token is required'});
    });

    readonly invalidCredentials = signal<boolean>(false);

    ngOnInit(): void {
        this.userService
            .getSessionOrUndefinedStream()
            .pipe(first())
            .subscribe((session) => {
                if (session) {
                    this.redirectRoute();
                }
            });
    }

    async login(): Promise<void> {
        this.formStatus.set(FormStatus.Loading);

        const sessionToken: UUID = this.loginModel().sessionToken;

        try {
            await this.userService.createSessionWithToken(sessionToken);

            this.invalidCredentials.set(false);
            this.formStatus.set(FormStatus.LoggedIn);

            this.redirectRoute();
        } catch {
            // on error
            this.invalidCredentials.set(true);
            this.loginForm.sessionToken().value.set('');
            this.formStatus.set(FormStatus.LoggedOut);
        }
    }

    redirectRoute(): void {
        const routeTo = this.routeTo();
        if (!routeTo) {
            return;
        }
        void this.router.navigate(routeTo);
    }
}

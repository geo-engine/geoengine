import {BehaviorSubject, Observable} from 'rxjs';

import {AfterViewInit, ChangeDetectionStrategy, Component, input, inject} from '@angular/core';
import {UntypedFormControl, UntypedFormGroup, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';

import {map} from 'rxjs/operators';
import {Router, RouterLink} from '@angular/router';
import {CommonConfig} from '../config.service';
import {UserService} from '../user/user.service';
import {NotificationService} from '../notification.service';
import {geoengineValidators} from '../util/form.validators';
import {GeoEngineError} from '../util/errors';
import {FxLayoutDirective, FxLayoutAlignDirective, FxLayoutGapDirective} from '../util/directives/flexbox-legacy.directive';
import {MatCard, MatCardHeader, MatCardSubtitle, MatCardContent, MatCardActions} from '@angular/material/card';
import {MatFormField, MatInput, MatHint} from '@angular/material/input';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {MatButton} from '@angular/material/button';
import {AsyncPipe} from '@angular/common';

@Component({
    selector: 'geoengine-register',
    templateUrl: './register.component.html',
    styleUrls: ['./register.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        FormsModule,
        ReactiveFormsModule,
        FxLayoutDirective,
        FxLayoutAlignDirective,
        MatCard,
        MatCardHeader,
        MatCardSubtitle,
        MatCardContent,
        MatFormField,
        MatInput,
        MatHint,
        MatProgressSpinner,
        MatCardActions,
        FxLayoutGapDirective,
        MatButton,
        RouterLink,
        AsyncPipe,
    ],
})
export class RegisterComponent implements AfterViewInit {
    readonly config = inject(CommonConfig);
    private readonly userService = inject(UserService);
    private readonly notificationService = inject(NotificationService);
    private readonly router = inject(Router);

    PASSWORD_MIN_LENGTH = 8;

    readonly loginRedirect = input('/map');

    loading$ = new BehaviorSubject<boolean>(false);
    notLoading$ = this.loading$.pipe(map((loading) => !loading));
    formIsInvalid$: Observable<boolean>;

    registrationError$ = new BehaviorSubject<string>('');

    registrationForm: UntypedFormGroup;

    constructor() {
        this.registrationForm = new UntypedFormGroup({
            name: new UntypedFormControl('', Validators.required),
            email: new UntypedFormControl(
                '',
                Validators.compose([Validators.required, Validators.email, geoengineValidators.keyword([this.config.USER.GUEST.NAME])]),
            ),
            password: new UntypedFormControl('', [Validators.required, Validators.minLength(this.PASSWORD_MIN_LENGTH)]),
        });

        this.formIsInvalid$ = this.registrationForm.statusChanges.pipe(map((status) => status !== 'VALID'));
    }

    ngAfterViewInit(): void {
        // do this once for observables
        setTimeout(() => this.registrationForm.updateValueAndValidity());
    }

    async register(): Promise<void> {
        this.loading$.next(true);
        this.registrationError$.next('');

        const realName: string = this.registrationForm.controls['name'].value;
        const email: string = this.registrationForm.controls['email'].value;
        const password: string = this.registrationForm.controls['password'].value;

        try {
            await this.userService.registerUser({
                email,
                password,
                realName,
            });

            this.notificationService.info('Registration successful!');
            this.redirectToMainView();
        } catch (error) {
            if (error instanceof GeoEngineError) {
                this.registrationError$.next(error.message);
            }

            this.loading$.next(false);
        }
    }

    redirectToMainView(): void {
        this.router.navigate([this.loginRedirect()]);
    }
}

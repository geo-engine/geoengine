import {BehaviorSubject, Observable, Subscription} from 'rxjs';

import {ChangeDetectionStrategy, Component, OnDestroy, inject} from '@angular/core';
import {UntypedFormControl, UntypedFormGroup, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';

import {CoreConfig} from '../../config.service';
import {User} from '../user.model';
import {Session} from '../session.model';
import {MatDialogRef} from '@angular/material/dialog';
import {geoengineValidators} from '@geoengine/common';
import {MatFormField, MatInput} from '@angular/material/input';
import {MatButton} from '@angular/material/button';
import {AsyncPipe} from '@angular/common';

enum FormStatus {
    LoggedOut,
    LoggedIn,
    Loading,
}

export interface UserLogin {
    email: string;
    password: string;
}

@Component({
    selector: 'geoengine-modal-login',
    templateUrl: './modal-login.component.html',
    styleUrls: ['./modal-login.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [FormsModule, ReactiveFormsModule, MatFormField, MatInput, MatButton, AsyncPipe],
})
export class ModalLoginComponent implements OnDestroy {
    private readonly config = inject(CoreConfig);
    private dialogRef = inject<MatDialogRef<ModalLoginComponent>>(MatDialogRef);

    readonly FormStatus = FormStatus;

    formStatus$ = new BehaviorSubject<FormStatus>(FormStatus.LoggedOut);

    loginForm: UntypedFormGroup;

    user?: User;
    invalidCredentials$ = new BehaviorSubject<boolean>(false);

    loginCallback!: (credential: UserLogin) => Observable<Session>;

    private formStatusSubscription?: Subscription;

    constructor() {
        this.loginForm = new UntypedFormGroup({
            email: new UntypedFormControl(
                '',
                Validators.compose([Validators.required, geoengineValidators.keyword([this.config.USER.GUEST.NAME])]),
            ),
            password: new UntypedFormControl('', Validators.required),
        });
    }

    ngOnDestroy(): void {
        if (this.formStatusSubscription) {
            this.formStatusSubscription.unsubscribe();
        }
    }

    login(): void {
        this.formStatus$.next(FormStatus.Loading);

        this.loginCallback({
            email: this.loginForm.controls['email'].value,
            password: this.loginForm.controls['password'].value,
        }).subscribe(
            (session) => {
                this.user = session.user;
                this.invalidCredentials$.next(false);
                this.dialogRef.close();
            },
            () => {
                // on error
                this.invalidCredentials$.next(true);
                (this.loginForm.controls['password'] as UntypedFormControl).setValue('');
                this.formStatus$.next(FormStatus.LoggedOut);
            },
        );
    }
}

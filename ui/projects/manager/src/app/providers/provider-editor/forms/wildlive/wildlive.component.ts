import {Component, inject, input, effect, forwardRef} from '@angular/core';
import {
    ControlValueAccessor,
    FormBuilder,
    FormControl,
    FormGroup,
    NG_VALUE_ACCESSOR,
    ReactiveFormsModule,
    Validators,
} from '@angular/forms';
import {
    TypedDataProviderDefinition,
    instanceOfWildliveDataConnectorDefinition,
    WildliveDataConnectorDefinitionTypeEnum,
} from '@geoengine/api-client';
import {isValidUuid, UUID, CommonModule} from '@geoengine/common';
import {IdInputComponent} from '../util/id-input/id-input.component';
import {MatCard, MatCardContent} from '@angular/material/card';
import {MatError, MatFormField} from '@angular/material/form-field';
import {MatInput, MatLabel} from '@angular/material/input';
import {CdkTextareaAutosize} from '@angular/cdk/text-field';
import {ErrorStateMatcher} from '@angular/material/core';
import {toSignal} from '@angular/core/rxjs-interop';
import {map, pairwise, startWith} from 'rxjs';
import {MatButton} from '@angular/material/button';
import {OidcPopupMessage} from '../../../../oidc-popup/oidc-popup.component';
import {oidcRedirectPath} from '../../../../util/location';

const nop = (): null => null;

interface WildLiveForm {
    id: FormControl<UUID>;
    name: FormControl<string>;
    description: FormControl<string>;
    auth?: FormGroup<{
        user: FormControl<string>;
        refreshToken: FormControl<string>;
        expiryDate: FormControl<Date>;
    }>;
    priority: FormControl<number | undefined>;
}
type WildLiveFormRaw = ReturnType<WildLiveComponent['form']['getRawValue']>;

@Component({
    selector: 'geoengine-manager-wildlive-editor-form',
    templateUrl: './wildlive.component.html',
    styleUrl: './wildlive.component.scss',
    imports: [
        CdkTextareaAutosize,
        IdInputComponent,
        MatButton,
        MatCard,
        MatCardContent,
        MatError,
        MatFormField,
        MatInput,
        MatLabel,
        ReactiveFormsModule,
        CommonModule,
    ],
    providers: [
        {
            provide: NG_VALUE_ACCESSOR,
            useExisting: forwardRef(() => WildLiveComponent),
            multi: true,
        },
    ],
})
export class WildLiveComponent implements ControlValueAccessor {
    protected readonly formBuilder = inject(FormBuilder).nonNullable;

    readonly isNew = input<boolean>(false);

    readonly form: FormGroup<WildLiveForm>;

    errorStateMatcher: ErrorStateMatcher = {
        isErrorState: (control: FormControl | null): boolean => !!control && control.invalid,
    };

    protected onChange: (value: TypedDataProviderDefinition) => void = nop;
    protected onTouched: () => void = nop;

    constructor() {
        this.form = this.formBuilder.group({
            id: this.formBuilder.control('', [isValidUuid, Validators.required]),
            name: this.formBuilder.control('', Validators.required),
            description: this.formBuilder.control(''),
            // auth: formBuilder.group({
            //     refreshToken: formBuilder.control('', Validators.required),
            //     expiryDate: formBuilder.control('', Validators.required),
            // }),
            priority: this.formBuilder.control<number | undefined>(undefined, [
                Validators.min(-32768),
                Validators.max(32767),
                Validators.pattern('^-?\\d+$'),
            ]),
        });

        const formChanged = toSignal<WildLiveFormRaw | undefined>(
            this.form.valueChanges.pipe(
                map(() => this.form.getRawValue()),
                startWith(this.form.getRawValue()),
                pairwise(),
                map(([oldValue, newValue]) => {
                    if (JSON.stringify(oldValue) === JSON.stringify(newValue)) {
                        return undefined;
                    }

                    return newValue;
                }),
            ),
        );

        effect(() => {
            const value = formChanged();
            if (!value) return;

            const definition = definitionFromForm(value);

            // console.log('WildLIVE! form changed', definition);
            this.onChange(definition);
        });
    }

    writeValue(definition: unknown): void {
        if (!definition) return;

        // console.log('WildLIVE! form writeValue', definition);

        if (!instanceOfWildliveDataConnectorDefinition(definition)) {
            throw new Error('WildLIVE! form only support WildliveDataConnectorDefinition');
        }

        let auth = undefined;

        if (definition.user && definition.refreshToken && definition.expiryDate) {
            auth = {
                user: definition.user,
                refreshToken: definition.refreshToken,
                expiryDate: definition.expiryDate,
            };

            this.setAuthInForm(true);
        } else {
            this.setAuthInForm(false);
        }

        this.form.patchValue(
            {
                id: definition.id,
                name: definition.name,
                description: definition.description,
                priority: definition.priority ?? undefined,
                auth,
            },
            {emitEvent: true},
        );
    }
    registerOnChange(fn: unknown): void {
        this.onChange = fn as (value: TypedDataProviderDefinition) => void;
    }
    registerOnTouched(fn: unknown): void {
        this.onTouched = fn as () => void;
    }
    setDisabledState?(isDisabled: boolean): void {
        if (isDisabled) {
            this.form.disable({emitEvent: false});
        } else {
            this.form.enable({emitEvent: false});
        }
    }

    setAuthInForm(hasAuth: boolean): void {
        if (hasAuth && !this.form.controls.auth) {
            this.form.addControl(
                'auth',
                this.formBuilder.group({
                    user: this.formBuilder.control({value: '', disabled: true}),
                    refreshToken: this.formBuilder.control('', Validators.required),
                    expiryDate: this.formBuilder.control({value: new Date(), disabled: true}, Validators.required),
                }),
            );
        } else if (!hasAuth && this.form.controls.auth) {
            this.form.removeControl('auth');
        }
    }

    async connectToWildlivePortal(): Promise<void> {
        const orientation = window.innerWidth > window.innerHeight ? 'landscape' : 'portrait';
        const [popupWidth, popupHeight] = orientation === 'landscape' ? [700, 500] : [360, 660];

        const redirectUri = oidcRedirectPath(window.location, '/oidc-popup');

        const clientId = 'geoengine';
        const keycloakBaseUrl = 'https://auth.geoengine.io/realms/AI4WildLIVE/protocol/openid-connect/auth';

        // const state = Math.random().toString(36).substring(2);
        const {verifier, challenge} = await generatePkcePair();

        const oidcRequestParams = new URLSearchParams({
            client_id: clientId, // eslint-disable-line @typescript-eslint/naming-convention
            redirect_uri: redirectUri, // eslint-disable-line @typescript-eslint/naming-convention
            response_type: 'code', // eslint-disable-line @typescript-eslint/naming-convention
            scope: 'openid offline_access',
            code_challenge_method: 'S256', // eslint-disable-line @typescript-eslint/naming-convention
            code_challenge: challenge, // eslint-disable-line @typescript-eslint/naming-convention
        });

        const oidcUrl = `${keycloakBaseUrl}?${oidcRequestParams.toString()}`;

        // console.log('Opening Wildlive OIDC popup:', oidcRequestParams);

        const popupResponse = await new Promise<Map<string, string>>((resolve, reject) => {
            const popup = window.open(oidcUrl, 'WildliveOIDC', `width=${popupWidth},height=${popupHeight}`);

            if (popup) {
                popup.focus();
            } else {
                return reject(new Error('Failed to open OIDC popup window'));
            }

            popup.addEventListener('message', (event: MessageEvent): void => {
                const oidcResponseParams = new Map((event.data as OidcPopupMessage).oidcParams);

                popup.close();

                resolve(oidcResponseParams);
            });
        });

        // console.log('OIDC popup message event:', popupResponse);

        const code = popupResponse.get('code');
        if (!code) {
            throw new Error('No authorization code received from OIDC popup');
        }

        this.setAuthInForm(true);
        this.form.controls.auth?.patchValue({
            refreshToken: JSON.stringify({
                code,
                pkceVerifier: verifier,
                redirectUri,
            }),
            expiryDate: new Date(), // TODO: set real expiry date
            user: '', // TODO: set real user
        });
    }
}

const definitionFromForm = (form: WildLiveFormRaw): TypedDataProviderDefinition => ({
    type: WildliveDataConnectorDefinitionTypeEnum.WildLive,
    id: form.id,
    name: form.name,
    description: form.description,
    refreshToken: form.auth?.refreshToken,
    expiryDate: form.auth?.expiryDate,
    user: form.auth?.user,
    priority: form.priority,
});

const generatePkcePair = async (): Promise<{verifier: string; challenge: string}> => {
    const array = crypto.getRandomValues(new Uint8Array(32));
    const verifier = btoa(String.fromCharCode(...array))
        .replace(/=/g, '')
        .replace(/\+/g, '-')
        .replace(/\//g, '_');

    const encoder = new TextEncoder();
    const data = encoder.encode(verifier);
    const digest = await crypto.subtle.digest('SHA-256', data);
    const challenge = btoa(String.fromCharCode(...new Uint8Array(digest)))
        .replace(/=/g, '')
        .replace(/\+/g, '-')
        .replace(/\//g, '_');

    return {verifier, challenge};
};

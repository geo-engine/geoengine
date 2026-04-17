import {Component, inject, output, input, effect, signal} from '@angular/core';
import {
    AbstractControl,
    FormBuilder,
    FormControl,
    FormGroup,
    ReactiveFormsModule,
    ValidationErrors,
    ValidatorFn,
    Validators,
} from '@angular/forms';
import {ArunaDataProviderDefinition, TypedDataProviderDefinition} from '@geoengine/api-client';
import {isValidUuid} from '@geoengine/common';
import {IdInputComponent} from '../util/id-input/id-input.component';
import {MatCard, MatCardContent} from '@angular/material/card';
import {MatError, MatFormField} from '@angular/material/form-field';
import {MatInput, MatLabel} from '@angular/material/input';

import {CdkTextareaAutosize} from '@angular/cdk/text-field';
import {MatTooltip} from '@angular/material/tooltip';
import {ErrorStateMatcher} from '@angular/material/core';
import {priorityValidator} from '../util/validators';

@Component({
    selector: 'geoengine-manager-aruna-editor-form',
    templateUrl: './aruna.component.html',
    styleUrl: './aruna.component.scss',
    imports: [
        IdInputComponent,
        ReactiveFormsModule,
        MatCard,
        MatCardContent,
        MatFormField,
        MatLabel,
        MatInput,
        MatError,
        CdkTextareaAutosize,
        MatTooltip,
    ],
})
export class ArunaComponent {
    readonly provider = input<TypedDataProviderDefinition>();
    readonly _provider = signal<TypedDataProviderDefinition | undefined>(undefined);
    readonly createNew = input<boolean>(false);
    readonly readonly = input<boolean>(false);
    readonly visible = input<boolean>(false);

    readonly changed = output<TypedDataProviderDefinition | undefined>();

    form!: FormGroup<{
        apiToken: FormControl<string>;
        apiUrl: FormControl<string>;
        description: FormControl<string>;
        filterLabel: FormControl<string>;
        id: FormControl<string>;
        name: FormControl<string>;
        projectId: FormControl<string>;
        cacheTtl: FormControl<number | undefined>;
        priority: FormControl<number | null | undefined>;
    }>;

    errorStateMatcher: ErrorStateMatcher = {
        isErrorState: (control: FormControl | null): boolean => !!control && control.invalid,
    };

    private fb = inject(FormBuilder);

    constructor() {
        this.form = this.fb.group({
            apiToken: this.fb.nonNullable.control('', Validators.required),
            apiUrl: this.fb.nonNullable.control('', Validators.required),
            description: this.fb.nonNullable.control(''),
            filterLabel: this.fb.nonNullable.control(''),
            id: this.fb.nonNullable.control('', [isValidUuid, Validators.required]),
            name: this.fb.nonNullable.control('', Validators.required),
            projectId: this.fb.nonNullable.control('', Validators.required),
            cacheTtl: this.fb.nonNullable.control<number | undefined>(0, [
                Validators.min(0),
                Validators.max(31536000),
                this.integerValidator(),
            ]),
            priority: this.fb.nonNullable.control<number | null | undefined>(0, [priorityValidator()]),
        });

        effect(() => {
            this._provider.set(this.provider());

            let definition = this.provider() as ArunaDataProviderDefinition;

            if (!definition) {
                definition = {
                    apiToken: '',
                    apiUrl: '',
                    description: '',
                    filterLabel: '',
                    id: '',
                    name: '',
                    projectId: '',
                    cacheTtl: 0,
                    priority: 0,
                    type: 'Aruna',
                };
            }

            this.setFormValue(definition);

            if (this.readonly()) {
                this.form.disable({emitEvent: false});
            } else {
                this.form.enable({emitEvent: false});
            }
        });

        this.form.valueChanges.subscribe((_) => {
            if (!this.form.valid) {
                this.changed.emit(undefined);
            } else {
                this._provider.set({
                    type: 'Aruna',
                    apiToken: this.form.controls['apiToken'].value,
                    apiUrl: this.form.controls['apiUrl'].value,
                    cacheTtl: this.convertToNumber(this.form.controls['cacheTtl'].value) ?? 0,
                    description: this.form.controls['description'].value,
                    filterLabel: this.form.controls['filterLabel'].value,
                    id: this.form.controls['id'].value,
                    name: this.form.controls['name'].value,
                    priority: this.convertToNumber(this.form.controls['priority'].value) ?? 0,
                    projectId: this.form.controls['projectId'].value,
                });
                this.changed.emit(this._provider());
            }
        });
    }

    get priority(): FormControl<number | null | undefined> {
        return this.form.controls['priority'];
    }

    get cacheTtl(): FormControl<number | undefined> {
        return this.form.controls['cacheTtl'];
    }

    private integerValidator(): ValidatorFn {
        return (control: AbstractControl): ValidationErrors | null => {
            const value = control.value;

            const num = Number(value);
            return Number.isInteger(num) ? null : {notInteger: true};
        };
    }

    private convertToNumber(value: number | null | undefined): number | undefined {
        return value ? Number(value) : undefined;
    }

    private setFormValue(provider: ArunaDataProviderDefinition): void {
        this.form.patchValue(
            {
                apiToken: provider.apiToken,
                apiUrl: provider.apiUrl,
                description: provider.description,
                cacheTtl: provider.cacheTtl ?? 0,
                filterLabel: provider.filterLabel,
                id: provider.id,
                name: provider.name,
                priority: provider.priority ?? 0,
                projectId: provider.projectId,
            },
            {emitEvent: false},
        );
    }
}

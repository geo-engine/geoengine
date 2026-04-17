import {AfterViewInit, Component, effect, forwardRef, signal} from '@angular/core';
import {v4 as uuidv4} from 'uuid';
import {ControlValueAccessor, FormControl, NG_VALUE_ACCESSOR, ReactiveFormsModule} from '@angular/forms';
import {MatFormField} from '@angular/material/form-field';
import {MatInput, MatLabel} from '@angular/material/input';
import {MatButton} from '@angular/material/button';
import {UUID} from '@geoengine/common';
import {toSignal} from '@angular/core/rxjs-interop';

@Component({
    selector: 'geoengine-manager-id-input',
    templateUrl: './id-input.component.html',
    styleUrl: './id-input.component.scss',
    providers: [
        {
            provide: NG_VALUE_ACCESSOR,
            useExisting: forwardRef(() => IdInputComponent),
            multi: true,
        },
    ],
    imports: [ReactiveFormsModule, MatFormField, MatInput, MatButton, MatLabel],
})
export class IdInputComponent implements ControlValueAccessor, AfterViewInit {
    readonly idControl = new FormControl<UUID>('', {nonNullable: true});

    readonly isDisabled = signal(false);

    constructor() {
        const valueChanged = toSignal<UUID | undefined>(this.idControl.valueChanges, {initialValue: undefined});

        effect(() => {
            const newValue = valueChanged();
            if (!newValue) return;
            this.onChange(newValue);
        });
    }

    protected onTouched: () => void = () => {
        /* do nothing */
    };

    private onChange: (value: string) => void = () => {
        /* do nothing */
    };

    ngAfterViewInit(): void {
        if (!this.idControl.value) {
            this.generate();
        }
    }

    generate(): void {
        const newId = uuidv4();
        this.idControl.patchValue(newId);
    }

    writeValue(value: unknown): void {
        if (typeof value !== 'string') {
            throw new Error('Value must be a string');
        }

        this.idControl.patchValue(value, {emitEvent: false});
    }

    registerOnChange(fn: (value: string) => void): void {
        this.onChange = fn;
    }

    registerOnTouched(fn: () => void): void {
        this.onTouched = fn;
    }

    setDisabledState(isDisabled: boolean): void {
        this.isDisabled.set(isDisabled);
        if (isDisabled) {
            this.idControl.disable();
        } else {
            this.idControl.enable();
        }
    }
}

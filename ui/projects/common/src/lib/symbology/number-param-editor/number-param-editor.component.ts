import {Component, ChangeDetectionStrategy, forwardRef, HostListener, input} from '@angular/core';
import {ControlValueAccessor, NG_VALUE_ACCESSOR, FormsModule} from '@angular/forms';
import {NumberParam, StaticNumber, DerivedNumber} from '../symbology.model';
import {MatFormField, MatLabel, MatInput} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';

/**
 * An edit component for `NumberParam`
 */
@Component({
    selector: 'geoengine-number-param-editor',
    templateUrl: 'number-param-editor.component.html',
    styleUrls: ['number-param-editor.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    providers: [{provide: NG_VALUE_ACCESSOR, useExisting: forwardRef(() => NumberParamEditorComponent), multi: true}],
    imports: [MatFormField, MatLabel, MatInput, FormsModule, MatSelect, MatOption],
})
export class NumberParamEditorComponent implements ControlValueAccessor {
    readonly attributes = input(new Array<string>());
    readonly min = input(Number.MIN_VALUE);

    numberParam: NumberParam;

    protected _numberAttributeName?: string;
    protected _factor = 1.0;

    protected defaultNumberParam: NumberParam = new StaticNumber(1.0);

    constructor() {
        this.numberParam = this.defaultNumberParam;
    }

    @HostListener('blur') onBlur(): void {
        this.onTouched();
    }

    onTouched = (): void => {
        // do nothing
    };
    onChange = (_: NumberParam | null): void => {
        // do nothing
    };

    writeValue(value: NumberParam | null): void {
        value ??= this.defaultNumberParam;

        if (value instanceof StaticNumber) {
            this.update(
                {
                    defaultNumber: value.getDefault(),
                    numberAttributeName: null,
                },
                false,
            );
        } else if (value instanceof DerivedNumber) {
            this.update(
                {
                    defaultNumber: value.defaultValue,
                    numberAttributeName: value.attribute,
                    factor: value.factor,
                },
                false,
            );
        } else {
            throw Error('Unexpected NumberParam Variant');
        }
    }

    registerOnChange(fn: (_: NumberParam | null) => void): void {
        this.onChange = fn;
    }

    registerOnTouched(fn: () => void): void {
        this.onTouched = fn;
    }

    get defaultNumber(): number {
        return this.numberParam.getDefault();
    }

    set defaultNumber(defaultNumber: number) {
        if (defaultNumber === undefined || defaultNumber === null || defaultNumber < this.min()) {
            return;
        }

        this.update({defaultNumber});
    }

    get isDerived(): boolean {
        return !!this.numberAttributeName;
    }

    get numberAttributeName(): string | undefined {
        return this._numberAttributeName;
    }

    set numberAttributeName(name: string | undefined) {
        this.update({numberAttributeName: name ?? null});
    }

    get factor(): number {
        return this._factor;
    }

    set factor(factor: number) {
        this.update({factor});
    }

    update(
        params: {
            defaultNumber?: number;
            numberAttributeName?: string | null;
            factor?: number;
        },
        emit = true,
    ): void {
        const defaultNumber = params.defaultNumber ?? this.numberParam.getDefault();
        this._numberAttributeName =
            params.numberAttributeName === null ? undefined : (params.numberAttributeName ?? this.numberAttributeName);

        if (this._numberAttributeName) {
            this._factor = params.factor ?? this.factor;

            this.numberParam = new DerivedNumber(this._numberAttributeName, this._factor, defaultNumber);
        } else {
            this.numberParam = new StaticNumber(defaultNumber);
        }

        if (emit) {
            this.onChange(this.numberParam);
        }
    }
}

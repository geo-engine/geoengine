import {
    Component,
    ChangeDetectionStrategy,
    ChangeDetectorRef,
    forwardRef,
    OnChanges,
    SimpleChanges,
    ViewEncapsulation,
    inject,
    input,
} from '@angular/core';

import {ControlValueAccessor, NG_VALUE_ACCESSOR, FormsModule} from '@angular/forms';
import {Color, stringToRgbaStruct} from '../color';
import {
    FxLayoutDirective,
    FxLayoutAlignDirective,
    FxLayoutGapDirective,
    FxFlexDirective,
} from '../../util/directives/flexbox-legacy.directive';
import {MatFormField, MatInput, MatHint} from '@angular/material/input';
import {ColorPickerDirective} from 'ngx-color-picker';

export interface ColorAttributeInput {
    readonly key: string;
    readonly value: Color;
}

export interface ColorAttributeInputHinter {
    colorHint(key: string): string | undefined;
}

@Component({
    selector: 'geoengine-color-attribute-input',
    templateUrl: './color-attribute-input.component.html',
    styleUrls: ['./color-attribute-input.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    providers: [{provide: NG_VALUE_ACCESSOR, useExisting: forwardRef(() => ColorAttributeInputComponent), multi: true}],
    encapsulation: ViewEncapsulation.Emulated,
    imports: [
        FxLayoutDirective,
        FxLayoutAlignDirective,
        FxLayoutGapDirective,
        MatFormField,
        FxFlexDirective,
        MatInput,
        FormsModule,
        MatHint,
        ColorPickerDirective,
    ],
})
export class ColorAttributeInputComponent implements ControlValueAccessor, OnChanges {
    private changeDetectorRef = inject(ChangeDetectorRef);

    readonly readonlyAttribute = input(false);
    readonly readonlyColor = input(false);
    readonly attributePlaceholder = input('attribute');
    readonly colorPlaceholder = input('color');
    readonly colorAttributeHinter = input<ColorAttributeInputHinter>();

    onTouched?: () => void;
    onChange?: (_: ColorAttributeInput) => void = undefined;

    colorAttributeInput?: ColorAttributeInput;
    cssString = '';

    hasColorHint(): boolean {
        return this.colorAttributeHinter() !== undefined;
    }

    colorHint(key: string | undefined): string | undefined {
        if (!key) {
            return undefined;
        }
        const colorAttributeHinter = this.colorAttributeHinter();
        if (colorAttributeHinter) {
            return colorAttributeHinter.colorHint(key);
        }
        return undefined;
    }

    updateKey(key?: string): void {
        if (!key || !this.colorAttributeInput || key === this.colorAttributeInput.key) {
            return;
        }

        this.colorAttributeInput = {
            key,
            value: this.colorAttributeInput.value,
        };
    }

    updateColor(value: string): void {
        if (!value || !this.colorAttributeInput) {
            return;
        }

        const color = Color.fromRgbaLike(stringToRgbaStruct(value));

        this.colorAttributeInput = {
            key: this.colorAttributeInput.key,
            value: color,
        };
        this.cssString = color.rgbaCssString();

        this.propagateChange();
    }

    ngOnChanges(changes: SimpleChanges): void {
        if (changes.inputType || changes.attributePlaceholder || changes.colorPlaceholder) {
            this.changeDetectorRef.markForCheck();
        }
    }

    // Set touched on blur
    onBlur(): void {
        if (this.onTouched) {
            this.onTouched();
        }
    }

    writeValue(colorAttributeInput?: ColorAttributeInput): void {
        this.colorAttributeInput = colorAttributeInput;

        if (!colorAttributeInput) {
            return;
        }

        this.cssString = colorAttributeInput.value.rgbaCssString();
    }

    registerOnChange(fn: (_: ColorAttributeInput) => void): void {
        this.onChange = fn;
        this.propagateChange();
    }

    registerOnTouched(fn: () => void): void {
        this.onTouched = fn;
    }

    propagateChange(): void {
        if (this.onChange && this.colorAttributeInput) {
            this.onChange(this.colorAttributeInput);
        }
    }
}

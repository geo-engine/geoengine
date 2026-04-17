import {Component, ChangeDetectionStrategy, forwardRef, HostListener, input} from '@angular/core';
import {ControlValueAccessor, NG_VALUE_ACCESSOR, FormsModule} from '@angular/forms';
import {BLACK, Color} from '../../colors/color';
import {ColorAttributeInput, ColorAttributeInputComponent} from '../../colors/color-attribute-input/color-attribute-input.component';
import {ColorBreakpoint} from '../../colors/color-breakpoint.model';
import {Colorizer, LinearGradient, PaletteColorizer} from '../../colors/colorizer.model';
import {ColorParam, DerivedColor, StaticColor} from '../symbology.model';
import {MatFormField, MatLabel} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {ColorBreakpointInputComponent} from '../../colors/color-breakpoint-input/color-breakpoint-input.component';
import {FxFlexDirective} from '../../util/directives/flexbox-legacy.directive';
import {MatIconButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';

/**
 * An edit component for `ColorParam`
 */
@Component({
    selector: 'geoengine-color-param-editor',
    templateUrl: 'color-param-editor.component.html',
    styleUrls: ['color-param-editor.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    providers: [{provide: NG_VALUE_ACCESSOR, useExisting: forwardRef(() => ColorParamEditorComponent), multi: true}],
    imports: [
        ColorAttributeInputComponent,
        FormsModule,
        MatFormField,
        MatLabel,
        MatSelect,
        MatOption,
        ColorBreakpointInputComponent,
        FxFlexDirective,
        MatIconButton,
        MatIcon,
    ],
})
export class ColorParamEditorComponent implements ControlValueAccessor {
    readonly attributes = input(new Array<string>());

    colorParam: ColorParam;

    protected _colorAttributeName?: string;
    // TODO: logarithmic gradient
    protected _colorizerType: 'linearGradient' | 'palette' = 'linearGradient';
    protected _colorizerBreakpoints = new Array<ColorBreakpoint>();

    protected defaultColorParam: ColorParam = new StaticColor(BLACK);

    protected _defaultColorAttribute: ColorAttributeInput;

    constructor() {
        this.colorParam = this.defaultColorParam;
        this._defaultColorAttribute = {key: '', value: this.colorParam.getDefault()};
    }

    @HostListener('blur') onBlur(): void {
        this.onTouched();
    }

    onTouched = (): void => {
        // do nothing
    };
    onChange = (_: ColorParam | null): void => {
        // do nothing
    };

    writeValue(value: ColorParam | null): void {
        value ??= this.defaultColorParam;

        if (value instanceof StaticColor) {
            this.update(
                {
                    defaultColor: value.getDefault(),
                    colorAttributeName: null,
                },
                false,
            );
        } else if (value instanceof DerivedColor) {
            this._colorAttributeName = value.attribute;

            let colorizerType: 'linearGradient' | 'palette';
            let defaultColor: Color;
            if (value.colorizer instanceof LinearGradient) {
                colorizerType = 'linearGradient';
                // TODO: refactor over/under color
                defaultColor = value.colorizer.underColor;
            } else if (value.colorizer instanceof PaletteColorizer) {
                colorizerType = 'palette';
                defaultColor = value.colorizer.defaultColor;
            } else {
                throw Error('Unexpected Colorizer Type');
            }

            this.update(
                {
                    defaultColor,
                    colorAttributeName: value.attribute,
                    colorizerType,
                    colorizerBreakpoints: value.colorizer.getBreakpoints(),
                },
                false,
            );
        } else {
            throw Error('Unexpected ColorParam Variant');
        }
    }

    registerOnChange(fn: (_: ColorParam | null) => void): void {
        this.onChange = fn;
    }

    registerOnTouched(fn: () => void): void {
        this.onTouched = fn;
    }

    get isDerived(): boolean {
        return !!this.colorAttributeName;
    }

    get defaultColorAttribute(): ColorAttributeInput {
        return this._defaultColorAttribute;
    }

    set defaultColorAttribute(colorAttribute: ColorAttributeInput) {
        this.update({
            defaultColor: colorAttribute.value,
        });
    }

    get colorAttributeName(): string | undefined {
        return this._colorAttributeName;
    }

    set colorAttributeName(name: string | undefined) {
        this.update({colorAttributeName: name ?? null});
    }

    get colorizerType(): 'linearGradient' | 'palette' {
        return this._colorizerType;
    }

    set colorizerType(colorizerType: 'linearGradient' | 'palette') {
        this.update({colorizerType});
    }

    get colorizerBreakpoints(): Array<ColorBreakpoint> {
        return this._colorizerBreakpoints;
    }

    set colorizerBreakpoints(colorizerBreakpoints: Array<ColorBreakpoint>) {
        this.update({colorizerBreakpoints});
    }

    updateBreakpointAt(i: number, breakpoint: ColorBreakpoint): void {
        if (i >= this.colorizerBreakpoints.length) {
            return;
        }

        const oldBreakpoint = this.colorizerBreakpoints[i];

        if (oldBreakpoint.equals(breakpoint)) {
            return;
        }

        const colorizerBreakpoints = [...this.colorizerBreakpoints];
        colorizerBreakpoints[i] = breakpoint;
        this.colorizerBreakpoints = colorizerBreakpoints;
    }

    /**
     *  Helps tracking the items in the ngFor by their color, to prevent re-rendering and unfocusing the input
     */
    identify(index: number, item: ColorBreakpoint): Color {
        return item.color;
    }

    addBreakpointAt(i: number): void {
        if (i >= this.colorizerBreakpoints.length) {
            return;
        }

        const colorizerBreakpoints = this.colorizerBreakpoints;

        const valueSuggestion = colorizerBreakpoints[i].value + 1;
        const colorSuggestion = colorizerBreakpoints[i].color;

        colorizerBreakpoints.splice(i + 1, 0, new ColorBreakpoint(valueSuggestion, colorSuggestion));

        this.colorizerBreakpoints = colorizerBreakpoints;
    }

    removeBreakpointAt(i: number): void {
        const colorizerBreakpoints = this.colorizerBreakpoints;

        colorizerBreakpoints.splice(i, 1);

        this.colorizerBreakpoints = colorizerBreakpoints;
    }

    update(
        params: {
            defaultColor?: Color;
            colorAttributeName?: string | null;
            colorizerType?: 'linearGradient' | 'palette';
            colorizerBreakpoints?: Array<ColorBreakpoint>;
        },
        emit = true,
    ): void {
        const defaultColor = params.defaultColor ?? this.colorParam.getDefault();
        this._colorAttributeName = params.colorAttributeName === null ? undefined : (params.colorAttributeName ?? this.colorAttributeName);

        if (this._colorAttributeName) {
            this._colorizerType = params.colorizerType ?? this.colorizerType;
            this._colorizerBreakpoints = params.colorizerBreakpoints ?? this.colorizerBreakpoints;

            if (this._colorizerBreakpoints.length <= 0) {
                // if there is no breakpoint yet, add one
                this._colorizerBreakpoints.push(new ColorBreakpoint(0, BLACK));
            }

            this.colorParam = new DerivedColor(
                this._colorAttributeName,
                this.computeColorizer({defaultColor, colorizerType: this._colorizerType, colorizerBreakpoints: this._colorizerBreakpoints}),
            );
        } else {
            this.colorParam = new StaticColor(defaultColor);
        }

        this._defaultColorAttribute = {key: '', value: this.colorParam.getDefault()};

        if (emit) {
            this.onChange(this.colorParam);
        }
    }

    protected computeColorizer(params: {
        defaultColor: Color;
        colorizerType: 'linearGradient' | 'palette';
        colorizerBreakpoints: Array<ColorBreakpoint>;
    }): Colorizer {
        switch (this.colorizerType) {
            case 'linearGradient':
                // TODO: refactor default color -> over/under color
                return new LinearGradient(params.colorizerBreakpoints, params.defaultColor, params.defaultColor, params.defaultColor);
            case 'palette': {
                return new PaletteColorizer(
                    new Map(params.colorizerBreakpoints.map((breakpoint) => [breakpoint.value, breakpoint.color])),
                    params.defaultColor,
                    params.defaultColor,
                );
            }
        }
    }
}

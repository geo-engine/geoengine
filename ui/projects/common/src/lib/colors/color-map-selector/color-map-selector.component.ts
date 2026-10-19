import {NgClass, KeyValuePipe} from '@angular/common';
import {
    ChangeDetectionStrategy,
    ChangeDetectorRef,
    Component,
    OnChanges,
    OnDestroy,
    OnInit,
    SimpleChanges,
    inject,
    input,
    output,
} from '@angular/core';
import {FormControl, FormGroup, NonNullableFormBuilder, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {MatOption} from '@angular/material/autocomplete';
import {MatCheckbox} from '@angular/material/checkbox';
import {MatFormField, MatLabel, MatInput, MatHint} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatSlider, MatSliderThumb} from '@angular/material/slider';
import {Subscription} from 'rxjs';
import {FxLayoutDirective, FxLayoutAlignDirective, FxFlexDirective} from '../../util/directives/flexbox-legacy.directive';
import {geoengineValidators} from '../../util/form.validators';
import {ColorBreakpointsCssGradientPipe, RgbaArrayCssGradientPipe} from '../../util/pipes/color-gradients.pipe';
import {Color, RgbaTuple} from '../color';
import {ColorBreakpoint} from '../color-breakpoint.model';
import {ALL_COLORMAPS} from '../colormaps/colormaps';

interface ColorMapSelectorForm {
    bounds: FormGroup<{min: FormControl<number>; max: FormControl<number>}>;
    colorMap: FormControl<Array<RgbaTuple>>;
    colorMapSteps: FormControl<number>;
    colorMapReverseColors: FormControl<boolean>;
}

/**
 * The ColormapColorizerComponent is a dialog to generate ColorizerData from colormaps.
 */
@Component({
    selector: 'geoengine-color-map-selector',
    templateUrl: 'color-map-selector.component.html',
    styleUrls: ['color-map-selector.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        FormsModule,
        FxLayoutDirective,
        ReactiveFormsModule,
        FxLayoutAlignDirective,
        MatFormField,
        FxFlexDirective,
        MatLabel,
        MatInput,
        MatHint,
        NgClass,
        MatSelect,
        MatOption,
        MatCheckbox,
        MatSlider,
        MatSliderThumb,
        KeyValuePipe,
        ColorBreakpointsCssGradientPipe,
        RgbaArrayCssGradientPipe,
    ],
})
export class ColorMapSelectorComponent implements OnInit, OnDestroy, OnChanges {
    protected readonly changeDetectorRef = inject(ChangeDetectorRef);
    protected readonly formBuilder = inject(NonNullableFormBuilder);

    /**
     * Emmits colorizer breakpoint arrays
     */
    readonly breakpointsChange = output<Array<ColorBreakpoint>>();

    /**
     * Informs parent to enable "Apply Changes" button
     */
    readonly changesToForm = output<void>();

    /**
     * Number of breakpoints used in the ColorizerData.
     */
    readonly defaultNumberOfSteps = input(16);

    /**
     * Max allowed number of breakpoints in the ColorizerData.
     */
    readonly maxColormapSteps = input(16);

    /**
     * Sets the min value used for ColorizerData generation.
     */
    readonly minValue = input<number | undefined>(0);

    /**
     * Sets the max value used for ColorizerData generation.
     */
    readonly maxValue = input<number | undefined>(1);

    readonly scale = input<'linear' | 'logarithmic'>('linear');

    /**
     * Sends the min value selected in the ui.
     */
    readonly minValueChange = output<number>();

    /**
     * Sends the max value selected in the ui.
     */
    readonly maxValueChange = output<number>();

    readonly colorMaps = ALL_COLORMAPS;

    /**
     * The form control used in the template.
     */
    form: FormGroup<ColorMapSelectorForm>;

    /**
     * The local (work-in-progress) Colorizer.
     */
    breakpoints?: Array<ColorBreakpoint>;

    protected subscriptions: Array<Subscription> = [];

    protected readonly largerThanZeroValidator = geoengineValidators.largerThan(0);

    constructor() {
        const formBuilder = this.formBuilder;

        const initialColorMapName = Object.keys(this.colorMaps)[0];

        this.form = formBuilder.group({
            bounds: formBuilder.group(
                {
                    min: formBuilder.control(0),
                    max: formBuilder.control(1),
                },
                {
                    validators: [geoengineValidators.minAndMax('min', 'max', {checkBothExist: true, mustNotEqual: true})],
                },
            ),
            colorMap: formBuilder.control(this.colorMaps[initialColorMapName], [Validators.required]),
            colorMapSteps: formBuilder.control(this.defaultNumberOfSteps(), [Validators.required, Validators.min(2)]),
            colorMapReverseColors: formBuilder.control(false),
        });

        this.breakpoints = this.createBreakpoints();

        this.updateOnScaleChange();
    }

    ngOnInit(): void {
        const sub = this.form.valueChanges.subscribe((_) => {
            if (this.form.invalid) {
                this.removeColorizerData();
            }
            this.updateColorizerData();
        });
        this.subscriptions.push(sub);

        const minValue = this.minValue();
        const maxValue = this.maxValue();
        if (minValue && maxValue) {
            this.patchMinMaxValues(minValue, maxValue);
        }

        const subMinMax = this.form.controls.bounds.valueChanges.subscribe((x) => {
            if (x.min !== undefined && Number.isFinite(x.min)) {
                this.minValueChange.emit(x.min);
            }
            if (x.max !== undefined && Number.isFinite(x.max)) {
                this.maxValueChange.emit(x.max);
            }
        });
        this.subscriptions.push(subMinMax);

        this.form.valueChanges.subscribe(() => {
            this.changesToForm.emit(undefined);
        });
    }

    ngOnDestroy(): void {
        this.subscriptions.forEach((s) => s.unsubscribe());
    }

    ngOnChanges(changes: SimpleChanges): void {
        if (changes.minValue || changes.maxValue) {
            this.patchMinMaxValues(this.minValue(), this.maxValue());
        }

        if (changes.scale) {
            this.updateOnScaleChange();
        }
    }

    /**
     * Replace the min and max values.
     */
    patchMinMaxValues(min?: number, max?: number): void {
        if (typeof min !== 'number' || typeof max !== 'number') {
            return;
        }

        const bounds = this.form.controls.bounds.getRawValue();

        if (bounds.min === min && bounds.max === max) {
            return;
        }

        this.form.controls.bounds.setValue({min, max});

        this.updateColorizerData();
    }

    updateOnScaleChange(): void {
        const minControl = this.form.controls.bounds.controls.min;

        const scale = this.scale();
        if (scale === 'linear') {
            minControl.removeValidators(this.largerThanZeroValidator);
        } else if (scale === 'logarithmic') {
            minControl.setValidators(this.largerThanZeroValidator);
        }

        minControl.updateValueAndValidity();
    }

    /**
     * Clears the local colorizer data.
     */
    removeColorizerData(): void {
        this.breakpoints = undefined;
    }

    /**
     * Apply changes to color table to the colorizer data.
     */
    applyChanges(): void {
        if (!this.breakpoints) {
            return;
        }

        this.breakpointsChange.emit(this.breakpoints);
    }

    public static createLinearBreakpoints(
        colorMap: Array<RgbaTuple>,
        colorMapSteps: number,
        colorMapReverseColors: boolean,
        bounds: {min: number; max: number},
    ): Array<ColorBreakpoint> {
        const breakpoints = new Array<ColorBreakpoint>();

        for (let i = 0; i < colorMapSteps; i++) {
            const frac = i / (colorMapSteps - 1);
            const value = bounds.min + frac * (bounds.max - bounds.min);

            const colorMapPos = frac * (colorMap.length - 1);
            let colorMapIndex = Math.floor(colorMapPos);
            const colorMapPosRemainder = colorMapPos - colorMapIndex;
            let nextMapIndex = Math.min(colorMapIndex + 1, colorMap.length - 1);

            if (colorMapReverseColors) {
                colorMapIndex = colorMap.length - colorMapIndex - 1;
                nextMapIndex = Math.max(0, colorMapIndex - 1);
            }

            // we use the colorMapPosRemainder to interpolate between the two colors
            //if the remainder is 0, we can just use the colorMapIndex and return the color
            if (colorMapPosRemainder < Number.EPSILON) {
                breakpoints.push(new ColorBreakpoint(value, Color.fromRgbaLike(colorMap[colorMapIndex])));
                continue;
            }

            //otherwise we need to interpolate between the two colors
            const color = Color.fromRgbaLike(colorMap[colorMapIndex]);
            const nextColor = Color.fromRgbaLike(colorMap[nextMapIndex]);

            const r = color.r + colorMapPosRemainder * (nextColor.r - color.r);
            const g = color.g + colorMapPosRemainder * (nextColor.g - color.g);
            const b = color.b + colorMapPosRemainder * (nextColor.b - color.b);
            const a = color.a + colorMapPosRemainder * (nextColor.a - color.a);

            const realColor = Color.fromRgbaLike([Math.trunc(r), Math.trunc(g), Math.trunc(b), Math.trunc(a)]);

            breakpoints.push(new ColorBreakpoint(value, realColor));
        }

        // override last because of rounding errors
        breakpoints[breakpoints.length - 1] = breakpoints[breakpoints.length - 1].cloneWithValue(bounds.max);

        return breakpoints;
    }

    protected updateColorizerData(): void {
        if (!this.checkValidConfig()) {
            this.breakpoints = undefined;
            return;
        }

        this.breakpoints = this.createBreakpoints();

        this.changeDetectorRef.markForCheck();
    }

    protected checkValidConfig(): boolean {
        const {colorMap, colorMapSteps, bounds} = this.form.getRawValue();
        const boundsMin = bounds.min;
        const boundsMax = bounds.max;

        if (!colorMap) {
            return false;
        }
        if (colorMapSteps > this.maxColormapSteps()) {
            return false;
        }
        if (boundsMin >= boundsMax) {
            return false;
        }

        if (this.scale() === 'logarithmic' && boundsMin <= 0) {
            return false;
        }

        return true;
    }

    protected createBreakpoints(): Array<ColorBreakpoint> {
        const {colorMap, colorMapSteps, colorMapReverseColors, bounds} = this.form.getRawValue();

        if (this.scale() === 'logarithmic') {
            return this.createLogarithmicBreakpoints(colorMap, colorMapSteps, colorMapReverseColors, bounds);
        } else {
            // `this.scale === 'linear'`
            return ColorMapSelectorComponent.createLinearBreakpoints(colorMap, colorMapSteps, colorMapReverseColors, bounds);
        }
    }

    protected createLogarithmicBreakpoints(
        colorMap: Array<RgbaTuple>,
        colorMapSteps: number,
        colorMapReverseColors: boolean,
        bounds: {min: number; max: number},
    ): Array<ColorBreakpoint> {
        if (bounds.min <= 0) {
            // TODO: handle
            throw new Error('logarithmic scale requires min > 0');
        }

        const breakpoints = new Array<ColorBreakpoint>();

        for (let i = 0; i < colorMapSteps; i++) {
            const frac = i / (colorMapSteps - 1);
            const value = Math.exp(Math.log(bounds.min) + frac * (Math.log(bounds.max) - Math.log(bounds.min)));

            const colorMapPos = frac * (colorMap.length - 1);
            let colorMapIndex = Math.floor(colorMapPos);
            const colorMapPosRemainder = colorMapPos - colorMapIndex;
            let nextMapIndex = Math.min(colorMapIndex + 1, colorMap.length - 1);

            if (colorMapReverseColors) {
                colorMapIndex = colorMap.length - colorMapIndex - 1;
                nextMapIndex = Math.max(0, colorMapIndex - 1);
            }

            // we use the colorMapPosRemainder to interpolate between the two colors
            //if the remainder is 0, we can just use the colorMapIndex and return the color
            if (colorMapPosRemainder < Number.EPSILON) {
                breakpoints.push(new ColorBreakpoint(value, Color.fromRgbaLike(colorMap[colorMapIndex])));
                continue;
            }

            //otherwise we need to interpolate between the two colors
            const color = Color.fromRgbaLike(colorMap[colorMapIndex]);
            const nextColor = Color.fromRgbaLike(colorMap[nextMapIndex]);

            const r = color.r + colorMapPosRemainder * (nextColor.r - color.r);
            const g = color.g + colorMapPosRemainder * (nextColor.g - color.g);
            const b = color.b + colorMapPosRemainder * (nextColor.b - color.b);
            const a = color.a + colorMapPosRemainder * (nextColor.a - color.a);

            const realColor = Color.fromRgbaLike([Math.trunc(r), Math.trunc(g), Math.trunc(b), Math.trunc(a)]);

            breakpoints.push(new ColorBreakpoint(value, realColor));
        }

        // override last because of rounding errors
        breakpoints[breakpoints.length - 1] = breakpoints[breakpoints.length - 1].cloneWithValue(bounds.max);

        return breakpoints;
    }
}

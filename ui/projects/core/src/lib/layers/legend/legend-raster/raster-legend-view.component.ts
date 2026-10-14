import {ChangeDetectionStrategy, Component, computed, input, Pipe, PipeTransform} from '@angular/core';
import {
    BreakpointToCssStringPipe,
    ColorBreakpoint,
    MultiBandRasterColorizer,
    RasterColorizer,
    RasterColorizerCssGradientPipe,
    RasterLayer,
    RasterLayerMetadata,
    SingleBandRasterColorizer,
} from '@geoengine/common';
import {RasterBandDescriptor, Measurement, ContinuousMeasurement, ClassificationMeasurement} from '@geoengine/api-client';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {CommonModule as AngularCommonModule} from '@angular/common';

/**
 * calculate the decimal places for the legend of raster data
 */
export function calculateNumberPipeParameters(breakpoints: Array<ColorBreakpoint>): string {
    //minimal and maximal breakpoint
    const firstNumber = breakpoints[0].value.toString(10);
    const lastNumber = breakpoints[breakpoints.length - 1].value.toString(10);
    //maximal decimal places of the minimal and maximal breakpoint
    const decimalPlacesFirst = firstNumber.includes('.') ? firstNumber.split('.')[1].length : 0;
    const decimalPlacesLast = lastNumber.includes('.') ? lastNumber.split('.')[1].length : 0;
    const maximumDecimalPlaces = Math.max(decimalPlacesFirst, decimalPlacesLast);
    //stepsize
    const range = breakpoints[breakpoints.length - 1].value - breakpoints[0].value;
    const steps = breakpoints.length - 1;
    const stepSize = range / steps;

    if (stepSize >= 1) return `1.0-${Math.max(0, maximumDecimalPlaces)}`;
    else if (stepSize >= 0.1) return `1.0-${Math.max(1, maximumDecimalPlaces)}`;
    else return `1.0-${Math.max(2, maximumDecimalPlaces)}`;
}

@Pipe({
    name: 'classificationMeasurement',
    pure: true,
})
export class CastMeasurementToClassificationPipe implements PipeTransform {
    transform(value: Measurement, _args?: unknown): ClassificationMeasurement | null {
        if (value.type == 'classification') {
            return value;
        } else {
            return null;
        }
    }
}

@Pipe({
    name: 'continuousMeasurement',
    pure: true,
})
export class CastMeasurementToContinuousPipe implements PipeTransform {
    transform(value: Measurement, _args?: unknown): ContinuousMeasurement | null {
        if (value.type == 'continuous') {
            return value;
        } else {
            return null;
        }
    }
}

/**
 * Human readable text of a band measurement, e.g. `Reflectance (in %)`
 */
export function measurementText(measurement: Measurement): string {
    switch (measurement.type) {
        case 'continuous':
            return measurement.unit ? `${measurement.measurement} (in ${measurement.unit})` : measurement.measurement;
        case 'classification':
            return measurement.measurement;
        case 'unitless':
            return 'unitless';
    }
}

@Pipe({
    name: 'legendMeasurementText',
    pure: true,
})
export class LegendMeasurementTextPipe implements PipeTransform {
    transform(value: Measurement, _args?: unknown): string {
        return measurementText(value);
    }
}

/**
 * The number of significant decimal digits a double represents without error (`DBL_DIG` in C).
 * Floating point errors like `0.29 * 100 = 28.999999999999996` only show up in the 16th or 17th digit,
 * so rounding to this precision removes them without changing any real digit.
 */
const DOUBLE_SIGNIFICANT_DIGITS = 15;

/**
 * Non-zero values with a smaller magnitude are shown in exponential notation (e.g. `1e-7`),
 * since a fixed number of decimals would cut them off to `0`.
 */
const EXPONENTIAL_NOTATION_THRESHOLD = 1e-4;

/**
 * Format values for display, cutting off decimals that are not needed to tell neighboring values apart.
 * Trailing zeros are omitted, e.g. `[1, 1.0001]` becomes `['1', '1.0001']`.
 */
export function unifyDecimals(values: number[], minDecimals = 0): string[] {
    const minRoundAt = Math.pow(10, minDecimals);
    // tiny values cannot be compared digit by digit, so they stay the same
    const isTiny = (x: number): boolean => x !== 0 && Math.abs(x) < EXPONENTIAL_NOTATION_THRESHOLD;
    // remove floating point errors before flooring, e.g. `0.29 * 100 = 28.999999999999996`
    const floorAt = (x: number, roundAt: number): string =>
        (Math.floor(Number((x * roundAt).toPrecision(DOUBLE_SIGNIFICANT_DIGITS))) / roundAt).toString();

    // Early return if all values differ by more than 1
    if (oneApart(values.map((x) => Math.floor(x)).sort((a, b) => a - b))) {
        return values.map((x) => (isTiny(x) ? x.toExponential() : floorAt(x, minRoundAt)));
    }

    // Find highest overlap for cut-off point
    let maxOverlap = 0;
    for (let i = 0; i < values.length - 1; i++) {
        if (isTiny(values[i]) || isTiny(values[i + 1])) {
            continue;
        }
        const overlap: number = overlappingDigits(values[i], values[i + 1]);
        maxOverlap = overlap > maxOverlap ? overlap : maxOverlap;
    }

    return values.map((x) => {
        if (isTiny(x)) {
            return x.toExponential();
        }
        const preDecimals = Math.floor(x).toString().length;
        const roundAt = Math.max(minRoundAt, Math.pow(10, Math.max(0, maxOverlap + 2 - preDecimals)));
        return floorAt(x, roundAt);
    });
}

export function overlappingDigits(val1: number, val2: number): number {
    let overlap = 0;
    // pad the decimals to the same length, so that e.g. `1` and `1.0001` share the digits `1.000`
    const [int1, frac1 = ''] = val1.toString().split('.');
    const [int2, frac2 = ''] = val2.toString().split('.');
    const fracLength = Math.max(frac1.length, frac2.length);
    const str1 = fracLength > 0 ? `${int1}.${frac1.padEnd(fracLength, '0')}` : int1;
    const str2 = fracLength > 0 ? `${int2}.${frac2.padEnd(fracLength, '0')}` : int2;
    const maxLength = Math.min(str1.length, str2.length);
    let passedDecimal = false;
    for (let i = 0; i < maxLength; i++) {
        if (str1.charAt(i) === '.' && str2.charAt(i) === '.') {
            passedDecimal = true;
        }
        if (str1.charAt(i) !== str2.charAt(i)) {
            break;
        }
        overlap++;
    }
    return passedDecimal ? overlap - 1 : overlap;
}

export function oneApart(values: number[]): boolean {
    let apart = true;
    for (let i = 0; i < values.length - 1; i++) {
        if (values[i + 1] - values[i] < 1) {
            apart = false;
            break;
        }
    }
    return apart;
}

/**
 * Select the band descriptors that are used by the raster colorizer
 */
export function selectBands(bands: Array<RasterBandDescriptor>, rasterColorizer: RasterColorizer): Array<RasterBandDescriptor> {
    if (rasterColorizer instanceof SingleBandRasterColorizer) {
        return [bands[rasterColorizer.band]];
    } else if (rasterColorizer instanceof MultiBandRasterColorizer) {
        return [bands[rasterColorizer.redBand], bands[rasterColorizer.greenBand], bands[rasterColorizer.blueBand]];
    } else {
        throw new Error('Unknown raster colorizer');
    }
}

export interface RgbLegendChannel {
    label: 'Red' | 'Green' | 'Blue';
    cssColor: string;
    band: RasterBandDescriptor;
    min: string;
    max: string;
    scale: number;
}

/**
 * Describe the red, green and blue channels of a multi band raster colorizer for the legend.
 * Returns `undefined` for other colorizers.
 */
export function selectRgbChannels(
    bands: Array<RasterBandDescriptor>,
    rasterColorizer: RasterColorizer,
): Array<RgbLegendChannel> | undefined {
    if (!(rasterColorizer instanceof MultiBandRasterColorizer)) {
        return undefined;
    }

    const channel = (
        label: RgbLegendChannel['label'],
        cssColor: string,
        bandIndex: number,
        min: number,
        max: number,
        scale: number,
    ): RgbLegendChannel => {
        const [unifiedMin, unifiedMax] = unifyDecimals([min, max], 2);
        return {label, cssColor, band: bands[bandIndex], min: unifiedMin, max: unifiedMax, scale};
    };

    const c = rasterColorizer;
    return [
        channel('Red', '#e5484d', c.redBand, c.redMin, c.redMax, c.redScale),
        channel('Green', '#30a46c', c.greenBand, c.greenMin, c.greenMax, c.greenScale),
        channel('Blue', '#3e63dd', c.blueBand, c.blueMin, c.blueMax, c.blueScale),
    ];
}

/**
 * The raster legend view component.
 * It displays the legend for a raster layer with the given metadata and has no service dependencies.
 * Shows a loading spinner as long as no metadata is given.
 */
@Component({
    selector: 'geoengine-raster-legend-view',
    templateUrl: 'raster-legend-view.component.html',
    styleUrls: ['raster-legend-view.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        AngularCommonModule,
        BreakpointToCssStringPipe,
        CastMeasurementToClassificationPipe,
        LegendMeasurementTextPipe,
        MatProgressSpinner,
        RasterColorizerCssGradientPipe,
    ],
})
export class RasterLegendViewComponent {
    readonly layer = input.required<RasterLayer>();
    readonly metadata = input<RasterLayerMetadata | undefined>(undefined);
    readonly orderValuesDescending = input<boolean>(false);
    readonly showBandNames = input<boolean>(true);

    readonly selectedBands = computed<Array<RasterBandDescriptor> | undefined>(() => {
        const metadata = this.metadata();
        if (!metadata) {
            return undefined;
        }
        return selectBands(metadata.bands, this.layer().symbology.rasterColorizer);
    });
    readonly rgbChannels = computed<Array<RgbLegendChannel> | undefined>(() => {
        const metadata = this.metadata();
        if (!metadata) {
            return undefined;
        }
        return selectRgbChannels(metadata.bands, this.layer().symbology.rasterColorizer);
    });
    readonly displayedBreakpoints = computed<Array<string>>(() =>
        calculateDisplayedBreakpoints(this.layer(), this.orderValuesDescending()),
    );
    readonly colorizerBreakpoints = computed<Array<ColorBreakpoint>>(() => {
        const layer = this.layer();
        if (this.orderValuesDescending()) {
            return layer.symbology.rasterColorizer.getBreakpoints().slice().reverse();
        } else {
            return layer.symbology.rasterColorizer.getBreakpoints();
        }
    });
    readonly gradientAngle = computed<number>(() => (this.orderValuesDescending() ? 0 : 180));
    readonly bandNamesText = computed<string>(() => (this.selectedBands() ?? []).map((band) => band.name).join(', '));
    readonly measurementsText = computed<string>(() =>
        (this.selectedBands() ?? []).map((band) => measurementText(band.measurement)).join(', '),
    );
    readonly bandsHaveUnits = computed<boolean>(() => {
        const selectedBands = this.selectedBands();
        if (!selectedBands) {
            return false;
        }
        return selectedBands.some((band) => band.measurement.type !== 'unitless');
    });
}

/**
 * Calculate the displayed breakpoints for the legend
 */
function calculateDisplayedBreakpoints(layer: RasterLayer, orderValuesDescending: boolean): Array<string> {
    let displayedBreakpoints = unifyDecimals(layer.symbology.rasterColorizer.getBreakpoints().map((x) => x.value));

    if (orderValuesDescending) {
        displayedBreakpoints = displayedBreakpoints.reverse();
    }

    return displayedBreakpoints;
}

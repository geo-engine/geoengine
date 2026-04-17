import {ChangeDetectionStrategy, Component, computed, effect, inject, input, Pipe, PipeTransform, signal, untracked} from '@angular/core';
import {ProjectService} from '../../../project/project.service';
import {firstValueFrom} from 'rxjs';
import {
    BreakpointToCssStringPipe,
    ColorBreakpoint,
    MultiBandRasterColorizer,
    RasterColorizerCssGradientPipe,
    RasterLayer,
    SingleBandRasterColorizer,
} from '@geoengine/common';
import {
    RasterBandDescriptor,
    Measurement,
    ContinuousMeasurement,
    ClassificationMeasurement,
    UnitlessMeasurement,
} from '@geoengine/api-client';
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

@Pipe({
    name: 'unitlessMeasurement',
    pure: true,
})
export class CastMeasurementToUnitlessPipe implements PipeTransform {
    transform(value: Measurement, _args?: unknown): UnitlessMeasurement | null {
        if (value.type == 'unitless') {
            return value;
        } else {
            return null;
        }
    }
}

export function unifyDecimals(values: number[]): number[] {
    // Early return if all values differ by more than 1
    if (oneApart(values.map((x) => Math.floor(x)).sort((a, b) => a - b))) {
        return values.map((x) => Math.floor(x));
    }

    // Find highest overlap for cut-off point
    let maxOverlap = 0;
    for (let i = 0; i < values.length - 1; i++) {
        const overlap: number = overlappingDigits(values[i], values[i + 1]);
        maxOverlap = overlap > maxOverlap ? overlap : maxOverlap;
    }

    return values.map((x) => {
        const preDecimals = Math.floor(x).toString().length;
        const roundAt = Math.pow(10, Math.max(0, maxOverlap + 2 - preDecimals));
        return Math.floor(x * roundAt) / roundAt;
    });
}

export function overlappingDigits(val1: number, val2: number): number {
    let overlap = 0;
    const str1 = val1.toString();
    const str2 = val2.toString();
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
 * The raster legend component.
 */
@Component({
    selector: 'geoengine-raster-legend',
    templateUrl: 'raster-legend.component.html',
    styleUrls: ['raster-legend.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        AngularCommonModule,
        BreakpointToCssStringPipe,
        CastMeasurementToClassificationPipe,
        CastMeasurementToContinuousPipe,
        CastMeasurementToUnitlessPipe,
        MatProgressSpinner,
        RasterColorizerCssGradientPipe,
    ],
})
export class RasterLegendComponent {
    private readonly projectService = inject(ProjectService);

    readonly layer = input.required<RasterLayer>();
    readonly orderValuesDescending = input<boolean>(false);
    readonly showBandNames = input<boolean>(true);

    readonly selectedBands = signal<Array<RasterBandDescriptor> | undefined>(undefined);
    readonly displayedBreakpoints = computed<Array<number>>(() =>
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
    readonly bandsHaveUnits = computed<boolean>(() => {
        const selectedBands = this.selectedBands();
        if (!selectedBands) {
            return false;
        }
        return selectedBands.some((band) => band.measurement.type !== 'unitless');
    });

    constructor() {
        effect(() => {
            const layer = this.layer();
            untracked(() => {
                firstValueFrom(this.projectService.getRasterLayerMetadata(layer)).then((metadata) => {
                    const bands = metadata.bands;
                    const rasterColorizer = layer.symbology.rasterColorizer;

                    let bandDescriptors: Array<RasterBandDescriptor>;
                    if (rasterColorizer instanceof SingleBandRasterColorizer) {
                        bandDescriptors = [bands[rasterColorizer.band]];
                    } else if (rasterColorizer instanceof MultiBandRasterColorizer) {
                        bandDescriptors = [
                            bands[rasterColorizer.redBand],
                            bands[rasterColorizer.greenBand],
                            bands[rasterColorizer.blueBand],
                        ];
                    } else {
                        throw new Error('Unknown raster colorizer');
                    }
                    this.selectedBands.set(bandDescriptors);
                });
            });
        });
    }
}

/**
 * Calculate the displayed breakpoints for the legend
 */
function calculateDisplayedBreakpoints(layer: RasterLayer, orderValuesDescending: boolean): Array<number> {
    let displayedBreakpoints = layer.symbology.rasterColorizer.getBreakpoints().map((x) => x.value);
    displayedBreakpoints = unifyDecimals(displayedBreakpoints);

    if (orderValuesDescending) {
        displayedBreakpoints = displayedBreakpoints.reverse();
    }

    return displayedBreakpoints;
}

import {ChangeDetectionStrategy, ChangeDetectorRef, Component, inject, input, output} from '@angular/core';
import {FormArray, UntypedFormBuilder, UntypedFormGroup, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {BehaviorSubject} from 'rxjs';
import {Color, RgbaTuple} from '../color';
import {ColorBreakpoint} from '../color-breakpoint.model';
import {geoengineValidators} from '../../util/form.validators';
import {UUID} from '../../datasets/dataset.model';
import {WorkflowsService} from '../../workflows/workflows.service';
import {StatisticsDict} from '../../operators/operator.model';
import {SymbologyQueryParams} from '../../symbology/symbology.model';
import {PlotsService} from '../../plots/plots.service';
import {ALL_COLORMAPS} from '../colormaps/colormaps';
import {MatFormField, MatLabel, MatInput} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {MatCheckbox} from '@angular/material/checkbox';
import {MatProgressBar} from '@angular/material/progress-bar';
import {AsyncPipe, KeyValuePipe} from '@angular/common';
import {RgbaArrayCssGradientPipe} from '../../util/pipes/color-gradients.pipe';

/**
 * The ColormapColorizerComponent is a dialog to generate ColorizerData from colormaps.
 */
@Component({
    selector: 'geoengine-percentile-breakpoint-selector',
    templateUrl: 'percentile-breakpoint-selector.component.html',
    styleUrls: ['percentile-breakpoint-selector.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        FormsModule,
        ReactiveFormsModule,
        MatFormField,
        MatLabel,
        MatInput,
        MatSelect,
        MatOption,
        MatCheckbox,
        MatProgressBar,
        AsyncPipe,
        KeyValuePipe,
        RgbaArrayCssGradientPipe,
    ],
})
export class PercentileBreakpointSelectorComponent {
    protected readonly changeDetectorRef = inject(ChangeDetectorRef);
    protected readonly formBuilder = inject(UntypedFormBuilder);
    protected readonly workflowsService = inject(WorkflowsService);
    protected readonly plotsService = inject(PlotsService);

    readonly band = input.required<string>();

    readonly workflowId = input.required<UUID>();

    readonly queryParams = input.required<SymbologyQueryParams>();

    /**
     * Emmits colorizer breakpoint arrays
     */
    readonly breakpointsChange = output<Array<ColorBreakpoint>>();

    /**
     * Informs parent to enable "Apply Changes" button
     */
    readonly changesToForm = output<void>();

    readonly colorMaps = ALL_COLORMAPS;

    readonly MAX_PERCENTILES = 8;

    /**
     * The form control used in the template.
     */
    form: UntypedFormGroup;

    /**
     * The local (work-in-progress) Colorizer.
     */
    breakpoints?: Array<ColorBreakpoint>;

    statisticsLoading$ = new BehaviorSubject(false);

    protected readonly largerThanZeroValidator = geoengineValidators.largerThan(0);

    constructor() {
        const formBuilder = this.formBuilder;

        const initialColorMapName = Object.keys(this.colorMaps)[0];

        this.form = formBuilder.group({
            numPercentiles: [3, [Validators.required, Validators.min(2), Validators.max(this.MAX_PERCENTILES + 2)]],
            percentiles: this.formBuilder.array(
                [0.5].map((p) => [p, [Validators.required]]),
                [Validators.maxLength(this.MAX_PERCENTILES)],
            ),
            colorMap: [this.colorMaps[initialColorMapName], [Validators.required]],
            colorMapReverseColors: [false],
        });
    }

    /**
     * Replace the min and max values.
     */
    patchMinMaxValues(min?: number, max?: number): void {
        if (typeof min !== 'number' || typeof max !== 'number') {
            return;
        }

        const bounds: {min: number; max: number} = this.form.controls['bounds'].value;

        if (bounds.min === min && bounds.max === max) {
            return;
        }

        this.form.controls.bounds.setValue({min, max});

        this.updateColorizerData();
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

    async createColorTable(): Promise<void> {
        if (this.form.invalid) {
            return;
        }

        await this.updateColorizerData();
        this.applyChanges();
    }

    get percentiles(): FormArray {
        return this.form.get('percentiles') as FormArray;
    }

    setNumPercentiles(num: number): void {
        if (num < 2 || num > this.MAX_PERCENTILES + 2) {
            return;
        }

        const percentiles = num - 1;

        this.percentiles.clear();

        for (let i = 1; i < percentiles; i++) {
            this.percentiles.push(this.formBuilder.control(i / percentiles, [Validators.required]));
        }
    }

    public static createBreakpoints(
        colorMap: Array<RgbaTuple>,
        colorMapReverseColors: boolean,
        percentiles: number[],
    ): Array<ColorBreakpoint> {
        const breakpoints = new Array<ColorBreakpoint>();

        for (let i = 0; i < percentiles.length; i++) {
            const value = percentiles[i];

            const frac = i / percentiles.length;

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

        return breakpoints;
    }

    async updateColorizerData(): Promise<void> {
        if (!this.form.valid) {
            this.breakpoints = undefined;
            return;
        }

        this.breakpoints = await this.createBreakpoints();

        this.changeDetectorRef.markForCheck();
    }

    protected async createBreakpoints(): Promise<Array<ColorBreakpoint>> {
        const colorMap: Array<RgbaTuple> = this.form.controls['colorMap'].value;
        const colorMapReverseColors: boolean = this.form.controls['colorMapReverseColors'].value;
        const percentiles: number[] = this.form.controls['percentiles'].value;

        const statisticsWorkflowsId = await this.createStatisticsWorkflow(percentiles);
        const statistics = await this.createStatistics(statisticsWorkflowsId, this.queryParams());

        // add min and max to percentiles
        const percentileValues = statistics.percentiles.map((p) => p.value);
        percentileValues.unshift(statistics.min);
        percentileValues.push(statistics.max);

        percentileValues.sort((a, b) => a - b);

        return PercentileBreakpointSelectorComponent.createBreakpoints(colorMap, colorMapReverseColors, percentileValues);
    }

    private createStatistics(
        histogramWorkflowId: UUID,
        queryParams: SymbologyQueryParams,
    ): Promise<{min: number; max: number; percentiles: {percentile: number; value: number}[]}> {
        this.statisticsLoading$.next(true);
        return this.plotsService
            .getPlot(histogramWorkflowId, queryParams.bbox, queryParams.time, queryParams.resolution, queryParams.spatialReference)
            .then((plotData) => {
                this.statisticsLoading$.next(false);

                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                const statistics = plotData.data as any;

                const band = this.band();
                if (!(band in plotData.data)) {
                    throw new Error('Band not found in statistics');
                }

                return statistics[band] as {min: number; max: number; percentiles: {percentile: number; value: number}[]};
            });
    }

    protected createStatisticsWorkflow(percentiles: number[]): Promise<UUID> {
        return this.workflowsService.getWorkflow(this.workflowId()).then((workflow) =>
            this.workflowsService.registerWorkflow({
                type: 'Plot',
                operator: {
                    type: 'Statistics',
                    params: {
                        columnNames: [this.band()],
                        percentiles,
                    },
                    sources: {
                        source: [workflow.operator],
                    },
                } as StatisticsDict,
            }),
        );
    }
}

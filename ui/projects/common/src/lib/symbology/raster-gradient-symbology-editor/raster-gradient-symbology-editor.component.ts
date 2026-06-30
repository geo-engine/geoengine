import {
    Component,
    ChangeDetectionStrategy,
    ChangeDetectorRef,
    inject,
    input,
    output,
    viewChild,
    effect,
    linkedSignal,
    untracked,
} from '@angular/core';
import {BehaviorSubject, ReplaySubject} from 'rxjs';
import {LinearGradient, LogarithmicGradient} from '../../colors/colorizer.model';
import {ColorAttributeInput, ColorAttributeInputComponent} from '../../colors/color-attribute-input/color-attribute-input.component';
import {ColorBreakpoint} from '../../colors/color-breakpoint.model';
import {ColorMapSelectorComponent} from '../../colors/color-map-selector/color-map-selector.component';
import {Color} from '../../colors/color';
import {ColorTableEditorComponent} from '../../colors/color-table-editor/color-table-editor.component';
import {UUID} from '../../datasets/dataset.model';
import {VegaChartData} from '../../plots/plot.model';
import {WorkflowsService} from '../../workflows/workflows.service';
import {HistogramDict} from '../../operators/operator.model';
import {PlotsService} from '../../plots/plots.service';
import {SymbologyQueryParams} from '../symbology.model';
import {PercentileBreakpointSelectorComponent} from '../../colors/percentile-breakpoint-selector/percentile-breakpoint-selector.component';
import {MatCard, MatCardHeader, MatCardTitleGroup, MatCardTitle, MatCardSubtitle, MatCardContent} from '@angular/material/card';
import {MatIcon} from '@angular/material/icon';
import {FormsModule} from '@angular/forms';
import {MatTabGroup, MatTab} from '@angular/material/tabs';
import {VegaViewerComponent} from '../../plots/vega-viewer/vega-viewer.component';
import {MatProgressBar} from '@angular/material/progress-bar';
import {MatButton} from '@angular/material/button';
import {MatDivider} from '@angular/material/list';
import {AsyncPipe} from '@angular/common';
import {ColorizerCssGradientPipe} from '../../util/pipes/color-gradients.pipe';

/**
 * An editor for generating raster symbologies.
 */
@Component({
    selector: 'geoengine-raster-gradient-symbology-editor',
    templateUrl: 'raster-gradient-symbology-editor.component.html',
    styleUrls: ['raster-gradient-symbology-editor.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        MatCard,
        MatCardHeader,
        MatCardTitleGroup,
        MatCardTitle,
        MatCardSubtitle,
        MatIcon,
        MatCardContent,
        ColorAttributeInputComponent,
        FormsModule,
        MatTabGroup,
        MatTab,
        VegaViewerComponent,
        MatProgressBar,
        MatButton,
        MatDivider,
        ColorMapSelectorComponent,
        PercentileBreakpointSelectorComponent,
        ColorTableEditorComponent,
        AsyncPipe,
        ColorizerCssGradientPipe,
    ],
})
export class RasterGradientSymbologyEditorComponent {
    private readonly workflowsService = inject(WorkflowsService);
    private readonly plotsService = inject(PlotsService);
    private changeDetectorRef = inject(ChangeDetectorRef);

    readonly colorMapSelector = viewChild.required(ColorMapSelectorComponent);
    readonly percentileBreakpointSelector = viewChild.required(PercentileBreakpointSelectorComponent);
    readonly colorTableEditor = viewChild.required(ColorTableEditorComponent);

    readonly band = input.required<string>();

    readonly workflowId = input.required<UUID>();

    // eslint-disable-next-line @angular-eslint/no-input-rename
    readonly colorizerInput = input.required<LinearGradient | LogarithmicGradient>({alias: 'colorizer'});

    readonly queryParams = input<SymbologyQueryParams>();

    readonly colorizerChange = output<LinearGradient | LogarithmicGradient>();

    readonly colorizer = linkedSignal<LinearGradient | LogarithmicGradient>(() => this.colorizerInput(), {
        equal: (a, b) => a.equals(b),
    });

    // The min value used for color table generation
    layerMinValue: number | undefined = undefined;
    // The max value used for color table generation
    layerMaxValue: number | undefined = undefined;

    scale: 'linear' | 'logarithmic' = 'linear';

    histogramData = new ReplaySubject<VegaChartData>(1);
    histogramLoading = new BehaviorSubject(false);
    histogramCreated = false;

    protected underColor?: ColorAttributeInput;
    protected overColor?: ColorAttributeInput;
    protected noDataColor?: ColorAttributeInput;

    constructor() {
        effect(() => {
            const colorizer = this.colorizer();
            untracked(() => {
                this.updateScale(colorizer);
                this.updateNodataAndDefaultColor(colorizer);
                this.updateLayerMinMaxFromColorizer(colorizer);
            });
        });

        effect(() => {
            const colorizer = this.colorizer();
            this.colorizerChange.emit(colorizer);
        });
    }

    get colorTable(): Array<ColorBreakpoint> {
        return this.colorizer().getBreakpoints();
    }

    updateColorTable(colorTable: Array<ColorBreakpoint>): void {
        const colors = new Map<number, Color>();
        for (const breakpoint of colorTable) {
            colors.set(breakpoint.value, breakpoint.color);
        }
        const colorizer = this.colorizer().cloneWith({breakpoints: colorTable});
        this.colorizer.set(colorizer);
    }

    /**
     * Set the max value to use for color table generation
     */
    updateLayerMinValue(min: number): void {
        if (this.layerMinValue !== min) {
            this.layerMinValue = min;
        }
    }

    /**
     * Set the max value to use for color table generation
     */
    updateLayerMaxValue(max: number): void {
        if (this.layerMaxValue !== max) {
            this.layerMaxValue = max;
        }
    }

    updateBounds(histogramSignal: {binStart: [number, number]}): void {
        if (histogramSignal?.binStart?.length !== 2) {
            return;
        }

        const [min, max] = histogramSignal.binStart;

        this.updateLayerMinValue(min);
        this.updateLayerMaxValue(max);
    }

    getUnderColor(): ColorAttributeInput {
        if (!this.underColor) {
            throw new Error('uninitialized underColor');
        }

        return this.underColor;
    }

    getOverColor(): ColorAttributeInput {
        if (!this.overColor) {
            throw new Error('uninitialized overColor');
        }

        return this.overColor;
    }

    getNoDataColor(): ColorAttributeInput {
        if (!this.noDataColor) {
            throw new Error('uninitialized noDataColor');
        }

        return this.noDataColor;
    }

    updateUnderColor(underColorInput: ColorAttributeInput): void {
        const underColor = underColorInput.value;
        this.colorizer.set(this.colorizer().cloneWith({underColor}));
    }

    updateOverColor(overColorInput: ColorAttributeInput): void {
        const overColor = overColorInput.value;
        this.colorizer.set(this.colorizer().cloneWith({overColor}));
    }

    updateNoDataColor(noDataColorInput: ColorAttributeInput): void {
        const noDataColor = noDataColorInput.value;
        this.colorizer.set(this.colorizer().cloneWith({noDataColor}));
    }

    updateScale(colorizer: LinearGradient | LogarithmicGradient): void {
        if (colorizer instanceof LinearGradient) {
            this.scale = 'linear';
            return;
        }

        if (colorizer instanceof LogarithmicGradient) {
            this.scale = 'logarithmic';
            return;
        }
    }

    updateBreakpoints(breakpoints: Array<ColorBreakpoint>): void {
        if (!breakpoints) {
            return;
        }

        this.colorizer.set(this.colorizer().cloneWith({breakpoints}));
    }

    /**
     * Sets the layer min/max values from the colorizer.
     */
    updateLayerMinMaxFromColorizer(colorizer: LinearGradient | LogarithmicGradient): void {
        const breakpoints = colorizer.getBreakpoints();
        this.updateLayerMinValue(breakpoints[0].value);
        this.updateLayerMaxValue(breakpoints[breakpoints.length - 1].value);
    }

    updateHistogram(): void {
        const histogramParams = this.queryParams();
        if (!histogramParams) {
            return;
        }

        this.histogramCreated = true;
        this.createHistogramWorkflowId()
            .then((histogramWorkflowId) => this.createHistogram(histogramWorkflowId, histogramParams))
            .then((histogramData) => {
                this.histogramData.next(histogramData);
            })
            .catch((error) => console.error('Error:', error));
    }

    createColorTable(): void {
        this.colorMapSelector()?.applyChanges();
        this.changeDetectorRef.detectChanges();
    }

    async createPercentilesColorTable(): Promise<void> {
        await this.percentileBreakpointSelector().createColorTable();
    }

    private updateNodataAndDefaultColor(colorizer: LinearGradient | LogarithmicGradient): void {
        this.noDataColor = {
            key: 'No Data Color',
            value: colorizer.noDataColor,
        };

        this.underColor = {
            key: 'Under Color',
            value: colorizer.underColor,
        };

        this.overColor = {
            key: 'Over Color',
            value: colorizer.overColor,
        };
    }

    private createHistogram(histogramWorkflowId: UUID, histogramParams: SymbologyQueryParams): Promise<VegaChartData> {
        return this.plotsService
            .getPlot(
                histogramWorkflowId,
                histogramParams.bbox,
                histogramParams.time,
                histogramParams.resolution,
                histogramParams.spatialReference,
            )
            .then((plotData) => {
                this.histogramLoading.next(false);
                return plotData.data as VegaChartData;
            });
    }

    private createHistogramWorkflowId(): Promise<UUID> {
        return this.workflowsService.getWorkflow(this.workflowId()).then((workflow) =>
            this.workflowsService.registerWorkflow({
                type: 'Plot',
                operator: {
                    type: 'Histogram',
                    params: {
                        attributeName: this.band(),
                        buckets: {
                            type: 'number',
                            value: 20,
                        },
                        bounds: 'data',
                        interactive: true,
                    },
                    sources: {
                        source: workflow.operator,
                    },
                } as HistogramDict,
            }),
        );
    }
}

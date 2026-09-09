import {
    afterNextRender,
    ChangeDetectionStrategy,
    Component,
    computed,
    DestroyRef,
    ElementRef,
    inject,
    linkedSignal,
    resource,
    signal,
} from '@angular/core';
import {BackendService, CoreModule, Extent, LoadingState, MapService, ProjectService, UUID} from '@geoengine/core';
import {A11yModule} from '@angular/cdk/a11y';
import {MatButtonModule} from '@angular/material/button';
import {MatFormFieldModule} from '@angular/material/form-field';
import {MatSelectModule} from '@angular/material/select';
import {MatProgressSpinnerModule} from '@angular/material/progress-spinner';
import {
    BoundingBox2D,
    ClassHistogramDict,
    HistogramDict,
    LayersService,
    NotificationService,
    PlotsService,
    RasterLayerMetadata,
    UserService,
    VegaChartData,
    WorkflowDict,
} from '@geoengine/common';
import {firstValueFrom} from 'rxjs';
import OlPolygon from 'ol/geom/Polygon';
import {ProviderLayerId} from '@geoengine/api-client/dist/models/ProviderLayerId';
import {LayerIdPair} from '../main/main.component';
import {PlotOutputFormat, RasterBandDescriptor, WrappedPlotOutput} from '@geoengine/api-client';

@Component({
    selector: 'geoengine-compute',
    template: `
        <div class="compute-actions">
            <mat-form-field appearance="outline">
                <mat-label>Raster band</mat-label>
                <mat-select [value]="selectedBand()" (selectionChange)="selectedBand.set($event.value)" [disabled]="isLoadingMetadata()">
                    @for (band of bands(); track band) {
                        <mat-option [value]="band">{{ band }}</mat-option>
                    } @empty {
                        <mat-option disabled>No bands available</mat-option>
                    }
                </mat-select>
            </mat-form-field>

            <button type="button" mat-raised-button color="primary" [disabled]="cannotComputeHistogram()" (click)="computeHistogram()">
                Compute histogram
            </button>

            @if (cannotComputeHistogram()) {
                <p class="hint">Draw a box on the map before computing a histogram.</p>
                <p class="hint">Also select a raster band.</p>
            }

            @if (isLoading()) {
                <mat-progress-spinner></mat-progress-spinner>
            }

            @if (plotData(); as plotData) {
                <geoengine-vega-viewer
                    [chartData]="vegaPlotData(plotData)"
                    [width]="plotWidthPx()"
                    [height]="plotWidthPx()"
                ></geoengine-vega-viewer>
            }
        </div>
    `,
    styles: [
        `
            :host {
                display: block;
                padding: 1rem;
            }

            .compute-actions {
                display: flex;
                flex-direction: column;
                gap: 0.75rem;
            }

            .hint {
                margin: 0;
                color: var(--geoengine-foreground-text-color, #444);
                font-size: 0.85rem;
            }

            mat-form-field {
                width: 100%;
            }

            mat-progress-spinner {
                display: block;
                margin: 0 auto;
            }
        `,
    ],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [A11yModule, CoreModule, MatButtonModule, MatFormFieldModule, MatSelectModule, MatProgressSpinnerModule],
})
export class ComputeComponent {
    private readonly backendService = inject(BackendService);
    private readonly destroyRef = inject(DestroyRef);
    private readonly layerService = inject(LayersService);
    private readonly mapService = inject(MapService);
    private readonly notificationService = inject(NotificationService);
    private readonly plotsService = inject(PlotsService);
    private readonly projectService = inject(ProjectService);
    private readonly userService = inject(UserService);

    readonly plotWidthPx = signal(0);
    readonly hostElement = inject(ElementRef).nativeElement as HTMLElement;

    readonly selectedRasterLayer = resource({
        params: () => ({}),
        loader: async ({params: _}): Promise<LayerIdPair | undefined> => {
            const connectorId = 'cbb21ee3-d15d-45c5-a175-66964adf4e85';

            const items = await this.layerService.getLayerCollectionItems(connectorId, 'tags:*');

            const landCover = items.items.find((item) => item.name === 'Land Cover');

            if (!landCover) return;

            const id = landCover.id as ProviderLayerId;

            return {
                dataConnectorId: id.providerId,
                layerId: id.layerId,
            };
        },
    });
    readonly selectedProcessingGraphId = resource<string | undefined, LayerIdPair | undefined>({
        params: () => this.selectedRasterLayer.value(),
        loader: async ({params: rasterLayer}): Promise<string | undefined> => {
            if (!rasterLayer) return undefined;

            return await this.layerService.registerAndGetLayerWorkflowId(rasterLayer.dataConnectorId, rasterLayer.layerId);
        },
    });
    readonly selecterLayerMetadata = resource<RasterLayerMetadata | undefined, UUID | undefined>({
        params: () => this.selectedProcessingGraphId.value(),
        loader: async ({params: processingGraphId}): Promise<RasterLayerMetadata | undefined> => {
            if (!processingGraphId) return undefined;

            const workflowIdMetadata = await this.layerService.getWorkflowIdMetadata(processingGraphId);

            if (workflowIdMetadata.layerType !== 'raster') return undefined;

            return workflowIdMetadata;
        },
    });
    readonly bands = computed(() => this.selecterLayerMetadata.value()?.bands.map((band) => band.name));
    readonly selectedBand = linkedSignal<string | undefined>(() => {
        const bands = this.bands();
        if (!bands || bands.length === 0) return undefined;
        return bands[0];
    });
    readonly layerOverlay = this.mapService.getLayerOverlay();
    readonly computationBbox = computed<Extent | undefined>(() => {
        const layerOverlay = this.layerOverlay();
        if (!layerOverlay) return undefined;

        const feature = layerOverlay.getSource()?.getFeatures()[0];
        if (!feature) return undefined;

        const geometry = feature.getGeometry();
        if (!(geometry instanceof OlPolygon)) return undefined;

        const [minx, miny, maxx, maxy] = geometry.getExtent();

        return [minx, miny, maxx, maxy];
    });
    readonly cannotComputeHistogram = computed(() => {
        return !this.computationBbox() || !this.selectedRasterLayer.value() || !this.selectedBand();
    });

    readonly isLoadingMetadata = computed(
        () => this.selectedRasterLayer.isLoading() || this.selectedProcessingGraphId.isLoading() || this.selecterLayerMetadata.isLoading(),
    );
    readonly isComputingHistogram = signal(false);
    readonly isLoading = computed(() => this.isComputingHistogram() || this.isLoadingMetadata());

    readonly plotData = linkedSignal<WrappedPlotOutput | undefined>(() => {
        this.computationBbox(); // reset the computation when the bounding box changes
        return undefined;
    });
    readonly defaultLoadingState = LoadingState.LOADING;

    constructor() {
        let resizeObserver: ResizeObserver | undefined;

        afterNextRender(() => {
            resizeObserver?.disconnect();
            resizeObserver = new ResizeObserver(() => {
                const cardElement = this.hostElement.querySelector('div') as HTMLElement | undefined;
                if (!cardElement) return;

                this.plotWidthPx.set(getContentWidth(cardElement));
            });
            resizeObserver.observe(this.hostElement);
        });

        this.destroyRef.onDestroy(() => {
            resizeObserver?.disconnect();
        });
    }

    vegaPlotData(plotData: WrappedPlotOutput): VegaChartData {
        if (plotData.outputFormat !== PlotOutputFormat.JsonVega) throw new Error('Invalid plot data format');
        return plotData.data as VegaChartData;
    }

    async computeHistogram(): Promise<void> {
        const bbox = this.computationBbox();
        const layer = this.selectedRasterLayer.value();
        const metadata = this.selecterLayerMetadata.value();
        const processingGraphId = this.selectedProcessingGraphId.value();
        const band = this.selectedBand();

        if (!bbox || !layer || !band || !processingGraphId || !metadata) return;

        this.isComputingHistogram.set(true);
        this.plotData.set(undefined);

        try {
            const sessionToken = await this.userService.getSessionToken();
            const sourceProcessingGraph = await firstValueFrom(this.backendService.getWorkflow(processingGraphId, sessionToken));
            const measurementType = bandMeasurementType(metadata.bands, band);
            let processingGraph: WorkflowDict;
            if (measurementType === 'classification') {
                processingGraph = {
                    type: 'Plot',
                    operator: {
                        type: 'ClassHistogram',
                        params: {},
                        sources: {
                            source: sourceProcessingGraph.operator,
                        },
                    } as ClassHistogramDict,
                };
            } else {
                processingGraph = {
                    type: 'Plot',
                    operator: {
                        type: 'Histogram',
                        params: {
                            attributeName: band,
                            bounds: 'data',
                            buckets: {
                                type: 'squareRootChoiceRule',
                                maxNumberOfBuckets: 20,
                            },
                        },
                        sources: {
                            source: sourceProcessingGraph.operator,
                        },
                    } as HistogramDict,
                };
            }
            const plotWorkflowId = (await firstValueFrom(this.backendService.registerWorkflow(processingGraph, sessionToken))).id;

            const plotData = await this.plotsService.getPlot(
                plotWorkflowId,
                new BoundingBox2D(bbox),
                await this.projectService.getTimeOnce(),
                {x: Math.abs(metadata.pixelSizeX), y: Math.abs(metadata.pixelSizeY)},
                metadata.spatialReference,
            );

            this.plotData.set(plotData);
        } catch (error) {
            this.notificationService.error(error instanceof Error ? error.message : String(error));
        } finally {
            this.isComputingHistogram.set(false);
        }
    }
}

/**
 * Calculates the exact content width of an HTML element,
 * excluding padding, border, and margin.
 *
 * @param element - The target HTMLElement.
 * @returns The inner content width in pixels.
 */
function getContentWidth(element: HTMLElement): number {
    const style = window.getComputedStyle(element);
    const totalWidth = element.getBoundingClientRect().width;

    const paddingLeft = parseFloat(style.paddingLeft) || 0;
    const paddingRight = parseFloat(style.paddingRight) || 0;
    const borderLeft = parseFloat(style.borderLeftWidth) || 0;
    const borderRight = parseFloat(style.borderRightWidth) || 0;

    return totalWidth - paddingLeft - paddingRight - borderLeft - borderRight;
}

/**
 * Determines the measurement type of a specific band from the provided bands metadata.
 *
 * @param bandsMetadata - Array of raster band descriptors.
 * @param band - The name of the band to check.
 * @returns The measurement type of the band ('classification', 'continuous', or 'unitless').
 */
function bandMeasurementType(bandsMetadata: RasterBandDescriptor[], band: string): 'classification' | 'continuous' | 'unitless' {
    for (const bandMetadata of bandsMetadata) {
        if (bandMetadata.name !== band) continue;

        return bandMetadata.measurement.type;
    }

    return 'unitless'; // fallback if the band is not found
}

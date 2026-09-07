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
import {CoreModule, Extent, LoadingState, MapService, ProjectService, UUID} from '@geoengine/core';
import {A11yModule} from '@angular/cdk/a11y';
import {MatButtonModule} from '@angular/material/button';
import {MatFormFieldModule} from '@angular/material/form-field';
import {MatSelectModule} from '@angular/material/select';
import {MatProgressSpinnerModule} from '@angular/material/progress-spinner';
import {HistogramDict, isNullOrUndefined, LayersService, NotificationService, Plot, RasterLayerMetadata} from '@geoengine/common';
import {firstValueFrom, of} from 'rxjs';
import OlPolygon from 'ol/geom/Polygon';
import {ProviderLayerId} from '@geoengine/api-client/dist/models/ProviderLayerId';
import {LayerIdPair} from '../main/main.component';
import {rxResource} from '@angular/core/rxjs-interop';

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

            @if (plotData.value(); as plotData) {
                <geoengine-vega-viewer [chartData]="plotData.data" [width]="plotWidthPx()" [height]="plotWidthPx()"></geoengine-vega-viewer>
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
    readonly projectService = inject(ProjectService);
    private readonly mapService = inject(MapService);
    private readonly notificationService = inject(NotificationService);
    private readonly layerService = inject(LayersService);
    private readonly destroyRef = inject(DestroyRef);

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
    readonly cannotComputeHistogram = computed(
        () =>
            isNullOrUndefined(this.computationBbox()) ||
            isNullOrUndefined(this.selectedRasterLayer.value()) ||
            isNullOrUndefined(this.selectedBand()),
    );

    readonly isLoadingMetadata = computed(
        () => this.selectedRasterLayer.isLoading() || this.selectedProcessingGraphId.isLoading() || this.selecterLayerMetadata.isLoading(),
    );
    readonly isComputingHistogram = signal(false);
    readonly isLoading = computed(() => this.isComputingHistogram() || this.plotData.isLoading() || this.isLoadingMetadata());

    readonly plot = signal<Plot | undefined>(undefined);
    readonly plotData = rxResource({
        params: () => ({
            plot: this.plot(),
        }),
        stream: ({params}) => (params.plot ? this.projectService.getPlotDataStream(params.plot) : of(undefined)),
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

    async computeHistogram(): Promise<void> {
        const bbox = this.computationBbox();
        const layer = this.selectedRasterLayer.value();
        const metadata = this.selecterLayerMetadata.value();
        const processingGraphId = this.selectedProcessingGraphId.value();
        const band = this.selectedBand();

        if (!bbox || !layer || !band || !processingGraphId || !metadata) return;

        this.isComputingHistogram.set(true);

        try {
            const sourceProcessingGraph = await firstValueFrom(this.projectService.getWorkflow(processingGraphId));
            const plotWorkflowId = await firstValueFrom(
                this.projectService.registerWorkflow({
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
                }),
            );

            const plot = new Plot({
                workflowId: plotWorkflowId,
                name: `Histogram`, // TODO: incorporate name of layer
            });
            await this.projectService.addPlot(plot);

            // const plotData = await firstValueFrom(this.projectService.getPlotDataStream(plot));

            this.plot.set(plot);
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

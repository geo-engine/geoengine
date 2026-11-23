import {A11yModule} from '@angular/cdk/a11y';
import {
    ChangeDetectionStrategy,
    Component,
    ElementRef,
    afterNextRender,
    afterRenderEffect,
    booleanAttribute,
    computed,
    inject,
    input,
    signal,
    viewChild,
} from '@angular/core';
import {toSignal} from '@angular/core/rxjs-interop';
import {MatButtonModule} from '@angular/material/button';
import {MatButtonToggleModule} from '@angular/material/button-toggle';
import {MatIconModule} from '@angular/material/icon';
import {MatRadioModule} from '@angular/material/radio';
import {MatSidenavModule} from '@angular/material/sidenav';
import {MatToolbar, MatToolbarModule} from '@angular/material/toolbar';
import {MatTooltipModule} from '@angular/material/tooltip';
import {isActive, Router, RouterModule} from '@angular/router';
import {Layer, Time, UserService} from '@geoengine/common';
import {ProjectService, MapService, MapContainerComponent, CoreModule, SpatialReferenceService, TMSId, WGS_84} from '@geoengine/core';
import {MeasureDirective, MeasurementType} from './measure.directive';
import {AppConfig} from '../app-config.service';
import {addCitationToMapImage} from './map-image-export';
import {EOdysseyLayerService} from '../layers/layers.service';
import {
    estimateViewportCost,
    viewportLoadingLimit,
    ViewportCost,
    ViewportLimits,
    ViewportLoadingLimit,
} from '../layers/viewport-pixel-budget';

interface ZoomDiagnostics {
    reason: string;
    limits?: ViewportLimits;
    current?: ViewportCost;
    blocking?: ViewportCost;
}

@Component({
    selector: 'geoengine-main',
    templateUrl: './main.component.html',
    styleUrls: ['./main.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        A11yModule,
        CoreModule,
        MapContainerComponent,
        MatButtonModule,
        MatButtonToggleModule,
        MatIconModule,
        MatRadioModule,
        MatSidenavModule,
        MatToolbarModule,
        MatTooltipModule,
        MeasureDirective,
        RouterModule,
    ],
    host: {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        '(window:resize)': 'onResize()',
    },
})
export class MainComponent {
    readonly config = inject(AppConfig);
    readonly projectService = inject(ProjectService);
    readonly userService = inject(UserService);
    private readonly mapService = inject(MapService);
    private readonly router = inject(Router);
    private readonly eodysseyLayerService = inject(EOdysseyLayerService);

    private readonly spatialReferenceService = inject(SpatialReferenceService);

    readonly topToolbar = viewChild.required<MatToolbar, ElementRef<HTMLElement>>('topToolbar', {read: ElementRef});
    readonly mapComponent = viewChild.required(MapContainerComponent);
    private readonly mapSize = signal<readonly number[]>([0, 0]);

    readonly layersReverse = signal<Array<Layer>>([]);

    readonly totalHeight = signal(window.innerHeight);
    readonly topToolbarHeight = signal(64);
    readonly middleContainerHeight = computed(() => this.totalHeight() - this.topToolbarHeight());

    readonly sessionToken = toSignal(this.userService.getSessionTokenStream());
    readonly isGuestUser = toSignal(this.userService.isGuestUserStream(), {initialValue: true});

    readonly spatialReference = toSignal(this.projectService.getSpatialReferenceStream());
    readonly currentTime = toSignal(this.projectService.getTimeStream());
    private readonly viewport = toSignal(this.mapService.getViewportSizeStream());

    // Temporary comparison controls, enabled by the router's `debug` query parameter.
    readonly debug = input(false, {transform: booleanAttribute});
    readonly frontendReprojection = signal(false);
    readonly tileMatrixSet = computed<TMSId>(() => (this.debug() && this.frontendReprojection() ? 'Custom' : 'CustomWebMercator'));

    readonly mapTileLayer = computed(() => this.eodysseyLayerService.mapTileLayer());
    readonly minTileMatrixIndex = this.eodysseyLayerService.appliedMinTileMatrixIndex;
    private readonly loadingLimit = computed<ViewportLoadingLimit | undefined>(() => {
        const budget = this.eodysseyLayerService.appliedViewportPixelBudget();
        if (!budget) return undefined;
        if (budget.crs !== this.spatialReference()?.srsString) return {maxResolution: 0};
        if (budget.tileMatrixSet !== this.tileMatrixSet()) return {maxResolution: 0};
        // Search keys contain the dataset, native grid cell and displayed time step.
        // EOdyssey displays instants; do not undercount an unsupported multi-step query.
        if (this.currentTime()?.type !== 'TimePoint') return {maxResolution: 0};
        // Native STAC cell intersections depend on location, even at unchanged zoom.
        this.eodysseyLayerService.mapCenter();
        return viewportLoadingLimit(budget, this.mapSize(), this.mapService.view().getCenter());
    });
    readonly maxViewResolution = computed(() => this.loadingLimit()?.maxResolution);
    readonly zoomRequired = signal(false);
    readonly zoomDiagnostics = computed<ZoomDiagnostics | undefined>(() => {
        if (!this.zoomRequired()) return undefined;
        const budget = this.eodysseyLayerService.appliedViewportPixelBudget();
        if (!budget) return {reason: 'The layer requires a finer tile matrix.'};
        if (budget.crs !== this.spatialReference()?.srsString) return {reason: 'The map projection does not match the cost model.'};
        if (budget.tileMatrixSet !== this.tileMatrixSet()) return {reason: 'The selected tile matrix set has no cost model.'};
        if (this.currentTime()?.type !== 'TimePoint') return {reason: 'The cost model requires a single time instant.'};
        this.eodysseyLayerService.mapCenter();
        const current = estimateViewportCost(budget, this.mapSize(), this.mapService.view().getCenter(), this.viewport()?.resolution);
        if (!current) return {reason: 'Waiting for valid map dimensions, center and resolution.'};
        const reason = exceededBudgetReason(current, budget);
        const blocking = reason ? undefined : this.loadingLimit()?.blockingCost;
        return {
            reason: reason
                ? `${reason}.`
                : blocking
                  ? `${exceededBudgetReason(blocking, budget) ?? 'Loading limit exceeded'} at the zoom cutoff.`
                  : 'The view is at the minimum permitted zoom level.',
            limits: budget,
            current,
            blocking,
        };
    });
    readonly tileLoading = signal(false);
    readonly isLoading = computed(() => this.eodysseyLayerService.catalogueLoading() || (this.tileLoading() && !this.zoomRequired()));

    readonly isLayersActive = isActive('/map/layers', this.router);
    readonly isComputeActive = isActive('/map/compute', this.router);
    readonly isDownloadActive = isActive('/map/download', this.router);
    readonly isAboutActive = isActive('/map/about', this.router);

    readonly MeasurementType = MeasurementType;

    readonly mapImageLoading = signal(false);

    readonly testIsVisible = signal(true);

    constructor() {
        afterRenderEffect((onCleanup) => {
            // The real map target appears after the child finishes projection/layout setup.
            // Follow its rendered grid tile, rather than the inline map-container host.
            const mapElement = this.mapComponent().mapContainers()[0]?.nativeElement;
            if (!mapElement) {
                this.mapSize.set([0, 0]);
                return;
            }
            const updateMapSize = (): void => {
                this.mapSize.set([mapElement.clientWidth, mapElement.clientHeight]);
            };
            const observer = new ResizeObserver(updateMapSize);
            observer.observe(mapElement);
            updateMapSize();
            onCleanup(() => observer.disconnect());
        });
        afterNextRender({
            read: () => {
                this.mapService.registerMapComponent(this.mapComponent());

                this.zoomToGermany();

                this.onToolbarResize();
                const topToolbarObserver = new ResizeObserver(() => this.onToolbarResize());
                topToolbarObserver.observe(this.topToolbar().nativeElement);
            },
        });
    }

    /**
     * Zoom the map to the configured focus extent (Germany) once the project's
     * spatial reference is known. The extent is defined in WGS 84 and is
     * reprojected into the map's projection before fitting the view.
     */
    private zoomToGermany(): void {
        this.projectService.getSpatialReferenceOnce().subscribe((projection) => {
            const extent = this.spatialReferenceService.reprojectExtent(
                this.config.DEFAULTS.FOCUS_EXTENT,
                WGS_84.spatialReference,
                projection,
            );
            this.mapService.zoomTo(extent);
        });
    }

    onTileLoading(loading: boolean): void {
        this.tileLoading.set(loading);
    }

    formatCount(value: number): string {
        return Math.ceil(value).toLocaleString('en-US');
    }

    onResize(): void {
        this.totalHeight.set(window.innerHeight);
    }

    onToolbarResize(): void {
        this.topToolbarHeight.set(this.topToolbar().nativeElement.offsetHeight);
    }

    idFromLayer(index: number, layer: Layer): number {
        return layer.id;
    }

    /**
     * Downloads the current map view as an image.
     */
    async downloadMapImage(): Promise<void> {
        if (this.mapImageLoading()) return;

        const [currentDate] = (this.currentTime()?.toString() ?? new Date().toISOString()).split('T');
        const currentLayer = this.layersReverse().at(-1)?.name ?? 'eodyssey-map';
        const citation = replaceCitationPlaceholders(
            this.eodysseyLayerService.appliedDataSource()?.citation ?? '',
            this.currentTime() ?? new Date(),
        );

        this.mapImageLoading.set(true);

        try {
            const mapImage = await this.mapComponent().mapAsImage();
            const mapImageWithCitation = await addCitationToMapImage(mapImage, citation);
            const link = document.createElement('a');
            link.href = mapImageWithCitation;
            link.download = `${currentDate} ${currentLayer}.png`;
            link.click();
            link.remove();
        } finally {
            this.mapImageLoading.set(false);
        }
    }
}
export interface LayerIdPair {
    dataConnectorId: string;
    layerId: string;
}

function replaceCitationPlaceholders(citation: string, currentTime: Time | Date): string {
    let year;
    if (currentTime instanceof Date) {
        year = currentTime.getFullYear().toString();
    } else {
        year = currentTime.toString().substring(0, 4);
    }
    return citation.replace('[Year]', year);
}

function exceededBudgetReason(cost: ViewportCost, limits: ViewportLimits): string | undefined {
    const reasons: string[] = [];
    if (cost.sourcePixels > limits.maxSourcePixels) reasons.push('Pixel limit');
    if (cost.stacSearches > limits.maxStacSearches) reasons.push('STAC search limit');
    return reasons.length ? `${reasons.join(' and ')} exceeded` : undefined;
}

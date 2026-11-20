import {A11yModule} from '@angular/cdk/a11y';
import {
    ChangeDetectionStrategy,
    Component,
    ElementRef,
    afterNextRender,
    booleanAttribute,
    computed,
    effect,
    inject,
    Injector,
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
import {Layer, LayersService, Time, UserService, UUID} from '@geoengine/common';
import {
    FootprintsService,
    ProjectService,
    MapService,
    MapContainerComponent,
    CoreModule,
    SpatialReferenceService,
    TMSId,
    WGS_84,
} from '@geoengine/core';
import {MeasureDirective, MeasurementType} from './measure.directive';
import {AppConfig} from '../app-config.service';
import {addCitationToMapImage} from './map-image-export';
import {EOdysseyLayerService} from '../layers/layers.service';

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
    private readonly injector = inject(Injector);
    private readonly footprintsService = inject(FootprintsService);
    readonly projectService = inject(ProjectService);
    readonly userService = inject(UserService);
    private readonly mapService = inject(MapService);
    private readonly router = inject(Router);
    private readonly eodysseyLayerService = inject(EOdysseyLayerService);
    private readonly layersService = inject(LayersService);

    private readonly spatialReferenceService = inject(SpatialReferenceService);

    readonly topToolbar = viewChild.required<MatToolbar, ElementRef<HTMLElement>>('topToolbar', {read: ElementRef});
    readonly mapComponent = viewChild.required(MapContainerComponent);

    readonly layersReverse = signal<Array<Layer>>([]);

    readonly totalHeight = signal(window.innerHeight);
    readonly topToolbarHeight = signal(64);
    readonly middleContainerHeight = computed(() => this.totalHeight() - this.topToolbarHeight());

    readonly sessionToken = toSignal(this.userService.getSessionTokenStream());
    readonly isGuestUser = toSignal(this.userService.isGuestUserStream(), {initialValue: true});

    readonly spatialReference = toSignal(this.projectService.getSpatialReferenceStream());
    readonly currentTime = toSignal(this.projectService.getTimeStream());

    // Temporary comparison controls, enabled by the router's `debug` query parameter.
    readonly debug = input(false, {transform: booleanAttribute});
    readonly frontendReprojection = signal(false);
    readonly tileMatrixSet = computed<TMSId>(() => (this.debug() && this.frontendReprojection() ? 'Custom' : 'CustomWebMercator'));

    readonly mapTileLayer = computed(() => this.eodysseyLayerService.mapTileLayer());
    /**
     * The layer on the map, or the selected one hidden while nothing or the data availability is shown. A hidden layer loads no tiles, but tells
     * whether its tiles would load at this zoom, so the coverage of a date is not asked for where they would not.
     */
    readonly tileLayer = computed(() => {
        const onMap = this.mapTileLayer();
        if (onMap) return {...onMap, minTileMatrixIndex: this.eodysseyLayerService.appliedMinTileMatrixIndex(), isVisible: true};
        const preset = this.eodysseyLayerService.selectedPreset();
        return (
            preset && {
                dataConnectorId: preset.connectorId,
                layerId: preset.layerId,
                minTileMatrixIndex: this.eodysseyLayerService.selectedMinTileMatrixIndex(),
                isVisible: false,
            }
        );
    });
    readonly zoomRequired = signal(false);
    readonly tilesLoadable = signal(false);
    /** One prompt on the map for the imagery and the data availability, which have the same zoom limit. */
    readonly zoomPrompt = computed(() => {
        if (this.mapTileLayer() && this.zoomRequired()) return 'Zoom in further to load the selected layer.';
        const footprints = this.footprintsService;
        if (footprints.overlayTooLarge() || (footprints.overlayVisible() && footprints.overlayBlocked())) {
            return 'Zoom in further to see the data availability.';
        }
        return undefined;
    });
    readonly tileLoading = signal(false);
    readonly isLoading = computed(() => this.eodysseyLayerService.catalogueLoading() || (this.tileLoading() && !this.zoomRequired()));

    readonly isLayersActive = isActive('/map/layers', this.router);
    readonly isComputeActive = isActive('/map/compute', this.router);
    readonly isDownloadActive = isActive('/map/download', this.router);
    readonly isAboutActive = isActive('/map/about', this.router);

    readonly MeasurementType = MeasurementType;

    readonly mapImageLoading = signal(false);

    constructor() {
        // The selected preset, so availability is shown before the visualization is applied. Else the layer on the map.
        effect(() => {
            const preset = this.eodysseyLayerService.selectedPreset();
            const layer = preset
                ? {dataConnectorId: preset.connectorId, layerId: preset.layerId}
                : this.eodysseyLayerService.mapTileLayer();
            this.footprintsService.layer.set(
                layer && {
                    key: `${layer.dataConnectorId}/${layer.layerId}`,
                    processingGraphId: (): Promise<UUID> =>
                        this.layersService.registerAndGetLayerProcessingGraphId(layer.dataConnectorId, layer.layerId),
                },
            );
        });
        // without a limit, the coverage of a date zoomed out would search the catalog over a large area
        effect(() => this.footprintsService.overlayBlocked.set(!this.tilesLoadable()));

        // The step of the selected data source until it is applied, because the project keeps the step of the applied one.
        effect(() => {
            const source = this.eodysseyLayerService.selectedDataSource();
            const isNewSource = source !== this.eodysseyLayerService.dataSourceOfTime();
            this.footprintsService.timeStepOverride.set(isNewSource ? source?.defaultTimeStep : undefined);
        });

        afterNextRender({
            read: () => {
                this.mapService.registerMapComponent(this.mapComponent());

                // the map component is only available after the first render
                effect(
                    () =>
                        this.mapComponent().dataOverlayLayer.set(
                            this.footprintsService.overlayVisible() ? this.footprintsService.overlayLayer : undefined,
                        ),
                    {injector: this.injector},
                );

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

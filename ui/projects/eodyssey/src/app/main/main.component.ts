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
import {formatDate} from '../layers/layers.component';
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
     * how far the zoom is from loading them, so the coverage of a date is not asked for far zoomed out.
     */
    readonly tileLayer = computed(() => {
        const onMap = this.mapTileLayer();
        if (onMap) {
            return {
                ...onMap,
                minTileMatrixIndex: this.eodysseyLayerService.appliedMinTileMatrixIndex(),
                isVisible: true,
            };
        }
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
    /** Shared, so the panel can tell why the imagery does not show. */
    readonly zoomRequired = this.eodysseyLayerService.zoomRequired;
    /**
     * The coarsest matrix level at which the data availability of a date is shown, also before the tiles load. Measured on CODE-DE for one
     * date and a cold view with its margin: level 5 needs 12 catalog searches in 1.9 s, level 4 28 in 4.7 s, level 3 48; the server refuses
     * more than its `max_footprint_searches` (50).
     */
    readonly COARSEST_AVAILABILITY_LEVEL = 4;
    /** Matrix levels before the tiles load at which the data availability is shown. */
    readonly availabilityLevelsOut = computed(() =>
        Math.max(0, (this.tileLayer()?.minTileMatrixIndex ?? 0) - this.COARSEST_AVAILABILITY_LEVEL),
    );
    /** Whether the map is zoomed in far enough for the data availability, which is a bit before the tiles load. */
    readonly tilesLoadable = signal(false);
    /**
     * One notice at the top of the map: why it shows nothing, or that the app moved the date, each with its way out. A bar at
     * the edge, never over the middle, so the map never looks blocked: panning and zooming go on as usual.
     */
    readonly mapNotice = computed<MapNotice | undefined>(() => {
        const footprints = this.footprintsService;
        const time = footprints.time()?.start.valueOf();
        const unit = footprints.timeStep()?.durationUnit;
        const source = this.eodysseyLayerService.selectedDataSource()?.name ?? 'This data source';
        const moved = this.eodysseyLayerService.dateMoved();
        if (moved && time === moved.to) {
            return {
                icon: 'event',
                title: `Date moved to ${formatDate(moved.to, unit)}`,
                detail: `${source} has no data ${moved.direction === 1 ? 'after' : 'before'} it.`,
                // the date of the previous source, whose time step may differ: as a day, with its time if it has one
                action: {
                    label: `Back to ${formatDate(moved.from.start.valueOf(), moved.from.start.valueOf() % 86_400_000 ? 'minute' : undefined)}`,
                    run: (): void => void this.moveBack(),
                },
                close: (): void => this.eodysseyLayerService.dateMoved.set(undefined),
            };
        }
        const shown = !!this.mapTileLayer() || footprints.overlayVisible();
        // the data has no dates there at all, e.g. after "Back to" a date of another source
        const outside = footprints.selectedOutsideData();
        if (shown && outside) {
            const date = formatDate(outside.step.time.start, unit);
            return {
                icon: 'event_busy',
                title: `${source} has no data ${outside.direction === 1 ? 'after' : 'before'} ${date}`,
                detail: 'Not here, and nowhere else on this date.',
                action: {label: `Show ${date}`, run: (): void => void this.projectService.setTime(new Time(outside.step.time.start))},
            };
        }
        // the dates of this view already tell, so zooming in would show nothing
        if (shown && footprints.selectedDateEmpty() && time !== undefined) {
            // kept dates of an earlier view may have scenes where this view has none
            const nearest = footprints.datesStale() ? undefined : this.nearestDateWithData();
            return {
                icon: 'event_busy',
                title: `No scenes here on ${formatDate(time, unit)}`,
                detail: 'Moving the map may show scenes of this date.',
                action: nearest && {
                    label: `Nearest date: ${formatDate(nearest.start.valueOf(), unit)}`,
                    run: (): void => void this.projectService.setTime(nearest),
                },
            };
        }
        const zoomIn = {label: 'Zoom in', run: (): void => this.mapComponent().zoomIn()};
        if (this.mapTileLayer() && this.zoomRequired()) {
            return {
                icon: 'zoom_in',
                title: 'The imagery shows from a closer zoom',
                detail: 'The map is too far out to load it.',
                action: zoomIn,
            };
        }
        if (footprints.overlayVisible() && (footprints.overlayTooLarge() || footprints.overlayBlocked())) {
            return {
                icon: 'zoom_in',
                title: 'The data availability shows from a closer zoom',
                detail: 'The map is too far out to tell where scenes are.',
                action: zoomIn,
            };
        }
        return undefined;
    });
    readonly tileLoading = signal(false);
    /** Also while the boxes of a date load, which can be before the tiles load and take a while for a view not seen before. */
    readonly isLoading = computed(
        () =>
            this.eodysseyLayerService.catalogueLoading() ||
            (this.tileLoading() && !this.zoomRequired()) ||
            this.footprintsService.overlayLoading(),
    );

    readonly isLayersActive = isActive('/map/layers', this.router);
    readonly isComputeActive = isActive('/map/compute', this.router);
    readonly isDownloadActive = isActive('/map/download', this.router);
    readonly isAboutActive = isActive('/map/about', this.router);

    readonly MeasurementType = MeasurementType;

    readonly mapImageLoading = signal(false);

    constructor() {
        // The selected preset, so availability is shown before the visualization is applied. None while the presets of a
        // newly selected source or variant load: the layer on the map is still the previous one, whose dates must not stand
        // for the new one.
        effect(() => {
            const preset = this.eodysseyLayerService.selectedPreset();
            const layer = preset && {dataConnectorId: preset.connectorId, layerId: preset.layerId};
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

    /** The loaded date with scenes nearest to the selected one, the later one of two as near. */
    private nearestDateWithData(): Time | undefined {
        const now = this.footprintsService.time()?.start.valueOf();
        const later = this.footprintsService.dateWithData(1);
        const earlier = this.footprintsService.dateWithData(-1);
        if (now === undefined || !later || !earlier) return later ?? earlier;
        return later.start.valueOf() - now <= now - earlier.start.valueOf() ? later : earlier;
    }

    /** Selects the date again that a newly selected data source moved, see `mapNotice`. */
    private async moveBack(): Promise<void> {
        const moved = this.eodysseyLayerService.dateMoved();
        if (!moved) return;
        this.eodysseyLayerService.dateMoved.set(undefined);
        await this.projectService.setTime(moved.from);
    }
}

/** A notice on the map, see `MainComponent.mapNotice`. */
interface MapNotice {
    icon: string;
    title: string;
    detail?: string;
    action?: {label: string; run: () => void};
    /** Only for a notice of something that happened, not of a state, which goes by itself. */
    close?: () => void;
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

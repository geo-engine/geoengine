import {computed, effect, inject, linkedSignal, resource, Service, signal, untracked} from '@angular/core';
import {toSignal} from '@angular/core/rxjs-interop';
import {MatCalendarCellClassFunction, MatCalendarView} from '@angular/material/datepicker';
import OlFeature from 'ol/Feature';
import OlGeometry from 'ol/geom/Geometry';
import OlLayerVector from 'ol/layer/Vector';
import OlSourceVector from 'ol/source/Vector';
import {Style} from 'ol/style';
import {firstValueFrom} from 'rxjs';
import {debounceTime} from 'rxjs/operators';
import {RasterFootprintsPrecision, TimeInterval, TimeStepFootprint, TimeStepFootprintStatus} from '@geoengine/api-client';
import {
    LayersService,
    ProcessingGraphsService,
    RasterLayerMetadata,
    SpatialReference,
    Time,
    TimeStepDuration,
    UUID,
} from '@geoengine/common';
import {boxFeatures, overlayStyle, viewCoverage, ViewCoverage} from './footprint-overlay';
import {
    calendarHint,
    calendarStartView,
    canPickDate,
    cellClasses,
    Footprints,
    footprintsOrTooLarge,
    isShorterThanMonth,
    mergeFootprints,
    monthStartOfTime,
    stepAt,
    stepsAround,
    visiblePeriod,
} from './footprints';
import {MapService, ViewportSize} from '../map/map.service';
import {ProjectService} from '../project/project.service';
import {SpatialReferenceService} from '../spatial-references/spatial-reference.service';

/** One request covers at most this many time steps of the dataset. */
const WINDOW_TIME_STEPS = 100;
/**
 * The most requests for one window. A month of hourly data needs 8, a year of daily data 5.
 * shortcut: a month of data with steps below an hour is cut off after 800 steps, it would need a window per day
 */
const MAX_WINDOW_REQUESTS = 8;
/** Requests wait until the view rests this long, so panning and zooming do not start catalog searches for every view in between. */
const VIEWPORT_DEBOUNCE_MS = 500;
/** Extra view widths and heights the overlay asks for on each side, so a scene larger than the view is not drawn as the view. */
const OVERLAY_MARGIN = 0.25;
/** A view is a rectangle in the map's projection only, so its bounds in the layer's projection need its edges. */
const EXTENT_EDGE_STOPS = 16;

/** The raster layer an app wants to know the footprints of. */
export interface FootprintLayer {
    /** Identifies the layer, so its graph and metadata are asked for once. */
    key: string;
    /** The id of the processing graph, registering the layer first if it is not one yet. */
    processingGraphId: () => Promise<UUID>;
}

/**
 * Tells the calendar and the map overlay when and where the selected layer has data in the map's view.
 * Nothing is requested until the calendar is open, the overlay is on or the app says that it shows the dates.
 */
@Service()
export class FootprintsService {
    private readonly layerService = inject(LayersService);
    private readonly processingGraphsService = inject(ProcessingGraphsService);
    private readonly projectService = inject(ProjectService);
    private readonly mapService = inject(MapService);
    private readonly spatialReferenceService = inject(SpatialReferenceService);

    private readonly mapSpatialReference = toSignal(this.projectService.getSpatialReferenceStream());
    /** The time the footprints are for. */
    readonly time = toSignal(this.projectService.getTimeStream());
    private readonly projectTimeStep = toSignal(this.projectService.getTimeStepDurationStream());
    /** The layer to show the footprints of. Set by the app. */
    readonly layer = signal<FootprintLayer | undefined>(undefined, {
        // an app sets a new object for the layer it has set already, which must not ask again
        equal: (a, b) => a?.key === b?.key,
    });
    /** A step to use instead of the project's, e.g. the one of a layer that is not applied yet. Set by the app. */
    readonly timeStepOverride = signal<TimeStepDuration | undefined>(undefined);
    readonly timeStep = computed(() => this.timeStepOverride() ?? this.projectTimeStep(), {
        // the project and the app hand out new objects for the same step
        equal: (a, b) => a?.durationAmount === b?.durationAmount && a?.durationUnit === b?.durationUnit,
    });
    /** The calendar view that has one cell per time step. */
    readonly calendarStartView = computed(() => calendarStartView(this.timeStep()?.durationUnit));
    private readonly viewport = toSignal(this.mapService.getViewportSizeStream().pipe(debounceTime(VIEWPORT_DEBOUNCE_MS)));

    private readonly graphs = new Map<string, Promise<{processingGraphId: UUID; metadata: RasterLayerMetadata | undefined}>>();

    readonly calendarOpen = signal(false);
    /** The app shows the dates of the layer outside of the calendar, e.g. next to the date with arrows. Set by the app. */
    readonly datesNeeded = signal(false);
    readonly overlayVisible = signal(false);
    /** Draw the boxes with faint stripes and no fill, e.g. while imagery of the layer lies below them. Set by the app. */
    readonly overlayLight = signal(false);
    /** Its own signal, so opening the calendar does not load dates again that the app shows already. */
    private readonly datesWanted = computed(() => this.calendarOpen() || this.datesNeeded());
    /** The map does not load the tiles of the layer at this zoom, so the overlay asks for nothing either. Set by the app. */
    readonly overlayBlocked = signal(false);
    /** What the calendar shows. At least `WINDOW_TIME_STEPS` steps from its start are loaded. */
    private readonly window = signal<TimeInterval | undefined>(undefined, {
        equal: (a, b) => a?.start === b?.start && a?.end === b?.end,
    });
    /** The loaded part of a window while the rest of it loads. */
    private readonly partialFootprints = signal<Footprints | undefined>(undefined);

    private readonly footprintsResource = resource({
        params: () => {
            const layer = this.layer();
            const mapSpatialReference = this.mapSpatialReference();
            const timeStep = this.timeStep();
            const viewport = this.viewport();
            const window = this.window();
            if (!this.datesWanted() || !layer || !mapSpatialReference || !timeStep || !viewport || !window) {
                return undefined;
            }
            return {layer, mapSpatialReference, timeStep, viewport, window};
        },
        loader: async ({
            params: {layer, mapSpatialReference, timeStep, viewport, window},
            abortSignal,
        }): Promise<Footprints | undefined> => {
            this.partialFootprints.set(undefined);
            const query = await this.queryOf(layer, mapSpatialReference, viewport);
            if (!query) return undefined;

            const minimalEnd = new Time(new Date(window.start)).add(timeStep.durationAmount * WINDOW_TIME_STEPS, timeStep.durationUnit);
            const end = Math.max(window.end, minimalEnd.start.valueOf());
            // aborting makes the server stop starting catalog searches for a view the user already left
            const get = (range: TimeInterval): Promise<Footprints> =>
                footprintsOrTooLarge(
                    this.processingGraphsService.getFootprints(
                        query.processingGraphId,
                        {
                            spatialBounds: query.spatialBounds,
                            timeInterval: new Time(new Date(range.start), new Date(range.end)).asRequestString(),
                            attributes: '0',
                            limit: WINDOW_TIME_STEPS,
                            // the calendar only needs to know which days have data
                            precision: RasterFootprintsPrecision.Availability,
                        },
                        abortSignal,
                    ),
                );

            // the month of the start first, so the visible month shows soonest, then the server's cursor is followed;
            // a month holds one step of a month or more, so then the window is asked at once
            const startDate = new Date(window.start);
            const monthEnd = Date.UTC(startDate.getUTCFullYear(), startDate.getUTCMonth() + 1, 1);
            const firstEnd = isShorterThanMonth(timeStep.durationUnit) ? Math.min(monthEnd, end) : end;
            let range = {start: window.start, end: firstEnd};
            let footprints: Footprints | undefined;
            for (let request = 0; request < MAX_WINDOW_REQUESTS; request++) {
                const part = await get(range);
                // a newer load has reset the partial answer, which this one must not bring back
                if (abortSignal.aborted) return undefined;
                footprints = footprints ? mergeFootprints(footprints, part) : part;
                // a step that crosses the end of the range is answered whole, so the rest starts after it
                const next = part.next?.start ?? Math.max(range.end, part.steps.at(-1)?.time.end ?? range.end);
                if (part.tooLarge || next >= end) break;
                this.partialFootprints.set(footprints);
                range = {start: next, end};
            }
            return footprints;
        },
    });

    /** The boxes of the selected time step alone, which the availability of the calendar window does not have. */
    private readonly overlayStepResource = resource({
        params: () => {
            const layer = this.layer();
            const mapSpatialReference = this.mapSpatialReference();
            const viewport = this.viewport();
            const time = this.time();
            if (!this.overlayVisible() || this.overlayBlocked() || !layer || !mapSpatialReference || !viewport || !time) return undefined;
            return {layer, mapSpatialReference, viewport, time};
        },
        loader: async ({params: {layer, mapSpatialReference, viewport, time}, abortSignal}): Promise<Footprints | undefined> => {
            const query = await this.queryOf(layer, mapSpatialReference, viewport, OVERLAY_MARGIN);
            if (!query) return undefined;
            return await footprintsOrTooLarge(
                this.processingGraphsService.getFootprints(
                    query.processingGraphId,
                    {
                        spatialBounds: query.spatialBounds,
                        timeInterval: time.asRequestString(),
                        attributes: '0',
                        limit: 1,
                        precision: RasterFootprintsPrecision.Coverage,
                    },
                    abortSignal,
                ),
            );
        },
    });

    /** `value()` of a resource throws while it is in an error state. */
    private readonly overlayFootprints = computed(() =>
        this.overlayStepResource.hasValue() ? this.overlayStepResource.value() : undefined,
    );

    /** The dates of the current request, or the loaded part of them. */
    private readonly currentFootprints = computed(() =>
        this.footprintsResource.hasValue() ? this.footprintsResource.value() : this.partialFootprints(),
    );
    /**
     * The dates to show. While those of a moved view load, the ones of the same layer and time step stay, like map
     * tiles, so a strip of dates does not blink; `datesStale` tells. Another layer or time step clears them at once.
     */
    private readonly footprints = linkedSignal<{key: string; current?: Footprints; isLoading: boolean}, Footprints | undefined>({
        source: () => ({
            key: `${this.layer()?.key}/${this.timeStep()?.durationAmount} ${this.timeStep()?.durationUnit}`,
            current: this.currentFootprints(),
            isLoading: this.footprintsResource.isLoading(),
        }),
        computation: (source, previous) =>
            source.current ?? (source.isLoading && previous?.source.key === source.key ? previous.value : undefined),
    });
    readonly steps = computed(() => this.footprints()?.steps ?? []);
    /** The dates shown are those of an earlier view, kept while the ones of this view load. */
    readonly datesStale = computed(() => !this.currentFootprints() && this.footprints() !== undefined);
    /** The dates of this view only, never kept ones. For decisions that must not act on an earlier view. */
    readonly freshSteps = computed(() => this.currentFootprints()?.steps ?? []);
    /** The dates with data of the view need too many catalog searches, so the user has to zoom in. */
    readonly tooLarge = computed(() => this.footprints()?.tooLarge ?? false);
    /**
     * The boxes of the overlay need too many catalog searches. The dates with data may still be known, because counting
     * them needs no search where the catalog can count.
     */
    readonly overlayTooLarge = computed(() => this.overlayVisible() && (this.overlayFootprints()?.tooLarge ?? false));
    readonly isLoading = computed(() => this.footprintsResource.isLoading());
    readonly hasError = computed(
        () => this.footprintsResource.status() === 'error' || (this.overlayVisible() && this.overlayStepResource.status() === 'error'),
    );

    readonly overlayLoading = computed(() => this.overlayVisible() && this.overlayStepResource.isLoading());
    /**
     * The boxes of the selected time step in the projection of the map, undefined until they are loaded.
     * The status an app shows is computed from these same boxes (`viewCoverage`), so the two cannot disagree.
     */
    private readonly overlayFeatures = computed(() => {
        const footprints = this.overlayFootprints();
        const mapSpatialReference = this.mapSpatialReference();
        const time = this.time();
        if (!footprints || footprints.tooLarge || !mapSpatialReference || !time) return undefined;
        const step = stepAt(footprints.steps, time.start.valueOf());
        return {
            key: this.overlayKey(),
            isUnknown: step?.status === TimeStepFootprintStatus.Unknown,
            features: boxFeatures(step?.bounds ?? [], footprints.spatialReference, mapSpatialReference.srsString),
        };
    });
    /**
     * How much of the view the selected time step covers, `unknown` if the layer cannot tell, undefined until it is loaded.
     * While the answer for a moved view loads (`overlayLoading`), the last one of the same layer and date stays.
     */
    readonly viewCoverage = linkedSignal<
        {key: string; isLoading: boolean; loaded?: ViewCoverage | 'unknown'},
        ViewCoverage | 'unknown' | undefined
    >({
        source: () => {
            const overlay = this.overlayFeatures();
            const viewport = this.viewport();
            const mapSpatialReference = this.mapSpatialReference();
            const extent =
                viewport && mapSpatialReference && this.spatialReferenceService.clipBoundsIfAvailable(viewport.extent, mapSpatialReference);
            return {
                key: this.overlayKey(),
                isLoading: this.overlayLoading(),
                loaded: !overlay || !extent ? undefined : overlay.isUnknown ? 'unknown' : viewCoverage(overlay.features, extent),
            };
        },
        computation: (source, previous) =>
            source.loaded ?? (source.isLoading && previous?.source.key === source.key ? previous.value : undefined),
    });

    /** Stays one object, because replacing a layer redraws all layers of the map. */
    readonly overlayLayer = new OlLayerVector({
        source: new OlSourceVector<OlFeature<OlGeometry>>({wrapX: false}),
        // OpenLayers asks for the style of every box in every frame
        style: (): Style => (this.overlayStyleCache ??= overlayStyle(this.overlayLight())),
    });
    /** Read from the CSS when the overlay is drawn anew, so a theme change shows up then. */
    private overlayStyleCache?: Style;
    /** The layer and date whose boxes the overlay shows, see `overlayKey`. */
    private overlayShows?: string;

    constructor() {
        effect(() => this.updateOverlay());
        // while the calendar is closed, the window follows the selected time; the calendar header moves it when open
        effect(() => {
            const time = this.time();
            if (!time || this.calendarOpen() || !this.datesNeeded()) return;
            untracked(() => {
                const start = monthStartOfTime(time.start.valueOf());
                if (!stepAt(this.steps(), time.start.valueOf())) this.window.set({start, end: start});
            });
        });
    }

    /**
     * The nearest time step with data before (-1) or after (1) the selected time, in the loaded window.
     * shortcut: no search beyond the window (100 steps), the caller goes on at `dateBeyondLoaded` then and the window follows.
     */
    dateWithData(direction: 1 | -1): Time | undefined {
        const time = this.time();
        if (!time) return undefined;
        const now = time.start.valueOf();
        const currentStart = stepAt(this.steps(), now)?.time.start ?? now;
        const withData = this.steps().filter(
            (step) =>
                step.status === TimeStepFootprintStatus.Data && (direction === 1 ? step.time.start > now : step.time.start < currentStart),
        );
        const found = direction === 1 ? withData[0] : withData[withData.length - 1];
        return found && new Time(new Date(found.time.start));
    }

    /**
     * Where to look on when `dateWithData` finds nothing: the first time step after (1) the loaded ones or the last
     * before (-1) them. Undefined while no dates are loaded, if the layer cannot tell them, or if it has one step only.
     */
    dateBeyondLoaded(direction: 1 | -1): Time | undefined {
        const steps = this.steps();
        const timeStep = this.timeStep();
        if (!timeStep || steps.length < 2 || steps.every((step) => step.status === TimeStepFootprintStatus.Unknown)) return undefined;
        return direction === 1
            ? new Time(new Date(steps[steps.length - 1].time.end))
            : new Time(new Date(steps[0].time.start)).subtract(timeStep.durationAmount, timeStep.durationUnit);
    }

    /** At most `count` loaded time steps around the selected time, e.g. for a strip of dates. */
    stepsAround(count: number): TimeStepFootprint[] {
        const time = this.time();
        return time ? stepsAround(this.steps(), time.start.valueOf(), count) : [];
    }

    /** The graph and the metadata of a layer. They do not change while the page is open, so they are asked for once. */
    private graphOf(layer: FootprintLayer): Promise<{processingGraphId: UUID; metadata: RasterLayerMetadata | undefined}> {
        const key = layer.key;
        let graph = this.graphs.get(key);
        if (!graph) {
            graph = (async (): Promise<{processingGraphId: UUID; metadata: RasterLayerMetadata | undefined}> => {
                const processingGraphId = await layer.processingGraphId();
                const metadata = await this.layerService.getProcessingGraphIdMetadata(processingGraphId);
                return {processingGraphId, metadata: metadata instanceof RasterLayerMetadata ? metadata : undefined};
            })();
            this.graphs.set(key, graph);
            // a failure must not stay in the cache
            graph.catch(() => this.graphs.delete(key));
        }
        return graph;
    }

    /** The graph id and the view's bounds in the graph's spatial reference, for both requests. */
    private async queryOf(
        layer: FootprintLayer,
        mapSpatialReference: SpatialReference,
        viewport: ViewportSize,
        margin = 0,
    ): Promise<{processingGraphId: UUID; spatialBounds: string} | undefined> {
        const {processingGraphId, metadata} = await this.graphOf(layer);
        if (!metadata) return undefined;

        // makes the layer's spatial reference known to OpenLayers
        await firstValueFrom(this.spatialReferenceService.getSpatialReferenceSpecification(metadata.spatialReference.srsString));

        const [minX, minY, maxX, maxY] = viewport.extent;
        const [width, height] = [maxX - minX, maxY - minY];
        const extent: ViewportSize['extent'] = [
            minX - width * margin,
            minY - height * margin,
            maxX + width * margin,
            maxY + height * margin,
        ];

        const spatialBounds = this.spatialReferenceService.reprojectExtent(
            this.spatialReferenceService.clipBoundsIfAvailable(extent, mapSpatialReference),
            mapSpatialReference,
            metadata.spatialReference,
            EXTENT_EDGE_STOPS,
        );
        return {processingGraphId, spatialBounds: spatialBounds.join(',')};
    }

    readonly dateClass: MatCalendarCellClassFunction<Date> = (date: Date, view: MatCalendarView) =>
        this.calendarHint(view) ? [] : cellClasses(this.steps(), date, view);

    /** Only dates with data can be picked; dates whose data is unknown or not loaded can too. */
    readonly dateFilter = (date: Date | null): boolean => !date || canPickDate(this.steps(), date);

    /** The hint of a view that is too coarse for the time step to show the dates with data, see `calendarHint`. */
    calendarHint(view: MatCalendarView): string | undefined {
        return calendarHint(this.timeStep()?.durationUnit, view);
    }

    /** Loads what the calendar shows in `view`, unless it is loaded already or the view has a hint instead. Takes a calendar date. */
    showDate(date: Date, view: MatCalendarView): void {
        if (this.calendarHint(view)) return;
        const period = visiblePeriod(date, view);
        if (stepAt(this.steps(), period.start) && stepAt(this.steps(), period.end - 1)) return;
        this.window.set(period);
    }

    private updateOverlay(): void {
        const source = this.overlayLayer.getSource();

        this.overlayStyleCache = undefined;
        // read here, so the boxes are drawn anew when the style changes
        this.overlayLight();
        // the calendar window only has availability, so the boxes come from the request of the overlay
        const overlay = this.overlayFeatures();
        // like map tiles, the boxes of the layer and date stay while those of a moved view load;
        // a new layer or date clears them at once, else the map would show the coverage of the wrong one
        const isSame = this.overlayKey() === this.overlayShows;
        if (!overlay && this.overlayStepResource.isLoading() && isSame && this.overlayVisible()) return;

        source?.clear();
        this.overlayShows = undefined;
        if (!source || !this.overlayVisible() || !overlay) return;

        source.addFeatures(overlay.features);
        this.overlayShows = overlay.key;
    }

    /** What the boxes of the overlay belong to: the layer and the date. */
    private overlayKey(): string {
        return `${this.layer()?.key}/${this.time()?.start.valueOf()}`;
    }
}

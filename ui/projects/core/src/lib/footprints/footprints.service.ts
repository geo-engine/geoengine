import {computed, effect, inject, linkedSignal, resource, Service, signal, untracked} from '@angular/core';
import {toSignal} from '@angular/core/rxjs-interop';
import {MatCalendarCellClassFunction, MatCalendarView} from '@angular/material/datepicker';
import {containsExtent} from 'ol/extent';
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
    calendarCellRange,
    calendarHint,
    calendarStartView,
    canPickDate,
    cellClasses,
    Footprints,
    footprintsOrTooLarge,
    isShorterThanDay,
    isShorterThanMonth,
    mergeFootprints,
    monthStartOfTime,
    stepAt,
    stepsAround,
    visiblePeriod,
    windowSteps,
} from './footprints';
import {MapService, ViewportSize} from '../map/map.service';
import {ProjectService} from '../project/project.service';
import {SpatialReferenceService} from '../spatial-references/spatial-reference.service';

/** One request covers at most this many time steps of the dataset; a window has fewer of long steps, see `windowSteps`. */
const MAX_REQUEST_TIME_STEPS = 100;
/**
 * The most requests for one window. A month of hourly data needs 8, a year of daily data 5.
 * shortcut: a month of data with steps below an hour is cut off after 800 steps, it would need a window per day
 */
const MAX_WINDOW_REQUESTS = 8;
/** Requests wait until the view rests this long, so panning and zooming do not start catalog searches for every view in between. */
const VIEWPORT_DEBOUNCE_MS = 500;
/** Extra view widths and heights the overlay asks for on each side, so a scene larger than the view is not drawn as the view. */
const OVERLAY_MARGIN = 0.25;
/**
 * Extra view widths and heights the dates ask for on each side. Their answer is kept while the view stays inside, so small pans ask
 * nothing. Measured on CODE-DE, 13 pans of 5 to 600 px: 5% halves the aggregations (30 instead of 65) with at most 2 days whose
 * scenes lie only outside the view; 10% saves no more requests but shows up to 12 such days where a swath edge runs near the view.
 */
const DATES_BUFFER = 0.05;
/** A view is a rectangle in the map's projection only, so its bounds in the layer's projection need its edges. */
const EXTENT_EDGE_STOPS = 16;
/** Time steps before the selected one that its window loads too, so a strip of dates can show it in its middle. */
const STEPS_BEFORE_SELECTED = 7;
/** Opacity of the boxes of an earlier view while those of the current view load, like the faded dates of a strip. */
const STALE_OVERLAY_OPACITY = 0.4;

/**
 * The view to ask for: `viewport` grown by `margin`, or the `kept` one while the view stays inside it at the same zoom,
 * so small pans ask nothing again.
 */
function keptViewport(viewport: ViewportSize | undefined, kept: ViewportSize | undefined, margin: number): ViewportSize | undefined {
    if (!viewport) return undefined;
    if (kept?.resolution === viewport.resolution && containsExtent(kept.extent, viewport.extent)) return kept;
    return {...viewport, extent: bufferedExtent(viewport.extent, margin)};
}

/** `extent` grown by `margin` of its width and height on each side. */
function bufferedExtent([minX, minY, maxX, maxY]: ViewportSize['extent'], margin: number): ViewportSize['extent'] {
    const [width, height] = [maxX - minX, maxY - minY];
    return [minX - width * margin, minY - height * margin, maxX + width * margin, maxY + height * margin];
}

/** Which layer and time step dates belong to. */
function datesKeyOf(layer: FootprintLayer | undefined, timeStep: TimeStepDuration | undefined): string {
    return `${layer?.key}/${timeStep?.durationAmount} ${timeStep?.durationUnit}`;
}

/** The time `count` steps before `timeMs`. */
function minusSteps(timeMs: number, timeStep: TimeStepDuration, count: number): number {
    return new Time(new Date(timeMs)).subtract(timeStep.durationAmount * count, timeStep.durationUnit).start.valueOf();
}

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
    /** The view the dates are asked for: the view and its buffer, kept while the view stays inside it at the same zoom. */
    private readonly datesViewport = linkedSignal<ViewportSize | undefined, ViewportSize | undefined>({
        source: () => this.viewport(),
        computation: (viewport, previous) => keptViewport(viewport, previous?.value, DATES_BUFFER),
    });
    /** The view the boxes of a date are asked for, like `datesViewport` with the margin of the overlay. */
    private readonly overlayViewport = linkedSignal<ViewportSize | undefined, ViewportSize | undefined>({
        source: () => this.viewport(),
        computation: (viewport, previous) => keptViewport(viewport, previous?.value, OVERLAY_MARGIN),
    });

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
    /** What the calendar shows. At least `windowSteps` steps from its start are loaded, within `timeRange`. */
    private readonly window = signal<TimeInterval | undefined>(undefined, {
        equal: (a, b) => a?.start === b?.start && a?.end === b?.end,
    });
    /** The layer and time step whose dates are shown. Dates of another key are never shown for this one. */
    private readonly datesKey = computed(() => datesKeyOf(this.layer(), this.timeStep()));
    /** The time of the layer's data, from its metadata. */
    private readonly dataTimeResource = resource({
        params: () => this.layer(),
        // open where the layer does not tell; never undefined, which a resource takes for no value
        loader: async ({params: layer}): Promise<TimeInterval> => {
            // open after a failure too, so the dates still load: their request asks for the graph again and shows its failure
            // shortcut: the dates of that layer are not clipped to the start of its data then, until the layer is set again
            const time = await this.graphOf(layer).then(
                (graph) => graph.metadata?.time,
                () => undefined,
            );
            return {start: time?.start.valueOf() ?? -Infinity, end: time?.end.valueOf() ?? Infinity};
        },
    });
    /**
     * The time with dates that can have data: that of the layer's data, up to now. Undefined until the layer's metadata is known.
     * shortcut: now is read when the layer changes; a page left open for days does not ask for the dates of those days until then
     */
    readonly timeRange = computed<TimeInterval | undefined>(() => {
        if (!this.dataTimeResource.hasValue()) return undefined;
        const time = this.dataTimeResource.value();
        return {start: time.start, end: Math.min(time.end, Date.now())};
    });
    /** The loaded part of a window while the rest of it loads, with the key of its layer and time step. */
    private readonly partialFootprints = signal<{key: string; footprints: Footprints} | undefined>(undefined);

    private readonly footprintsResource = resource({
        params: () => {
            const layer = this.layer();
            const mapSpatialReference = this.mapSpatialReference();
            const timeStep = this.timeStep();
            const viewport = this.datesViewport();
            const window = this.window();
            const timeRange = this.timeRange();
            if (!this.datesWanted() || !layer || !mapSpatialReference || !timeStep || !viewport || !window || !timeRange) {
                return undefined;
            }
            return {layer, mapSpatialReference, timeStep, viewport, window, timeRange};
        },
        loader: async ({
            params: {layer, mapSpatialReference, timeStep, viewport, window: requested, timeRange},
            abortSignal,
        }): Promise<Footprints | undefined> => {
            this.partialFootprints.set(undefined);
            // no dates before the data starts or after now, where there cannot be any
            const minimalEnd = new Time(new Date(requested.start)).add(
                timeStep.durationAmount * windowSteps(timeStep.durationUnit, timeStep.durationAmount),
                timeStep.durationUnit,
            );
            const window = {start: Math.max(requested.start, timeRange.start), end: requested.end};
            const end = Math.min(Math.max(requested.end, minimalEnd.start.valueOf()), timeRange.end);
            if (window.start >= end) return undefined;
            const query = await this.queryOf(layer, mapSpatialReference, viewport);
            if (!query) return undefined;

            // aborting makes the server stop starting catalog searches for a view the user already left
            const get = (range: TimeInterval): Promise<Footprints> =>
                footprintsOrTooLarge(
                    this.processingGraphsService.getFootprints(
                        query.processingGraphId,
                        {
                            spatialBounds: query.spatialBounds,
                            timeInterval: new Time(new Date(range.start), new Date(range.end)).asRequestString(),
                            attributes: '0',
                            limit: MAX_REQUEST_TIME_STEPS,
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
                this.partialFootprints.set({key: datesKeyOf(layer, timeStep), footprints});
                range = {start: next, end};
            }
            // the answer is complete; a partial one left behind would be taken for the next layer's until its load starts
            this.partialFootprints.set(undefined);
            return footprints;
        },
    });

    /** The boxes of the selected time step alone, which the availability of the calendar window does not have. */
    private readonly overlayStepResource = resource({
        params: () => {
            const layer = this.layer();
            const mapSpatialReference = this.mapSpatialReference();
            const viewport = this.overlayViewport();
            const time = this.time();
            if (!this.overlayVisible() || this.overlayBlocked() || !layer || !mapSpatialReference || !viewport || !time) return undefined;
            return {layer, mapSpatialReference, viewport, time};
        },
        loader: async ({params: {layer, mapSpatialReference, viewport, time}, abortSignal}): Promise<Footprints | undefined> => {
            const query = await this.queryOf(layer, mapSpatialReference, viewport);
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
    private readonly currentFootprints = computed(() => {
        if (this.footprintsResource.hasValue()) return this.footprintsResource.value();
        // until the load of another layer starts and clears it, the partial answer is still that of the old layer
        const partial = this.partialFootprints();
        return partial?.key === this.datesKey() ? partial.footprints : undefined;
    });
    /**
     * The dates to show. While those of a moved view load, the ones of the same layer and time step stay, like map
     * tiles, so a strip of dates does not blink; `datesStale` tells. Another layer or time step clears them at once.
     */
    private readonly footprints = linkedSignal<
        {key: string; current?: Footprints; isLoading: boolean; time?: number},
        Footprints | undefined
    >({
        source: () => ({
            key: this.datesKey(),
            current: this.currentFootprints(),
            isLoading: this.footprintsResource.isLoading(),
            time: this.time()?.start.valueOf(),
        }),
        computation: (source, previous) => {
            const kept = source.isLoading && previous?.source.key === source.key ? previous.value : undefined;
            // a first part of the new dates may end before the selected time, e.g. the month the window starts in; the
            // kept dates show it until a part does, so a strip around it does not empty while the rest loads
            const partialWithoutTime =
                source.isLoading && source.time !== undefined && !!source.current && !stepAt(source.current.steps, source.time);
            if (kept && partialWithoutTime) return kept;
            return source.current ?? kept;
        },
    });
    readonly steps = computed(() => this.footprints()?.steps ?? []);
    /** The dates shown are those of an earlier view, kept while the ones of this view load. */
    readonly datesStale = computed(() => this.footprints() !== undefined && this.footprints() !== this.currentFootprints());
    /** The dates of this view only, never kept ones. For decisions that must not act on an earlier view. */
    readonly freshSteps = computed(() => this.currentFootprints()?.steps ?? []);
    /**
     * Where the dates said that the selected date has no data, kept while the dates of a moved view load, since a part of
     * that area has none either. Another layer, time step, date or projection, or dates that say otherwise, forget it.
     */
    private readonly emptyExtent = linkedSignal<
        {key: string; time?: number; spatialReference?: unknown; status?: TimeStepFootprintStatus; extent?: ViewportSize['extent']},
        ViewportSize['extent'] | undefined
    >({
        source: () => {
            const time = this.time()?.start.valueOf();
            return {
                key: this.datesKey(),
                time,
                spatialReference: this.mapSpatialReference(),
                status: time === undefined ? undefined : stepAt(this.freshSteps(), time)?.status,
                extent: this.datesViewport()?.extent,
            };
        },
        computation: (source, previous) => {
            if (source.status === TimeStepFootprintStatus.Empty) return source.extent;
            const same =
                previous?.source.key === source.key &&
                previous.source.time === source.time &&
                previous.source.spatialReference === source.spatialReference;
            return source.status === undefined && same ? previous.value : undefined;
        },
    });
    /**
     * The selected date certainly has no data in this view, so an app need not load its tiles: the dates of this view
     * say so, or those of an earlier view around it. Elsewhere, while they load, or where the layer cannot tell, this is
     * false and tiles load as usual.
     */
    readonly selectedDateEmpty = computed(() => {
        const empty = this.emptyExtent();
        const viewport = this.viewport();
        return !!empty && !!viewport && containsExtent(empty, viewport.extent);
    });
    /** The dates with data of the view need too many catalog searches, so the user has to zoom in. */
    readonly tooLarge = computed(() => this.footprints()?.tooLarge ?? false);
    /**
     * The boxes of the overlay need too many catalog searches. The dates with data may still be known, because counting
     * them needs no search where the catalog can count.
     */
    readonly overlayTooLarge = computed(() => this.overlayVisible() && (this.overlayFootprints()?.tooLarge ?? false));
    readonly isLoading = computed(() => this.footprintsResource.isLoading());
    /** The dates of the view could not be loaded, see `retryDates`. */
    readonly datesFailed = computed(() => this.footprintsResource.status() === 'error');
    /**
     * The selected time lies after (1) or before (-1) all time steps of the data, e.g. after switching to a source whose data
     * ends earlier, with the nearest step there. The dates of a window are those of the data within it, so outside of them
     * there are none.
     */
    readonly selectedOutsideData = computed<{direction: 1 | -1; step: TimeStepFootprint} | undefined>(() => {
        const steps = this.steps();
        const time = this.time()?.start.valueOf();
        if (this.isLoading() || !steps.length || time === undefined || stepAt(steps, time)) return undefined;
        if (time >= steps[steps.length - 1].time.end) return {direction: 1, step: steps[steps.length - 1]};
        if (time < steps[0].time.start) return {direction: -1, step: steps[0]};
        return undefined;
    });
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
        // while the calendar is closed, the window follows the selected time and the layer with its time step, whose
        // steps around the time differ; the calendar header moves it when open
        effect(() => {
            const time = this.time();
            const timeStep = this.timeStep();
            this.layer();
            if (!time || this.calendarOpen() || !this.datesNeeded()) return;
            untracked(() => {
                // from a month start a few steps before the time, so dates before it are loaded too
                const before = timeStep ? minusSteps(time.start.valueOf(), timeStep, STEPS_BEFORE_SELECTED) : time.start.valueOf();
                // a window of steps below a day spans a few days only, so from the month start it would not reach the time
                const start = isShorterThanDay(timeStep?.durationUnit) ? before : monthStartOfTime(before);
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
        if (this.datesReachTimeRange(direction)) return undefined;
        return direction === 1
            ? new Time(new Date(steps[steps.length - 1].time.end))
            : new Time(new Date(steps[0].time.start)).subtract(timeStep.durationAmount, timeStep.durationUnit);
    }

    /** The loaded dates reach the start of the data (-1) or now (1), so there are no others in that direction, see `timeRange`. */
    datesReachTimeRange(direction: 1 | -1): boolean {
        const steps = this.steps();
        const timeRange = this.timeRange();
        if (!timeRange || !steps.length) return false;
        return direction === 1 ? steps[steps.length - 1].time.end >= timeRange.end : steps[0].time.start <= timeRange.start;
    }

    /**
     * At most `count` loaded time steps around the selected time, e.g. for a strip of dates. The nearest ones where the
     * selected time lies outside the dates of the data, see `selectedOutsideData`.
     */
    stepsAround(count: number): TimeStepFootprint[] {
        const time = this.time();
        if (!time) return [];
        const timeMs = time.start.valueOf();
        // a part of the dates that loads first may end before the selected time, which the rest then reaches
        if (this.isLoading() && !stepAt(this.steps(), timeMs)) return [];
        return stepsAround(this.steps(), timeMs, count);
    }

    retryDates(): void {
        this.footprintsResource.reload();
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
    ): Promise<{processingGraphId: UUID; spatialBounds: string} | undefined> {
        const {processingGraphId, metadata} = await this.graphOf(layer);
        if (!metadata) return undefined;

        // makes the layer's spatial reference known to OpenLayers
        await firstValueFrom(this.spatialReferenceService.getSpatialReferenceSpecification(metadata.spatialReference.srsString));

        const extent = viewport.extent;
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

    /** Only dates with data can be picked; dates whose data is unknown or not loaded can too, but none outside `timeRange`. */
    readonly dateFilter = (date: Date | null): boolean => {
        if (!date) return true;
        const day = calendarCellRange(date, 'month');
        const timeRange = this.timeRange();
        const inRange = !timeRange || (day.end > timeRange.start && day.start < timeRange.end);
        return inRange && canPickDate(this.steps(), date);
    };

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
        if (!overlay && this.overlayStepResource.isLoading() && isSame && this.overlayVisible()) {
            // faded, so the boxes of the earlier view are not taken for those of this one
            this.overlayLayer.setOpacity(STALE_OVERLAY_OPACITY);
            return;
        }

        source?.clear();
        this.overlayLayer.setOpacity(1);
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

import {computed, effect, inject, resource, Service, signal} from '@angular/core';
import {toSignal} from '@angular/core/rxjs-interop';
import {MatCalendarCellClassFunction, MatCalendarView} from '@angular/material/datepicker';
import OlFeature from 'ol/Feature';
import OlGeometry from 'ol/geom/Geometry';
import OlPolygon from 'ol/geom/Polygon';
import OlLayerVector from 'ol/layer/Vector';
import OlSourceVector from 'ol/source/Vector';
import {Fill, Stroke, Style} from 'ol/style';
import {firstValueFrom} from 'rxjs';
import {debounceTime} from 'rxjs/operators';
import {RasterFootprints, RasterFootprintsPrecision} from '@geoengine/api-client';
import {
    LayersService,
    ProcessingGraphsService,
    RasterLayerMetadata,
    SpatialReference,
    Time,
    TimeStepDuration,
    UUID,
} from '@geoengine/common';
import {
    TimeRange,
    boxRing,
    calendarStartView,
    cellHasData,
    cellHasStepAfter,
    cellHasStepStart,
    mergeFootprints,
    monthStartUtc,
    stepAt,
    windowRequests,
} from './footprints';
import {MapService, ViewportSize} from '../map/map.service';
import {ProjectService} from '../project/project.service';
import {SpatialReferenceService} from '../spatial-references/spatial-reference.service';

/** One request covers at most this many time steps of the dataset. */
const WINDOW_TIME_STEPS = 100;
const VIEWPORT_DEBOUNCE_MS = 500;
/**
 * The overlay asks for this many view widths and heights more on each side. The boxes are clipped to the
 * requested area, so a scene larger than the view would otherwise be drawn as the view itself, which
 * looks like a shape that moves with the map. The area, and with it the number of catalog searches,
 * grows with the square of this, so it stays small.
 */
const OVERLAY_MARGIN = 0.5;
/** The view is a rectangle of the map's projection, so its bounds in the layer's projection need its edges, not only its corners. */
const EXTENT_EDGE_STOPS = 16;

/** The raster layer an app wants to know the footprints of. */
export interface FootprintLayer {
    /** Identifies the layer, so its graph and metadata are asked for once. */
    key: string;
    /** The id of the processing graph, registering the layer first if it is not one yet. */
    processingGraphId: () => Promise<UUID>;
}

export const CELL_WITH_DATA_CLASS = 'geoengine-calendar-cell-with-data';
/** The time step of the cell goes on in the next cell, so a band joins them. */
export const CELL_BAND_AFTER_CLASS = 'geoengine-calendar-cell-band-after';
/** A cell a time step reaches into from an earlier cell. The cell where the step starts has a disc, the others a band. */
export const CELL_WITH_DATA_CONTINUED_CLASS = 'geoengine-calendar-cell-with-data-continued';

/** Period and line width of the stripes of the overlay in CSS pixels. */
const STRIPE_PERIOD = 10;
const STRIPE_WIDTH = 3;

/**
 * Diagonal stripes over the faded accent color. The map stays readable through them, and boxes that
 * overlap are told apart from one big box by the stripes meeting.
 */
function stripePattern(fill: string, stripe: string): CanvasPattern | undefined {
    const ratio = window.devicePixelRatio || 1;
    const canvas = document.createElement('canvas');
    canvas.width = canvas.height = STRIPE_PERIOD * ratio;
    const context = canvas.getContext('2d');
    if (!context) return undefined;

    context.scale(ratio, ratio);
    context.fillStyle = fill;
    context.fillRect(0, 0, STRIPE_PERIOD, STRIPE_PERIOD);
    context.strokeStyle = stripe;
    context.lineWidth = STRIPE_WIDTH;
    // three segments, so the stripe is continuous across the edges of the pattern tile
    context.beginPath();
    for (const offset of [-STRIPE_PERIOD, 0, STRIPE_PERIOD]) {
        context.moveTo(offset, STRIPE_PERIOD);
        context.lineTo(offset + STRIPE_PERIOD, 0);
    }
    context.stroke();

    const pattern = context.createPattern(canvas, 'repeat');
    // the bitmap has `ratio` pixels per CSS pixel, which the map's canvas scales up by `ratio` again
    pattern?.setTransform(new DOMMatrix().scale(1 / ratio));
    return pattern ?? undefined;
}

/** The theme's accent colors, which are CSS variables of the app. */
function overlayStyle(): Style {
    const css = getComputedStyle(document.documentElement);
    const color = (name: string, fallback: string): string => css.getPropertyValue(name).trim() || fallback;
    const accent = color('--geoengine-accent-color', '#ffc107');
    const faded = color('--geoengine-faded-accent-color', 'rgba(255, 193, 7, 0.32)');
    return new Style({
        fill: new Fill({color: stripePattern(faded, accent) ?? faded}),
        stroke: new Stroke({color: accent, width: 2}),
    });
}

/**
 * Tells the calendar and the map overlay when and where the selected layer has data in the map's view.
 * Nothing is requested until the calendar is open or the overlay is on.
 */
@Service()
export class FootprintsService {
    private readonly layerService = inject(LayersService);
    private readonly processingGraphsService = inject(ProcessingGraphsService);
    private readonly projectService = inject(ProjectService);
    private readonly mapService = inject(MapService);
    private readonly spatialReferenceService = inject(SpatialReferenceService);

    private readonly mapSpatialReference = toSignal(this.projectService.getSpatialReferenceStream());
    private readonly projectTime = toSignal(this.projectService.getTimeStream());
    /** A time to use instead of the project's, e.g. the default time of a dataset that is not applied yet. Set by the app. */
    readonly timeOverride = signal<Time | undefined>(undefined);
    private readonly time = computed(() => this.timeOverride() ?? this.projectTime());
    private readonly projectTimeStep = toSignal(this.projectService.getTimeStepDurationStream());
    /** The layer to show the footprints of. Set by the app. */
    readonly layer = signal<FootprintLayer | undefined>(undefined);
    /** A step to use instead of the project's, e.g. the one of a layer that is not applied yet. Set by the app. */
    readonly timeStepOverride = signal<TimeStepDuration | undefined>(undefined);
    readonly timeStep = computed(() => this.timeStepOverride() ?? this.projectTimeStep());
    /** The calendar view that has one cell per time step. */
    readonly calendarStartView = computed(() => calendarStartView(this.timeStep()?.durationUnit));
    private readonly viewport = toSignal(this.mapService.getViewportSizeStream().pipe(debounceTime(VIEWPORT_DEBOUNCE_MS)));

    private readonly graphs = new Map<string, Promise<{processingGraphId: UUID; metadata: RasterLayerMetadata | undefined}>>();

    readonly calendarOpen = signal(false);
    readonly overlayVisible = signal(false);
    private readonly windowStart = signal<number | undefined>(undefined);
    /** The visible month of a window while the rest of it loads. */
    private readonly partialFootprints = signal<RasterFootprints | undefined>(undefined);

    private readonly footprintsResource = resource({
        params: () => {
            const layer = this.layer();
            const mapSpatialReference = this.mapSpatialReference();
            const timeStep = this.timeStep();
            const viewport = this.viewport();
            const windowStart = this.windowStart();
            const isNeeded = this.calendarOpen();

            if (!isNeeded || !layer || !mapSpatialReference || !timeStep || !viewport || windowStart === undefined) {
                return undefined;
            }
            return {layer, mapSpatialReference, timeStep, viewport, windowStart};
        },
        loader: async ({
            params: {layer, mapSpatialReference, timeStep, viewport, windowStart},
            abortSignal,
        }): Promise<RasterFootprints | undefined> => {
            this.partialFootprints.set(undefined);
            const query = await this.queryOf(layer, mapSpatialReference, viewport);
            if (!query) return undefined;

            const start = new Time(new Date(windowStart));
            const end = start.add(timeStep.durationAmount * WINDOW_TIME_STEPS, timeStep.durationUnit);
            // aborting makes the server stop starting catalog searches for a view the user already left
            const get = (range: TimeRange): Promise<RasterFootprints> =>
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
                );

            const [month, rest] = windowRequests(start.start.valueOf(), end.start.valueOf());
            const first = await get(month);
            if (!rest || first.next || first.tooLarge) return first;
            // a newer load has reset the partial answer, which this one must not bring back
            if (abortSignal.aborted) return undefined;
            this.partialFootprints.set(first);
            return mergeFootprints(first, await get(rest));
        },
    });

    /**
     * The boxes of the selected time step alone. One step costs what a map at that zoom costs and gives
     * the boxes of the data, which the window of the calendar does not.
     */
    private readonly overlayStepResource = resource({
        params: () => {
            const layer = this.layer();
            const mapSpatialReference = this.mapSpatialReference();
            const viewport = this.viewport();
            const time = this.time();
            if (!this.overlayVisible() || !layer || !mapSpatialReference || !viewport || !time) return undefined;
            return {layer, mapSpatialReference, viewport, time};
        },
        loader: async ({params: {layer, mapSpatialReference, viewport, time}, abortSignal}): Promise<RasterFootprints | undefined> => {
            const query = await this.queryOf(layer, mapSpatialReference, viewport, OVERLAY_MARGIN);
            if (!query) return undefined;
            return await this.processingGraphsService.getFootprints(
                query.processingGraphId,
                {
                    spatialBounds: query.spatialBounds,
                    timeInterval: time.asRequestString(),
                    attributes: '0',
                    limit: 1,
                    precision: RasterFootprintsPrecision.Coverage,
                },
                abortSignal,
            );
        },
    });

    /** `value()` of a resource throws while it is in an error state. */
    private readonly overlayFootprints = computed(() =>
        this.overlayStepResource.hasValue() ? this.overlayStepResource.value() : undefined,
    );

    private readonly footprints = computed(() =>
        this.footprintsResource.hasValue() ? this.footprintsResource.value() : this.partialFootprints(),
    );
    readonly steps = computed(() => this.footprints()?.steps ?? []);
    /** The view needs too many catalog searches, so the user has to zoom in. */
    readonly tooLarge = computed(
        () => (this.footprints()?.tooLarge ?? false) || (this.overlayVisible() && (this.overlayFootprints()?.tooLarge ?? false)),
    );
    readonly isLoading = computed(() => this.footprintsResource.isLoading());
    readonly hasError = computed(
        () => this.footprintsResource.status() === 'error' || (this.overlayVisible() && this.overlayStepResource.status() === 'error'),
    );

    readonly overlayLoading = computed(() => this.overlayVisible() && this.overlayStepResource.isLoading());
    /** The areas of the selected time step in view, undefined until they are known. */
    readonly overlayAreaCount = computed(() => {
        const time = this.time();
        const footprints = this.overlayFootprints();
        if (!time || !footprints) return undefined;
        return stepAt(footprints.steps, time.start.valueOf())?.bounds.length ?? 0;
    });

    /** Stays one object, because replacing a layer redraws all layers of the map. */
    readonly overlayLayer = new OlLayerVector({
        source: new OlSourceVector<OlFeature<OlGeometry>>({wrapX: false}),
        // OpenLayers asks for the style of every box in every frame
        style: (): Style => (this.overlayStyleCache ??= overlayStyle()),
    });
    /** Read from the CSS when the overlay is drawn anew, so a theme change shows up then. */
    private overlayStyleCache?: Style;

    constructor() {
        effect(() => this.updateOverlay());
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

    readonly dateClass: MatCalendarCellClassFunction<Date> = (date: Date, view: MatCalendarView) => {
        const steps = this.steps();
        if (!cellHasData(steps, date, view)) return '';
        return [
            cellHasStepStart(steps, date, view) ? CELL_WITH_DATA_CLASS : CELL_WITH_DATA_CONTINUED_CLASS,
            ...(cellHasStepAfter(steps, date, view) ? [CELL_BAND_AFTER_CLASS] : []),
        ];
    };

    /** Loads the window of `date` unless it is loaded already. Takes a calendar date. */
    showDate(date: Date): void {
        const timeMs = Date.UTC(date.getFullYear(), date.getMonth(), date.getDate());
        if (stepAt(this.steps(), timeMs)) return;
        this.windowStart.set(monthStartUtc(date));
    }

    private updateOverlay(): void {
        const source = this.overlayLayer.getSource();
        const mapSpatialReference = this.mapSpatialReference();
        const time = this.time();

        this.overlayStyleCache = undefined;
        source?.clear();
        if (!source || !mapSpatialReference || !time || !this.overlayVisible()) return;

        // The window only says which days have data. Its boxes are the whole view, so the overlay
        // draws the boxes of its own request for the selected step.
        const footprints = this.overlayFootprints();
        if (!footprints) return;

        const step = stepAt(footprints.steps, time.start.valueOf());
        source.addFeatures(
            (step?.bounds ?? []).map((bounds) => {
                const geometry = new OlPolygon([
                    boxRing([
                        bounds.upperLeftCoordinate.x,
                        bounds.lowerRightCoordinate.y,
                        bounds.lowerRightCoordinate.x,
                        bounds.upperLeftCoordinate.y,
                    ]),
                ]);
                geometry.transform(footprints.spatialReference, mapSpatialReference.srsString);
                return new OlFeature<OlGeometry>(geometry);
            }),
        );
    }
}

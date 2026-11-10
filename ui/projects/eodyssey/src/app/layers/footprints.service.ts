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
import {LayersService, ProcessingGraphsService, RasterLayerMetadata, SpatialReference, Time, UUID} from '@geoengine/common';
import {MapService, ProjectService, SpatialReferenceService, ViewportSize} from '@geoengine/core';
import {DataSourceLayer} from './data-sources';
import {TimeRange, boxRing, cellHasData, mergeFootprints, monthStartUtc, stepAt, windowRequests} from './footprints';
import {EOdysseyLayerService} from './layers.service';

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

export const CELL_WITH_DATA_CLASS = 'geoengine-calendar-cell-with-data';

/** The theme's accent colors, which are CSS variables of the app. */
function overlayStyle(): Style {
    const css = getComputedStyle(document.documentElement);
    const color = (name: string, fallback: string): string => css.getPropertyValue(name).trim() || fallback;
    return new Style({
        fill: new Fill({color: color('--geoengine-faded-accent-color', 'rgba(255, 193, 7, 0.32)')}),
        stroke: new Stroke({color: color('--geoengine-accent-color', '#ffc107'), width: 2}),
    });
}

/**
 * Tells the calendar and the map overlay when and where the selected layer has data in the map's view.
 * Nothing is requested until the calendar is open or the overlay is on.
 */
@Service()
export class FootprintsService {
    private readonly eodysseyLayerService = inject(EOdysseyLayerService);
    private readonly layerService = inject(LayersService);
    private readonly processingGraphsService = inject(ProcessingGraphsService);
    private readonly projectService = inject(ProjectService);
    private readonly mapService = inject(MapService);
    private readonly spatialReferenceService = inject(SpatialReferenceService);

    private readonly mapSpatialReference = toSignal(this.projectService.getSpatialReferenceStream());
    private readonly time = toSignal(this.projectService.getTimeStream());
    private readonly projectTimeStep = toSignal(this.projectService.getTimeStepDurationStream());
    /** The step of the selected dataset until it is applied, because the project keeps the step of the applied one. */
    readonly timeStep = computed(
        () =>
            (this.eodysseyLayerService.canApplyPreset() ? this.eodysseyLayerService.selectedDataSource()?.defaultTimeStep : undefined) ??
            this.projectTimeStep(),
    );
    private readonly viewport = toSignal(this.mapService.getViewportSizeStream().pipe(debounceTime(VIEWPORT_DEBOUNCE_MS)));

    /** The selected preset, so availability is shown before the visualization is applied. Else the layer on the map. */
    private readonly footprintLayer = computed<DataSourceLayer | undefined>(() => {
        const preset = this.eodysseyLayerService.selectedPreset();
        return preset ? {dataConnectorId: preset.connectorId, layerId: preset.layerId} : this.eodysseyLayerService.mapTileLayer();
    });

    private readonly graphs = new Map<string, Promise<{processingGraphId: UUID; metadata: RasterLayerMetadata | undefined}>>();

    readonly calendarOpen = signal(false);
    readonly overlayVisible = signal(false);
    /** The map shows no tiles at this zoom. The overlay costs what a map costs, so it waits as well. */
    readonly zoomRequired = signal(false);
    private readonly windowStart = signal<number | undefined>(undefined);
    /** The visible month of a window while the rest of it loads. */
    private readonly partialFootprints = signal<RasterFootprints | undefined>(undefined);

    private readonly footprintsResource = resource({
        params: () => {
            const layer = this.footprintLayer();
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
            const layer = this.footprintLayer();
            const mapSpatialReference = this.mapSpatialReference();
            const viewport = this.viewport();
            const time = this.time();
            if (!this.overlayVisible() || this.zoomRequired() || !layer || !mapSpatialReference || !viewport || !time) return undefined;
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
    private graphOf(layer: DataSourceLayer): Promise<{processingGraphId: UUID; metadata: RasterLayerMetadata | undefined}> {
        const key = `${layer.dataConnectorId}/${layer.layerId}`;
        let graph = this.graphs.get(key);
        if (!graph) {
            graph = (async (): Promise<{processingGraphId: UUID; metadata: RasterLayerMetadata | undefined}> => {
                const processingGraphId = await this.layerService.registerAndGetLayerProcessingGraphId(
                    layer.dataConnectorId,
                    layer.layerId,
                );
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
        layer: DataSourceLayer,
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
        cellHasData(this.steps(), date, view) ? CELL_WITH_DATA_CLASS : '';

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

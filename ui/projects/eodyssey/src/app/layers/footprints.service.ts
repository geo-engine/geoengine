import {computed, effect, inject, resource, Service, signal} from '@angular/core';
import {toSignal} from '@angular/core/rxjs-interop';
import {MatCalendarCellClassFunction, MatCalendarView} from '@angular/material/datepicker';
import OlFeature from 'ol/Feature';
import OlGeometry from 'ol/geom/Geometry';
import {fromExtent} from 'ol/geom/Polygon';
import OlLayerVector from 'ol/layer/Vector';
import OlSourceVector from 'ol/source/Vector';
import {firstValueFrom} from 'rxjs';
import {debounceTime} from 'rxjs/operators';
import {RasterFootprints, RasterFootprintsPrecision} from '@geoengine/api-client';
import {LayersService, ProcessingGraphsService, RasterLayerMetadata, SpatialReference, Time, UUID} from '@geoengine/common';
import {MapService, ProjectService, SpatialReferenceService, ViewportSize} from '@geoengine/core';
import {DataSourceLayer} from './data-sources';
import {TimeRange, calendarCellKey, calendarKeysWithData, mergeFootprints, monthStartUtc, stepAt, windowRequests} from './footprints';
import {EOdysseyLayerService} from './layers.service';

/** One request covers at most this many time steps of the dataset. */
const WINDOW_TIME_STEPS = 100;
const VIEWPORT_DEBOUNCE_MS = 500;

export const CELL_WITH_DATA_CLASS = 'geoengine-calendar-cell-with-data';

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
    private readonly timeStep = toSignal(this.projectService.getTimeStepDurationStream());
    private readonly viewport = toSignal(this.mapService.getViewportSizeStream().pipe(debounceTime(VIEWPORT_DEBOUNCE_MS)));

    readonly calendarOpen = signal(false);
    readonly overlayVisible = signal(false);
    /** The map shows no tiles at this zoom. The overlay costs what a map costs, so it waits as well. */
    readonly zoomRequired = signal(false);
    private readonly windowStart = signal<number | undefined>(undefined);
    /** The visible month of a window while the rest of it loads. */
    private readonly partialFootprints = signal<RasterFootprints | undefined>(undefined);

    private readonly footprintsResource = resource({
        params: () => {
            const layer = this.eodysseyLayerService.mapTileLayer();
            const mapSpatialReference = this.mapSpatialReference();
            const timeStep = this.timeStep();
            const viewport = this.viewport();
            const windowStart = this.windowStart();
            const isNeeded = this.calendarOpen() || this.overlayVisible();

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
            const layer = this.eodysseyLayerService.mapTileLayer();
            const mapSpatialReference = this.mapSpatialReference();
            const viewport = this.viewport();
            const time = this.time();
            if (!this.overlayVisible() || this.zoomRequired() || !layer || !mapSpatialReference || !viewport || !time) return undefined;
            return {layer, mapSpatialReference, viewport, time};
        },
        loader: async ({params: {layer, mapSpatialReference, viewport, time}, abortSignal}): Promise<RasterFootprints | undefined> => {
            const query = await this.queryOf(layer, mapSpatialReference, viewport);
            if (!query) return undefined;
            return await this.processingGraphsService.getFootprints(
                query.processingGraphId,
                {
                    spatialBounds: query.spatialBounds,
                    timeInterval: time.asRequestString(),
                    attributes: '0',
                    limit: 1,
                    precision: RasterFootprintsPrecision.Boxes,
                },
                abortSignal,
            );
        },
    });

    private readonly footprints = computed(() =>
        this.footprintsResource.hasValue() ? this.footprintsResource.value() : this.partialFootprints(),
    );
    readonly steps = computed(() => this.footprints()?.steps ?? []);
    /** The view needs too many catalog searches, so the user has to zoom in. */
    readonly tooLarge = computed(
        () => (this.footprints()?.tooLarge ?? false) || (this.overlayVisible() && (this.overlayStepResource.value()?.tooLarge ?? false)),
    );
    readonly isLoading = computed(() => this.footprintsResource.isLoading());
    readonly hasError = computed(() => this.footprintsResource.status() === 'error');

    private readonly keysWithData = computed(() => calendarKeysWithData(this.steps()));

    /** Stays one object, because replacing a layer redraws all layers of the map. */
    readonly overlayLayer = new OlLayerVector({source: new OlSourceVector<OlFeature<OlGeometry>>({wrapX: false})});

    constructor() {
        effect(() => {
            const time = this.time();
            if (time && this.overlayVisible()) this.showDate(time.start.toDate());
        });

        effect(() => this.updateOverlay());
    }

    /** The graph id and the view's bounds in the graph's spatial reference, for both requests. */
    private async queryOf(
        layer: DataSourceLayer,
        mapSpatialReference: SpatialReference,
        viewport: ViewportSize,
    ): Promise<{processingGraphId: UUID; spatialBounds: string} | undefined> {
        const processingGraphId = await this.layerService.registerAndGetLayerProcessingGraphId(layer.dataConnectorId, layer.layerId);
        const metadata = await this.layerService.getProcessingGraphIdMetadata(processingGraphId);
        if (!(metadata instanceof RasterLayerMetadata)) return undefined;

        // makes the layer's spatial reference known to OpenLayers
        await firstValueFrom(this.spatialReferenceService.getSpatialReferenceSpecification(metadata.spatialReference.srsString));

        const spatialBounds = this.spatialReferenceService.reprojectExtent(
            this.spatialReferenceService.clipBoundsIfAvailable(viewport.extent, mapSpatialReference),
            mapSpatialReference,
            metadata.spatialReference,
        );
        return {processingGraphId, spatialBounds: spatialBounds.join(',')};
    }

    readonly dateClass: MatCalendarCellClassFunction<Date> = (date: Date, view: MatCalendarView) =>
        this.keysWithData().has(calendarCellKey(date, view)) ? CELL_WITH_DATA_CLASS : '';

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

        source?.clear();
        if (!source || !mapSpatialReference || !time || !this.overlayVisible()) return;

        // The window only says which days have data. Its boxes are the whole view, so the overlay
        // draws the boxes of its own request for the selected step.
        const footprints = this.overlayStepResource.value();
        if (!footprints) return;

        const step = stepAt(footprints.steps, time.start.valueOf());
        source.addFeatures(
            (step?.bounds ?? []).map((bounds) => {
                const geometry = fromExtent([
                    bounds.upperLeftCoordinate.x,
                    bounds.lowerRightCoordinate.y,
                    bounds.lowerRightCoordinate.x,
                    bounds.upperLeftCoordinate.y,
                ]);
                geometry.transform(footprints.spatialReference, mapSpatialReference.srsString);
                return new OlFeature<OlGeometry>(geometry);
            }),
        );
    }
}

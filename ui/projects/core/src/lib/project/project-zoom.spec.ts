import {TestBed} from '@angular/core/testing';
import View from 'ol/View';
import {NEVER, Observable, of, Subscription} from 'rxjs';
import {afterEach, beforeEach, expect, it, vi} from 'vitest';
import {GeoJson, OGCWFSApi, PlotOutputFormat, WfsHandlerRequest, WrappedPlotOutput} from '@geoengine/api-client';
import {
    LayersService,
    NotificationService,
    Plot,
    PlotsService,
    PointSymbology,
    ProcessingGraphsService,
    SpatialReference,
    Time,
    UserService,
    VectorLayer,
} from '@geoengine/common';
import {BackendService} from '../backend/backend.service';
import {CoreConfig, DEFAULT_CORE_CONFIG} from '../config.service';
import {Project} from './project.model';
import {ProjectService} from './project.service';
import {MapService} from '../map/map.service';
import {SpatialReferenceService} from '../spatial-references/spatial-reference.service';

interface Pending<T> {
    signal: AbortSignal;
    complete: (value: T) => void;
}

let vectors: Array<Pending<GeoJson> & {query: WfsHandlerRequest}>;
let plots: Pending<WrappedPlotOutput>[];
let map: MapService;
let view: View;
let service: ProjectService;
let subscriptions: Subscription;
const plot = new Plot({name: 'Plot', workflowId: 'plot'});
const vector = new VectorLayer({
    name: 'Vector',
    processingGraphId: 'vector',
    isVisible: true,
    isLegendVisible: false,
    symbology: PointSymbology.fromPointSymbologyDict({
        type: 'point',
        radius: {type: 'static', value: 5},
        fillColor: {type: 'static', color: [0, 0, 0, 255]},
        stroke: {width: {type: 'static', value: 1}, color: {type: 'static', color: [0, 0, 0, 255]}},
    }),
});

async function flush(): Promise<void> {
    TestBed.tick();
    await vi.advanceTimersByTimeAsync(0);
}

beforeEach(async () => {
    vi.useFakeTimers();
    vectors = [];
    plots = [];
    subscriptions = new Subscription();
    vi.spyOn(OGCWFSApi.prototype, 'wfsHandler').mockImplementation(
        (query, init) =>
            new Promise((resolve) => {
                vectors.push({query, signal: (init as RequestInit).signal!, complete: resolve});
            }),
    );
    const getPlot = vi.fn<PlotsService['getPlot']>().mockImplementation(
        (_id, _bbox, _time, _res, _sref, signal) =>
            new Promise((resolve) => {
                plots.push({signal: signal!, complete: resolve});
            }),
    );
    TestBed.configureTestingModule({
        providers: [
            ProjectService,
            MapService,
            {provide: CoreConfig, useValue: DEFAULT_CORE_CONFIG},
            {provide: NotificationService, useValue: {error: vi.fn()}},
            {provide: BackendService, useValue: {setSessionProject: vi.fn(), updateProject: vi.fn().mockReturnValue(of({id: 'project'}))}},
            {
                provide: UserService,
                useValue: {
                    getSessionStream: (): typeof NEVER => NEVER,
                    getSessionTokenStream: (): Observable<string> => of('token'),
                    getSessionTokenForRequest: (): Observable<string> => of('token'),
                },
            },
            {provide: SpatialReferenceService, useValue: {}},
            {provide: LayersService, useValue: {}},
            {
                provide: ProcessingGraphsService,
                useValue: {
                    getMetadata: vi
                        .fn()
                        .mockResolvedValue({type: 'vector', dataType: 'MultiPoint', spatialReference: 'EPSG:3857', columns: {}}),
                    getProcessingGraph: vi.fn().mockResolvedValue({type: 'Vector'}),
                },
            },
            {provide: PlotsService, useValue: {getPlot}},
        ],
    });
    map = TestBed.inject(MapService);
    view = new View({center: [0, 0], zoom: 5});
    map.setView(view);
    map.setViewportSize({extent: [0, 0, 1, 1], resolution: view.getResolution()!});
    TestBed.tick();
    service = TestBed.inject(ProjectService);
    service.setProject(
        new Project({
            id: 'project',
            name: 'Project',
            description: '',
            spatialReference: new SpatialReference('EPSG:3857'),
            time: new Time('2026-04-01'),
            plots: [plot],
            layers: [vector],
            timeStepDuration: {durationAmount: 1, durationUnit: 'day'},
            bbox: {lowerLeftCoordinate: {x: 0, y: 0}, upperRightCoordinate: {x: 1, y: 1}},
            version: {id: '0', changed: new Date('2026-04-01T00:00:00Z')},
        }),
    );
    await flush();
    expect(vectors).toHaveLength(1);
    expect(plots).toHaveLength(1);
});

afterEach(() => {
    subscriptions.unsubscribe();
    TestBed.resetTestingModule();
    vi.useRealTimers();
    vi.restoreAllMocks();
});

it('cancels vectors and plots on zoom and restarts both on the shared deadline without a second debounce', async () => {
    view.setZoom(6);
    expect(vectors[0].signal.aborted).toBe(true);
    expect(plots[0].signal.aborted).toBe(true);
    map.setViewportSize({extent: [1, 1, 2, 2], resolution: view.getResolution()!});
    await vi.advanceTimersByTimeAsync(300);
    view.setZoom(7);
    map.setViewportSize({extent: [10, 10, 12, 12], resolution: view.getResolution()!});
    await vi.advanceTimersByTimeAsync(399);
    expect(vectors).toHaveLength(1);
    expect(plots).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(vectors).toHaveLength(2);
    expect(plots).toHaveLength(2);
    expect(vectors.at(-1)?.query.bbox).toBe('10,10,12,12');
    expect(vectors[1].signal.aborted).toBe(false);
    expect(plots[1].signal.aborted).toBe(false);
});

it('requests vector and plot data immediately after an ordinary pan', async () => {
    view.beginInteraction();
    view.setCenter([100, 100]);
    map.setViewportSize({extent: [1, 1, 2, 2], resolution: view.getResolution()!});
    await flush();
    expect(vectors).toHaveLength(2);
    expect(plots).toHaveLength(2);
});

it('keeps time changes behind the zoom gate and resumes with the latest date', async () => {
    view.setZoom(6);
    await service.setTime(new Time('2026-04-02'));
    await flush();
    expect(vectors).toHaveLength(1);
    expect(plots).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(400);
    expect(vectors).toHaveLength(2);
    expect(plots).toHaveLength(2);
    expect(vectors.at(-1)?.query.time).toContain('2026-04-02');
});

it('ignores old vector and plot results when an aborted request finishes after resuming', async () => {
    const vectorData = vi.fn();
    const plotData = vi.fn();
    subscriptions.add(service.getLayerDataStream(vector).subscribe(vectorData));
    subscriptions.add(service.getPlotDataStream(plot).subscribe(plotData));
    view.setZoom(6);
    await vi.advanceTimersByTimeAsync(400);
    const geojson: GeoJson = {type: 'FeatureCollection', features: []};
    const output: WrappedPlotOutput = {plotType: 'Histogram', outputFormat: PlotOutputFormat.JsonVega, data: {}};
    vectors[0].complete(geojson);
    plots[0].complete(output);
    await flush();
    expect(vectorData).not.toHaveBeenCalled();
    expect(plotData).not.toHaveBeenCalled();
    vectors[1].complete(geojson);
    plots[1].complete(output);
    await flush();
    expect(vectorData).toHaveBeenCalledOnce();
    expect(plotData).toHaveBeenCalledExactlyOnceWith(output);
});

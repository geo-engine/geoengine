import {ComponentFixture, TestBed} from '@angular/core/testing';
import type {FrameState} from 'ol/Map';
import Tile from 'ol/Tile';
import TileQueue, {getTilePriority} from 'ol/TileQueue';
import View from 'ol/View';
import {NEVER, Observable, of} from 'rxjs';
import {afterEach, beforeEach, expect, it, vi} from 'vitest';
import {NotificationService, RasterData, RasterSymbology, SpatialReference, Time} from '@geoengine/common';
import {BackendService} from '../backend/backend.service';
import {CoreConfig, DEFAULT_CORE_CONFIG} from '../config.service';
import {OlRasterLayerComponent} from './map-layer.component';
import {MapService} from './map.service';
import {ProjectService} from '../project/project.service';

interface TileEnqueuer {
    enqueueTiles(frame: FrameState, extent: number[], z: number, tilesByZ: Record<number, Set<Tile>>, preload: number): void;
}

let fixture: ComponentFixture<OlRasterLayerComponent>;
let view: View;
let signals: AbortSignal[];

function loadFrame(): TileQueue {
    const frame = {viewState: view.getState(), pixelRatio: 1, wantedTiles: {}} as FrameState;
    const queue = new TileQueue(
        (...args): number => getTilePriority(frame, ...args),
        (): void => undefined,
    );
    frame.tileQueue = queue;
    const renderer = fixture.componentInstance.mapLayer.getRenderer() as unknown as TileEnqueuer;
    renderer.enqueueTiles(frame, view.calculateExtent([256, 256]), Math.round(view.getZoom()!), {}, 0);
    queue.reprioritize();
    queue.loadMoreTiles(16, 16);
    return queue;
}

beforeEach(() => {
    vi.useFakeTimers();
    signals = [];
    vi.stubGlobal(
        'fetch',
        vi.fn((_url: string, init: RequestInit) => {
            signals.push(init.signal!);
            return new Promise<Response>((_resolve, reject) => {
                init.signal!.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')), {once: true});
            });
        }),
    );
    const time = new Time('2026-04-01');
    const projection = new SpatialReference('EPSG:3857');
    TestBed.configureTestingModule({
        imports: [OlRasterLayerComponent],
        providers: [
            MapService,
            {provide: CoreConfig, useValue: DEFAULT_CORE_CONFIG},
            {provide: BackendService, useValue: {wmsBaseUrl: 'https://tiles.test/wms'}},
            {provide: NotificationService, useValue: {error: vi.fn()}},
            {
                provide: ProjectService,
                useValue: {
                    getLayerDataStream: (): Observable<RasterData> => of(new RasterData(time, projection, 'workflow')),
                    createQueryAbortStream: (): typeof NEVER => NEVER,
                    changeRasterLayerDataStatus: vi.fn(),
                },
            },
        ],
    });
    const map = TestBed.inject(MapService);
    view = new View({center: [0, 0], zoom: 5, constrainResolution: true});
    map.setView(view);
    fixture = TestBed.createComponent(OlRasterLayerComponent);
    fixture.componentRef.setInput('layerId', 0);
    fixture.componentRef.setInput('workflow', 'workflow');
    fixture.componentRef.setInput('sessionToken', 'token');
    fixture.componentRef.setInput(
        'symbology',
        RasterSymbology.fromRasterSymbologyDict({
            type: 'raster',
            opacity: 1,
            rasterColorizer: {
                type: 'singleBand',
                band: 0,
                bandColorizer: {
                    type: 'linearGradient',
                    breakpoints: [
                        {value: 0, color: [0, 0, 0, 255]},
                        {value: 1, color: [255, 255, 255, 255]},
                    ],
                    underColor: [0, 0, 0, 255],
                    overColor: [255, 255, 255, 255],
                    noDataColor: [0, 0, 0, 0],
                },
            },
        }),
    );
    fixture.detectChanges();
});

afterEach(() => {
    fixture.destroy();
    TestBed.resetTestingModule();
    vi.useRealTimers();
    vi.unstubAllGlobals();
});

it('loads WMS during pans, cancels on zoom, and keeps the shared gate through pinch snapping', async () => {
    view.beginInteraction();
    view.setCenter([100, 100]);
    loadFrame();
    expect(signals.length).toBeGreaterThan(0);
    const initialCount = signals.length;
    view.setZoom(5.4);
    expect(signals.every((signal) => signal.aborted)).toBe(true);
    expect(loadFrame().getTilesLoading()).toBe(0);
    await vi.advanceTimersByTimeAsync(1000);
    view.endInteraction(400, -1);
    loadFrame();
    expect(signals).toHaveLength(initialCount);
    await vi.advanceTimersByTimeAsync(799);
    loadFrame();
    expect(signals).toHaveLength(initialCount);
    await vi.advanceTimersByTimeAsync(50);
    loadFrame();
    expect(signals.length).toBeGreaterThan(initialCount);
    expect(signals.at(-1)?.aborted).toBe(false);
});

it('preserves the WMS gate when the source is replaced during zoom', async () => {
    view.setZoom(6);
    fixture.componentRef.setInput('workflow', 'another-workflow');
    fixture.detectChanges();
    loadFrame();
    expect(signals).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(399);
    loadFrame();
    expect(signals).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(1);
    loadFrame();
    expect(signals.length).toBeGreaterThan(0);
});

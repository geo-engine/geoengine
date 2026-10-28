import {ComponentFixture, TestBed} from '@angular/core/testing';
import {FrameState} from 'ol/Map';
import {get as getProjection} from 'ol/proj';
import OGCMapTile from 'ol/source/OGCMapTile';
import Tile from 'ol/Tile';
import TileQueue, {getTilePriority} from 'ol/TileQueue';
import TileState from 'ol/TileState';
import View from 'ol/View';
import {NEVER} from 'rxjs';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {SpatialReference, Time} from '@geoengine/common';
import {BackendService} from '../backend/backend.service';
import {CoreConfig} from '../config.service';
import {OlOgcApiMapTileLayerComponent} from './map-layer.component';
import {MapService} from './map.service';
import {ProjectService} from '../project/project.service';

/** Exercise the native renderer's protected queue-building step without drawing a canvas. */
interface TileEnqueuer {
    enqueueTiles(frame: FrameState, extent: number[], initialZ: number, tilesByZ: Record<number, Set<Tile>>, preload: number): void;
}

const resolutions = [16000, 8000, 4000, 2000, 1000, 500];
const tileset = {
    dataType: 'map',
    links: [{rel: 'item', type: 'image/png', href: 'https://tiles.test/{tileMatrix}/{tileRow}/{tileCol}.png'}],
    tileMatrixSet: {
        crs: 'http://www.opengis.net/def/crs/EPSG/0/3857',
        tileMatrices: resolutions.map((cellSize, index) => ({
            id: String(index),
            cellSize,
            pointOfOrigin: [0, 0],
            tileWidth: 512,
            tileHeight: 512,
            matrixWidth: 64,
            matrixHeight: 64,
        })),
    },
};

describe('OGC map tile minimum matrix index', () => {
    let fixture: ComponentFixture<OlOgcApiMapTileLayerComponent>;
    let mapService: MapService;
    let view: View;
    let source: OGCMapTile;
    let tileRequests: AbortSignal[];
    let deferMetadata: boolean;
    let pendingMetadata: Array<() => void>;
    const zoomRequired = vi.fn();

    function createLayer(): void {
        fixture = TestBed.createComponent(OlOgcApiMapTileLayerComponent);
        for (const [key, value] of Object.entries({
            layerId: 0,
            dataConnectorId: 'provider',
            dataLayerId: 'layer',
            sessionToken: 'token',
            spatialReference: new SpatialReference('EPSG:3857'),
            time: new Time(new Date('2026-04-01T00:00:00Z')),
            tmsId: 'CustomWebMercator',
            minTileMatrixIndex: 4,
        })) {
            fixture.componentRef.setInput(key, value);
        }
        fixture.componentInstance.zoomRequired.subscribe(zoomRequired);
        fixture.detectChanges();
    }

    function enqueueTiles(matrixIndex: number, preload = 0): {queue: TileQueue; tilesByZ: Record<number, Set<Tile>>} {
        const frame = {viewState: view.getState(), pixelRatio: 1, wantedTiles: {}} as FrameState;
        const queue = new TileQueue(
            (tile, sourceKey, center, resolution): number => getTilePriority(frame, tile, sourceKey, center, resolution),
            (): void => {
                // These tests drain the queue explicitly.
            },
        );
        frame.tileQueue = queue;
        const tilesByZ: Record<number, Set<Tile>> = {};
        const renderer = fixture.componentInstance.mapLayer.getRenderer() as unknown as TileEnqueuer;
        renderer.enqueueTiles(frame, [0, -512000, 512000, 0], matrixIndex, tilesByZ, preload);
        return {queue, tilesByZ};
    }

    beforeEach(async () => {
        zoomRequired.mockReset();
        tileRequests = [];
        deferMetadata = false;
        pendingMetadata = [];
        // OpenLayers reads the metadata object URL with XHR; retain its real grid parsing.
        vi.stubGlobal(
            'XMLHttpRequest',
            class extends EventTarget {
                readonly status = 200;
                readonly responseText = JSON.stringify(tileset);
                open(): void {
                    // Metadata is provided locally.
                }
                setRequestHeader(): void {
                    // Metadata is provided locally.
                }
                send(): void {
                    const complete = (): void => {
                        this.dispatchEvent(new Event('load'));
                    };
                    if (deferMetadata) pendingMetadata.push(complete);
                    else queueMicrotask(complete);
                }
            },
        );
        vi.stubGlobal(
            'fetch',
            vi.fn((url: string, init: RequestInit) => {
                if (!url.endsWith('.png')) return Promise.resolve({ok: true, json: (): Promise<unknown> => Promise.resolve(tileset)});
                tileRequests.push(init.signal!);
                return new Promise((_resolve, reject) => {
                    init.signal!.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')));
                });
            }),
        );
        vi.spyOn(URL, 'createObjectURL').mockReturnValue('https://tiles.test/metadata');
        vi.spyOn(URL, 'revokeObjectURL').mockImplementation(() => undefined);
        await TestBed.configureTestingModule({
            imports: [OlOgcApiMapTileLayerComponent],
            providers: [
                MapService,
                {provide: CoreConfig, useValue: {MAP: {DEBUG_TILES: false}}},
                {provide: BackendService, useValue: {ogcApiBaseUrl: 'https://tiles.test/ogc'}},
                {provide: ProjectService, useValue: {createQueryAbortStream: (): typeof NEVER => NEVER}},
            ],
        }).compileComponents();
        mapService = TestBed.inject(MapService);
        view = new View({center: [1000, -1000], resolution: 2000});
        mapService.setView(view);
        createLayer();
        await fixture.whenStable();
        source = fixture.componentInstance.mapLayer.getSource() as OGCMapTile;
        expect(source.getState()).toBe('ready');
    });

    afterEach(() => {
        fixture.destroy();
        vi.restoreAllMocks();
        vi.unstubAllGlobals();
    });

    it.each([
        {resolution: 1000, blocked: false},
        {resolution: 2000, blocked: true},
    ])('waits for metadata before deciding whether resolution $resolution requires zooming in', async ({resolution, blocked}) => {
        fixture.destroy();
        zoomRequired.mockReset();
        view.setResolution(resolution);
        deferMetadata = true;
        createLayer();
        await fixture.whenStable();
        source = fixture.componentInstance.mapLayer.getSource() as OGCMapTile;
        expect(source.getState()).toBe('loading');
        expect(fixture.componentInstance.mapLayer.getMaxResolution()).toBe(0);
        expect(zoomRequired).toHaveBeenLastCalledWith(false);
        expect(zoomRequired).not.toHaveBeenCalledWith(true);
        expect(tileRequests).toHaveLength(0);

        for (const complete of pendingMetadata) complete();
        await vi.waitFor(() => expect(source.getState()).toBe('ready'));
        expect(zoomRequired).toHaveBeenLastCalledWith(blocked);
        expect(fixture.componentInstance.mapLayer.isVisible(view)).toBe(!blocked);
        expect(tileRequests).toHaveLength(0);

        view.setResolution(blocked ? 1000 : 2000);
        expect(zoomRequired).toHaveBeenLastCalledWith(!blocked);
        expect(fixture.componentInstance.mapLayer.isVisible(view)).toBe(blocked);
    });

    it('uses the actual source index and keeps metadata available without loading imagery below it', () => {
        expect(view.getZoom()).toBeGreaterThan(4);
        expect(fixture.componentInstance.mapLayer.getVisible()).toBe(true);
        expect(fixture.componentInstance.mapLayer.isVisible(view)).toBe(false);
        expect(zoomRequired).toHaveBeenLastCalledWith(true);
        expect(tileRequests).toHaveLength(0);
    });

    it('uses the native exclusive midpoint cutoff and excludes coarser source tiles', () => {
        view.setResolution(1500.01);
        expect(zoomRequired).toHaveBeenLastCalledWith(true);
        view.setResolution(1500);
        expect(zoomRequired).toHaveBeenLastCalledWith(true);
        expect(fixture.componentInstance.mapLayer.isVisible(view)).toBe(false);
        view.setResolution(1499.99);
        expect(zoomRequired).toHaveBeenLastCalledWith(false);
        expect(fixture.componentInstance.mapLayer.isVisible(view)).toBe(true);
        const tile = source.getTile(3, 0, 0, 1, getProjection('EPSG:3857')!);
        tile.load();
        expect(tile.getState()).toBe(TileState.EMPTY);
        expect(tileRequests).toHaveLength(0);
    });

    it('keeps native preloading within the minimum matrix', () => {
        view.setResolution(1499.99);
        const {queue, tilesByZ} = enqueueTiles(4, 1);
        expect(fixture.componentInstance.mapLayer.isVisible(view)).toBe(true);
        expect(tilesByZ[3]).toBeUndefined();

        queue.loadMoreTiles(16, 16);

        expect(tileRequests).toHaveLength(1);
    });

    it('blocks an earlier frame queue after the view crosses maxResolution', () => {
        view.setResolution(1000);
        const {queue, tilesByZ} = enqueueTiles(4);
        expect(queue.getCount()).toBeGreaterThan(0);
        view.setResolution(2000);
        expect(fixture.componentInstance.mapLayer.isVisible(view)).toBe(false);

        // The queued frame's wantedTiles still includes these tiles even though
        // the live view is outside maxResolution now.
        queue.reprioritize();
        expect(queue.getCount()).toBeGreaterThan(0);
        queue.loadMoreTiles(16, 16);

        expect(tileRequests).toHaveLength(0);
        for (const tile of tilesByZ[4]) expect(tile.getState()).toBe(TileState.IDLE);
    });

    it('cancels in-flight imagery on zoom-out and resumes the same tile on zoom-in', async () => {
        view.setResolution(1000);
        const tile = source.getTile(4, 0, 0, 1, getProjection('EPSG:3857')!);
        tile.load();
        expect(tileRequests).toHaveLength(1);
        view.setResolution(2000);
        expect(tileRequests[0].aborted).toBe(true);
        expect(fixture.componentInstance.mapLayer.isVisible(view)).toBe(false);
        await vi.waitFor(() => expect(tile.getState()).toBe(TileState.IDLE));
        view.setResolution(1000);
        tile.load();
        expect(tileRequests).toHaveLength(2);
        expect(tileRequests[1].aborted).toBe(false);
    });

    it('cancels imagery when hidden and rejects queued tiles until shown again', async () => {
        view.setResolution(1000);
        const tile = source.getTile(4, 0, 0, 1, getProjection('EPSG:3857')!);
        tile.load();
        fixture.componentRef.setInput('isVisible', false);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(tileRequests[0].aborted).toBe(true);
        await vi.waitFor(() => expect(tile.getState()).toBe(TileState.IDLE));
        tile.load();
        expect(tile.getState()).toBe(TileState.IDLE);
        expect(tileRequests).toHaveLength(1);
        fixture.componentRef.setInput('isVisible', true);
        fixture.detectChanges();
        await fixture.whenStable();
        tile.load();
        expect(tileRequests).toHaveLength(2);
    });

    it('tracks replacement views and ignores changes to the previous view', async () => {
        const replacement = new View({center: [1000, -1000], resolution: 1000});
        mapService.setView(replacement);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(zoomRequired).toHaveBeenLastCalledWith(false);
        view.setResolution(4000);
        expect(zoomRequired).toHaveBeenLastCalledWith(false);
        replacement.setResolution(2000);
        expect(zoomRequired).toHaveBeenLastCalledWith(true);
    });

    it('compares source resolution when the view uses a different projection', async () => {
        const replacement = new View({projection: 'EPSG:4326', center: [0, 0], resolution: 1000 / 111319.49079327358});
        mapService.setView(replacement);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(zoomRequired).toHaveBeenLastCalledWith(false);
        replacement.setResolution(2000 / 111319.49079327358);
        expect(zoomRequired).toHaveBeenLastCalledWith(true);
    });

    it('waits for a defined view and removes zoom listeners on destruction', async () => {
        const replacement = new View();
        mapService.setView(replacement);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(zoomRequired).toHaveBeenLastCalledWith(true);
        replacement.setCenter([1000, -1000]);
        replacement.setResolution(1000);
        expect(zoomRequired).toHaveBeenLastCalledWith(false);
        fixture.destroy();
        const calls = zoomRequired.mock.calls.length;
        replacement.setResolution(4000);
        expect(zoomRequired).toHaveBeenCalledTimes(calls);
    });

    it('leaves sources without a zoom limit unrestricted', async () => {
        fixture.componentRef.setInput('minTileMatrixIndex', 0);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(zoomRequired).toHaveBeenLastCalledWith(false);
        expect(fixture.componentInstance.mapLayer.getMaxResolution()).toBe(Infinity);
        expect(fixture.componentInstance.mapLayer.isVisible(view)).toBe(true);
        source = fixture.componentInstance.mapLayer.getSource() as OGCMapTile;
        source.getTile(3, 0, 0, 1, getProjection('EPSG:3857')!).load();
        expect(tileRequests).toHaveLength(1);
    });

    it('hides a layer whose minimum matrix is outside the tileset', async () => {
        fixture.componentRef.setInput('minTileMatrixIndex', resolutions.length);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(fixture.componentInstance.mapLayer.getMaxResolution()).toBe(0);
        expect(fixture.componentInstance.mapLayer.isVisible(view)).toBe(false);
        expect(zoomRequired).toHaveBeenLastCalledWith(true);
        expect(tileRequests).toHaveLength(0);
    });
});

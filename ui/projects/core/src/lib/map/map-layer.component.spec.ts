import {ComponentFixture, TestBed} from '@angular/core/testing';
import {FrameState} from 'ol/Map';
import {fromLonLat, get as getProjection} from 'ol/proj';
import ReprojTile from 'ol/reproj/Tile';
import OGCMapTile from 'ol/source/OGCMapTile';
import Tile from 'ol/Tile';
import TileQueue, {getTilePriority} from 'ol/TileQueue';
import TileState from 'ol/TileState';
import View from 'ol/View';
import {NEVER} from 'rxjs';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {SpatialReference, Time} from '@geoengine/common';
import {BackendService} from '../backend/backend.service';
import {CoreConfig, DEFAULT_CORE_CONFIG} from '../config.service';
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

    function publishViewport(currentView: View): void {
        mapService.setViewportSize({extent: [0, -512000, 512000, 0], resolution: currentView.getResolution() ?? Infinity});
    }

    async function setResolution(resolution: number, currentView: View = view): Promise<void> {
        currentView.setResolution(resolution);
        if (currentView === mapService.view()) publishViewport(currentView);
        if (!fixture.componentRef.hostView.destroyed) {
            fixture.detectChanges();
            await fixture.whenStable();
        }
    }

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
        publishViewport(view);
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
        {resolution: 999, blocked: false},
        {resolution: 2000, blocked: true},
    ])('waits for metadata before deciding whether resolution $resolution requires zooming in', async ({resolution, blocked}) => {
        fixture.destroy();
        zoomRequired.mockReset();
        await setResolution(resolution);
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

        await setResolution(blocked ? 999 : 2000);
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

    it('uses the source matrix resolution as the exclusive cutoff and excludes coarser source tiles', async () => {
        await setResolution(1000.01);
        expect(zoomRequired).toHaveBeenLastCalledWith(true);
        await setResolution(1000);
        expect(zoomRequired).toHaveBeenLastCalledWith(true);
        expect(fixture.componentInstance.mapLayer.isVisible(view)).toBe(false);
        await setResolution(999.99);
        expect(zoomRequired).toHaveBeenLastCalledWith(false);
        expect(fixture.componentInstance.mapLayer.isVisible(view)).toBe(true);
        const tile = source.getTile(3, 0, 0, 1, getProjection('EPSG:3857')!);
        tile.load();
        expect(tile.getState()).toBe(TileState.EMPTY);
        expect(tileRequests).toHaveLength(0);
    });

    it('keeps native preloading within the minimum matrix', async () => {
        await setResolution(999.99);
        const {queue, tilesByZ} = enqueueTiles(4, 1);
        expect(fixture.componentInstance.mapLayer.isVisible(view)).toBe(true);
        expect(tilesByZ[3]).toBeUndefined();

        queue.loadMoreTiles(16, 16);

        expect(tileRequests).toHaveLength(1);
    });

    it('blocks an earlier frame queue after the view crosses maxResolution', async () => {
        await setResolution(999);
        const {queue, tilesByZ} = enqueueTiles(4);
        expect(queue.getCount()).toBeGreaterThan(0);
        await setResolution(2000);
        expect(fixture.componentInstance.mapLayer.isVisible(view)).toBe(false);

        // The queued frame's wantedTiles still includes these tiles even though
        // the live view is outside maxResolution now.
        queue.reprioritize();
        expect(queue.getCount()).toBeGreaterThan(0);
        queue.loadMoreTiles(16, 16);

        expect(tileRequests).toHaveLength(0);
        for (const tile of tilesByZ[4]) expect(tile.getState()).toBe(TileState.IDLE);
    });

    it('rejects queued tiles during zoom-out before the viewport stream updates', async () => {
        await setResolution(999);
        const {queue, tilesByZ} = enqueueTiles(4);
        view.setResolution(2000);
        queue.loadMoreTiles(16, 16);
        expect(tileRequests).toHaveLength(0);
        for (const tile of tilesByZ[4]) expect(tile.getState()).toBe(TileState.IDLE);
        expect(zoomRequired).toHaveBeenLastCalledWith(false);
        await setResolution(2000);
        expect(zoomRequired).toHaveBeenLastCalledWith(true);
    });

    it('cancels in-flight imagery on zoom-out and resumes the same tile on zoom-in', async () => {
        await setResolution(999);
        const tile = source.getTile(4, 0, 0, 1, getProjection('EPSG:3857')!);
        tile.load();
        expect(tileRequests).toHaveLength(1);
        await setResolution(2000);
        expect(tileRequests[0].aborted).toBe(true);
        expect(fixture.componentInstance.mapLayer.isVisible(view)).toBe(false);
        await vi.waitFor(() => expect(tile.getState()).toBe(TileState.IDLE));
        await setResolution(999);
        tile.load();
        expect(tileRequests).toHaveLength(2);
        expect(tileRequests[1].aborted).toBe(false);
    });

    it('cancels imagery when hidden and rejects queued tiles until shown again', async () => {
        await setResolution(999);
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
        const replacement = new View({center: [1000, -1000], resolution: 999});
        mapService.setView(replacement);
        publishViewport(replacement);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(zoomRequired).toHaveBeenLastCalledWith(false);
        await setResolution(4000);
        expect(zoomRequired).toHaveBeenLastCalledWith(false);
        await setResolution(2000, replacement);
        expect(zoomRequired).toHaveBeenLastCalledWith(true);
    });

    it('sets the resolution limit once for the grid rather than recalculating on view changes', async () => {
        const getResolution = vi.spyOn(source.getTileGrid()!, 'getResolution');
        expect(fixture.componentInstance.mapLayer.getMaxResolution()).toBe(resolutions[4]);
        await setResolution(999);
        view.setCenter([5000, -5000]);
        await setResolution(2000);
        expect(getResolution).not.toHaveBeenCalled();
        expect(fixture.componentInstance.mapLayer.getMaxResolution()).toBe(resolutions[4]);
    });

    it('waits for a defined view and removes zoom listeners on destruction', async () => {
        const replacement = new View();
        mapService.setView(replacement);
        publishViewport(replacement);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(zoomRequired).toHaveBeenLastCalledWith(true);
        replacement.setCenter([1000, -1000]);
        await setResolution(999, replacement);
        expect(zoomRequired).toHaveBeenLastCalledWith(false);
        fixture.destroy();
        const calls = zoomRequired.mock.calls.length;
        await setResolution(4000, replacement);
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

    it('warns and ignores both restrictions for mismatched projections, restoring them when the view matches again', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
        const replacement = new View({projection: 'EPSG:4326', center: [0, 0], resolution: 2});
        mapService.setView(replacement);
        publishViewport(replacement);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(warn).toHaveBeenCalledWith('Ignoring minTileMatrixIndex because the limit grid and view projections differ.');
        expect(fixture.componentInstance.mapLayer.getMaxResolution()).toBe(Infinity);
        expect(zoomRequired).toHaveBeenLastCalledWith(false);
        const tile = source.getTile(3, 0, 0, 1, getProjection('EPSG:3857')!);
        tile.load();
        expect(tileRequests).toHaveLength(1);
        await setResolution(4, replacement);
        expect(warn).toHaveBeenCalledTimes(1);

        mapService.setView(view);
        publishViewport(view);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(fixture.componentInstance.mapLayer.getMaxResolution()).toBe(resolutions[4]);
        expect(zoomRequired).toHaveBeenLastCalledWith(true);
        expect(tileRequests[0].aborted).toBe(true);
        await setResolution(999);
        const excluded = source.getTile(3, 1, 0, 1, getProjection('EPSG:3857')!);
        excluded.load();
        expect(excluded.getState()).toBe(TileState.EMPTY);
        expect(tileRequests).toHaveLength(1);
    });

    it('accepts equivalent projection aliases without ignoring the limit', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
        const replacement = new View({projection: 'EPSG:900913', center: [0, 0], resolution: 2000});
        mapService.setView(replacement);
        publishViewport(replacement);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(warn).not.toHaveBeenCalled();
        expect(fixture.componentInstance.mapLayer.getMaxResolution()).toBe(resolutions[4]);
        expect(zoomRequired).toHaveBeenLastCalledWith(true);
    });

    it.each([resolutions.length, -1, 1.5])('warns and blocks an invalid matrix index %s without asking the user to zoom', async (index) => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
        fixture.componentRef.setInput('minTileMatrixIndex', index);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(warn).toHaveBeenCalledWith(expect.stringContaining(`Invalid minTileMatrixIndex ${index}:`));
        expect(fixture.componentInstance.mapLayer.getMaxResolution()).toBe(0);
        expect(fixture.componentInstance.mapLayer.isVisible(view)).toBe(false);
        expect(zoomRequired).toHaveBeenLastCalledWith(false);
        await setResolution(1);
        expect(zoomRequired).toHaveBeenLastCalledWith(false);
        source = fixture.componentInstance.mapLayer.getSource() as OGCMapTile;
        source.getTile(4, 0, 0, 1, getProjection('EPSG:3857')!).load();
        expect(tileRequests).toHaveLength(0);
    });
});

describe('OGC map tile projection switching', () => {
    let fixture: ComponentFixture<OlOgcApiMapTileLayerComponent>;
    let nativeEpsg: number;
    let nativeMatrixCount: number;
    let tileRequests: AbortSignal[];
    let revokeObjectUrl: ReturnType<typeof vi.spyOn>;
    let fetchMock: ReturnType<typeof vi.fn>;
    const baseUrl = 'https://example.com/ogc/data/layer';

    beforeEach(() => {
        nativeEpsg = 4326;
        nativeMatrixCount = 1;
        tileRequests = [];
        const documents = new Map<string, Blob>();
        vi.spyOn(URL, 'createObjectURL').mockImplementation((blob) => {
            const url = `blob:metadata-${documents.size}`;
            documents.set(url, blob as Blob);
            return url;
        });
        revokeObjectUrl = vi.spyOn(URL, 'revokeObjectURL').mockImplementation(() => undefined);
        // OpenLayers reads the authenticated object URLs with XMLHttpRequest, separately from fetch.
        vi.stubGlobal(
            'XMLHttpRequest',
            class extends EventTarget {
                readonly status = 200;
                responseText = '';
                private url = '';
                open(_method: string, url: string): void {
                    this.url = url;
                }
                setRequestHeader(): void {
                    /* Object URL reads need no headers. */
                }
                send(): void {
                    void documents
                        .get(this.url)!
                        .text()
                        .then((text) => {
                            this.responseText = text;
                            this.dispatchEvent(new Event('load'));
                        });
                }
            },
        );
        fetchMock = vi.fn((url: string, init: RequestInit) => {
            if (url.includes('?datetime=')) {
                tileRequests.push(init.signal!);
                return new Promise((_resolve, reject) => {
                    init.signal!.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')));
                });
            }
            const tms = url.endsWith('/CustomWebMercator') ? 'CustomWebMercator' : 'Custom';
            const epsg = tms === 'CustomWebMercator' ? 3857 : nativeEpsg;
            const crs = `http://www.opengis.net/def/crs/EPSG/0/${epsg}`;
            const metadata = url.includes('/tileMatrixSets/')
                ? {
                      id: tms,
                      crs,
                      orderedAxes: epsg === 4326 ? ['Lon', 'Lat'] : ['X', 'Y'],
                      tileMatrices: Array.from({length: tms === 'CustomWebMercator' ? 6 : nativeMatrixCount}, (_, index) => ({
                          id: String(index),
                          cellSize:
                              (epsg === 4326 ? 360 / 256 : epsg === 3857 ? 40075016.68557849 / (tms === 'Custom' ? 512 : 256) : 10000) /
                              2 ** index,
                          pointOfOrigin:
                              epsg === 4326 ? [-180, 90] : epsg === 3857 ? [-20037508.342789244, 20037508.342789244] : [-1000000, 11000000],
                          tileWidth: 256,
                          tileHeight: 256,
                          matrixWidth: 4 * 2 ** index,
                          matrixHeight: 4 * 2 ** index,
                      })),
                  }
                : {
                      dataType: 'map',
                      crs,
                      links: [
                          {rel: 'http://www.opengis.net/def/rel/ogc/1.0/tiling-scheme', href: `${baseUrl}/tileMatrixSets/${tms}`},
                          {
                              rel: 'item',
                              type: 'image/png',
                              href: `${baseUrl}/collections/layer/map/tiles/${tms}/{tileMatrix}/{tileRow}/{tileCol}?datetime={datetime}`,
                          },
                      ],
                  };
            return Promise.resolve({ok: true, json: () => Promise.resolve(metadata)});
        });
        vi.stubGlobal('fetch', fetchMock);
        TestBed.configureTestingModule({
            imports: [OlOgcApiMapTileLayerComponent],
            providers: [
                MapService,
                {provide: BackendService, useValue: {ogcApiBaseUrl: 'https://example.com/ogc'}},
                {provide: CoreConfig, useValue: DEFAULT_CORE_CONFIG},
                {provide: ProjectService, useValue: {createQueryAbortStream: vi.fn().mockReturnValue(NEVER)}},
            ],
        });
        fixture = TestBed.createComponent(OlOgcApiMapTileLayerComponent);
        fixture.componentRef.setInput('layerId', 0);
        fixture.componentRef.setInput('dataConnectorId', 'data');
        fixture.componentRef.setInput('dataLayerId', 'layer');
        fixture.componentRef.setInput('sessionToken', 'session');
        fixture.componentRef.setInput('spatialReference', new SpatialReference('EPSG:3857'));
        fixture.componentRef.setInput('time', new Time(new Date('2026-04-01T00:00:00Z')));
        fixture.componentRef.setInput('tmsId', 'CustomWebMercator');
    });

    afterEach(() => {
        fixture.destroy();
        vi.restoreAllMocks();
        vi.unstubAllGlobals();
    });

    async function readySource(): Promise<NonNullable<ReturnType<OlOgcApiMapTileLayerComponent['tileSource']['value']>>> {
        fixture.detectChanges();
        await vi.waitFor(() => {
            fixture.detectChanges();
            expect(fixture.componentInstance.tileSource.value()?.getState()).toBe('ready');
        });
        return fixture.componentInstance.tileSource.value()!;
    }

    it.each([4326, 32632, 32755, 3857].flatMap((epsg) => [1, 6].map((matrices) => ({epsg, matrices}))))(
        'keeps the Web Mercator cutoff for EPSG:$epsg imagery with $matrices native matrices',
        async ({epsg, matrices}) => {
            nativeEpsg = epsg;
            nativeMatrixCount = matrices;
            const mapService = TestBed.inject(MapService);
            const cutoff = 40075016.68557849 / 256 / 2 ** 4;
            const view = new View({center: fromLonLat(epsg === 32755 ? [147, -30] : [9, 50]), resolution: cutoff * 2});
            mapService.setView(view);
            const zoomRequired = vi.fn();
            fixture.componentInstance.zoomRequired.subscribe(zoomRequired);
            fixture.componentRef.setInput('minTileMatrixIndex', 4);
            fixture.componentRef.setInput('minTileMatrixSetId', 'CustomWebMercator');

            async function setResolution(resolution: number): Promise<void> {
                view.setResolution(resolution);
                mapService.setViewportSize({extent: [0, 0, 1, 1], resolution});
                fixture.detectChanges();
                await fixture.whenStable();
            }

            await setResolution(cutoff * 2);
            await readySource();
            expect(fixture.componentInstance.mapLayer.getMaxResolution()).toBe(cutoff);
            expect(zoomRequired).toHaveBeenLastCalledWith(true);
            fixture.componentRef.setInput('tmsId', 'Custom');
            const nativeSource = await readySource();
            expect(nativeSource.getTileGrid()!.getMaxZoom()).toBe(matrices - 1);
            expect(fixture.componentInstance.mapLayer.getMaxResolution()).toBe(cutoff);
            expect(zoomRequired).toHaveBeenLastCalledWith(true);

            const tile = nativeSource.getTile(0, 0, 0, 1, nativeSource.getProjection()!);
            tile.load();
            expect(tileRequests).toHaveLength(0);
            await setResolution(cutoff);
            expect(fixture.componentInstance.mapLayer.isVisible(view)).toBe(false);
            await setResolution(cutoff / 2);
            expect(fixture.componentInstance.mapLayer.isVisible(view)).toBe(true);
            expect(zoomRequired).toHaveBeenLastCalledWith(false);
            tile.load();
            expect(tileRequests).toHaveLength(1);
            expect(tile.getState()).toBe(TileState.LOADING);
            await setResolution(cutoff * 2);
            expect(tileRequests[0].aborted).toBe(true);
            await vi.waitFor(() => expect(tile.getState()).toBe(TileState.IDLE));
            await setResolution(cutoff / 2);
            const projection = view.getProjection();
            const displayGrid = nativeSource.getTileGridForProjection(projection);
            const [z, x, y] = displayGrid.getTileCoordForCoordAndResolution(view.getCenter()!, view.getResolution()!);
            const displayTile = nativeSource.getTile(z, x, y, 1, projection);
            if (epsg !== 3857) expect(displayTile).toBeInstanceOf(ReprojTile);
            displayTile.load();
            expect(tileRequests.length).toBeGreaterThan(1);
            expect(tileRequests[1].aborted).toBe(false);

            fixture.componentRef.setInput('tmsId', 'CustomWebMercator');
            await readySource();
            expect(tileRequests[1].aborted).toBe(true);
            expect(fixture.componentInstance.mapLayer.getMaxResolution()).toBe(cutoff);
            await setResolution(cutoff * 2);
            expect(zoomRequired).toHaveBeenLastCalledWith(true);
        },
    );

    it.each([4326, 32632, 32633, 32755])('reloads native EPSG:%s tiles for OpenLayers reprojection and switches back', async (epsg) => {
        nativeEpsg = epsg;
        const backendSource = await readySource();
        expect(backendSource.getProjection()?.getCode()).toContain('3857');
        const metadataRequest = fetchMock.mock.calls[0] as [string, RequestInit];
        expect(metadataRequest[0]).toBe(`${baseUrl}/collections/layer/map/tiles/CustomWebMercator`);
        expect(metadataRequest[1].headers).toEqual({Authorization: 'Bearer session'});
        const previousSignal = metadataRequest[1].signal!;

        fixture.componentRef.setInput('tmsId', 'Custom');
        const nativeSource = await readySource();
        expect(nativeSource).not.toBe(backendSource);
        expect(previousSignal.aborted).toBe(true);
        expect(revokeObjectUrl).toHaveBeenCalled();
        expect(nativeSource.getProjection()?.getCode()).toContain(String(epsg));
        const url = nativeSource.getTileUrlFunction()([0, 0, 0], 1, nativeSource.getProjection()!);
        expect(url).toContain('/map/tiles/Custom/0/0/0?datetime=2026-04-01');
        const viewProjection = getProjection('EPSG:3857')!;
        const center = fromLonLat(epsg === 32755 ? [147, -30] : [9, 50]);
        const [z, x, y] = nativeSource.getTileGridForProjection(viewProjection).getTileCoordForCoordAndZ(center, 5);
        const displayTile = nativeSource.getTile(z, x, y, 1, viewProjection);
        expect(displayTile).toBeInstanceOf(ReprojTile);
        expect(displayTile.getState()).not.toBe(TileState.EMPTY);

        fixture.componentRef.setInput('tmsId', 'CustomWebMercator');
        const restoredSource = await readySource();
        expect(restoredSource).not.toBe(nativeSource);
        expect(restoredSource.getProjection()?.getCode()).toContain('3857');
    });
});

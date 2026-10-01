import {ComponentFixture, TestBed} from '@angular/core/testing';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import {BehaviorSubject, Observable, Subject, filter, skip, take} from 'rxjs';
import {ImageTile} from 'ol';
import TileQueue from 'ol/TileQueue';
import TileState from 'ol/TileState';
import {get as getProjection, transformExtent} from 'ol/proj';
import TileGrid from 'ol/tilegrid/TileGrid';
import {FrameState} from 'ol/Map';
import {intersects} from 'ol/extent';
import TileImageSource from 'ol/source/TileImage';
import Tile from 'ol/Tile';
import LRUCache from 'ol/structs/LRUCache';
import {NotificationService, RasterData, SpatialReference, Time} from '@geoengine/common';
import {OlOgcApiMapTileLayerComponent, OlRasterLayerComponent} from './map-layer.component';
import {ProjectService} from '../project/project.service';
import {BackendService} from '../backend/backend.service';
import {CoreConfig} from '../config.service';
import {Extent} from './map.service';

describe.each(['OGC', 'WMS'] as const)('%s tile cancellation while panning', (kind) => {
    let fixture: ComponentFixture<OlOgcApiMapTileLayerComponent | OlRasterLayerComponent>;
    let source: TileImageSource;
    let viewport$: BehaviorSubject<Extent>;
    let requests: Array<{signal: AbortSignal; complete: () => void}>;
    const projection = getProjection('EPSG:3857')!;
    const png = (): Blob =>
        new Blob(
            [
                Uint8Array.from(atob('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aS1cAAAAASUVORK5CYII='), (c) =>
                    c.charCodeAt(0),
                ),
            ],
            {type: 'image/png'},
        );

    beforeEach(async () => {
        if (!HTMLImageElement.prototype.decode) {
            // jsdom needs a canvas stub; Chromium runs exercise native reprojection rendering.
            vi.spyOn(HTMLCanvasElement.prototype, 'getContext').mockImplementation(function (this: HTMLCanvasElement) {
                const noop = vi.fn();
                return {
                    canvas: this,
                    scale: noop,
                    save: noop,
                    restore: noop,
                    beginPath: noop,
                    moveTo: noop,
                    lineTo: noop,
                    closePath: noop,
                    clip: noop,
                    fillRect: noop,
                    clearRect: noop,
                    rect: noop,
                    drawImage: noop,
                    transform: noop,
                    translate: noop,
                    getImageData: () => ({data: new Uint8ClampedArray(36)}),
                } as unknown as CanvasRenderingContext2D;
            });
        }
        viewport$ = new BehaviorSubject<Extent>([0, 0, 256, 256]);
        requests = [];
        const rasterData$ = new Subject<RasterData>();
        TestBed.configureTestingModule({
            imports: [OlOgcApiMapTileLayerComponent, OlRasterLayerComponent],
            providers: [
                {
                    provide: ProjectService,
                    useValue: {
                        createQueryAbortStream: (_id: number, extent: Extent): Observable<Extent> =>
                            viewport$.pipe(
                                skip(1),
                                filter((viewport) => !intersects(extent, viewport)),
                                take(1),
                            ),
                        changeRasterLayerDataStatus: vi.fn(),
                        getLayerDataStream: (): Observable<RasterData> => rasterData$,
                    },
                },
                {provide: BackendService, useValue: {wmsBaseUrl: 'https://tiles.test/wms'}},
                {provide: CoreConfig, useValue: {MAP: {REFRESH_LAYERS_ON_CHANGE: false}}},
                {provide: NotificationService, useValue: {error: vi.fn()}},
            ],
        });

        // Only metadata and the network are mocked. Tiles and the load queue are real OpenLayers objects.
        const metadata = {
            dataType: 'map',
            links: [{rel: 'item', type: 'image/png', href: 'https://tiles.test/{tileMatrix}/{tileRow}/{tileCol}'}],
            tileMatrixSet: {
                crs: 'EPSG:3857',
                tileMatrices: [
                    {id: '0', cellSize: 1, pointOfOrigin: [0, 256], matrixWidth: 4, matrixHeight: 1, tileWidth: 256, tileHeight: 256},
                ],
            },
        };
        vi.stubGlobal(
            'XMLHttpRequest',
            class extends EventTarget {
                status = 200;
                responseText = JSON.stringify(metadata);
                response: Blob | null = null;
                private controller = new AbortController();
                open(): void {
                    // The mock only needs request events.
                }
                setRequestHeader(): void {
                    // Authentication is not needed for the mock network.
                }
                send(): void {
                    if (kind === 'OGC') {
                        queueMicrotask(() => this.dispatchEvent(new Event('load')));
                        return;
                    }
                    requests.push({
                        signal: this.controller.signal,
                        complete: () => {
                            this.response = png();
                            this.dispatchEvent(new Event('loadend'));
                        },
                    });
                }
                abort(): void {
                    this.controller.abort();
                    this.dispatchEvent(new Event('abort'));
                    this.dispatchEvent(new Event('loadend'));
                }
            },
        );
        vi.stubGlobal(
            'fetch',
            vi.fn(
                (_url: string, options: RequestInit) =>
                    new Promise<Response>((resolve, reject) => {
                        const signal = options.signal!;
                        signal.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')), {once: true});
                        requests.push({signal, complete: () => resolve(new Response(png()))});
                    }),
            ),
        );

        if (kind === 'OGC') {
            const ogcFixture = TestBed.createComponent(OlOgcApiMapTileLayerComponent);
            fixture = ogcFixture;
            vi.spyOn(ogcFixture.componentInstance, 'tmsBlobUrl').mockResolvedValue('https://tiles.test/metadata');
            fixture.componentRef.setInput('dataConnectorId', 'connector');
            fixture.componentRef.setInput('dataLayerId', 'layer');
            fixture.componentRef.setInput('spatialReference', new SpatialReference('EPSG:3857'));
            fixture.componentRef.setInput('time', new Time('2026-01-01'));
        } else {
            fixture = TestBed.createComponent(OlRasterLayerComponent);
            fixture.componentRef.setInput('workflow', 'workflow');
            fixture.componentRef.setInput('symbology', {opacity: 1, rasterColorizer: {toDict: (): object => ({})}});
        }
        fixture.componentRef.setInput('layerId', 0);
        fixture.componentRef.setInput('sessionToken', 'session');
        fixture.detectChanges();
        const component = fixture.componentInstance;
        if (component instanceof OlOgcApiMapTileLayerComponent) {
            await vi.waitFor(() => expect(component.tileSource.value()?.source.getState()).toBe('ready'));
            source = component.tileSource.value()!.source;
        } else {
            rasterData$.next(new RasterData(new Time('2026-01-01'), new SpatialReference('EPSG:3857'), 'workflow'));
            source = component.mapLayer.getSource()!;
        }
        fixture.detectChanges();
    });

    afterEach(() => {
        fixture?.destroy();
        vi.restoreAllMocks();
        vi.unstubAllGlobals();
    });

    it('reloads a cancelled tile when panning back without blocking the tile queue', async () => {
        const grid = source.getTileGridForProjection(projection);
        const [z, x, y] = grid.getTileCoordForCoordAndZ([128, 128], grid.getZForResolution(1));
        const tile = source.getTile(z, x, y, 1, projection) as ImageTile;
        const queue = new TileQueue(
            () => 1,
            () => undefined,
        );
        queue.enqueue([tile, 'source', [128, 128], 1]);
        queue.loadMoreTiles(1, 1);
        expect(tile.getState()).toBe(TileState.LOADING);
        expect(requests).toHaveLength(1);

        for (let pan = 0; pan < 3; pan++) {
            viewport$.next([512, 0, 768, 256]);
            expect(requests[pan].signal.aborted).toBe(true);
            await vi.waitFor(() => expect(tile.getState()).toBe(TileState.IDLE));
            expect(queue.getTilesLoading()).toBe(0);

            viewport$.next([0, 0, 256, 256]);
            // OpenLayers' renderer keeps the same tile in its cache and only enqueues IDLE tiles.
            if (tile.getState() === TileState.IDLE) {
                queue.enqueue([tile, 'source', [128, 128], 1]);
            }
            queue.loadMoreTiles(1, 1);
            expect(requests).toHaveLength(pan + 2);
        }
        requests[3].complete();
        await vi.waitFor(() => expect((tile.getImage() as HTMLImageElement).src).toMatch(/^blob:/));
        await finishImage(tile);
        expect(queue.getTilesLoading()).toBe(0);
    });

    it.runIf(kind === 'OGC')('uses the source grid and transforms cancellation extents into the map projection', async () => {
        const component = fixture.componentInstance as OlOgcApiMapTileLayerComponent;
        const previousSource = source;
        fixture.componentRef.setInput('spatialReference', new SpatialReference('EPSG:4326'));
        fixture.detectChanges();
        await vi.waitFor(() => {
            expect(component.tileSource.value()?.source).not.toBe(previousSource);
            expect(component.tileSource.value()?.source.getState()).toBe('ready');
        });
        source = component.tileSource.value()!.source;
        fixture.detectChanges();
        const expectedExtent = transformExtent([0, 0, 256, 256], 'EPSG:3857', 'EPSG:4326');
        viewport$.next(expectedExtent as Extent);
        const abortStream = vi.spyOn(TestBed.inject(ProjectService), 'createQueryAbortStream');
        const tile = source.getTile(0, 0, 0, 1, projection) as ImageTile;
        tile.load();
        expect(abortStream).toHaveBeenCalledWith(0, expectedExtent);
        viewport$.next(transformExtent([512, 0, 768, 256], 'EPSG:3857', 'EPSG:4326') as Extent);
        expect(requests[0].signal.aborted).toBe(true);
        await vi.waitFor(() => expect(tile.getState()).toBe(TileState.IDLE));
    });

    it('reloads a cancelled reprojected tile when panning back', async () => {
        const component = fixture.componentInstance;
        const targetProjection = getProjection('EPSG:4326')!;
        const extent = transformExtent([1, 1, 255, 255], projection, targetProjection);
        source.setTileGridForProjection(
            targetProjection,
            new TileGrid({
                origin: [extent[0], extent[3]],
                resolutions: [(extent[2] - extent[0]) / 256],
                tileSize: 256,
            }),
        );
        const queue = new TileQueue(
            () => 1,
            () => undefined,
        );
        const renderer = component.mapLayer.getRenderer()!;
        const frame = {
            viewState: {projection: targetProjection, rotation: 0},
            pixelRatio: 1,
            tileQueue: queue,
            wantedTiles: {},
        } as FrameState;
        renderer.prepareFrame(frame);
        renderer.enqueueTiles(frame, extent, 0, {}, 0);
        const cache = renderer.getTileCache() as LRUCache<Tile>;
        const originalTile = cache.peek(cache.peekFirstKey())!;
        queue.loadMoreTiles(1, 1);
        expect(requests).toHaveLength(1);

        viewport$.next([512, 0, 768, 256]);
        expect(requests[0].signal.aborted).toBe(true);
        await vi.waitFor(() => expect(queue.getTilesLoading()).toBe(0));

        viewport$.next([0, 0, 256, 256]);
        renderer.prepareFrame(frame);
        renderer.enqueueTiles(frame, extent, 0, {}, 0);
        queue.loadMoreTiles(1, 1);
        expect(requests).toHaveLength(2);
        const replacementTile = cache.peek(cache.peekFirstKey())!;
        expect(replacementTile).not.toBe(originalTile);
        requests[1].complete();
        const sourceCache = renderer.getSourceTileCache() as LRUCache<ImageTile>;
        const sourceTile = sourceCache.peek(sourceCache.peekFirstKey())!;
        await finishImage(sourceTile);
        await vi.waitFor(() => expect(replacementTile.getState()).toBe(TileState.LOADED));
        expect(queue.getTilesLoading()).toBe(0);
    });

    it('reuses completed source tiles when the remaining sources of a reprojection are cancelled', async () => {
        const targetProjection = getProjection('EPSG:4326')!;
        const extent = transformExtent([100, 100, 800, 800], projection, targetProjection);
        source.setTileGridForProjection(
            targetProjection,
            new TileGrid({
                origin: [extent[0], extent[3]],
                resolutions: [(extent[2] - extent[0]) / 256],
                tileSize: 256,
            }),
        );
        const startedTiles: ImageTile[] = [];
        source.on('tileloadstart', (event) => startedTiles.push(event.tile as ImageTile));
        const queue = new TileQueue(
            () => 1,
            () => undefined,
        );
        const renderer = fixture.componentInstance.mapLayer.getRenderer()!;
        const frame = {
            viewState: {projection: targetProjection, rotation: 0},
            pixelRatio: 1,
            tileQueue: queue,
            wantedTiles: {},
        } as FrameState;
        viewport$.next([100, 100, 800, 800]);
        renderer.prepareFrame(frame);
        renderer.enqueueTiles(frame, extent, 0, {}, 0);
        queue.loadMoreTiles(1, 1);
        const initialRequests = requests.length;
        expect(initialRequests).toBeGreaterThan(1);
        requests[0].complete();
        await finishImage(startedTiles[0]);

        viewport$.next([10000, 0, 10256, 256]);
        for (const request of requests.slice(1)) {
            expect(request.signal.aborted).toBe(true);
        }
        await vi.waitFor(() => expect(queue.getTilesLoading()).toBe(0));
        expect(startedTiles[0].getState()).toBe(TileState.LOADED);

        viewport$.next([100, 100, 800, 800]);
        renderer.prepareFrame(frame);
        renderer.enqueueTiles(frame, extent, 0, {}, 0);
        queue.loadMoreTiles(1, 1);
        // The completed source stays cached; only cancelled requests run again.
        expect(requests).toHaveLength(2 * initialRequests - 1);
        for (const request of requests.slice(initialRequests)) {
            request.complete();
        }
        for (const tile of startedTiles.slice(initialRequests)) {
            await finishImage(tile);
        }
        const cache = renderer.getTileCache() as LRUCache<Tile>;
        await vi.waitFor(() => expect(cache.peek(cache.peekFirstKey())!.getState()).toBe(TileState.LOADED));
        expect(queue.getTilesLoading()).toBe(0);
    });

    const finishImage = async (tile: ImageTile): Promise<void> => {
        await vi.waitFor(() => expect((tile.getImage() as HTMLImageElement).src).toMatch(/^blob:/));
        // jsdom cannot decode images or draw reprojections. Browser runs use native decoding and canvas.
        if (!HTMLImageElement.prototype.decode) {
            const image = tile.getImage() as HTMLImageElement;
            Object.defineProperties(image, {naturalWidth: {value: 1}, naturalHeight: {value: 1}});
            image.dispatchEvent(new Event('load'));
        }
        await vi.waitFor(() => expect(tile.getState()).toBe(TileState.LOADED));
    };
});

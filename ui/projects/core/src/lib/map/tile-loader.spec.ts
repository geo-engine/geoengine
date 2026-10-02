// @vitest-environment jsdom

import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import ImageTile from 'ol/ImageTile';
import Tile from 'ol/Tile';
import TileState from 'ol/TileState';
import TileGrid from 'ol/tilegrid/TileGrid';
import TileWMS from 'ol/source/TileWMS';
import {FrameState} from 'ol/Map';
import {get as getProjection, transformExtent} from 'ol/proj';
import {Observable, Subject} from 'rxjs';

import {AbortableTileLayer} from './abortable-tile-layer';
import {TileLoadState, TileLoader, tileExtentInViewProjection} from './tile-loader';

/** Exposes the renderer's protected tile lookup so tests can exercise its real caches without drawing a map. */
interface TileRendererAccess {
    getOrCreateTile(z: number, x: number, y: number, frameState: FrameState): Tile;
}

interface FakeTile {
    tile: ImageTile;
    setState: ReturnType<typeof vi.fn>;
    /** Stands in for `ImageTile.load`, which OpenLayers uses to re-request a tile. */
    load: ReturnType<typeof vi.fn>;
}

const makeTile = (image: HTMLImageElement | null = document.createElement('img')): FakeTile => {
    // The loader reads the tile state to tell a retry from a fresh request and to skip released
    // tiles, so the double has to keep track of it like OpenLayers does.
    let state: number = TileState.LOADING;
    const setState = vi.fn((next: number) => {
        if (state !== TileState.EMPTY) {
            state = next;
        }
    });
    const load = vi.fn();
    return {
        tile: {
            getTileCoord: () => [0, 0, 0],
            getKey: () => '0/0/0',
            getImage: () => image,
            getState: () => state,
            setState,
            load,
        } as unknown as ImageTile,
        setState,
        load,
    };
};

/** Never resolves and rejects as soon as its request is aborted. Records the signals of all aborted requests. */
const hangingRequest = (signals: AbortSignal[], init: RequestInit): Promise<unknown> => {
    signals.push(init.signal!);
    return new Promise((_resolve, reject) =>
        init.signal!.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError'))),
    );
};

const stubHangingFetch = (signals: AbortSignal[]): void => {
    vi.stubGlobal(
        'fetch',
        vi.fn((_url: string, init: RequestInit) => hangingRequest(signals, init)),
    );
};

const authHeaders = {Authorization: 'Bearer token'};

/**
 * A stand-in for `Blob`, so that the tests do not depend on how a browser schedules its reads.
 * The real `Blob.text()` settles on the event loop in Chromium but on a microtask in jsdom, which
 * makes assertions that follow `advanceTimersByTimeAsync` pass in one and fail in the other.
 */
const mockBlob = (content: string, type: string): Blob =>
    ({type, size: content.length, text: (): Promise<string> => Promise.resolve(content)}) as unknown as Blob;

/** The parts of a `Response` the tile loader reads. */
interface MockResponse {
    readonly ok: boolean;
    readonly status: number;
    readonly blob: () => Promise<Blob>;
}

const imageResponse: MockResponse = {
    ok: true,
    status: 200,
    blob: (): Promise<Blob> => Promise.resolve(mockBlob('image', 'image/png')),
};

const unavailableResponse: MockResponse = {
    ok: false,
    status: 503,
    blob: (): Promise<Blob> => Promise.resolve(mockBlob('', '')),
};

/** A response of the WMS endpoint that reports a failed request with HTTP 200 and a JSON body. */
const exceptionDocument = (error: string, message: string): MockResponse => ({
    ok: true,
    status: 200,
    blob: (): Promise<Blob> => Promise.resolve(mockBlob(JSON.stringify({error, message}), 'application/json')),
});

describe('TileLoader', () => {
    let revokeObjectUrl: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
        vi.restoreAllMocks();
        vi.unstubAllGlobals();
        vi.spyOn(URL, 'createObjectURL').mockReturnValue('blob:tile');
        revokeObjectUrl = vi.spyOn(URL, 'revokeObjectURL').mockImplementation(() => undefined);
    });

    afterEach(() => {
        vi.useRealTimers();
    });

    it('fetches the tile with the authentication headers and frees the object URL after loading', async () => {
        const fetchMock = vi.fn().mockResolvedValue({ok: true, status: 200, blob: () => Promise.resolve(mockBlob('image', 'image/png'))});
        vi.stubGlobal('fetch', fetchMock);

        const image = document.createElement('img');
        const {tile, setState} = makeTile(image);
        new TileLoader({authHeaders: (): Record<string, string> => authHeaders}).load(tile, 'https://example.com/tile');

        await vi.waitFor(() => expect(image.src).toContain('blob:tile'));

        expect(fetchMock).toHaveBeenCalledWith('https://example.com/tile', {headers: authHeaders, signal: expect.anything()});
        expect(setState).not.toHaveBeenCalled();

        image.dispatchEvent(new Event('load'));
        expect(revokeObjectUrl).toHaveBeenCalledWith('blob:tile');
    });

    it('makes an aborted tile loadable again', async () => {
        const signals: AbortSignal[] = [];
        stubHangingFetch(signals);

        const image = document.createElement('img');
        const {tile, setState} = makeTile(image);
        const loader = new TileLoader({authHeaders: (): Record<string, string> => authHeaders});
        loader.load(tile, 'https://example.com/tile');

        loader.abortAll();

        await vi.waitFor(() => expect(setState).toHaveBeenCalledWith(TileState.IDLE));
        expect(signals[0].aborted).toBe(true);
        expect(setState).toHaveBeenCalledWith(TileState.ERROR);
        expect(image.src).toBe('');
    });

    it('retries a transient failure after a delay that grows with every attempt', async () => {
        vi.useFakeTimers();
        const fetchMock = vi.fn().mockResolvedValue({ok: false, status: 503, blob: () => Promise.resolve(mockBlob('', ''))});
        vi.stubGlobal('fetch', fetchMock);

        const url = 'https://example.com/tile';
        const {tile, setState, load} = makeTile();
        const loader = new TileLoader({authHeaders: (): Record<string, string> => authHeaders});
        // OpenLayers re-requests a tile through `load`, which a retry does on its own
        load.mockImplementation(() => loader.load(tile, url));

        loader.load(tile, url);
        await vi.advanceTimersByTimeAsync(0);
        expect(fetchMock).toHaveBeenCalledTimes(1);
        expect(setState).toHaveBeenCalledWith(TileState.ERROR);

        // no request before the first delay elapsed
        await vi.advanceTimersByTimeAsync(999);
        expect(fetchMock).toHaveBeenCalledTimes(1);

        await vi.advanceTimersByTimeAsync(1);
        expect(fetchMock).toHaveBeenCalledTimes(2);

        // the second failure waits twice as long
        await vi.advanceTimersByTimeAsync(1999);
        expect(fetchMock).toHaveBeenCalledTimes(2);

        await vi.advanceTimersByTimeAsync(1);
        expect(fetchMock).toHaveBeenCalledTimes(3);
    });

    it('does not retry a request that is rejected as broken', async () => {
        vi.useFakeTimers();
        const fetchMock = vi.fn().mockResolvedValue({ok: false, status: 404, blob: () => Promise.resolve(mockBlob('', ''))});
        vi.stubGlobal('fetch', fetchMock);

        const url = 'https://example.com/tile';
        const {tile, setState, load} = makeTile();
        const loader = new TileLoader({authHeaders: (): Record<string, string> => authHeaders});
        load.mockImplementation(() => loader.load(tile, url));

        loader.load(tile, url);
        await vi.advanceTimersByTimeAsync(0);
        expect(setState).toHaveBeenCalledWith(TileState.ERROR);

        await vi.advanceTimersByTimeAsync(10000);
        expect(fetchMock).toHaveBeenCalledTimes(1);
    });

    it('gives up on a tile that keeps failing transiently', async () => {
        vi.useFakeTimers();
        const fetchMock = vi.fn().mockResolvedValue({ok: false, status: 503, blob: () => Promise.resolve(mockBlob('', ''))});
        vi.stubGlobal('fetch', fetchMock);

        const states: TileLoadState[] = [];
        const url = 'https://example.com/tile';
        const {tile, setState, load} = makeTile();
        const loader = new TileLoader({
            authHeaders: (): Record<string, string> => authHeaders,
            onStateChange: (state): void => {
                states.push(state);
            },
        });
        load.mockImplementation(() => loader.load(tile, url));

        loader.load(tile, url);
        await vi.advanceTimersByTimeAsync(0);
        // a tile waiting for its retry keeps the layer loading instead of flickering to idle
        expect(states).toEqual(['loading']);

        await vi.advanceTimersByTimeAsync(1000);
        expect(states).toEqual(['loading']);

        await vi.advanceTimersByTimeAsync(2000);

        // the last attempt is final, so the tile is not left in a loadable state
        expect(states).toEqual(['loading', 'error']);
        expect(setState).toHaveBeenLastCalledWith(TileState.ERROR);
        expect(fetchMock).toHaveBeenCalledTimes(3);

        await vi.advanceTimersByTimeAsync(10000);
        expect(fetchMock).toHaveBeenCalledTimes(3);
    });

    it('forgets the failed attempts of a tile that loads again', async () => {
        vi.useFakeTimers();
        const fetchMock = vi
            .fn()
            .mockResolvedValueOnce(unavailableResponse)
            .mockResolvedValueOnce(unavailableResponse)
            .mockResolvedValueOnce(imageResponse)
            .mockResolvedValueOnce(unavailableResponse);
        vi.stubGlobal('fetch', fetchMock);

        const element = document.createElement('img');
        const url = 'https://example.com/tile';
        const {tile, load} = makeTile(element);
        const loader = new TileLoader({authHeaders: (): Record<string, string> => authHeaders});
        load.mockImplementation(() => loader.load(tile, url));

        loader.load(tile, url);
        await vi.advanceTimersByTimeAsync(0);
        await vi.advanceTimersByTimeAsync(1000);
        await vi.advanceTimersByTimeAsync(2000);
        expect(fetchMock).toHaveBeenCalledTimes(3);
        expect(element.src).toContain('blob:tile');

        // the budget is fresh again, so a later failure is retried instead of being final
        loader.load(tile, url);
        await vi.advanceTimersByTimeAsync(0);
        expect(fetchMock).toHaveBeenCalledTimes(4);

        await vi.advanceTimersByTimeAsync(1000);
        expect(fetchMock).toHaveBeenCalledTimes(5);
    });

    it('starts a fresh attempt budget when a tile is requested again after an abort', async () => {
        vi.useFakeTimers();
        const signals: AbortSignal[] = [];
        const fetchMock = vi
            .fn()
            .mockResolvedValueOnce(unavailableResponse)
            .mockImplementationOnce((_url: string, init: RequestInit) => hangingRequest(signals, init))
            .mockResolvedValue(unavailableResponse);
        vi.stubGlobal('fetch', fetchMock);

        const url = 'https://example.com/tile';
        const obsolete = new Subject<string>();
        const {tile, load} = makeTile();
        const loader = new TileLoader({
            authHeaders: (): Record<string, string> => authHeaders,
            abortWhen: (): Observable<string> => obsolete,
        });
        load.mockImplementation(() => loader.load(tile, url));

        // fail once, then have the retry aborted while it is in flight
        loader.load(tile, url);
        await vi.advanceTimersByTimeAsync(0);
        await vi.advanceTimersByTimeAsync(1000);
        expect(fetchMock).toHaveBeenCalledTimes(2);
        obsolete.next('resolution changed');
        expect(signals[0].aborted).toBe(true);
        await vi.advanceTimersByTimeAsync(0);

        // the abort is not a failure of this visit, so the old failures are not carried over
        loader.load(tile, url);
        await vi.advanceTimersByTimeAsync(0);
        expect(fetchMock).toHaveBeenCalledTimes(3);

        await vi.advanceTimersByTimeAsync(1000);
        expect(fetchMock).toHaveBeenCalledTimes(4);
    });

    it('reports a failure only once the tile is given up on', async () => {
        vi.useFakeTimers();
        const fetchMock = vi
            .fn()
            .mockResolvedValueOnce(exceptionDocument('QueryCanceled', 'the query was canceled'))
            .mockResolvedValueOnce(imageResponse);
        vi.stubGlobal('fetch', fetchMock);

        const onError = vi.fn();
        const url = 'https://example.com/tile';
        const {tile, load} = makeTile();
        const loader = new TileLoader({authHeaders: (): Record<string, string> => authHeaders, onError});
        load.mockImplementation(() => loader.load(tile, url));

        loader.load(tile, url);
        await vi.advanceTimersByTimeAsync(0);
        await vi.advanceTimersByTimeAsync(1000);
        expect(fetchMock).toHaveBeenCalledTimes(2);

        // the retry fixed the tile, so the user never sees an error that went away on its own
        expect(onError).not.toHaveBeenCalled();
    });

    it('does not retry after it is aborted', async () => {
        vi.useFakeTimers();
        const fetchMock = vi.fn().mockResolvedValue({ok: false, status: 503, blob: () => Promise.resolve(mockBlob('', ''))});
        vi.stubGlobal('fetch', fetchMock);

        const url = 'https://example.com/tile';
        const {tile, load} = makeTile();
        const loader = new TileLoader({authHeaders: (): Record<string, string> => authHeaders});
        load.mockImplementation(() => loader.load(tile, url));

        loader.load(tile, url);
        await vi.advanceTimersByTimeAsync(0);
        expect(fetchMock).toHaveBeenCalledTimes(1);

        loader.abortAll();
        await vi.advanceTimersByTimeAsync(10000);
        expect(fetchMock).toHaveBeenCalledTimes(1);
    });

    it('does not retry a tile that became obsolete while it waited for its retry', async () => {
        vi.useFakeTimers();
        const fetchMock = vi.fn().mockResolvedValue(unavailableResponse);
        vi.stubGlobal('fetch', fetchMock);

        const url = 'https://example.com/tile';
        const obsolete = new Subject<string>();
        const {tile, load} = makeTile();
        const loader = new TileLoader({
            authHeaders: (): Record<string, string> => authHeaders,
            abortWhen: (): Observable<string> => obsolete,
        });
        load.mockImplementation(() => loader.load(tile, url));

        // the first attempt fails transiently, so the retry is now waiting
        loader.load(tile, url);
        await vi.advanceTimersByTimeAsync(0);
        expect(fetchMock).toHaveBeenCalledTimes(1);

        // the tile leaves the viewport while it waits, and the retry is given up on
        obsolete.next('tile extent left the viewport');
        await vi.advanceTimersByTimeAsync(10000);
        expect(fetchMock).toHaveBeenCalledTimes(1);
    });

    it('makes a real tile loadable again when its pending retry becomes obsolete', async () => {
        vi.useFakeTimers();
        const fetchMock = vi.fn().mockResolvedValue(unavailableResponse);
        vi.stubGlobal('fetch', fetchMock);

        const obsolete = new Subject<string>();
        const loader = new TileLoader({
            authHeaders: (): Record<string, string> => authHeaders,
            abortWhen: (): Observable<string> => obsolete,
        });
        const tile = new ImageTile([0, 0, 0], TileState.IDLE, 'https://example.com/tile', {}, loader.load);

        tile.load();
        await vi.advanceTimersByTimeAsync(0);
        expect(tile.getState()).toBe(TileState.ERROR);

        obsolete.next('tile extent left the viewport');
        await vi.advanceTimersByTimeAsync(10000);
        expect(fetchMock).toHaveBeenCalledTimes(1);

        // OpenLayers only queues IDLE tiles when the user returns to this part of the map.
        expect(tile.getState()).toBe(TileState.IDLE);
    });

    it('releases its obsolescence subscription when a retry tile is evicted', async () => {
        vi.useFakeTimers();
        const fetchMock = vi.fn().mockResolvedValue(unavailableResponse);
        vi.stubGlobal('fetch', fetchMock);

        const obsolete = new Subject<string>();
        const loader = new TileLoader({
            authHeaders: (): Record<string, string> => authHeaders,
            abortWhen: (): Observable<string> => obsolete,
        });
        const tile = new ImageTile([0, 0, 0], TileState.IDLE, 'https://example.com/tile', {}, loader.load);
        tile.load();
        await vi.advanceTimersByTimeAsync(0);
        expect(tile.getState()).toBe(TileState.ERROR);
        expect(obsolete.observers).toHaveLength(1);

        tile.release();
        await vi.advanceTimersByTimeAsync(1000);

        expect(fetchMock).toHaveBeenCalledOnce();
        expect(obsolete.observers).toHaveLength(0);
        loader.abortAll();
    });

    it.each([200, 503])('does not revive a cancelled request while parsing a %s exception', async (status) => {
        vi.useFakeTimers();
        let finish!: (body: string) => void;
        const text = new Promise<string>((resolve) => {
            finish = resolve;
        });
        const readText = vi.fn().mockReturnValue(text);
        const fetchMock = vi.fn().mockResolvedValue({
            ok: status === 200,
            status,
            blob: (): Promise<Blob> => Promise.resolve({type: 'application/json', text: readText} as unknown as Blob),
        });
        vi.stubGlobal('fetch', fetchMock);

        const obsolete = new Subject<string>();
        const loader = new TileLoader({
            authHeaders: (): Record<string, string> => authHeaders,
            abortWhen: (): Observable<string> => obsolete,
        });
        const tile = new ImageTile([0, 0, 0], TileState.IDLE, 'https://example.com/tile', {}, loader.load);
        tile.load();
        await vi.advanceTimersByTimeAsync(0);
        expect(readText).toHaveBeenCalledOnce();

        obsolete.next('resolution changed');
        finish(JSON.stringify({error: 'QueryCanceled', message: 'canceled'}));
        await vi.advanceTimersByTimeAsync(10000);

        expect(fetchMock).toHaveBeenCalledOnce();
        expect(tile.getState()).toBe(TileState.IDLE);
        loader.abortAll();
    });

    it('recovers a reprojection cached during backoff after its source tiles successfully retry', async () => {
        vi.useFakeTimers();
        const fetchMock = vi.fn().mockResolvedValue(unavailableResponse);
        vi.stubGlobal('fetch', fetchMock);

        const source = new TileWMS({url: 'https://example.com/wms', params: {LAYERS: 'test'}, projection: 'EPSG:3857', wrapX: false});
        const layer = new AbortableTileLayer({source});
        const loader = new TileLoader({
            authHeaders: (): Record<string, string> => authHeaders,
            onTileError: (tile): void => layer.invalidateAbortedTile(tile),
        });
        source.setTileLoadFunction(loader.load);

        const renderer = layer.getRenderer()!;
        // Tile lookup and prepareFrame only read the projection and pixel ratio from the frame.
        const frameState = {viewState: {projection: getProjection('EPSG:4326')!}, pixelRatio: 1} as FrameState;
        renderer.prepareFrame(frameState);
        const displayTile = (): Tile => (renderer as unknown as TileRendererAccess).getOrCreateTile(1, 1, 0, frameState);

        try {
            displayTile().load();
            await vi.advanceTimersByTimeAsync(0);
            expect(renderer.getTileCache().getCount()).toBe(0);

            // The next frame runs before the one-second retry delay has elapsed.
            const cachedDuringBackoff = displayTile();
            cachedDuringBackoff.load();
            await vi.advanceTimersByTimeAsync(0);
            expect(cachedDuringBackoff.getState()).toBe(TileState.ERROR);

            const sourceTiles: Array<ImageTile> = [];
            renderer.getSourceTileCache().forEach((tile: ImageTile) => sourceTiles.push(tile));
            expect(sourceTiles.length).toBeGreaterThan(0);
            expect(fetchMock).toHaveBeenCalledTimes(sourceTiles.length);

            fetchMock.mockResolvedValue(imageResponse);
            await vi.advanceTimersByTimeAsync(1000);
            expect(fetchMock).toHaveBeenCalledTimes(2 * sourceTiles.length);

            // jsdom does not decode images; deliver the browser's load event to the real ImageTiles.
            for (const tile of sourceTiles) {
                expect(tile.getState()).toBe(TileState.LOADING);
                const image = tile.getImage() as HTMLImageElement;
                expect(image.src).toContain('blob:tile');
                Object.defineProperties(image, {naturalWidth: {value: 256}, naturalHeight: {value: 256}});
                image.dispatchEvent(new Event('load'));
                expect(tile.getState()).toBe(TileState.LOADED);
            }

            // The renderer must return a recovered or loadable reprojection, rather than the cached failure.
            expect(displayTile().getState()).not.toBe(TileState.ERROR);
        } finally {
            loader.abortAll();
            layer.dispose();
        }
    });

    it('makes an aborted geographic reprojection loadable again when the source grid crosses the poles', async () => {
        vi.useFakeTimers();
        const signals: AbortSignal[] = [];
        stubHangingFetch(signals);

        const obsolete = new Subject<string>();
        const source = new TileWMS({url: 'https://example.com/wms', params: {LAYERS: 'test'}, projection: 'EPSG:4326', wrapX: false});
        const layer = new AbortableTileLayer({source});
        const loader = new TileLoader({
            authHeaders: (): Record<string, string> => authHeaders,
            abortWhen: (): Observable<string> => obsolete,
            onTileError: (tile): void => layer.invalidateAbortedTile(tile),
        });
        source.setTileLoadFunction(loader.load);

        const renderer = layer.getRenderer()!;
        const frameState = {viewState: {projection: getProjection('EPSG:3857')!}, pixelRatio: 1} as FrameState;
        renderer.prepareFrame(frameState);
        const displayTile = (): Tile => (renderer as unknown as TileRendererAccess).getOrCreateTile(0, 0, 0, frameState);

        try {
            // The standard zoom-zero geographic grid extends below the valid latitude range.
            const sourceGrid = source.getTileGridForProjection(source.getProjection()!);
            expect(sourceGrid.getTileCoordExtent([0, 0, 0])).toEqual([-180, -270, 180, 90]);
            displayTile().load();
            expect(signals.length).toBeGreaterThan(0);

            obsolete.next('resolution changed');
            await vi.advanceTimersByTimeAsync(0);
            expect(signals.every((signal) => signal.aborted)).toBe(true);
            renderer.getSourceTileCache().forEach((tile: ImageTile) => expect(tile.getState()).toBe(TileState.IDLE));

            // The source is ready to load again, so its display tile must also be queueable.
            expect(displayTile().getState()).toBe(TileState.IDLE);
        } finally {
            loader.abortAll();
            layer.dispose();
        }
    });

    it('tells the caller about an aborted tile before the tile goes into ERROR', async () => {
        const signals: AbortSignal[] = [];
        stubHangingFetch(signals);

        const calls: string[] = [];
        const {tile, setState} = makeTile();
        setState.mockImplementation((state: number) => calls.push(`state:${state}`));
        const loader = new TileLoader({
            authHeaders: (): Record<string, string> => authHeaders,
            onTileError: (): void => {
                calls.push('onTileError');
            },
        });
        loader.load(tile, 'https://example.com/tile');
        loader.abortAll();

        await vi.waitFor(() => expect(calls).toContain('onTileError'));
        // The hook must come first: a parent reprojection reacts to the state change synchronously.
        expect(calls[0]).toBe('onTileError');
        expect(calls).toContain(`state:${TileState.ERROR}`);
    });

    it('tells the caller about a tile that failed, not just one that was aborted', async () => {
        vi.useFakeTimers();
        const fetchMock = vi.fn().mockResolvedValue({ok: false, status: 404, blob: () => Promise.resolve(mockBlob('', ''))});
        vi.stubGlobal('fetch', fetchMock);

        const onTileError = vi.fn();
        const loader = new TileLoader({authHeaders: (): Record<string, string> => authHeaders, onTileError});
        loader.load(makeTile().tile, 'https://example.com/tile');
        await vi.advanceTimersByTimeAsync(0);

        expect(onTileError).toHaveBeenCalled();
    });

    it('retries an exception document that reports a cancelled query', async () => {
        vi.useFakeTimers();
        const fetchMock = vi.fn().mockResolvedValue(exceptionDocument('QueryCanceled', 'query canceled'));
        vi.stubGlobal('fetch', fetchMock);

        const url = 'https://example.com/tile';
        const {tile, load} = makeTile();
        const loader = new TileLoader({authHeaders: (): Record<string, string> => authHeaders});
        load.mockImplementation(() => loader.load(tile, url));

        loader.load(tile, url);
        await vi.advanceTimersByTimeAsync(0);
        expect(fetchMock).toHaveBeenCalledTimes(1);

        await vi.advanceTimersByTimeAsync(1000);
        expect(fetchMock).toHaveBeenCalledTimes(2);
    });

    it('marks a response that is not an image as tile error', async () => {
        vi.useFakeTimers();
        // a document without a known transient error is not worth another try
        const fetchMock = vi.fn().mockResolvedValue(exceptionDocument('InvalidChannel', 'requested channel: 7'));
        vi.stubGlobal('fetch', fetchMock);

        const states: TileLoadState[] = [];
        const image = document.createElement('img');
        const url = 'https://example.com/tile';
        const {tile, setState, load} = makeTile(image);
        const loader = new TileLoader({
            authHeaders: (): Record<string, string> => authHeaders,
            onStateChange: (state): void => {
                states.push(state);
            },
        });
        load.mockImplementation(() => loader.load(tile, url));
        loader.load(tile, url);

        await vi.advanceTimersByTimeAsync(10000);
        expect(states).toEqual(['loading', 'error']);
        expect(setState).toHaveBeenLastCalledWith(TileState.ERROR);
        expect(fetchMock).toHaveBeenCalledTimes(1);
        expect(image.src).toBe('');
    });

    it('reports the message of an exception document once, not once per tile', async () => {
        // the WMS endpoint answers failed requests with HTTP 200 and an exception document
        const fetchMock = vi.fn().mockResolvedValue({
            ok: true,
            status: 200,
            blob: () => Promise.resolve(mockBlob(JSON.stringify({message: 'no such workflow'}), 'application/json')),
        });
        vi.stubGlobal('fetch', fetchMock);

        const messages: string[] = [];
        const loader = new TileLoader({
            authHeaders: (): Record<string, string> => authHeaders,
            onError: (message): void => {
                messages.push(message);
            },
        });

        loader.load(makeTile().tile, 'https://example.com/1');
        loader.load(makeTile().tile, 'https://example.com/2');
        await vi.waitFor(() => expect(messages).toEqual(['no such workflow']));

        // a tile that loads again makes the next error visible again
        vi.stubGlobal(
            'fetch',
            vi.fn().mockResolvedValue({ok: true, status: 200, blob: () => Promise.resolve(mockBlob('image', 'image/png'))}),
        );
        loader.load(makeTile().tile, 'https://example.com/3');
        vi.stubGlobal('fetch', fetchMock);
        loader.load(makeTile().tile, 'https://example.com/4');
        await vi.waitFor(() => expect(messages).toEqual(['no such workflow', 'no such workflow']));
    });

    it('does not request tiles once it is aborted', () => {
        const fetchMock = vi.fn();
        vi.stubGlobal('fetch', fetchMock);
        const controller = new AbortController();

        const loader = new TileLoader({authHeaders: (): Record<string, string> => authHeaders, signal: controller.signal});
        loader.load(makeTile().tile, 'https://example.com/before');
        controller.abort();
        loader.load(makeTile().tile, 'https://example.com/after');

        expect(fetchMock).toHaveBeenCalledTimes(1);
    });

    it('cancels requests once the query of a tile is obsolete', () => {
        const signals: AbortSignal[] = [];
        stubHangingFetch(signals);
        const obsolete = new Subject<string>();

        new TileLoader({authHeaders: (): Record<string, string> => authHeaders, abortWhen: (): Observable<string> => obsolete}).load(
            makeTile().tile,
            'https://example.com/tile',
        );

        expect(signals[0].aborted).toBe(false);
        obsolete.next('resolution changed');
        expect(signals[0].aborted).toBe(true);
    });

    it('reports the aggregated state of all tiles', async () => {
        stubHangingFetch([]);

        const states: TileLoadState[] = [];
        const loader = new TileLoader({
            authHeaders: (): Record<string, string> => authHeaders,
            onStateChange: (state): void => {
                states.push(state);
            },
        });

        loader.load(makeTile().tile, 'https://example.com/1');
        loader.load(makeTile().tile, 'https://example.com/2');
        expect(states).toEqual(['loading']);

        loader.abortAll();
        await vi.waitFor(() => expect(states).toEqual(['loading', 'idle']));
    });

    it('aborts all pending requests and frees metadata URLs', async () => {
        const signals: AbortSignal[] = [];
        vi.stubGlobal(
            'fetch',
            vi.fn((url: string, init: RequestInit) => {
                if (url.endsWith('/tms')) {
                    return Promise.resolve({ok: true, status: 200, json: () => Promise.resolve({links: []})});
                }
                return hangingRequest(signals, init);
            }),
        );
        vi.spyOn(URL, 'createObjectURL').mockReturnValue('blob:metadata');

        const loader = new TileLoader({authHeaders: (): Record<string, string> => authHeaders});
        const jsonUrl = await loader.jsonUrl('https://example.com/tms', new AbortController().signal);
        loader.load(makeTile().tile, 'https://example.com/tile');
        loader.abortAll();

        expect(jsonUrl).toBe('blob:metadata');
        expect(signals[0].aborted).toBe(true);
        expect(revokeObjectUrl).toHaveBeenCalledWith('blob:metadata');
    });
});

describe('tileExtentInViewProjection', () => {
    const tileGrid = new TileGrid({origin: [0, 256], resolutions: [1], tileSize: 256});
    const tile = {getTileCoord: () => [0, 0, 0]} as unknown as ImageTile;

    it('leaves the extent alone when the layer and the view share a projection', () => {
        const projection = getProjection('EPSG:3857')!;
        expect(tileExtentInViewProjection(tileGrid, tile, projection, projection)).toEqual([0, 0, 256, 256]);
    });

    it('moves the extent into the projection the viewport is in', () => {
        const source = getProjection('EPSG:3857')!;
        const view = getProjection('EPSG:4326')!;
        // reading the coordinate from the grid of the view projection would answer in metres
        // instead of degrees, which is what made the abort fire for tiles still on screen
        expect(tileExtentInViewProjection(tileGrid, tile, source, view)).toEqual(transformExtent([0, 0, 256, 256], source, view, 8));
    });

    it('clips a geographic tile that crosses the poles before transforming it', () => {
        const geographicGrid = new TileGrid({origin: [-180, 90], resolutions: [360 / 256], tileSize: 256});
        const geographicTile = {getTileCoord: () => [0, 0, 0]} as unknown as ImageTile;
        const source = getProjection('EPSG:4326')!;
        const view = getProjection('EPSG:3857')!;

        expect(geographicGrid.getTileCoordExtent([0, 0, 0])).toEqual([-180, -270, 180, 90]);
        const transformed = tileExtentInViewProjection(geographicGrid, geographicTile, source, view);
        expect(transformed.every(Number.isFinite)).toBe(true);
        expect(transformed[1]).toBeGreaterThan(-Infinity);
        expect(transformed[3]).toBeLessThan(Infinity);
    });
});

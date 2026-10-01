// @vitest-environment jsdom

import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
import ImageTile from 'ol/ImageTile';
import TileState from 'ol/TileState';
import {Observable, Subject} from 'rxjs';

import {TileLoadState, TileLoader} from './tile-loader';

interface FakeTile {
    tile: ImageTile;
    setState: ReturnType<typeof vi.fn>;
    /** Stands in for `ImageTile.load`, which OpenLayers uses to re-request a tile. */
    load: ReturnType<typeof vi.fn>;
}

const makeTile = (image: HTMLImageElement | null = document.createElement('img')): FakeTile => {
    const setState = vi.fn();
    const load = vi.fn();
    return {
        tile: {
            getTileCoord: () => [0, 0, 0],
            getKey: () => '0/0/0',
            getImage: () => image,
            getState: () => 1,
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

/** The parts of a `Response` the tile loader reads. */
interface MockResponse {
    readonly ok: boolean;
    readonly status: number;
    readonly blob: () => Promise<Blob>;
}

const imageResponse: MockResponse = {
    ok: true,
    status: 200,
    blob: (): Promise<Blob> => Promise.resolve(new Blob(['image'], {type: 'image/png'})),
};

const unavailableResponse: MockResponse = {
    ok: false,
    status: 503,
    blob: (): Promise<Blob> => Promise.resolve(new Blob([])),
};

/** A response of the WMS endpoint that reports a failed request with HTTP 200 and a JSON body. */
const exceptionDocument = (error: string, message: string): MockResponse => ({
    ok: true,
    status: 200,
    blob: (): Promise<Blob> => Promise.resolve(new Blob([JSON.stringify({error, message})], {type: 'application/json'})),
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
        const fetchMock = vi
            .fn()
            .mockResolvedValue({ok: true, status: 200, blob: () => Promise.resolve(new Blob(['image'], {type: 'image/png'}))});
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
        const fetchMock = vi.fn().mockResolvedValue({ok: false, status: 503, blob: () => Promise.resolve(new Blob([]))});
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
        const fetchMock = vi.fn().mockResolvedValue({ok: false, status: 404, blob: () => Promise.resolve(new Blob([]))});
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
        const fetchMock = vi.fn().mockResolvedValue({ok: false, status: 503, blob: () => Promise.resolve(new Blob([]))});
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
        expect(states).toEqual(['loading', 'idle']);

        await vi.advanceTimersByTimeAsync(1000);
        expect(states).toEqual(['loading', 'idle', 'loading', 'idle']);

        await vi.advanceTimersByTimeAsync(2000);

        // the last attempt is final, so the tile is not left in a loadable state
        expect(states).toEqual(['loading', 'idle', 'loading', 'idle', 'loading', 'error']);
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

    it('does not retry after it is aborted', async () => {
        vi.useFakeTimers();
        const fetchMock = vi.fn().mockResolvedValue({ok: false, status: 503, blob: () => Promise.resolve(new Blob([]))});
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
        const fetchMock = vi.fn().mockResolvedValue({ok: false, status: 404, blob: () => Promise.resolve(new Blob([]))});
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
            blob: () => Promise.resolve(new Blob([JSON.stringify({message: 'no such workflow'})], {type: 'application/json'})),
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
            vi.fn().mockResolvedValue({ok: true, status: 200, blob: () => Promise.resolve(new Blob(['image'], {type: 'image/png'}))}),
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

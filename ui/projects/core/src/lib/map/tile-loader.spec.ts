// @vitest-environment jsdom

import {beforeEach, describe, expect, it, vi} from 'vitest';
import ImageTile from 'ol/ImageTile';
import TileState from 'ol/TileState';
import {Observable, Subject} from 'rxjs';

import {TileLoadState, TileLoader} from './tile-loader';

interface FakeTile {
    tile: ImageTile;
    setState: ReturnType<typeof vi.fn>;
}

const makeTile = (image: HTMLImageElement | null = document.createElement('img')): FakeTile => {
    const setState = vi.fn();
    return {
        tile: {getTileCoord: () => [0, 0, 0], getImage: () => image, setState} as unknown as ImageTile,
        setState,
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

describe('TileLoader', () => {
    let revokeObjectUrl: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
        vi.restoreAllMocks();
        vi.unstubAllGlobals();
        vi.spyOn(URL, 'createObjectURL').mockReturnValue('blob:tile');
        revokeObjectUrl = vi.spyOn(URL, 'revokeObjectURL').mockImplementation(() => undefined);
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

    it('marks failed requests as tile error', async () => {
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ok: false, status: 500, blob: () => Promise.resolve(new Blob([]))}));

        const {tile, setState} = makeTile();
        new TileLoader({authHeaders: (): Record<string, string> => authHeaders}).load(tile, 'https://example.com/tile');

        await vi.waitFor(() => expect(setState).toHaveBeenCalledWith(TileState.ERROR));
    });

    it('marks responses that are not images as tile error', async () => {
        // the WMS endpoint answers failed requests with HTTP 200 and an exception document
        const fetchMock = vi.fn().mockResolvedValue({
            ok: true,
            status: 200,
            blob: () => Promise.resolve(new Blob([JSON.stringify({message: 'no such workflow'})], {type: 'application/json'})),
        });
        vi.stubGlobal('fetch', fetchMock);

        const states: TileLoadState[] = [];
        const image = document.createElement('img');
        const {tile, setState} = makeTile(image);
        const loader = new TileLoader({
            authHeaders: (): Record<string, string> => authHeaders,
            onStateChange: (state): void => {
                states.push(state);
            },
        });
        loader.load(tile, 'https://example.com/tile');

        await vi.waitFor(() => expect(states).toEqual(['loading', 'error']));
        expect(setState).toHaveBeenCalledWith(TileState.ERROR);
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
        const obsolete = new Subject<void>();

        new TileLoader({authHeaders: (): Record<string, string> => authHeaders, abortWhen: (): Observable<unknown> => obsolete}).load(
            makeTile().tile,
            'https://example.com/tile',
        );

        expect(signals[0].aborted).toBe(false);
        obsolete.next();
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

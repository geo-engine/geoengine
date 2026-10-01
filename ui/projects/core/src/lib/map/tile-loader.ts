import {Observable} from 'rxjs';

import ImageTile from 'ol/ImageTile';
import Tile from 'ol/Tile';
import TileState from 'ol/TileState';
import TileGrid from 'ol/tilegrid/TileGrid';

import {Extent} from './map.service';

export type TileLoadState = 'idle' | 'loading' | 'error';

/** How often a transient failure is retried before a tile is given up on. */
const MAX_TILE_ATTEMPTS = 3;

/** Delay before the first retry, multiplied by the attempt number. */
const RETRY_DELAY = 1000;

/** The statuses OpenLayers recommends retrying; everything else is a broken request. */
const TRANSIENT_STATUSES = new Set([408, 429, 500, 502, 503, 504]);

/** An exception document the server answered instead of a tile. */
interface ServiceException {
    /** The variant name of the server-side error, e.g. `QueryCanceled`. */
    readonly error?: string;
    readonly message: string;
}

/** The only exception documents worth another try: the server gave up on the query itself. */
const TRANSIENT_EXCEPTIONS = new Set(['QueryCanceled', 'QueryingProcessorFailed', 'Io', 'Reqwest']);

/** What a tile got out of the response it was sent. */
type AssignResult = 'ok' | 'transient' | 'terminal';

interface TileLoaderOptions {
    /** Aborts all pending requests and frees all object URLs. Aborted when the source is replaced or the layer is destroyed. */
    readonly signal?: AbortSignal;

    /** Headers of every request, e.g. `{Authorization: 'Bearer …'}`. Read per request, so a new session applies to pending loads. */
    readonly authHeaders: () => Record<string, string>;

    /** Emits once the request of the given tile is no longer needed (e.g. the tile left the viewport). */
    readonly abortWhen?: (tile: ImageTile) => Observable<unknown>;

    /** Emits the aggregated state of all tiles of this loader. */
    readonly onStateChange?: (state: TileLoadState) => void;

    /** Emits the message of an exception document that the server answered instead of a tile. */
    readonly onError?: (message: string) => void;
}

/**
 * The extent of a tile in map units. Used to determine whether a tile request is still of interest.
 */
export const tileExtent = (tileGrid: TileGrid, tile: ImageTile): Extent => tileGrid.getTileCoordExtent(tile.getTileCoord()) as Extent;

/**
 * Loads the tiles of an OpenLayers tile source with `fetch` and serves them as object URLs.
 *
 * OpenLayers would load tiles as plain `<img>` requests, which cannot carry an `Authorization`
 * header and cannot be cancelled. Cancelling frees the resources of the (potentially expensive)
 * backend query on the server, which is why requests are aborted whenever they become obsolete.
 *
 * A loader belongs to exactly one source instance. Create a new one whenever the source is
 * replaced and abort the previous one.
 */
export class TileLoader {
    private readonly controllers = new Set<AbortController>();
    private readonly objectUrls = new Set<string>();

    /** Pending retries, so that an obsolete loader does not request tiles anymore. */
    private readonly timers = new Set<ReturnType<typeof setTimeout>>();

    /** Failed attempts per tile, so that a transient failure can be retried a few times. */
    private readonly attempts = new WeakMap<ImageTile, number>();

    private pending = 0;
    private failures = 0;
    private lastError?: string;

    constructor(private readonly options: TileLoaderOptions) {
        this.options.signal?.addEventListener('abort', () => this.abortAll());
    }

    /**
     * An OpenLayers `tileLoadFunction`. Fetches the tile and hands it to the tile as object URL,
     * as OpenLayers only renders what a tile has assigned to its image element.
     */
    readonly load = (olTile: Tile, src: string): void => {
        const tile = olTile as ImageTile;
        if (this.options.signal?.aborted) {
            // This loader is obsolete (the source was replaced or the layer destroyed), but
            // OpenLayers re-requests tiles that end up `IDLE`. Marking it `ERROR` stops that
            // instead of firing a request that nobody would ever abort.
            tile.setState(TileState.ERROR);
            return;
        }

        const controller = new AbortController();
        this.controllers.add(controller);

        if (this.pending++ === 0) {
            this.state('loading');
        }

        const abortSubscription = this.options.abortWhen?.(tile).subscribe(() => controller.abort());

        void this.request(tile, src, controller.signal).then((failed) => {
            abortSubscription?.unsubscribe();
            this.controllers.delete(controller);

            this.failures += failed ? 1 : 0;
            if (--this.pending === 0) {
                const state = this.failures > 0 ? 'error' : 'idle';
                this.failures = 0;
                this.state(state);
            }
        });
    };

    /**
     * Fetches a JSON document and returns it as an object URL that can be read without
     * authentication headers, for sources that read their metadata themselves.
     * The URL is freed by {@link abortAll}.
     */
    readonly jsonUrl = async (url: string, signal: AbortSignal, transform?: (metadata: unknown) => Promise<void>): Promise<string> => {
        const response = await this.fetch(url, signal);
        if (!response.ok) {
            throw new Error(`Request failed with status ${response.status}: ${url}`);
        }

        const metadata = (await response.json()) as unknown;

        await transform?.(metadata);

        if (signal.aborted) {
            throw new DOMException('Aborted', 'AbortError');
        }

        const objectUrl = URL.createObjectURL(new Blob([JSON.stringify(metadata)], {type: 'application/json'}));
        this.objectUrls.add(objectUrl);
        return objectUrl;
    };

    /**
     * Aborts all pending requests and retries, and frees all object URLs of this loader.
     */
    abortAll(): void {
        for (const controller of this.controllers) {
            controller.abort();
        }
        this.controllers.clear();

        for (const timer of this.timers) {
            clearTimeout(timer);
        }
        this.timers.clear();

        for (const objectUrl of this.objectUrls) {
            URL.revokeObjectURL(objectUrl);
        }
        this.objectUrls.clear();
    }

    private async request(tile: ImageTile, src: string, signal: AbortSignal): Promise<boolean> {
        try {
            const response = await this.fetch(src, signal);
            if (!response.ok) {
                return this.fail(tile, TRANSIENT_STATUSES.has(response.status));
            }

            const result = await this.assignImage(tile, await response.blob());
            if (result !== 'ok') {
                return this.fail(tile, result === 'transient');
            }

            this.attempts.delete(tile);
            return false;
        } catch {
            if (signal.aborted) {
                // The request is obsolete, but the tile may be needed again. OpenLayers only
                // re-requests `IDLE` tiles and `setState` rejects `LOADING` to `IDLE`, so the
                // tile has to pass through `ERROR`.
                tile.setState(TileState.ERROR);
                tile.setState(TileState.IDLE);
                return false;
            }

            // anything that is not a refused request is a problem of the connection
            return this.fail(tile, true);
        }
    }

    /**
     * Marks a tile as failed and returns whether the failure is final. OpenLayers never
     * re-requests `ERROR` tiles, so a transient failure is retried by this loader after a delay,
     * up to {@link MAX_TILE_ATTEMPTS} tries.
     */
    private fail(tile: ImageTile, transient: boolean): boolean {
        const attempts = (this.attempts.get(tile) ?? 0) + 1;
        this.attempts.set(tile, attempts);

        tile.setState(TileState.ERROR);

        if (transient && attempts < MAX_TILE_ATTEMPTS) {
            // The tile stays in `ERROR` on purpose: OpenLayers re-requests `IDLE` tiles on the
            // very next frame, which would defeat the delay. A tile that left the viewport is
            // released as `EMPTY`, so its pending retry does nothing.
            const timer = setTimeout(() => {
                this.timers.delete(timer);
                tile.load();
            }, attempts * RETRY_DELAY);
            this.timers.add(timer);
            return false;
        }

        return true;
    }

    /** Fetches with authentication headers and reports the body of a request the server refused. */
    private async fetch(url: string, signal: AbortSignal): Promise<Response> {
        const response = await fetch(url, {headers: this.options.authHeaders(), signal});

        if (!response.ok) {
            this.reportError(await this.readException(await response.blob()));
        }

        return response;
    }

    /** Returns how the tile should be treated after it got the given blob. */
    private async assignImage(tile: ImageTile, blob: Blob): Promise<AssignResult> {
        const image = tile.getImage() as HTMLImageElement | null;
        if (!image) {
            // the tile was dropped from the cache while the request was in flight
            return 'ok';
        }

        if (!blob.type.startsWith('image/')) {
            // The WMS endpoint answers failed requests with HTTP 200 and an exception document,
            // so a successful response is not necessarily a tile.
            const exception = await this.readException(blob);
            this.reportError(exception);
            return TRANSIENT_EXCEPTIONS.has(exception.error ?? '') ? 'transient' : 'terminal';
        }

        this.lastError = undefined;

        if (image.src.startsWith('blob:')) {
            URL.revokeObjectURL(image.src);
        }

        const objectUrl = URL.createObjectURL(blob);
        image.addEventListener('load', () => URL.revokeObjectURL(objectUrl), {once: true});
        image.addEventListener('error', () => URL.revokeObjectURL(objectUrl), {once: true});
        image.src = objectUrl;
        return 'ok';
    }

    private state(state: TileLoadState): void {
        this.options.onStateChange?.(state);
    }

    /** Parses the exception document a server answered instead of a tile. */
    private async readException(blob: Blob): Promise<ServiceException> {
        const body = await blob.text();

        try {
            const {error, message} = JSON.parse(body) as {error?: string; message?: string};
            return {error, message: message ?? body};
        } catch {
            // not an exception document, report the raw body
            return {message: body};
        }
    }

    /**
     * Surfaces the message of an exception document, e.g. `{"error": …, "message": …}` of the WMS
     * endpoint. Repeating the same message for every failing tile would flood the user with
     * notifications, so it is reported once until a tile loads again.
     */
    private reportError(exception: ServiceException): void {
        if (exception.message !== this.lastError) {
            this.lastError = exception.message;
            this.options.onError?.(exception.message);
        }
    }
}

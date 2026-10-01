import {Observable} from 'rxjs';

import ImageTile from 'ol/ImageTile';
import Tile from 'ol/Tile';
import TileState from 'ol/TileState';
import TileGrid from 'ol/tilegrid/TileGrid';
import {equivalent, transformExtent, type Projection} from 'ol/proj';

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

/**
 * The exception documents worth another try. Only `QueryCanceled` is known to be transient: the
 * server gave up on the query itself, so a new request is a new query. The others are transport
 * shaped (`Io`, `Reqwest`) or wrap one (`QueryingProcessorFailed`), but they also wrap permanent
 * failures, so a retry can be wasted. A wasted retry costs one request and a few seconds, while a
 * wrongly terminal classification leaves the tile blank, which is the bug this set exists to avoid.
 * The backend has no notion of retryability to ask, so this stays a judgement call.
 */
const TRANSIENT_EXCEPTIONS = new Set(['QueryCanceled', 'QueryingProcessorFailed', 'Io', 'Reqwest']);

/** A request that did not produce a tile, and whether another attempt could still help. */
interface Failure {
    readonly transient: boolean;
    /** The exception document the server sent instead of a tile, if any. */
    readonly exception?: ServiceException;
}

/** A tile is done, waiting for its retry, or given up on. */
type LoadOutcome = 'ok' | 'retry' | 'failed';

/**
 * The two things that go wrong with a tile without any loader noticing.
 *
 * Both are emitted only when `onDiagnostic` is given. `aborted` says why a request was given up
 * on, which is the only way to tell a cancelled tile from a broken one. `decoded` reports the size
 * the browser actually decoded the image to, because OpenLayers turns an image that loads without
 * pixels into an `EMPTY` tile and never requests it again.
 */
export interface TileDiagnostic {
    readonly event: 'aborted' | 'decoded';
    readonly tile: string;
    /** For `aborted`, the condition that made the request obsolete. */
    readonly reason?: string;
    /** For `decoded`, the size of the decoded image. `0` means OpenLayers turns this into an `EMPTY` tile. */
    readonly naturalWidth?: number;
    readonly naturalHeight?: number;
    /** For `decoded`, the tile state after the image settled, see `TileState`. */
    readonly state?: number;
}

interface TileLoaderOptions {
    /** Aborts all pending requests and frees all object URLs. Aborted when the source is replaced or the layer is destroyed. */
    readonly signal?: AbortSignal;

    /** Headers of every request, e.g. `{Authorization: 'Bearer …'}`. Read per request, so a new session applies to pending loads. */
    readonly authHeaders: () => Record<string, string>;

    /** Emits, with the condition that made the tile obsolete, when a request should be given up on. */
    readonly abortWhen?: (tile: ImageTile) => Observable<string>;

    /**
     * Called right before a tile is put into `ERROR`, both when its request was aborted and when
     * it failed. Must not change the tile state itself.
     */
    readonly onTileError?: (tile: ImageTile) => void;

    /** Emits the aggregated state of all tiles of this loader. */
    readonly onStateChange?: (state: TileLoadState) => void;

    /** Emits the message of an exception document that the server answered instead of a tile. */
    readonly onError?: (message: string) => void;

    /** Reports the two failures a tile load can hide. Only for debugging, see {@link TileDiagnostic}. */
    readonly onDiagnostic?: (diagnostic: TileDiagnostic) => void;
}

/**
 * The extent of a tile in map units. Used to determine whether a tile request is still of interest.
 */
export const tileExtent = (tileGrid: TileGrid, tile: ImageTile): Extent => tileGrid.getTileCoordExtent(tile.getTileCoord()) as Extent;

/**
 * The extent of a tile in the projection the viewport is in, which is what
 * {@link ProjectService.createQueryAbortStream} compares against.
 *
 * The tile coordinate has to be read from the grid of the source projection. Asking for the grid of
 * the view projection silently hands out a default grid whenever the two differ, and the coordinate
 * means nothing in it: such an extent either aborts tiles that are still on screen or never aborts
 * tiles that left it.
 */
export const tileExtentInViewProjection = (
    tileGrid: TileGrid,
    tile: ImageTile,
    sourceProjection: Projection,
    viewProjection: Projection,
): Extent => {
    const extent = tileExtent(tileGrid, tile);
    return equivalent(sourceProjection, viewProjection) ? extent : (transformExtent(extent, sourceProjection, viewProjection, 8) as Extent);
};

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

    /** Pending retry timers per tile, so that an obsolete loader does not request tiles anymore. */
    private readonly retries = new Map<ImageTile, ReturnType<typeof setTimeout>>();

    /** Tiles with a request in flight or a retry waiting, which is what makes the loader `loading`. */
    private readonly outstanding = new Set<ImageTile>();

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
            this.options.onTileError?.(tile);
            tile.setState(TileState.ERROR);
            return;
        }

        const controller = new AbortController();
        this.controllers.add(controller);

        // A tile that is still counted as outstanding is the retry of a request that failed, so it
        // keeps the attempt budget of the request it follows. Any other call is a fresh visit and
        // gets a new budget.
        const retry = this.outstanding.has(tile);
        if (!retry) {
            this.attempts.delete(tile);
        }

        const wasIdle = this.outstanding.size === 0;
        this.outstanding.add(tile);
        if (wasIdle) {
            this.state('loading');
        }

        const abortSubscription = this.options.abortWhen?.(tile).subscribe((reason) => {
            this.diagnostic({event: 'aborted', tile: tile.getKey(), reason});
            controller.abort();
        });

        void this.request(tile, src, controller.signal).then((outcome) => {
            abortSubscription?.unsubscribe();
            this.controllers.delete(controller);

            if (outcome !== 'retry') {
                this.settle(tile, outcome === 'failed');
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
            // There is no retry for metadata, so the failure is final from the start.
            this.reportError(await this.readException(await response.blob()));
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

        for (const timer of this.retries.values()) {
            clearTimeout(timer);
        }
        // A tile that only waits for its retry is done without another request, and one whose
        // request was aborted is settled by that request. Both keep the loader from staying
        // `loading` forever.
        for (const tile of this.retries.keys()) {
            this.settle(tile, false);
        }
        this.retries.clear();

        for (const objectUrl of this.objectUrls) {
            URL.revokeObjectURL(objectUrl);
        }
        this.objectUrls.clear();
    }

    private async request(tile: ImageTile, src: string, signal: AbortSignal): Promise<LoadOutcome> {
        try {
            const response = await this.fetch(src, signal);
            if (!response.ok) {
                return this.fail(tile, {
                    transient: TRANSIENT_STATUSES.has(response.status),
                    exception: await this.readException(await response.blob()),
                });
            }

            const result = await this.assignImage(tile, await response.blob());
            if (result !== 'ok') {
                return this.fail(tile, result);
            }

            this.attempts.delete(tile);
            return 'ok';
        } catch {
            if (signal.aborted) {
                // The request is obsolete, but the tile may be needed again. OpenLayers only
                // re-requests `IDLE` tiles and `setState` rejects `LOADING` to `IDLE`, so the
                // tile has to pass through `ERROR`.
                this.options.onTileError?.(tile);
                tile.setState(TileState.ERROR);
                tile.setState(TileState.IDLE);
                return 'ok';
            }

            // anything that is not a refused request is a problem of the connection
            return this.fail(tile, {transient: true});
        }
    }

    /**
     * Marks a tile as failed and returns what the caller should do next. OpenLayers never
     * re-requests `ERROR` tiles, so a transient failure is retried by this loader after a delay,
     * up to {@link MAX_TILE_ATTEMPTS} tries.
     */
    private fail(tile: ImageTile, failure: Failure): LoadOutcome {
        const attempts = (this.attempts.get(tile) ?? 0) + 1;
        this.attempts.set(tile, attempts);

        // Both an abort and a failure put a tile into `ERROR`, which is what makes a parent
        // reprojection give up on it, so both have to invalidate that reprojection.
        this.options.onTileError?.(tile);
        tile.setState(TileState.ERROR);

        // A tile that was released in the meantime is gone from the display, so there is nothing
        // to finish and nothing to count.
        if (tile.getState() === TileState.EMPTY) {
            return 'ok';
        }

        if (!failure.transient || attempts >= MAX_TILE_ATTEMPTS) {
            // Reporting only once the tile is given up on keeps a failure that a retry fixes from
            // reaching the user as an error that went away on its own.
            if (failure.exception) {
                this.reportError(failure.exception);
            }
            return 'failed';
        }

        // The tile stays in `ERROR` on purpose: OpenLayers re-requests `IDLE` tiles on the
        // very next frame, which would defeat the delay. A tile that left the viewport is
        // released as `EMPTY`, so its pending retry does nothing.
        const timer = setTimeout(() => {
            this.retries.delete(tile);
            if (tile.getState() !== TileState.ERROR) {
                this.settle(tile, false);
                return;
            }
            tile.load();
        }, attempts * RETRY_DELAY);
        this.retries.set(tile, timer);
        return 'retry';
    }

    /** Fetches with authentication headers. The body of a refused request is read by its caller. */
    private async fetch(url: string, signal: AbortSignal): Promise<Response> {
        return fetch(url, {headers: this.options.authHeaders(), signal});
    }

    /** Returns how the tile should be treated after it got the given blob. */
    private async assignImage(tile: ImageTile, blob: Blob): Promise<'ok' | Failure> {
        const image = tile.getImage() as HTMLImageElement | null;
        if (!image) {
            // the tile was dropped from the cache while the request was in flight
            return 'ok';
        }

        if (!blob.type.startsWith('image/')) {
            // The WMS endpoint answers failed requests with HTTP 200 and an exception document,
            // so a successful response is not necessarily a tile.
            const exception = await this.readException(blob);
            return {transient: TRANSIENT_EXCEPTIONS.has(exception.error ?? ''), exception};
        }

        this.lastError = undefined;

        if (image.src.startsWith('blob:')) {
            URL.revokeObjectURL(image.src);
        }

        const objectUrl = URL.createObjectURL(blob);
        image.addEventListener('load', () => URL.revokeObjectURL(objectUrl), {once: true});
        image.addEventListener('error', () => URL.revokeObjectURL(objectUrl), {once: true});
        image.src = objectUrl;

        // A blob can be a valid image and still decode to nothing, which OpenLayers reports as
        // `EMPTY` and never requests again. Nothing above can see that, so it is watched here.
        if (this.options.onDiagnostic) {
            const decoded = (): void =>
                this.diagnostic({
                    event: 'decoded',
                    tile: tile.getKey(),
                    naturalWidth: image.naturalWidth,
                    naturalHeight: image.naturalHeight,
                    state: tile.getState(),
                });
            image.addEventListener('load', decoded, {once: true});
            image.addEventListener('error', decoded, {once: true});
        }

        return 'ok';
    }

    private state(state: TileLoadState): void {
        this.options.onStateChange?.(state);
    }

    /**
     * Records that a tile is done, counts a final failure, and reports the aggregate state once no
     * tile is left. A tile waiting for its retry is still outstanding, so the loader stays
     * `loading` across the backoff instead of flickering.
     */
    private settle(tile: ImageTile, failed: boolean): void {
        this.failures += failed ? 1 : 0;
        this.outstanding.delete(tile);

        if (this.outstanding.size === 0) {
            const state = this.failures > 0 ? 'error' : 'idle';
            this.failures = 0;
            this.state(state);
        }
    }

    private diagnostic(diagnostic: TileDiagnostic): void {
        this.options.onDiagnostic?.(diagnostic);
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

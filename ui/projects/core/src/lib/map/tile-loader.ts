import {Observable} from 'rxjs';

import ImageTile from 'ol/ImageTile';
import Tile from 'ol/Tile';
import TileState from 'ol/TileState';
import TileGrid from 'ol/tilegrid/TileGrid';

import {Extent} from './map.service';

export type TileLoadState = 'idle' | 'loading' | 'error';

export interface TileLoaderOptions {
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
     * Aborts all pending requests and frees all object URLs of this loader.
     */
    abortAll(): void {
        for (const controller of this.controllers) {
            controller.abort();
        }
        this.controllers.clear();

        for (const objectUrl of this.objectUrls) {
            URL.revokeObjectURL(objectUrl);
        }
        this.objectUrls.clear();
    }

    private async request(tile: ImageTile, src: string, signal: AbortSignal): Promise<boolean> {
        try {
            const response = await this.fetch(src, signal);
            return !(await this.assignImage(tile, await response.blob()));
        } catch {
            if (signal.aborted) {
                // The request is obsolete, but the tile may be needed again. OpenLayers only
                // re-requests `IDLE` tiles and `setState` rejects `LOADING` to `IDLE`, so the
                // tile has to pass through `ERROR`.
                tile.setState(TileState.ERROR);
                tile.setState(TileState.IDLE);
                return false;
            }

            tile.setState(TileState.ERROR);
            return true;
        }
    }

    private fetch(url: string, signal: AbortSignal): Promise<Response> {
        return fetch(url, {headers: this.options.authHeaders(), signal}).then(async (response) => {
            if (!response.ok) {
                await this.reportError(await response.blob());
                throw new Error(`Request failed with status ${response.status}: ${url}`);
            }
            return response;
        });
    }

    /** Returns `false` if the tile ended up in an error state and cannot be rendered. */
    private async assignImage(tile: ImageTile, blob: Blob): Promise<boolean> {
        const image = tile.getImage() as HTMLImageElement | null;
        if (!image) {
            // the tile was dropped from the cache while the request was in flight
            return true;
        }

        if (!blob.type.startsWith('image/')) {
            // The WMS endpoint answers failed requests with HTTP 200 and an exception document,
            // so a successful response is not necessarily a tile.
            tile.setState(TileState.ERROR);
            await this.reportError(blob);
            return false;
        }

        this.lastError = undefined;

        if (image.src.startsWith('blob:')) {
            URL.revokeObjectURL(image.src);
        }

        const objectUrl = URL.createObjectURL(blob);
        image.addEventListener('load', () => URL.revokeObjectURL(objectUrl), {once: true});
        image.addEventListener('error', () => URL.revokeObjectURL(objectUrl), {once: true});
        image.src = objectUrl;
        return true;
    }

    private state(state: TileLoadState): void {
        this.options.onStateChange?.(state);
    }

    /**
     * Surfaces the message of an exception document, e.g. `{"error": …, "message": …}` of the WMS
     * endpoint. Repeating the same message for every failing tile would flood the user with
     * notifications, so it is reported once until a tile loads again.
     */
    private async reportError(blob: Blob): Promise<void> {
        const body = await blob.text();
        let message = body;
        try {
            message = (JSON.parse(body) as {message?: string}).message ?? body;
        } catch {
            // not an exception document, report the raw body
        }

        if (message !== this.lastError) {
            this.lastError = message;
            this.options.onError?.(message);
        }
    }
}

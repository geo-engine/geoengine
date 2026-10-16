import ImageTile from 'ol/ImageTile';
import TileLayer from 'ol/layer/Tile';
import TileSource from 'ol/source/Tile';
import ReprojTile from 'ol/reproj/Tile';
import CanvasTileLayerRenderer from 'ol/renderer/canvas/TileLayer';
import {intersects} from 'ol/extent';
import {equivalent} from 'ol/proj';
import LRUCache from 'ol/structs/LRUCache';
import Tile from 'ol/Tile';

import {transformExtentBetweenProjections} from './tile-loader';
import {Extent} from './map.service';

/** Keeps cancelled source tiles from leaving failed or incomplete reprojections in the display cache. */
class AbortableTileLayerRenderer<L extends TileLayer> extends CanvasTileLayerRenderer<L> {
    /**
     * Discards the reprojections that were built from the given source tile.
     *
     * A `ReprojTile` treats a source tile that turns `ERROR` as final: with no usable source left
     * it goes `ERROR` itself, and with only some of them left it reprojects what it has and caches
     * the partial result as `LOADED`. Neither is ever requested again, which is what leaves a hole
     * on screen. Removing the cached tile makes the next frame build a fresh reprojection.
     *
     * Only the display cache is touched, so the source tiles themselves stay cached and are not
     * re-requested. Does nothing when the source and the view share a projection, because then
     * OpenLayers uses plain image tiles that have no reprojection to discard.
     */
    invalidateAbortedTile(tile: ImageTile): void {
        const source = this.getLayer().getSource();
        const projection = this.renderedProjection;
        // `TileImage` and its subclasses (`OGCMapTile`, `TileWMS`) are the only sources that can
        // wrap tiles in a `ReprojTile`. Checking for the methods avoids importing the deprecated
        // `TileImage` class just for an `instanceof`.
        // TODO: on the next OL major bump, check whether `TileWMS`/`OGCMapTile` still use
        // `ReprojTile`. If they switch to the `DataTileSource` tree and no longer wrap, the
        // eviction below becomes a no-op and this whole file can go.
        if (!projection || !source || !('getTileGridForProjection' in source)) {
            return;
        }

        const sourceProjection = source.getProjection();
        if (!sourceProjection || equivalent(sourceProjection, projection)) {
            return;
        }

        const sourceGrid = source.getTileGridForProjection(sourceProjection);
        const extent = transformExtentBetweenProjections(
            sourceGrid.getTileCoordExtent(tile.tileCoord) as Extent,
            sourceProjection,
            projection,
        );
        const displayGrid = source.getTileGridForProjection(projection);

        // ReprojTile treats the transient ERROR during cancellation as final. Discard its
        // cached result before notifying the source tile's listeners, so the next frame
        // constructs a new reprojection. Keep other tiles and the source cache intact.
        const cache = this.getTileCache() as LRUCache<Tile>;
        const keys: string[] = [];
        cache.forEach((cachedTile, key) => {
            if (
                cachedTile instanceof ReprojTile &&
                cachedTile.key === tile.key &&
                intersects(extent, displayGrid.getTileCoordExtent(cachedTile.tileCoord))
            ) {
                keys.push(key);
            }
        });
        for (const key of keys) {
            // pop() also resets both ends of a one-entry OpenLayers cache.
            const removed = cache.getCount() === 1 ? cache.pop() : cache.remove(key);
            removed.dispose();
        }
    }
}

export class AbortableTileLayer<S extends TileSource> extends TileLayer<S> {
    override createRenderer(): CanvasTileLayerRenderer<this> {
        return new AbortableTileLayerRenderer(this, {cacheSize: this.getCacheSize()});
    }

    invalidateAbortedTile(tile: ImageTile): void {
        if (this.hasRenderer()) {
            (this.getRenderer() as AbortableTileLayerRenderer<this>).invalidateAbortedTile(tile);
            this.changed();
        }
    }
}

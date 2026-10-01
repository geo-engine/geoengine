import ImageTile from 'ol/ImageTile';
import TileLayer from 'ol/layer/Tile';
import TileSource from 'ol/source/Tile';
import TileImageSource from 'ol/source/TileImage';
import ReprojTile from 'ol/reproj/Tile';
import CanvasTileLayerRenderer from 'ol/renderer/canvas/TileLayer';
import {intersects} from 'ol/extent';
import {equivalent, transformExtent} from 'ol/proj';
import LRUCache from 'ol/structs/LRUCache';
import Tile from 'ol/Tile';

/** Keeps cancelled source tiles from leaving failed or incomplete reprojections in the display cache. */
class AbortableTileLayerRenderer<L extends TileLayer> extends CanvasTileLayerRenderer<L> {
    invalidateAbortedTile(tile: ImageTile): void {
        const source = this.getLayer().getSource();
        const projection = this.renderedProjection;
        if (!(source instanceof TileImageSource) || !projection) {
            return;
        }
        const sourceProjection = source.getProjection();
        if (!sourceProjection || equivalent(sourceProjection, projection)) {
            return;
        }
        const sourceGrid = source.getTileGridForProjection(sourceProjection);
        const extent = transformExtent(sourceGrid.getTileCoordExtent(tile.tileCoord), sourceProjection, projection, 8);
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

import type {MapExtent} from '../layers/data-sources';

/**
 * The minimum zoom expressed as OpenLayers' exclusive maxResolution, in EPSG:3857 metres/pixel.
 * Use the entire viewport, including rotation, so either dimension can require zooming in.
 */
export function mapExtentMaxResolution(limit: MapExtent | undefined, size: readonly number[], rotation = 0): number {
    if (!limit) return Infinity;
    const [width, height] = size;
    if (!(width > 0 && height > 0)) return 0; // Wait for map layout before loading imagery.
    const cos = Math.abs(Math.cos(rotation));
    const sin = Math.abs(Math.sin(rotation));
    return Math.min(limit.width / (width * cos + height * sin), limit.height / (width * sin + height * cos));
}

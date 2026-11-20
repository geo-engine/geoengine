import OlFeature from 'ol/Feature';
import OlGeometry from 'ol/geom/Geometry';
import OlPolygon from 'ol/geom/Polygon';
import {Fill, Stroke, Style} from 'ol/style';
import {TimeStepFootprint} from '@geoengine/api-client';

/** Period and line width of the stripes of the overlay in CSS pixels. */
const STRIPE_PERIOD = 10;
const STRIPE_WIDTH = 3;

/**
 * Diagonal stripes over the faded accent color. The map stays readable through them, and boxes that
 * overlap are told apart from one big box by the stripes meeting.
 */
function stripePattern(fill: string | undefined, stripe: string, stripeAlpha: number): CanvasPattern | undefined {
    const ratio = window.devicePixelRatio || 1;
    const canvas = document.createElement('canvas');
    canvas.width = canvas.height = STRIPE_PERIOD * ratio;
    const context = canvas.getContext('2d');
    if (!context) return undefined;

    context.scale(ratio, ratio);
    if (fill) {
        context.fillStyle = fill;
        context.fillRect(0, 0, STRIPE_PERIOD, STRIPE_PERIOD);
    }
    context.globalAlpha = stripeAlpha;
    context.strokeStyle = stripe;
    context.lineWidth = STRIPE_WIDTH;
    // three segments, so the stripe is continuous across the edges of the pattern tile
    context.beginPath();
    for (const offset of [-STRIPE_PERIOD, 0, STRIPE_PERIOD]) {
        context.moveTo(offset, STRIPE_PERIOD);
        context.lineTo(offset + STRIPE_PERIOD, 0);
    }
    context.stroke();

    const pattern = context.createPattern(canvas, 'repeat');
    // the bitmap has `ratio` pixels per CSS pixel, which the map's canvas scales up by `ratio` again
    pattern?.setTransform(new DOMMatrix().scale(1 / ratio));
    return pattern ?? undefined;
}

/**
 * The theme's accent colors, which are CSS variables of the app. The light style has faint stripes without the
 * faded fill, so imagery below the boxes stays readable.
 */
export function overlayStyle(light: boolean): Style {
    const css = getComputedStyle(document.documentElement);
    const color = (name: string, fallback: string): string => css.getPropertyValue(name).trim() || fallback;
    const accent = color('--geoengine-accent-color', '#ffc107');
    const faded = color('--geoengine-faded-accent-color', 'rgba(255, 193, 7, 0.32)');
    const fill = light ? (stripePattern(undefined, accent, 0.85) ?? 'transparent') : (stripePattern(faded, accent, 1) ?? faded);
    return new Style({fill: new Fill({color: fill}), stroke: new Stroke({color: accent, width: 2})});
}

const BOX_EDGE_STEPS = 16;

/** The outline of a box with points along its edges, because a box is no rectangle in another projection. */
export function boxRing([minX, minY, maxX, maxY]: [number, number, number, number]): [number, number][] {
    const along = (from: number, to: number, i: number): number => from + ((to - from) * i) / BOX_EDGE_STEPS;
    const steps = Array.from({length: BOX_EDGE_STEPS}, (_, i) => i);
    const ring: [number, number][] = [
        ...steps.map((i): [number, number] => [along(minX, maxX, i), minY]),
        ...steps.map((i): [number, number] => [maxX, along(minY, maxY, i)]),
        ...steps.map((i): [number, number] => [along(maxX, minX, i), maxY]),
        ...steps.map((i): [number, number] => [minX, along(maxY, minY, i)]),
    ];
    return [...ring, ring[0]];
}

/**
 * The boxes of a time step as polygons in the projection of the map. They are bounding boxes, not the outlines of
 * the scenes, so they also cover edges without data and need not match the tiles exactly.
 */
export function boxFeatures(bounds: TimeStepFootprint['bounds'], fromSrs: string, toSrs: string): OlFeature<OlGeometry>[] {
    return bounds.map((box) => {
        const geometry = new OlPolygon([
            boxRing([box.upperLeftCoordinate.x, box.lowerRightCoordinate.y, box.lowerRightCoordinate.x, box.upperLeftCoordinate.y]),
        ]);
        geometry.transform(fromSrs, toSrs);
        return new OlFeature<OlGeometry>(geometry);
    });
}

/** How much of a view the boxes of a time step cover. */
export type ViewCoverage = 'full' | 'part' | 'none';

const COVERAGE_SAMPLES = 8;

/**
 * How much of `extent` the boxes cover, both in the projection of the map. Boxes overlap, so their areas cannot be added
 * up; a grid of points in the view is tested instead, which misses gaps smaller than an eighth of the view.
 */
export function viewCoverage(features: OlFeature<OlGeometry>[], extent: [number, number, number, number]): ViewCoverage {
    const geometries = features.flatMap((feature) => feature.getGeometry() ?? []).filter((geometry) => geometry.intersectsExtent(extent));
    if (!geometries.length) return 'none';
    const [minX, minY, maxX, maxY] = extent;
    for (let i = 0; i < COVERAGE_SAMPLES; i++) {
        for (let j = 0; j < COVERAGE_SAMPLES; j++) {
            const point = [minX + ((maxX - minX) * (i + 0.5)) / COVERAGE_SAMPLES, minY + ((maxY - minY) * (j + 0.5)) / COVERAGE_SAMPLES];
            if (!geometries.some((geometry) => geometry.intersectsCoordinate(point))) return 'part';
        }
    }
    return 'full';
}

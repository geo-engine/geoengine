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
function stripePattern(fill: string, stripe: string): CanvasPattern | undefined {
    const ratio = window.devicePixelRatio || 1;
    const canvas = document.createElement('canvas');
    canvas.width = canvas.height = STRIPE_PERIOD * ratio;
    const context = canvas.getContext('2d');
    if (!context) return undefined;

    context.scale(ratio, ratio);
    context.fillStyle = fill;
    context.fillRect(0, 0, STRIPE_PERIOD, STRIPE_PERIOD);
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

/** The theme's accent colors, which are CSS variables of the app. */
export function overlayStyle(): Style {
    const css = getComputedStyle(document.documentElement);
    const color = (name: string, fallback: string): string => css.getPropertyValue(name).trim() || fallback;
    const accent = color('--geoengine-accent-color', '#ffc107');
    const faded = color('--geoengine-faded-accent-color', 'rgba(255, 193, 7, 0.32)');
    return new Style({
        fill: new Fill({color: stripePattern(faded, accent) ?? faded}),
        stroke: new Stroke({color: accent, width: 2}),
    });
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

/** The boxes of a time step as polygons in the projection of the map. */
export function boxFeatures(bounds: TimeStepFootprint['bounds'], fromSrs: string, toSrs: string): OlFeature<OlGeometry>[] {
    return bounds.map((box) => {
        const geometry = new OlPolygon([
            boxRing([box.upperLeftCoordinate.x, box.lowerRightCoordinate.y, box.lowerRightCoordinate.x, box.upperLeftCoordinate.y]),
        ]);
        geometry.transform(fromSrs, toSrs);
        return new OlFeature<OlGeometry>(geometry);
    });
}

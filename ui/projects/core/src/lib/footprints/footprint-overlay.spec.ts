import OlPolygon from 'ol/geom/Polygon';
import {describe, expect, it} from 'vitest';
import {boxFeatures, boxRing} from './footprint-overlay';

describe('footprint overlay', () => {
    it('puts points along the edges of a box, from its corner around back to it', () => {
        const ring = boxRing([0, 0, 16, 8]);

        expect(ring.length).toBe(4 * 16 + 1);
        expect(ring[0]).toEqual([0, 0]);
        expect(ring[16]).toEqual([16, 0]);
        expect(ring[32]).toEqual([16, 8]);
        expect(ring[48]).toEqual([0, 8]);
        expect(ring[64]).toEqual(ring[0]);
        expect(ring.every(([x, y]) => x >= 0 && x <= 16 && y >= 0 && y <= 8)).toBe(true);
    });

    it('draws a box as one polygon with the points along its edges, in the projection of the map', () => {
        const [feature] = boxFeatures([{upperLeftCoordinate: {x: 0, y: 8}, lowerRightCoordinate: {x: 16, y: 0}}], 'EPSG:4326', 'EPSG:4326');

        const ring = (feature.getGeometry() as OlPolygon).getCoordinates()[0];
        expect(boxFeatures([], 'EPSG:4326', 'EPSG:4326')).toEqual([]);
        expect(ring.length).toBe(4 * 16 + 1);
        expect(ring[0]).toEqual([0, 0]);
        expect(ring[32]).toEqual([16, 8]);
    });
});

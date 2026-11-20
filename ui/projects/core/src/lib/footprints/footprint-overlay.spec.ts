import OlPolygon from 'ol/geom/Polygon';
import {describe, expect, it} from 'vitest';
import {boxFeatures, boxRing, viewCoverage} from './footprint-overlay';

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

    it('tells whether boxes cover a view, a part of it or nothing of it', () => {
        const box = (
            west: number,
            east: number,
        ): {upperLeftCoordinate: {x: number; y: number}; lowerRightCoordinate: {x: number; y: number}} => ({
            upperLeftCoordinate: {x: west, y: 10},
            lowerRightCoordinate: {x: east, y: 0},
        });
        const coverage = (...boxes: ReturnType<typeof box>[]): string =>
            viewCoverage(boxFeatures(boxes, 'EPSG:4326', 'EPSG:4326'), [0, 0, 10, 10]);

        expect(coverage()).toBe('none');
        expect(coverage(box(20, 30))).toBe('none');
        expect(coverage(box(-5, 5))).toBe('part');
        // two overlapping boxes cover the view together
        expect(coverage(box(-5, 6), box(4, 15))).toBe('full');
    });
});

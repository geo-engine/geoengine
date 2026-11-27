import View from 'ol/View';
import {describe, expect, it} from 'vitest';
import {mapExtentMaxResolution, zoomBelowResolution} from './map-extent';

describe('maximum map extent', () => {
    const germany = {width: 1021468, height: 1387069};

    it('limits landscape maps by width and portrait maps by height', () => {
        expect(mapExtentMaxResolution(germany, [1600, 900])).toBe(germany.width / 1600);
        expect(mapExtentMaxResolution(germany, [600, 1200])).toBe(germany.height / 1200);
    });

    it('accounts for the bounding box of a rotated viewport', () => {
        const limit = {width: 1000000, height: 2000000};
        expect(mapExtentMaxResolution(limit, [1600, 900], Math.PI / 2)).toBeCloseTo(1000000 / 900);
        expect(mapExtentMaxResolution(limit, [1600, 900], Math.PI / 4)).toBeCloseTo(1000000 / (2500 / Math.sqrt(2)));
    });

    it('blocks until the map has a size and allows sources without metadata', () => {
        expect(mapExtentMaxResolution(germany, [0, 0])).toBe(0);
        expect(mapExtentMaxResolution(germany, [100, 0])).toBe(0);
        expect(mapExtentMaxResolution(undefined, [100, 100])).toBe(Infinity);
    });

    it('zooms in to the first whole level below the limit, which is exclusive', () => {
        const view = new View({projection: 'EPSG:3857', zoom: 5});
        const at = (zoom: number): number => view.getResolutionForZoom(zoom);
        // between levels 7 and 8: level 8 loads
        expect(zoomBelowResolution(view, (at(7) + at(8)) / 2)).toBe(8);
        // exactly at level 8: it does not load yet, level 9 does
        expect(zoomBelowResolution(view, at(8))).toBe(9);
        expect(zoomBelowResolution(view, Infinity)).toBeUndefined();
        expect(zoomBelowResolution(view, 0)).toBeUndefined();
    });
});

import {describe, expect, it} from 'vitest';
import {mapExtentMaxResolution} from './map-extent';

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
});

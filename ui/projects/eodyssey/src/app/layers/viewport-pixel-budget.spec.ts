import TileGrid from 'ol/tilegrid/TileGrid';
import {describe, expect, it} from 'vitest';
import {parseViewportPixelBudget, viewportMaxResolution, ViewportPixelBudget} from './viewport-pixel-budget';

const budget: ViewportPixelBudget = {
    version: 1,
    tileMatrixSet: 'CustomWebMercator',
    crs: 'EPSG:3857',
    maxSourcePixels: 80_000_000,
    matrices: [{id: '0', cellSize: 4000, tileWidth: 128, tileHeight: 128, matrixWidth: 100, matrixHeight: 100, sourcePixels: 8_000_000}],
};

describe('viewport pixel budget', () => {
    it('allows a Germany-sized view and blocks a larger view using the same matrix', () => {
        const grid = new TileGrid({origin: [0, 0], resolutions: [4000], tileSize: 128});
        const germanyResolution = 500;
        const europeResolution = 2000;
        expect(grid.getZForResolution(germanyResolution)).toBe(grid.getZForResolution(europeResolution));
        const cutoff = viewportMaxResolution(budget, [1000, 1000]);
        // At 1024 m/pixel the view can intersect 3×3 tiles = 72M pixels. Any larger
        // view may intersect 4×4 tiles = 128M. The cutoff is exclusive and safe to reuse.
        expect(cutoff).toBeCloseTo(1024, 8);
        expect(germanyResolution).toBeLessThan(cutoff);
        expect(europeResolution).toBeGreaterThan(cutoff);
    });

    it('recalculates for map size and includes full partially visible edge tiles', () => {
        expect(viewportMaxResolution(budget, [2000, 2000])).toBeCloseTo(512, 8);
        // A tiny shift of a one-tile-wide view can need two tiles on each axis.
        expect(viewportMaxResolution({...budget, maxSourcePixels: 3 * 8_000_000}, [128, 128])).toBe(0);
        expect(viewportMaxResolution(budget, [0, 500])).toBe(0);
        expect(viewportMaxResolution(budget, [500, Number.NaN])).toBe(0);
    });

    it('caps the count at the matrix dimensions when the whole dataset is cheap enough', () => {
        const small = {...budget, matrices: [{...budget.matrices[0], matrixWidth: 2, matrixHeight: 3}]};
        expect(viewportMaxResolution(small, [1000, 1000])).toBe(Infinity);
    });

    it('does not reopen zoomed-out views after a cheaper coarser-asset transition', () => {
        const nonMonotonic: ViewportPixelBudget = {
            ...budget,
            maxSourcePixels: 10,
            matrices: [4, 2, 1].map((cellSize, index) => ({
                ...budget.matrices[0],
                id: String(index),
                cellSize,
                matrixWidth: 1,
                matrixHeight: 1,
                sourcePixels: index === 1 ? 100 : 1,
            })),
        };
        expect(viewportMaxResolution(nonMonotonic, [1000, 1000])).toBe(1.5);
    });

    it('accepts the bootstrap contract and leaves legacy catalogues unchanged', () => {
        expect(parseViewportPixelBudget(JSON.stringify(budget))).toEqual(budget);
        expect(parseViewportPixelBudget(undefined)).toBeUndefined();
    });

    it.each([
        '',
        'null',
        '{}',
        JSON.stringify({...budget, version: 2}),
        JSON.stringify({...budget, crs: 'EPSG:4326'}),
        JSON.stringify({...budget, maxSourcePixels: 0}),
        JSON.stringify({...budget, maxSourcePixels: Number.MAX_SAFE_INTEGER + 1}),
        JSON.stringify({...budget, matrices: []}),
        JSON.stringify({...budget, matrices: [{...budget.matrices[0], id: '1'}]}),
        JSON.stringify({...budget, matrices: [{...budget.matrices[0], sourcePixels: -1}]}),
        JSON.stringify({...budget, matrices: [{...budget.matrices[0], tileWidth: 1.5}]}),
        JSON.stringify({...budget, matrices: [budget.matrices[0], {...budget.matrices[0], id: '1'}]}),
    ])('rejects invalid viewport metadata: %s', (value) => {
        expect(() => parseViewportPixelBudget(value)).toThrow('Invalid edv:viewportPixelBudget');
    });
});

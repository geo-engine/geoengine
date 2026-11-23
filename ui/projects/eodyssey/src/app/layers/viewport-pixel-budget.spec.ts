import TileGrid from 'ol/tilegrid/TileGrid';
import {describe, expect, it} from 'vitest';
import {
    estimatedStacSearches,
    parseViewportLimits,
    parseViewportPixelBudget,
    viewportMaxResolution,
    ViewportPixelBudget,
} from './viewport-pixel-budget';

const budget: ViewportPixelBudget = {
    tileMatrixSet: 'CustomWebMercator',
    crs: 'EPSG:3857',
    maxSourcePixels: 80_000_000,
    maxStacSearches: 10_000,
    searchGrids: [
        {
            datasetId: 'one',
            projection: 'EPSG:3857',
            projString: 'EPSG:3857',
            extent: [0, -51200000, 51200000, 0],
            cellSize: 512000,
            columns: 100,
            rows: 100,
        },
    ],
    matrices: [
        {
            id: '0',
            cellSize: 4000,
            tileWidth: 128,
            tileHeight: 128,
            matrixWidth: 100,
            matrixHeight: 100,
            sourcePixels: 8_000_000,
            pointOfOrigin: [0, 0],
        },
    ],
};

const dualBudget: ViewportPixelBudget = {
    tileMatrixSet: 'CustomWebMercator',
    crs: 'EPSG:3857',
    maxSourcePixels: 1_000_000,
    maxStacSearches: 36,
    matrices: [4, 2, 1].map((cellSize, index) => ({
        id: String(index),
        cellSize,
        tileWidth: 128,
        tileHeight: 128,
        matrixWidth: 4096 / (128 * cellSize),
        matrixHeight: 4096 / (128 * cellSize),
        pointOfOrigin: [-2048, 2048],
        sourcePixels: 1,
    })),
    searchGrids: [
        {
            datasetId: 'dataset/one',
            projection: 'EPSG:3857',
            projString: 'EPSG:3857',
            extent: [-2048, -2048, 2048, 2048],
            cellSize: 256,
            columns: 16,
            rows: 16,
        },
    ],
};

describe('viewport pixel budget', () => {
    it('blocks excessive STAC cell searches even when the pixel budget fits', () => {
        const cutoff = viewportMaxResolution(dualBudget, [512, 512], [0, 0]);
        expect(cutoff).toBeCloseTo(2, 8);
        expect(estimatedStacSearches(dualBudget, dualBudget.matrices[1], [512, 512], [0, 0], 1.99)).toBe(36);
        expect(estimatedStacSearches(dualBudget, dualBudget.matrices[1], [512, 512], [0, 0], 2.01)).toBeGreaterThan(36);
        expect(viewportMaxResolution({...dualBudget, maxSourcePixels: 1}, [512, 512], [0, 0])).toBe(0);
    });

    it('keeps every finer matrix and intermediate resolution within both limits', () => {
        const grid = new TileGrid({origin: [-2048, 2048], resolutions: [4, 2, 1], tileSize: 128});
        for (const center of [
            [0, 0],
            [400, 700],
            [-1900, 1700],
        ]) {
            const cutoff = viewportMaxResolution(dualBudget, [512, 512], center);
            for (let step = 1; step < 200; step++) {
                const resolution = (cutoff * step) / 200;
                const matrix = dualBudget.matrices[grid.getZForResolution(resolution)];
                expect(estimatedStacSearches(dualBudget, matrix, [512, 512], center, resolution)).toBeLessThanOrEqual(
                    dualBudget.maxStacSearches,
                );
            }
        }
    });

    it('coalesces overlapping tile searches but counts distinct datasets separately', () => {
        const repeated = {
            ...dualBudget,
            searchGrids: [dualBudget.searchGrids[0], {...dualBudget.searchGrids[0], datasetId: 'dataset/two'}],
        };
        expect(estimatedStacSearches(repeated, repeated.matrices[1], [512, 512], [0, 0], 1.99)).toBe(72);
        expect(viewportMaxResolution(repeated, [512, 512], [0, 0])).toBeLessThan(viewportMaxResolution(dualBudget, [512, 512], [0, 0]));
        expect(viewportMaxResolution(dualBudget, [512, 512], undefined)).toBe(0);
    });

    it('does not allow unbounded zoom-out when the map center is outside the dataset', () => {
        const center = [1_000_000, 1_000_000];
        const cutoff = viewportMaxResolution(dualBudget, [512, 512], center);
        expect(Number.isFinite(cutoff)).toBe(true);
        expect(estimatedStacSearches(dualBudget, dualBudget.matrices[0], [512, 512], center, cutoff * 0.99)).toBeLessThanOrEqual(36);
        expect(estimatedStacSearches(dualBudget, dualBudget.matrices[0], [512, 512], center, cutoff * 1.01)).toBeGreaterThan(36);
    });

    it('projects a complete viewport into the native LAEA search grid', () => {
        const projected: ViewportPixelBudget = {
            ...dualBudget,
            maxStacSearches: 64,
            matrices: [{...budget.matrices[0], sourcePixels: 1, pointOfOrigin: [-20037508.343, 20037508.343]}],
            searchGrids: [
                {
                    datasetId: 'LAEA',
                    projection: 'EPSG:3035',
                    projString: '+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000 +ellps=GRS80 +units=m +no_defs',
                    extent: [-146473.113, 196462.701, 7824928.564, 6971477.485],
                    cellSize: 649557.326490495,
                    columns: 13,
                    rows: 11,
                },
            ],
        };
        const center = [1224848, 6649262];
        const searches = (resolution: number): number =>
            estimatedStacSearches(projected, projected.matrices[0], [1000, 1000], center, resolution);
        expect(searches(500)).toBeGreaterThan(0);
        expect(searches(500)).toBeLessThanOrEqual(64);
        expect(searches(8000)).toBeGreaterThan(64);
        const cutoff = viewportMaxResolution(projected, [1000, 1000], center);
        for (let step = 1; step < 100; step++) expect(searches((cutoff * step) / 100)).toBeLessThanOrEqual(64);
    });

    it('reads global limits from collection metadata and rejects incomplete cost models', () => {
        const limits = {maxSourcePixels: 1_000_000, maxStacSearches: 36};
        const {maxSourcePixels: _pixels, maxStacSearches: _searches, ...payload} = dualBudget;
        expect(parseViewportLimits(JSON.stringify(limits))).toEqual(limits);
        expect(parseViewportPixelBudget(JSON.stringify(payload), limits)).toEqual(dualBudget);
        expect(() => parseViewportPixelBudget(undefined, limits)).toThrow('Missing edv:viewportPixelBudget');
        expect(() => parseViewportPixelBudget(JSON.stringify({...payload, searchGrids: undefined}), limits)).toThrow(
            'Invalid edv:viewportPixelBudget',
        );
        expect(() => parseViewportPixelBudget(JSON.stringify(payload))).toThrow('Invalid edv:viewportPixelBudget');
        expect(() => parseViewportLimits('{"maxSourcePixels":100,"maxStacSearches":0}')).toThrow('Invalid edv:viewportLimits');
        expect(() =>
            parseViewportPixelBudget(JSON.stringify({...payload, searchGrids: [payload.searchGrids[0], payload.searchGrids[0]]}), limits),
        ).toThrow();
    });
    it('allows a Germany-sized view and blocks a larger view using the same matrix', () => {
        const grid = new TileGrid({origin: [0, 0], resolutions: [4000], tileSize: 128});
        const germanyResolution = 500;
        const europeResolution = 2000;
        expect(grid.getZForResolution(germanyResolution)).toBe(grid.getZForResolution(europeResolution));
        const cutoff = viewportMaxResolution(budget, [1000, 1000], [0, 0]);
        // At 1024 m/pixel the view can intersect 3×3 tiles = 72M pixels. Any larger
        // view may intersect 4×4 tiles = 128M. The cutoff is exclusive and safe to reuse.
        expect(cutoff).toBeCloseTo(1024, 8);
        expect(germanyResolution).toBeLessThan(cutoff);
        expect(europeResolution).toBeGreaterThan(cutoff);
    });

    it('recalculates for map size and includes full partially visible edge tiles', () => {
        expect(viewportMaxResolution(budget, [2000, 2000], [0, 0])).toBeCloseTo(512, 8);
        // A tiny shift of a one-tile-wide view can need two tiles on each axis.
        expect(viewportMaxResolution({...budget, maxSourcePixels: 3 * 8_000_000}, [128, 128], [0, 0])).toBe(0);
        expect(viewportMaxResolution(budget, [0, 500], [0, 0])).toBe(0);
        expect(viewportMaxResolution(budget, [500, Number.NaN], [0, 0])).toBe(0);
    });

    it('caps the count at the matrix dimensions when the whole dataset is cheap enough', () => {
        const small = {...budget, matrices: [{...budget.matrices[0], matrixWidth: 2, matrixHeight: 3}]};
        expect(viewportMaxResolution(small, [1000, 1000], [0, 0])).toBe(Infinity);
    });

    it('uses the same budget for expensive and cheap datasets and permits cheaper larger views', () => {
        const cheap = {...budget, matrices: [{...budget.matrices[0], sourcePixels: 80_000}]};
        const europeResolution = 2000;
        expect(cheap.maxSourcePixels).toBe(budget.maxSourcePixels);
        expect(viewportMaxResolution(budget, [1000, 1000], [0, 0])).toBeLessThan(europeResolution);
        expect(viewportMaxResolution(cheap, [1000, 1000], [0, 0])).toBeGreaterThan(europeResolution);
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
        expect(viewportMaxResolution(nonMonotonic, [1000, 1000], [0, 0])).toBe(1.5);
    });

    it.each([0, Number.MAX_SAFE_INTEGER + 1])('rejects an invalid global pixel limit: %s', (maxSourcePixels) => {
        expect(() => parseViewportLimits(JSON.stringify({maxSourcePixels, maxStacSearches: 64}))).toThrow('Invalid edv:viewportLimits');
    });

    it('accepts the unversioned bootstrap contract', () => {
        expect(
            parseViewportPixelBudget(JSON.stringify(budget), {
                maxSourcePixels: budget.maxSourcePixels,
                maxStacSearches: budget.maxStacSearches,
            }),
        ).toEqual(budget);
        expect(parseViewportPixelBudget(undefined)).toBeUndefined();
    });

    it.each([
        '',
        'null',
        '{}',
        JSON.stringify({...budget, searchGrids: []}),
        JSON.stringify({...budget, searchGrids: undefined}),
        JSON.stringify({...budget, matrices: [{...budget.matrices[0], pointOfOrigin: undefined}]}),
        JSON.stringify({...budget, crs: 'EPSG:4326'}),
        JSON.stringify({...budget, matrices: []}),
        JSON.stringify({...budget, matrices: [{...budget.matrices[0], id: '1'}]}),
        JSON.stringify({...budget, matrices: [{...budget.matrices[0], sourcePixels: -1}]}),
        JSON.stringify({...budget, matrices: [{...budget.matrices[0], tileWidth: 1.5}]}),
        JSON.stringify({...budget, matrices: [budget.matrices[0], {...budget.matrices[0], id: '1'}]}),
    ])('rejects invalid viewport metadata: %s', (value) => {
        expect(() =>
            parseViewportPixelBudget(value, {maxSourcePixels: budget.maxSourcePixels, maxStacSearches: budget.maxStacSearches}),
        ).toThrow('Invalid edv:viewportPixelBudget');
    });
});

import proj4 from 'proj4';

export interface ViewportLimits {
    maxSourcePixels: number;
    maxStacSearches: number;
}

/** Cost model combined with the global limits from EDV collection metadata. */
export interface ViewportPixelBudget {
    tileMatrixSet: 'CustomWebMercator';
    crs: 'EPSG:3857';
    maxSourcePixels: number;
    maxStacSearches: number;
    searchGrids: SearchGrid[];
    matrices: TilePixelCost[];
}

interface TilePixelCost {
    id: string;
    cellSize: number;
    tileWidth: number;
    tileHeight: number;
    matrixWidth: number;
    matrixHeight: number;
    sourcePixels: number;
    pointOfOrigin: number[];
}

interface SearchGrid {
    datasetId: string;
    projection: string;
    projString: string;
    extent: number[];
    cellSize: number;
    columns: number;
    rows: number;
}

export function parseViewportLimits(value: string | undefined): ViewportLimits | undefined {
    if (value === undefined) return undefined;
    try {
        const limits: unknown = JSON.parse(value);
        if (!isRecord(limits) || !positiveInteger(limits.maxSourcePixels) || !positiveInteger(limits.maxStacSearches)) throw new Error();
        return {maxSourcePixels: limits.maxSourcePixels, maxStacSearches: limits.maxStacSearches};
    } catch {
        throw new Error('Invalid edv:viewportLimits');
    }
}

export function parseViewportPixelBudget(value: string | undefined, limits?: ViewportLimits): ViewportPixelBudget | undefined {
    if (value === undefined) {
        if (limits) throw new Error('Missing edv:viewportPixelBudget for collection limits');
        return undefined;
    }
    try {
        const budget: unknown = JSON.parse(value);
        if (!isRecord(budget) || budget.tileMatrixSet !== 'CustomWebMercator' || budget.crs !== 'EPSG:3857') {
            throw new Error('unsupported grid');
        }
        if (!Array.isArray(budget.matrices) || !budget.matrices.length) {
            throw new Error('missing matrices');
        }
        let previousCellSize = Infinity;
        for (const [index, matrix] of budget.matrices.entries()) {
            if (
                !isRecord(matrix) ||
                matrix.id !== String(index) ||
                !positiveNumber(matrix.cellSize) ||
                matrix.cellSize >= previousCellSize ||
                !positiveInteger(matrix.tileWidth) ||
                !positiveInteger(matrix.tileHeight) ||
                !positiveInteger(matrix.matrixWidth) ||
                !positiveInteger(matrix.matrixHeight) ||
                !positiveNumber(matrix.sourcePixels) ||
                !coordinates(matrix.pointOfOrigin, 2)
            ) {
                throw new Error('invalid matrix');
            }
            previousCellSize = matrix.cellSize;
        }
        if (!limits || !Array.isArray(budget.searchGrids) || !budget.searchGrids.length) throw new Error('missing limits or search grids');
        const ids = new Set<string>();
        for (const grid of budget.searchGrids) {
            if (
                !isRecord(grid) ||
                typeof grid.datasetId !== 'string' ||
                !grid.datasetId ||
                ids.has(grid.datasetId) ||
                typeof grid.projection !== 'string' ||
                !grid.projection ||
                typeof grid.projString !== 'string' ||
                !grid.projString ||
                !coordinates(grid.extent, 4) ||
                grid.extent[0] >= grid.extent[2] ||
                grid.extent[1] >= grid.extent[3] ||
                !positiveNumber(grid.cellSize) ||
                !positiveInteger(grid.columns) ||
                !positiveInteger(grid.rows) ||
                grid.columns !== Math.ceil((grid.extent[2] - grid.extent[0]) / grid.cellSize) ||
                grid.rows !== Math.ceil((grid.extent[3] - grid.extent[1]) / grid.cellSize) ||
                !Number.isSafeInteger(grid.columns * grid.rows)
            )
                throw new Error('invalid STAC grid');
            // Resolve the definition before making the preset available for Apply.
            nativeProjector(grid.projString);
            ids.add(grid.datasetId);
        }
        return {...budget, ...limits} as unknown as ViewportPixelBudget;
    } catch {
        throw new Error('Invalid edv:viewportPixelBudget');
    }
}

/**
 * Exclusive resolution cutoff for a map size and center, independent of map zoom numbers.
 * Count full edge tiles conservatively for any alignment. Scan from fine to coarse so switches
 * to cheaper files cannot reopen a more distant view after an intermediate view exceeds budget.
 * EOdyssey disables map rotation. Matrix selection uses OGCMapTile's default nearest resolution.
 */
export function viewportMaxResolution(budget: ViewportPixelBudget, size: readonly number[], center: readonly number[] | undefined): number {
    const [width, height] = size;
    if (!positiveNumber(width) || !positiveNumber(height)) return 0;
    if (center?.length !== 2 || !center.every(Number.isFinite)) return 0;
    const matrices = budget.matrices;
    for (let index = matrices.length - 1; index >= 0; index--) {
        const matrix = matrices[index];
        const lower = index === matrices.length - 1 ? 0 : (matrix.cellSize + matrices[index + 1].cellSize) / 2;
        const matrixWidth = matrix.matrixWidth * matrix.tileWidth * matrix.cellSize;
        const matrixHeight = matrix.matrixHeight * matrix.tileHeight * matrix.cellSize;
        // The coarsest interval has no upper end. Inspect a view covering the entire
        // matrix even when its center is far outside the dataset, before allowing Infinity.
        const fullWidth =
            2 * Math.max(Math.abs(center[0] - matrix.pointOfOrigin[0]), Math.abs(center[0] - matrix.pointOfOrigin[0] - matrixWidth));
        const fullHeight =
            2 * Math.max(Math.abs(center[1] - matrix.pointOfOrigin[1]), Math.abs(center[1] - matrix.pointOfOrigin[1] + matrixHeight));
        const upper =
            index === 0 ? Math.max(lower, fullWidth / width, fullHeight / height) : (matrix.cellSize + matrices[index - 1].cellSize) / 2;
        const fits = (resolution: number): boolean =>
            estimatedViewPixels(matrix, size, resolution) <= budget.maxSourcePixels &&
            estimatedStacSearches(budget, matrix, size, center, resolution) <= budget.maxStacSearches;
        if (!fits(lower)) return lower;
        if (fits(upper)) continue;

        // Cost is monotonic within this matrix's selection interval, though it need not be
        // monotonic across coarser-asset transitions. Return the safe side of the boundary.
        let safe = lower;
        let unsafe = upper;
        for (let iteration = 0; iteration < 60; iteration++) {
            const middle = (safe + unsafe) / 2;
            if (fits(middle)) safe = middle;
            else unsafe = middle;
        }
        return safe;
    }
    return Infinity;
}

/** Cold logical searches: distinct dataset/cell pairs for the displayed time step. */
export function estimatedStacSearches(
    budget: ViewportPixelBudget,
    matrix: TilePixelCost,
    size: readonly number[],
    center: readonly number[],
    resolution: number,
): number {
    const [ox, oy] = matrix.pointOfOrigin;
    const sx = matrix.tileWidth * matrix.cellSize;
    const sy = matrix.tileHeight * matrix.cellSize;
    const left = Math.max(0, Math.floor((center[0] - (size[0] * resolution) / 2 - ox) / sx));
    const right = Math.min(matrix.matrixWidth, Math.ceil((center[0] + (size[0] * resolution) / 2 - ox) / sx));
    const top = Math.max(0, Math.floor((oy - center[1] - (size[1] * resolution) / 2) / sy));
    const bottom = Math.min(matrix.matrixHeight, Math.ceil((oy - center[1] + (size[1] * resolution) / 2) / sy));
    if (left >= right || top >= bottom) return 0;
    let cache = searchCounts.get(budget);
    if (!cache) {
        cache = new Map<string, number>();
        searchCounts.set(budget, cache);
    }
    const key = `${matrix.id}/${left}/${right}/${top}/${bottom}`;
    const cached = cache.get(key);
    if (cached !== undefined) return cached;
    const extent = [ox + left * sx, oy - bottom * sy, ox + right * sx, oy - top * sy];
    const count = budget.searchGrids.reduce((sum, grid) => sum + gridSearches(grid, extent), 0);
    if (cache.size >= 256) cache.clear();
    cache.set(key, count);
    return count;
}

// Bisection and repeated pans often cover the same tile rectangle; project it once.
const searchCounts = new WeakMap<ViewportPixelBudget, Map<string, number>>();
const projectors = new Map<string, (x: number, y: number) => readonly number[]>();

function nativeProjector(definition: string): (x: number, y: number) => readonly number[] {
    let project = projectors.get(definition);
    if (!project) {
        const converter = proj4('EPSG:3857', definition);
        project = (x: number, y: number): readonly number[] => converter.forward([x, y]);
        projectors.set(definition, project);
    }
    return project;
}

function gridSearches(grid: SearchGrid, extent: number[]): number {
    const project =
        grid.projection === 'EPSG:3857' ? (x: number, y: number): readonly number[] => [x, y] : nativeProjector(grid.projString);
    const bounds = [Infinity, Infinity, -Infinity, -Infinity];
    // Project the full border, not just the corners, for curved UTM/LAEA boundaries.
    // A complete native-cell halo protects numerical borders and query padding.
    for (let step = 0; step <= 64; step++) {
        const x = extent[0] + ((extent[2] - extent[0]) * step) / 64;
        const y = extent[1] + ((extent[3] - extent[1]) * step) / 64;
        for (const [px, py] of [
            [x, extent[1]],
            [x, extent[3]],
            [extent[0], y],
            [extent[2], y],
        ]) {
            let point: readonly number[];
            try {
                point = project(px, py);
            } catch {
                return grid.columns * grid.rows;
            }
            if (!point.every(Number.isFinite)) return grid.columns * grid.rows;
            bounds[0] = Math.min(bounds[0], point[0]);
            bounds[1] = Math.min(bounds[1], point[1]);
            bounds[2] = Math.max(bounds[2], point[0]);
            bounds[3] = Math.max(bounds[3], point[1]);
        }
    }
    const [x0, y0, x1, y1] = grid.extent;
    const side = grid.cellSize;
    const firstColumn = Math.max(0, Math.floor((bounds[0] - side - x0) / side));
    const lastColumn = Math.min(grid.columns, Math.ceil((bounds[2] + side - x0) / side));
    const firstRow = Math.max(0, Math.floor((y1 - bounds[3] - side) / side));
    const lastRow = Math.min(grid.rows, Math.ceil((y1 - bounds[1] + side) / side));
    if (bounds[2] + side < x0 || bounds[0] - side > x1 || bounds[3] + side < y0 || bounds[1] - side > y1) return 0;
    return Math.max(0, lastColumn - firstColumn) * Math.max(0, lastRow - firstRow);
}

function coordinates(value: unknown, length: number): value is number[] {
    return Array.isArray(value) && value.length === length && value.every((v) => typeof v === 'number' && Number.isFinite(v));
}

function estimatedViewPixels(matrix: TilePixelCost, size: readonly number[], resolution: number): number {
    const columns = Math.min(matrix.matrixWidth, Math.ceil((size[0] * resolution) / (matrix.tileWidth * matrix.cellSize)) + 1);
    const rows = Math.min(matrix.matrixHeight, Math.ceil((size[1] * resolution) / (matrix.tileHeight * matrix.cellSize)) + 1);
    return columns * rows * matrix.sourcePixels;
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function positiveNumber(value: unknown): value is number {
    return typeof value === 'number' && Number.isFinite(value) && value > 0;
}

function positiveInteger(value: unknown): value is number {
    return positiveNumber(value) && Number.isSafeInteger(value);
}

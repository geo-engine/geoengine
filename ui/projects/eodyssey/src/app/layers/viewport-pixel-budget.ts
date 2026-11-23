/** Version 1 of bootstrap2's edv:viewportPixelBudget catalogue property. */
export interface ViewportPixelBudget {
    version: 1;
    tileMatrixSet: 'CustomWebMercator';
    crs: 'EPSG:3857';
    maxSourcePixels: number;
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
}

export function parseViewportPixelBudget(value: string | undefined): ViewportPixelBudget | undefined {
    if (value === undefined) return undefined;
    try {
        const budget: unknown = JSON.parse(value);
        if (!isRecord(budget) || budget.version !== 1 || budget.tileMatrixSet !== 'CustomWebMercator' || budget.crs !== 'EPSG:3857') {
            throw new Error('unsupported version or grid');
        }
        if (!positiveInteger(budget.maxSourcePixels) || !Array.isArray(budget.matrices) || !budget.matrices.length) {
            throw new Error('missing budget or matrices');
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
                !positiveNumber(matrix.sourcePixels)
            ) {
                throw new Error('invalid matrix');
            }
            previousCellSize = matrix.cellSize;
        }
        return budget as unknown as ViewportPixelBudget;
    } catch {
        throw new Error('Invalid edv:viewportPixelBudget');
    }
}

/**
 * Exclusive resolution cutoff for a map size, independent of its center and map zoom numbers.
 * Count full edge tiles conservatively for any alignment. Scan from fine to coarse so switches
 * to cheaper files cannot reopen a more distant view after an intermediate view exceeds budget.
 * EOdyssey disables map rotation. Matrix selection uses OGCMapTile's default nearest resolution.
 */
export function viewportMaxResolution(budget: ViewportPixelBudget, size: readonly number[]): number {
    const [width, height] = size;
    if (!positiveNumber(width) || !positiveNumber(height)) return 0;
    const matrices = budget.matrices;
    for (let index = matrices.length - 1; index >= 0; index--) {
        const matrix = matrices[index];
        const lower = index === matrices.length - 1 ? 0 : (matrix.cellSize + matrices[index + 1].cellSize) / 2;
        const upper =
            index === 0
                ? Math.max(
                      lower,
                      (matrix.matrixWidth * matrix.tileWidth * matrix.cellSize) / width,
                      (matrix.matrixHeight * matrix.tileHeight * matrix.cellSize) / height,
                  )
                : (matrix.cellSize + matrices[index - 1].cellSize) / 2;
        const fits = (resolution: number): boolean => estimatedViewPixels(matrix, size, resolution) <= budget.maxSourcePixels;
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

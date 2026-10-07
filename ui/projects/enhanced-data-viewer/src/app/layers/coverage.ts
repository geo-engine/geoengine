export interface GeographicCenter {
    longitude: number;
    latitude: number;
}

export type GeographicCoverage = {type: 'Polygon'; coordinates: number[][][]} | {type: 'MultiPolygon'; coordinates: number[][][][]};

/** Parse and validate the JSON GeoJSON value used by edv:coverage. */
export function parseCoverage(value: string | undefined): GeographicCoverage | undefined {
    if (!value) return undefined;
    try {
        const geometry: unknown = JSON.parse(value);
        if (!isRecord(geometry) || (geometry.type !== 'Polygon' && geometry.type !== 'MultiPolygon')) return undefined;
        const polygons: unknown[] =
            geometry.type === 'Polygon' ? [geometry.coordinates] : Array.isArray(geometry.coordinates) ? geometry.coordinates : [];
        if (!polygons.length) return undefined;
        for (const polygon of polygons) {
            if (!Array.isArray(polygon) || !polygon.length) return undefined;
            for (const ring of polygon) {
                if (!Array.isArray(ring) || ring.length < 4) return undefined;
                const positions: number[][] = [];
                for (const position of ring) {
                    if (!Array.isArray(position) || position.length < 2) return undefined;
                    const longitude: unknown = position[0];
                    const latitude: unknown = position[1];
                    if (typeof longitude !== 'number' || !Number.isFinite(longitude) || longitude < -180 || longitude > 180)
                        return undefined;
                    if (typeof latitude !== 'number' || !Number.isFinite(latitude) || latitude < -90 || latitude > 90) return undefined;
                    const previous = positions[positions.length - 1];
                    if (previous?.[0] === longitude && previous?.[1] === latitude) return undefined;
                    positions.push([longitude, latitude]);
                }
                const first = positions[0];
                const last = positions[positions.length - 1];
                if (first[0] !== last[0] || first[1] !== last[1]) return undefined;
                const twiceArea = positions.slice(1).reduce((area, position, index) => {
                    const previous = positions[index];
                    return area + previous[0] * position[1] - position[0] * previous[1];
                }, 0);
                if (Math.abs(twiceArea) <= 1e-12) return undefined;
            }
        }
        return geometry as GeographicCoverage;
    } catch {
        return undefined;
    }
}

/** Boundary points count as covered; holes exclude their interior but include their boundary. */
export function coverageContains(coverage: GeographicCoverage, center: GeographicCenter): boolean {
    if (!Number.isFinite(center.longitude) || !Number.isFinite(center.latitude) || center.latitude < -90 || center.latitude > 90)
        return false;
    const longitude = normalizeLongitude(center.longitude);
    const longitudes = Math.abs(longitude) === 180 ? [longitude, -longitude] : [longitude];
    const polygons = coverage.type === 'Polygon' ? [coverage.coordinates] : coverage.coordinates;
    return longitudes.some((candidateLongitude) =>
        polygons.some((rings) => {
            const outer = pointInRing(candidateLongitude, center.latitude, rings[0]);
            if (outer === 'outside') return false;
            return !rings.slice(1).some((hole) => pointInRing(candidateLongitude, center.latitude, hole) === 'inside');
        }),
    );
}

export function normalizeLongitude(longitude: number): number {
    const normalized = ((((longitude + 180) % 360) + 360) % 360) - 180;
    return normalized === -180 && longitude > 0 ? 180 : normalized;
}

function pointInRing(longitude: number, latitude: number, ring: number[][]): 'inside' | 'outside' | 'boundary' {
    let inside = false;
    for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
        const [xi, yi] = ring[i];
        const [xj, yj] = ring[j];
        if (onSegment(longitude, latitude, xi, yi, xj, yj)) return 'boundary';
        if (yi > latitude !== yj > latitude && longitude < ((xj - xi) * (latitude - yi)) / (yj - yi) + xi) inside = !inside;
    }
    return inside ? 'inside' : 'outside';
}

function onSegment(x: number, y: number, x1: number, y1: number, x2: number, y2: number): boolean {
    const cross = (x - x1) * (y2 - y1) - (y - y1) * (x2 - x1);
    if (Math.abs(cross) > 1e-10) return false;
    return x >= Math.min(x1, x2) - 1e-10 && x <= Math.max(x1, x2) + 1e-10 && y >= Math.min(y1, y2) - 1e-10 && y <= Math.max(y1, y2) + 1e-10;
}

const isRecord = (value: unknown): value is Record<string, unknown> => typeof value === 'object' && value !== null;

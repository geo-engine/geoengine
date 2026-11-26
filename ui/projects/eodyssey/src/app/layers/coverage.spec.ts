import {get as getProjection, transformExtent} from 'ol/proj';
import {describe, expect, it} from 'vitest';
import {coverageContains, parseCoverage, viewportOutsideCoverage} from './coverage';

const polygon = (coordinates: number[][][]): string => JSON.stringify({type: 'Polygon', coordinates});

describe('geographic coverage', () => {
    it('parses arbitrary regions and treats boundaries as contained', () => {
        const coverage = parseCoverage(
            polygon([
                [
                    [10, 20],
                    [15, 20],
                    [15, 25],
                    [10, 25],
                    [10, 20],
                ],
            ]),
        );
        expect(coverage?.getType()).toBe('Polygon');
        expect(coverage?.getExtent()).toEqual([10, 20, 15, 25]);
        expect(coverageContains(coverage!, {longitude: 10, latitude: 22})).toBe(true);
        expect(coverageContains(coverage!, {longitude: 13, latitude: 23})).toBe(true);
        expect(coverageContains(coverage!, {longitude: 16, latitude: 23})).toBe(false);
    });

    it('respects polygon holes and split antimeridian multipolygons', () => {
        const coverage = parseCoverage(
            JSON.stringify({
                type: 'MultiPolygon',
                coordinates: [
                    [
                        [
                            [170, -10],
                            [180, -10],
                            [180, 10],
                            [170, 10],
                            [170, -10],
                        ],
                        [
                            [173, -2],
                            [177, -2],
                            [177, 2],
                            [173, 2],
                            [173, -2],
                        ],
                    ],
                    [
                        [
                            [-180, -10],
                            [-170, -10],
                            [-170, 10],
                            [-180, 10],
                            [-180, -10],
                        ],
                    ],
                ],
            }),
        );
        expect(coverage?.getType()).toBe('MultiPolygon');
        expect(coverageContains(coverage!, {longitude: 175, latitude: 5})).toBe(true);
        expect(coverageContains(coverage!, {longitude: 175, latitude: 0})).toBe(false);
        expect(coverageContains(coverage!, {longitude: -175, latitude: 0})).toBe(true);
        expect(coverageContains(coverage!, {longitude: 185, latitude: 0})).toBe(true);
        const projection = getProjection('EPSG:4326')!;
        expect(viewportOutsideCoverage(coverage!, {extent: [174, -1, 176, 1], projection})).toBe(true);
        expect(viewportOutsideCoverage(coverage!, {extent: [174, -1, 177, 1], projection})).toBe(false);
        expect(viewportOutsideCoverage(coverage!, {extent: [179, -1, 181, 1], projection})).toBe(false);
    });

    it('detects complete separation in the projected map viewport', () => {
        const coverage = parseCoverage(
            polygon([
                [
                    [6, 0],
                    [12, 0],
                    [12, 84],
                    [6, 84],
                    [6, 0],
                ],
            ]),
        )!;
        const projection = getProjection('EPSG:3857')!;
        const outside = (extent: number[]): boolean =>
            viewportOutsideCoverage(coverage, {extent: transformExtent(extent, 'EPSG:4326', projection), projection});

        expect(outside([11, 48, 16, 52])).toBe(false);
        expect(outside([12, 48, 16, 52])).toBe(false);
        expect(outside([12.01, 48, 16, 52])).toBe(true);
        expect(outside([0, -10, 20, 85])).toBe(false);
        expect(outside([367, 48, 370, 52])).toBe(true);
    });

    it.each([false, true])('includes every exterior and hole edge regardless of winding (reversed: %s)', (reversed) => {
        const rings = [
            [
                [0, 0],
                [10, 0],
                [10, 10],
                [0, 10],
                [0, 0],
            ],
            [
                [2, 2],
                [8, 2],
                [8, 8],
                [2, 8],
                [2, 2],
            ],
        ];
        const coverage = parseCoverage(polygon(reversed ? rings.map((ring) => [...ring].reverse()) : rings));
        expect(coverage).toBeDefined();
        for (const [longitude, latitude] of [
            [0, 5],
            [10, 5],
            [5, 0],
            [5, 10],
            [0, 0],
            [10, 10],
            [2, 5],
            [8, 5],
            [5, 2],
            [5, 8],
            [2, 2],
        ]) {
            expect(coverageContains(coverage!, {longitude, latitude})).toBe(true);
        }
        expect(coverageContains(coverage!, {longitude: 5, latitude: 5})).toBe(false);
        expect(coverageContains(coverage!, {longitude: 11, latitude: 5})).toBe(false);
    });

    it('uses the same coverage in positive and negative wrapped map worlds', () => {
        const coverage = parseCoverage(
            polygon([
                [
                    [10, 20],
                    [15, 20],
                    [15, 25],
                    [10, 25],
                    [10, 20],
                ],
            ]),
        );
        for (const longitude of [13, 373, -347, 733, -707]) {
            expect(coverageContains(coverage!, {longitude, latitude: 23})).toBe(true);
        }
        expect(coverageContains(coverage!, {longitude: NaN, latitude: 23})).toBe(false);
        expect(coverageContains(coverage!, {longitude: Infinity, latitude: 23})).toBe(false);
        expect(coverageContains(coverage!, {longitude: 13, latitude: 91})).toBe(false);
        expect(coverageContains(coverage!, {longitude: 13, latitude: NaN})).toBe(false);
    });

    it('treats positive and negative 180 degrees as the same map center', () => {
        const west = parseCoverage(
            polygon([
                [
                    [-180, -10],
                    [-170, -10],
                    [-170, 10],
                    [-180, 10],
                    [-180, -10],
                ],
            ]),
        );
        const east = parseCoverage(
            polygon([
                [
                    [170, -10],
                    [180, -10],
                    [180, 10],
                    [170, 10],
                    [170, -10],
                ],
            ]),
        );
        expect(coverageContains(west!, {longitude: 180, latitude: 0})).toBe(true);
        expect(coverageContains(west!, {longitude: 540, latitude: 0})).toBe(true);
        expect(coverageContains(east!, {longitude: -180, latitude: 0})).toBe(true);
    });

    it('rejects malformed, unclosed, non-finite, and out-of-range coordinates safely', () => {
        expect(parseCoverage(undefined)).toBeUndefined();
        expect(parseCoverage('')).toBeUndefined();
        expect(parseCoverage('no json')).toBeUndefined();
        expect(parseCoverage('null')).toBeUndefined();
        expect(parseCoverage('{"type":"Polygon","coordinates":[]}')).toBeUndefined();
        expect(parseCoverage('{"type":"MultiPolygon","coordinates":[]}')).toBeUndefined();
        expect(parseCoverage('{"type":"Polygon","coordinates":[[]]}')).toBeUndefined();
        expect(parseCoverage('{"type":"Polygon"}')).toBeUndefined();
        expect(
            parseCoverage(
                polygon([
                    [
                        [0, 0],
                        [1, 0],
                        [NaN, 1],
                        [0, 0],
                    ],
                ]),
            ),
        ).toBeUndefined();
        expect(
            parseCoverage(
                polygon([
                    [
                        [0, 0],
                        [1, 0],
                        [1, 91],
                        [0, 0],
                    ],
                ]),
            ),
        ).toBeUndefined();
        expect(
            parseCoverage(
                polygon([
                    [
                        [0, 0],
                        [1, 0],
                        [2, 0],
                        [0, 0],
                    ],
                ]),
            ),
        ).toBeUndefined();
        expect(parseCoverage('{"type":"Point","coordinates":[0,0]}')).toBeUndefined();
        expect(
            parseCoverage(
                polygon([
                    [
                        [0, 0],
                        [1, 0],
                        [1, 1],
                        [0, 1],
                    ],
                ]),
            ),
        ).toBeUndefined();
        expect(
            parseCoverage(
                polygon([
                    [
                        [181, 0],
                        [1, 0],
                        [1, 1],
                        [181, 0],
                    ],
                ]),
            ),
        ).toBeUndefined();
        expect(
            parseCoverage(
                polygon([
                    [
                        [0, 0],
                        [1, 0],
                        [1, 1],
                        [0, 1],
                        [0, 0],
                    ],
                ]),
            ),
        ).toBeDefined();
    });
});

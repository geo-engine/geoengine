import {describe, expect, it} from 'vitest';
import {coverageContains, parseCoverage} from './coverage';

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
        expect(coverage).toBeDefined();
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
        expect(coverage).toBeDefined();
        expect(coverageContains(coverage!, {longitude: 175, latitude: 5})).toBe(true);
        expect(coverageContains(coverage!, {longitude: 175, latitude: 0})).toBe(false);
        expect(coverageContains(coverage!, {longitude: -175, latitude: 0})).toBe(true);
        expect(coverageContains(coverage!, {longitude: 185, latitude: 0})).toBe(true);
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
        expect(parseCoverage('no json')).toBeUndefined();
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

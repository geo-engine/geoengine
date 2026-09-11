import {describe, expect, it} from 'vitest';
import OlGeomLineString from 'ol/geom/LineString';
import OlGeomPolygon from 'ol/geom/Polygon';
import {formatLength, measureLengthInMeters, measureAreaInSquareMeters, formatArea} from './measure.directive';

describe('formatArea', () => {
    it('formats areas under 1 km² in square meters', () => {
        expect(formatArea(123.45)).toBe('123 m²');
    });

    it('formats areas above 1 km² in square kilometers', () => {
        expect(formatArea(1234567.89)).toBe('1.23 km²');
    });
});

describe('formatLength', () => {
    it('formats lengths under 1 km in meters', () => {
        expect(formatLength(123.45)).toBe('123 m');
    });

    it('formats lengths above 1 km in kilometers', () => {
        expect(formatLength(1234.56)).toBe('1.23 km');
    });
});

describe('measureLengthInMeters', () => {
    it('returns a realistic meter distance for a Germany-sized span', () => {
        const geometry = new OlGeomLineString([
            [8.0, 47.0],
            [14.0, 54.0],
        ]);

        const length = measureLengthInMeters(geometry, 'EPSG:4326');

        expect(length).toBeCloseTo(885_795.1675020781);
    });
});

describe('measureAreaInSquareMeters', () => {
    it('returns a realistic square meter area for a Germany-sized polygon', () => {
        const geometry = new OlGeomPolygon([
            [
                [8.0, 47.0],
                [14.0, 47.0],
                [14.0, 54.0],
                [8.0, 54.0],
                [8.0, 47.0],
            ],
        ]);

        const area = measureAreaInSquareMeters(geometry, 'EPSG:4326');

        expect(area).toBeCloseTo(330_111_631_989.41235);
    });
});

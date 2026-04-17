import {BLACK, ColorBreakpoint} from '@geoengine/common';
import {Measurement} from '@geoengine/api-client';
import {
    calculateNumberPipeParameters,
    CastMeasurementToClassificationPipe,
    CastMeasurementToContinuousPipe,
    unifyDecimals,
} from './raster-legend.component';

describe('RasterLegend', () => {
    it('calculateNumberPipeParameters', () => {
        const breakpoints1: Array<ColorBreakpoint> = [
            new ColorBreakpoint(0, BLACK),
            new ColorBreakpoint(1, BLACK),
            new ColorBreakpoint(2, BLACK),
        ];
        expect(calculateNumberPipeParameters(breakpoints1)).toBe('1.0-0');

        const breakpoints2: Array<ColorBreakpoint> = [
            new ColorBreakpoint(0, BLACK),
            new ColorBreakpoint(0.75, BLACK),
            new ColorBreakpoint(1, BLACK),
        ];
        expect(calculateNumberPipeParameters(breakpoints2)).toBe('1.0-1');

        const breakpoints3: Array<ColorBreakpoint> = [
            new ColorBreakpoint(0, BLACK),
            new ColorBreakpoint(0.05, BLACK),
            new ColorBreakpoint(0.1, BLACK),
        ];
        expect(calculateNumberPipeParameters(breakpoints3)).toBe('1.0-2');

        const breakpoints4: Array<ColorBreakpoint> = [
            new ColorBreakpoint(0.123, BLACK),
            new ColorBreakpoint(1, BLACK),
            new ColorBreakpoint(2, BLACK),
        ];
        expect(calculateNumberPipeParameters(breakpoints4)).toBe('1.0-3');

        const breakpoints5: Array<ColorBreakpoint> = [
            new ColorBreakpoint(0, BLACK),
            new ColorBreakpoint(1, BLACK),
            new ColorBreakpoint(2.123, BLACK),
        ];
        expect(calculateNumberPipeParameters(breakpoints5)).toBe('1.0-3');

        const breakpoints6: Array<ColorBreakpoint> = [
            new ColorBreakpoint(0, BLACK),
            new ColorBreakpoint(2, BLACK),
            new ColorBreakpoint(5, BLACK),
        ];
        expect(calculateNumberPipeParameters(breakpoints6)).toBe('1.0-0');
    });

    it('unifyDecimals', () => {
        const values1: number[] = [12.34567, 12.345678];
        const expect1: number[] = [12.34567, 12.345678];
        expect(unifyDecimals(values1)).toEqual(expect1);

        const values2: number[] = [12.345, 12.34567];
        const expect2: number[] = [12.345, 12.34567];
        expect(unifyDecimals(values2)).toEqual(expect2);

        const values3: number[] = [12345, 12345, 12345.3236434];
        const expect3: number[] = [12345, 12345, 12345.32];
        expect(unifyDecimals(values3)).toEqual(expect3);

        const values4: number[] = [
            12.423562343, 3.0034456333, 2, 2.9, 2.09643543, 1235.0009634526, 3.00000065, 4.11111123234234, 4.111111654234234,
        ];
        const expect4: number[] = [12.4235623, 3.00344563, 2, 2.9, 2.09643543, 1235.00096, 3.00000065, 4.11111123, 4.11111165];
        expect(unifyDecimals(values4)).toEqual(expect4);

        // eslint-disable-next-line no-loss-of-precision
        const values5: number[] = [122.4222235625234343, 122.42222034432356, 122.422225654352, 122.422226441233];
        const expect5: number[] = [122.4222235, 122.4222203, 122.4222256, 122.4222264];
        expect(unifyDecimals(values5)).toEqual(expect5);

        // eslint-disable-next-line no-loss-of-precision
        const values6: number[] = [4.0000123464234235234, 4.000005243423424, 4.000000009];
        const expect6: number[] = [4.0000123, 4.0000052, 4];
        expect(unifyDecimals(values6)).toEqual(expect6);

        const values7: number[] = [5243.42352135234, 1643.262342314436, 23676.23464564, 23675];
        const expect7: number[] = [5243, 1643, 23676, 23675];
        expect(unifyDecimals(values7)).toEqual(expect7);

        const values8: number[] = [1.2345, 1.2345679912358765];
        const expect8: number[] = [1.2345, 1.234567];
        expect(unifyDecimals(values8)).toEqual(expect8);

        const values9: number[] = [12345, 12345, 12345.3236434];
        const expect9: number[] = [12345, 12345, 12345.32];
        expect(unifyDecimals(values9)).toEqual(expect9);

        const values10: number[] = [81.123123, 81.123456, 81.123987];
        const expect10: number[] = [81.12312, 81.12345, 81.12398];
        expect(unifyDecimals(values10)).toEqual(expect10);
    });

    it('convertsToContinuousMeasurement', () => {
        const pipe = new CastMeasurementToContinuousPipe();

        let measurement: Measurement = {
            type: 'continuous',
            measurement: 'measurement',
            unit: 'unit',
        };

        let transformed = pipe.transform(measurement);

        expect(transformed).not.toBeNull();

        measurement = {
            type: 'classification',
            measurement: 'measurement',
            classes: {class: 'class'},
        };

        transformed = pipe.transform(measurement);

        expect(transformed).toBeNull();
    });

    it('convertsToClassificationMeasurement', () => {
        const pipe = new CastMeasurementToClassificationPipe();

        let measurement: Measurement = {
            type: 'classification',
            measurement: 'measurement',
            classes: {class: 'class'},
        };

        let transformed = pipe.transform(measurement);

        expect(transformed).not.toBeNull();

        measurement = {
            type: 'continuous',
            measurement: 'measurement',
            unit: 'unit',
        };

        transformed = pipe.transform(measurement);

        expect(transformed).toBeNull();
    });
});

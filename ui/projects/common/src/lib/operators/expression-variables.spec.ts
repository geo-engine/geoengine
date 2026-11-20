import {describe, expect, it} from 'vitest';
import {canonicalizeBandName, expressionBandVariables, isValidExpressionVariable} from './expression-variables';

describe('Expression variables', () => {
    it('canonicalizes band names', () => {
        expect(canonicalizeBandName('nir')).toEqual('nir');
        expect(canonicalizeBandName('nir 1')).toEqual('nir_1');
        expect(canonicalizeBandName('nir-1 (2)')).toEqual('nir_1__2_');
        expect(canonicalizeBandName('1st')).toEqual('_1st');
        expect(canonicalizeBandName('')).toEqual('_');
        expect(canonicalizeBandName('bänd')).toEqual('bänd');
    });

    it('checks variable names', () => {
        for (const name of ['A', 'nir', 'band_1', 'B04', 'type', 'None', 'iff', 'nodata2']) {
            expect(isValidExpressionVariable(name), name).toBe(true);
        }

        for (const name of [
            '',
            '_1',
            '1st',
            'nir-1',
            'nir 1',
            'bänd',
            'if',
            'NODATA',
            'Is',
            'true',
            'while',
            'Return',
            'expression_fn_foo',
        ]) {
            expect(isValidExpressionVariable(name), name).toBe(false);
        }
    });

    it('names band variables', () => {
        expect(expressionBandVariables(['red', 'nir'])).toEqual(['red', 'nir']);
        expect(expressionBandVariables(['nir 1', 'B04'])).toEqual(['nir_1', 'B04']);
        expect(expressionBandVariables(['1st', 'if', 'bänd'])).toEqual(['A', 'B', 'C']);
        expect(expressionBandVariables(['nir-1', 'nir_1', 'red'])).toEqual(['A', 'B', 'red']);
    });
});

import {plotDataToText, tableToText, vegaDataTable, vegaDataValues} from './plot-data';

describe('plot data', () => {
    const table = {
        header: ['a', 'b', 'c'],
        rows: [
            [1.234567890123, null, 'x\ty, "z"'],
            [2, undefined, [1, 2]],
        ],
    };

    it('reads the inline data of a spec', () => {
        expect(vegaDataValues({vegaString: JSON.stringify({data: {values: [{a: 1}]}})})).toEqual([{a: 1}]);
        expect(vegaDataValues({vegaString: JSON.stringify({data: {url: 'data.csv'}})})).toBeUndefined();
        expect(vegaDataValues({vegaString: JSON.stringify({data: {values: [1, 2]}})})).toBeUndefined();
    });

    it('uses the fields of all records as columns', () => {
        expect(vegaDataTable({vegaString: JSON.stringify({data: {values: [{a: 1}, {b: 2, a: 3}]}})})).toEqual({
            header: ['a', 'b'],
            rows: [
                [1, undefined],
                [3, 2],
            ],
        });
    });

    it('creates tab-separated values', () => {
        expect(tableToText(table, 'tsv')).toBe('a\tb\tc\n1.234567890123\t\t"x\ty, ""z"""\n2\t\t[1,2]');
    });

    it('creates comma-separated values', () => {
        expect(tableToText(table, 'csv')).toBe('a,b,c\n1.234567890123,,"x\ty, ""z"""\n2,,"[1,2]"');
    });

    it('creates JSON of the raw values', () => {
        const values = [{a: 1, b: [{c: 2}]}];
        expect(JSON.parse(plotDataToText('json', table, values))).toEqual(values);
    });
});

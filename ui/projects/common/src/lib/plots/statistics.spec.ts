import {statisticsFromPlotData, statisticsTable} from './statistics';

describe('statisticsFromPlotData', () => {
    const row = (name: string, min: number): Record<string, unknown> => ({
        name,
        valueCount: 4,
        validCount: 3,
        min,
        max: 10,
        mean: 5,
        stddev: 1,
        percentiles: [{percentile: 0.5, value: 4}],
    });

    it('reads the rows from the spec', () => {
        const statistics = statisticsFromPlotData({
            vegaString: JSON.stringify({data: {values: [row('red', 1), row('green', 2)]}, mark: 'text'}),
        });

        expect([...statistics.keys()]).toEqual(['red', 'green']);
        expect(statistics.get('green')?.min).toBe(2);
        expect(statistics.get('red')?.percentiles[0].value).toBe(4);
    });

    it('fails without rows', () => {
        expect(() => statisticsFromPlotData({vegaString: JSON.stringify({mark: 'bar'})})).toThrowError();
    });

    it('fails for rows without a name', () => {
        expect(() => statisticsFromPlotData({vegaString: JSON.stringify({data: {values: [{min: 1}]}})})).toThrowError();
    });

    it('creates a table with one column per percentile', () => {
        const statistics = statisticsFromPlotData({
            vegaString: JSON.stringify({
                data: {
                    values: [
                        {...row('red', 1.123456789), percentiles: [{percentile: 1 / 3, value: 4.5}]},
                        {...row('blue', 2), min: null, percentiles: [{percentile: 1 / 3, value: null}]},
                    ],
                },
            }),
        });

        expect(statisticsTable(statistics)).toEqual({
            header: ['name', 'valueCount', 'validCount', 'min', 'max', 'mean', 'stddev', 'p33.333'],
            rows: [
                ['red', 4, 3, 1.123456789, 10, 5, 1, 4.5],
                ['blue', 4, 3, null, 10, 5, 1, null],
            ],
        });
    });
});

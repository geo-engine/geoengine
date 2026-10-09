import {VegaChartData} from './plot.model';
import {PlotDataTable, vegaDataValues} from './plot-data';

/**
 * The statistics of one band or attribute, as computed by the `Statistics` plot operator.
 * Values that could not be computed, e.g., the `min` of only no-data values, are `null`.
 */
export interface StatisticsRow {
    readonly name: string;
    readonly valueCount: number;
    readonly validCount: number;
    readonly min: number | null;
    readonly max: number | null;
    readonly mean: number | null;
    readonly stddev: number | null;
    readonly percentiles: ReadonlyArray<{readonly percentile: number; readonly value: number | null}>;
}

/**
 * Reads the rows of a `Statistics` plot from the `data.values` of its Vega spec, keyed by band or attribute name.
 */
export function statisticsFromPlotData(data: VegaChartData): Map<string, StatisticsRow> {
    const rows = vegaDataValues(data);

    if (!rows) {
        throw new Error('The plot does not contain statistics.');
    }

    return new Map(
        rows.map((row) => {
            if (typeof row['name'] !== 'string') {
                throw new Error('The plot contains a statistics row without a name.');
            }
            const statisticsRow = row as unknown as StatisticsRow;
            return [statisticsRow.name, statisticsRow];
        }),
    );
}

/**
 * Creates a table of the statistics with their exact values and one column per percentile, e.g., `p25`.
 */
export function statisticsTable(statistics: ReadonlyMap<string, StatisticsRow>): PlotDataTable {
    const rows = [...statistics.values()];
    // the percentiles are the same for all rows
    const percentiles = rows[0]?.percentiles.map(({percentile}) => percentile) ?? [];

    // round to avoid titles like `p33.300000000000004`, like the column titles of the plot
    const percentileTitles = percentiles.map((percentile) => `p${Math.round(percentile * 100_000) / 1_000}`);

    return {
        header: ['name', 'valueCount', 'validCount', 'min', 'max', 'mean', 'stddev', ...percentileTitles],
        rows: rows.map((row) => [
            row.name,
            row.valueCount,
            row.validCount,
            row.min,
            row.max,
            row.mean,
            row.stddev,
            ...row.percentiles.map(({value}) => value),
        ]),
    };
}

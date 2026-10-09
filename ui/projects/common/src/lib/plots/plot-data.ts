import {csvFormatRows, tsvFormatRows} from 'd3';
import {VegaChartData} from './plot.model';

/**
 * Reads the inline data of a Vega spec, i.e., its `data.values`, or returns `undefined` if there is none.
 */
export function vegaDataValues(data: VegaChartData): Array<Record<string, unknown>> | undefined {
    const spec = JSON.parse(data.vegaString) as {data?: {values?: unknown}};
    const values = spec.data?.values;

    if (!Array.isArray(values) || !values.every((value) => typeof value === 'object' && value !== null && !Array.isArray(value))) {
        return undefined;
    }

    return values as Array<Record<string, unknown>>;
}

/**
 * A table of plot data with a header and rows of values
 */
export interface PlotDataTable {
    readonly header: ReadonlyArray<string>;
    readonly rows: ReadonlyArray<ReadonlyArray<unknown>>;
}

/**
 * The text formats for exporting plot data
 */
export type PlotDataFormat = 'csv' | 'tsv' | 'json';

/**
 * Creates the table of the inline data of a Vega spec, or returns `undefined` if there is none.
 * The columns are the fields of the records in the order of their first occurrence.
 */
export function vegaDataTable(data: VegaChartData): PlotDataTable | undefined {
    const values = vegaDataValues(data);
    if (!values) {
        return undefined;
    }

    const header = [...new Set(values.flatMap((value) => Object.keys(value)))];

    return {
        header,
        rows: values.map((value) => header.map((field) => value[field])),
    };
}

/**
 * Writes a table as comma-separated or tab-separated values, e.g., for pasting into a spreadsheet.
 * Numbers keep their full precision, missing values become empty cells and nested values become JSON.
 * Values that contain the separator, quotes or line breaks are quoted.
 */
export function tableToText(table: PlotDataTable, format: 'csv' | 'tsv'): string {
    const cell = (value: unknown): string => {
        if (value === null || value === undefined) {
            return '';
        }
        return typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean' ? String(value) : JSON.stringify(value);
    };

    const rows = [table.header, ...table.rows].map((row) => row.map(cell));

    return format === 'csv' ? csvFormatRows(rows) : tsvFormatRows(rows);
}

/**
 * Writes plot data as CSV or TSV of its `table`, or as JSON of its raw `values`.
 */
export const plotDataToText = (format: PlotDataFormat, table: PlotDataTable, values: ReadonlyArray<Record<string, unknown>>): string =>
    format === 'json' ? JSON.stringify(values, null, 2) : tableToText(table, format);

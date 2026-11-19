import {describe, expect, it} from 'vitest';
import {ResponseError, TimeStepFootprint, TimeStepFootprintStatus} from '@geoengine/api-client';
import {
    calendarStartView,
    cellClasses,
    footprintsOrTooLarge,
    mergeFootprints,
    stepAt,
    visiblePeriodStart,
    windowRequests,
} from './footprints';

function step(startIso: string, endIso: string, status: TimeStepFootprintStatus): TimeStepFootprint {
    return {time: {start: Date.parse(startIso), end: Date.parse(endIso)}, status, bounds: []};
}

describe('footprints', () => {
    const data = step('2026-03-15T00:00:00Z', '2026-03-16T00:00:00Z', TimeStepFootprintStatus.Data);
    const empty = step('2026-04-01T00:00:00Z', '2026-04-02T00:00:00Z', TimeStepFootprintStatus.Empty);
    const unknown = step('2027-01-01T00:00:00Z', '2027-01-02T00:00:00Z', TimeStepFootprintStatus.Unknown);

    const WITH_DATA = 'geoengine-calendar-cell-with-data';
    const CONTINUED = 'geoengine-calendar-cell-with-data-continued';
    const BAND_AFTER = 'geoengine-calendar-cell-band-after';
    const eightDays = step('2026-03-30T00:00:00Z', '2026-04-07T00:00:00Z', TimeStepFootprintStatus.Data);

    it('marks the day of a time step with data in all calendar views', () => {
        expect(cellClasses([data], new Date(2026, 2, 15), 'month')).toEqual([WITH_DATA]);
        expect(cellClasses([data], new Date(2026, 2, 1), 'year')).toEqual([WITH_DATA]);
        expect(cellClasses([data], new Date(2026, 0, 1), 'multi-year')).toEqual([WITH_DATA]);
    });

    it('does not mark other days', () => {
        expect(cellClasses([data], new Date(2026, 2, 14), 'month')).toEqual([]);
        expect(cellClasses([data], new Date(2026, 2, 16), 'month')).toEqual([]);
        expect(cellClasses([data], new Date(2026, 3, 15), 'month')).toEqual([]);
    });

    it('gives a long time step a disc on its first day, a band over the days between and an end on its last day', () => {
        const days = [29, 30, 31, 32, 36, 37, 38].map((day) => cellClasses([eightDays], new Date(2026, 2, day), 'month'));
        expect(days).toEqual([
            [],
            [WITH_DATA, BAND_AFTER],
            [CONTINUED, BAND_AFTER],
            [CONTINUED, BAND_AFTER],
            [CONTINUED, BAND_AFTER],
            [CONTINUED],
            [],
        ]);
    });

    it('marks every month and year a long time step covers', () => {
        const months = step('2026-01-01T00:00:00Z', '2026-03-01T00:00:00Z', TimeStepFootprintStatus.Data);
        expect([0, 1, 2].map((m) => cellClasses([months], new Date(2026, m, 1), 'year'))).toEqual([
            [WITH_DATA, BAND_AFTER],
            [CONTINUED],
            [],
        ]);
        expect(cellClasses([months], new Date(2025, 0, 1), 'multi-year')).toEqual([]);
    });

    it('prefers the disc where one step starts and another goes on', () => {
        const startsOnDay32 = step('2026-04-01T00:00:00Z', '2026-04-02T00:00:00Z', TimeStepFootprintStatus.Data);
        expect(cellClasses([eightDays, startsOnDay32], new Date(2026, 2, 32), 'month')).toEqual([WITH_DATA, BAND_AFTER]);
    });

    it('marks neither empty nor unknown time steps', () => {
        expect(cellClasses([empty, unknown], new Date(2026, 3, 1), 'month')).toEqual([]);
        expect(cellClasses([empty, unknown], new Date(2027, 0, 1), 'multi-year')).toEqual([]);
    });

    it('opens the calendar on the cell size of the time step', () => {
        expect(['hours', 'minutes', 'day', 'days'].map(calendarStartView)).toEqual(['month', 'month', 'month', 'month']);
        expect(calendarStartView('months')).toBe('year');
        expect(calendarStartView('years')).toBe('multi-year');
    });

    it('finds the time step of a time and treats its end as excluded', () => {
        expect(stepAt([data, empty], Date.parse('2026-03-15T12:00:00Z'))).toBe(data);
        expect(stepAt([data, empty], Date.parse('2026-03-16T00:00:00Z'))).toBeUndefined();
    });

    it('starts the visible period at the month, the year or the page of years the calendar shows', () => {
        // the local fields of a calendar date count
        expect(visiblePeriodStart(new Date(2026, 2, 15), 'month')).toBe(Date.UTC(2026, 2, 1));
        // a monthly dataset opened in April shows January to March in the year view as well
        expect(visiblePeriodStart(new Date(2026, 3, 15), 'year')).toBe(Date.UTC(2026, 0, 1));
        // 2016 is a multiple of 24, so the page of 2026 starts there
        expect(visiblePeriodStart(new Date(2026, 3, 15), 'multi-year')).toBe(Date.UTC(2016, 0, 1));
    });

    it('requests the month of the window start first and the rest afterwards', () => {
        const start = Date.UTC(2026, 2, 1);
        const end = Date.UTC(2026, 5, 9);

        expect(windowRequests(start, end)).toEqual([
            {start, end: Date.UTC(2026, 3, 1)},
            {start: Date.UTC(2026, 3, 1), end},
        ]);
    });

    it('requests a window inside one month at once', () => {
        const start = Date.UTC(2026, 2, 1);
        const end = Date.UTC(2026, 2, 20);

        expect(windowRequests(start, end)).toEqual([{start, end}]);
    });

    it('merges the steps of two requests and keeps the later cursor', () => {
        const next = {start: 1, end: 2};
        const merged = mergeFootprints(
            {spatialReference: 'EPSG:4326', steps: [data], next: null, tooLarge: false},
            {spatialReference: 'EPSG:4326', steps: [empty], next, tooLarge: true},
        );

        expect(merged.steps).toEqual([data, empty]);
        expect(merged.next).toBe(next);
        expect(merged.tooLarge).toBe(true);
    });

    it('turns the error of too many catalog searches into tooLarge and keeps other errors', async () => {
        const failed = (error: string): Promise<never> =>
            Promise.reject(new ResponseError(new Response(JSON.stringify({error, message: ''}), {status: 400})));

        expect((await footprintsOrTooLarge(failed('FootprintQueryTooLarge'))).tooLarge).toBe(true);
        await expect(footprintsOrTooLarge(failed('Operator'))).rejects.toBeInstanceOf(ResponseError);
        const answer = await footprintsOrTooLarge(Promise.resolve({spatialReference: 'EPSG:4326', steps: [], next: null}));
        expect(answer.tooLarge).toBe(false);
    });
});

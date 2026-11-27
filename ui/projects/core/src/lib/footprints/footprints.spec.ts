import {describe, expect, it} from 'vitest';
import {ResponseError, TimeStepFootprint, TimeStepFootprintStatus} from '@geoengine/api-client';
import {
    calendarHint,
    calendarStartView,
    canPickDate,
    cellClasses,
    footprintsOrTooLarge,
    mergeFootprints,
    stepAt,
    stepsAround,
    visiblePeriod,
    windowSteps,
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

    it('lets a day be picked unless all of its steps are empty, also with several steps per day', () => {
        const sixHours = (hour: number, status: TimeStepFootprintStatus): TimeStepFootprint =>
            step(`2026-04-01T${String(hour).padStart(2, '0')}:00:00Z`, `2026-04-01T${String(hour + 6).padStart(2, '0')}:00:00Z`, status);
        const day = new Date(2026, 3, 1);
        const [DATA, EMPTY] = [TimeStepFootprintStatus.Data, TimeStepFootprintStatus.Empty];

        // the step at midnight is empty, a later one has data
        expect(canPickDate([sixHours(0, EMPTY), sixHours(6, EMPTY), sixHours(12, DATA), sixHours(18, EMPTY)], day)).toBe(true);
        expect(canPickDate([sixHours(0, EMPTY), sixHours(6, EMPTY), sixHours(12, EMPTY), sixHours(18, EMPTY)], day)).toBe(false);
        // not loaded, so it may have data
        expect(canPickDate([], day)).toBe(true);
        // a long step with data over the day
        expect(canPickDate([eightDays], new Date(2026, 3, 3))).toBe(true);
    });

    it('finds the time step of a time and treats its end as excluded', () => {
        expect(stepAt([data, empty], Date.parse('2026-03-15T12:00:00Z'))).toBe(data);
        expect(stepAt([data, empty], Date.parse('2026-03-16T00:00:00Z'))).toBeUndefined();
    });

    it('takes the time steps around a time, shifted where one side has too few', () => {
        const days = Array.from({length: 10}, (_, i) => ({
            time: {start: i, end: i + 1},
            status: TimeStepFootprintStatus.Data,
            bounds: [],
        }));
        const starts = (timeMs: number): number[] => stepsAround(days, timeMs, 5).map((day) => day.time.start);

        expect(starts(5)).toEqual([3, 4, 5, 6, 7]);
        expect(starts(0)).toEqual([0, 1, 2, 3, 4]);
        expect(starts(9)).toEqual([5, 6, 7, 8, 9]);
        expect(starts(20)).toEqual([5, 6, 7, 8, 9]);
        expect(starts(-3)).toEqual([0, 1, 2, 3, 4]);
        expect(stepsAround(days.slice(0, 2), 1, 5)).toHaveLength(2);
    });

    it('finds a step of an instant at its instant', () => {
        const instant = {time: {start: 5, end: 5}, status: TimeStepFootprintStatus.Data, bounds: []};
        const after = {time: {start: 5, end: 9}, status: TimeStepFootprintStatus.Empty, bounds: []};
        expect(stepAt([instant], 5)).toBe(instant);
        expect(stepAt([instant], 6)).toBeUndefined();
        expect(stepsAround([instant], 5, 3)).toEqual([instant]);
        expect(stepAt([after], 5)).toBe(after);
        // a day with an instant at its start can be picked
        const midnight = Date.UTC(2026, 3, 1);
        const day = {time: {start: midnight, end: midnight}, status: TimeStepFootprintStatus.Data, bounds: []};
        expect(canPickDate([day], new Date(2026, 3, 1))).toBe(true);
    });

    it('knows the month, the year or the page of years the calendar shows', () => {
        // the local fields of a calendar date count
        expect(visiblePeriod(new Date(2026, 2, 15), 'month')).toEqual({start: Date.UTC(2026, 2, 1), end: Date.UTC(2026, 3, 1)});
        // a monthly dataset opened in April shows January to March in the year view as well
        expect(visiblePeriod(new Date(2026, 3, 15), 'year')).toEqual({start: Date.UTC(2026, 0, 1), end: Date.UTC(2027, 0, 1)});
        // 2016 is a multiple of 24, so the page of 2026 starts there
        expect(visiblePeriod(new Date(2026, 3, 15), 'multi-year')).toEqual({start: Date.UTC(2016, 0, 1), end: Date.UTC(2040, 0, 1)});
    });

    it('shows dates with data at most one level above the time step, and a hint in coarser views', () => {
        expect(calendarHint('days', 'month')).toBeUndefined();
        expect(calendarHint('days', 'year')).toBeUndefined();
        expect(calendarHint('days', 'multi-year')).toBe('Select a year to see the months with data.');
        expect(calendarHint('months', 'multi-year')).toBeUndefined();
        expect(calendarHint('hours', 'month')).toBeUndefined();
        expect(calendarHint('hours', 'year')).toBe('Select a month to see the days with data.');
        expect(calendarHint('hours', 'multi-year')).toBe('Select a year and a month to see the days with data.');
    });

    it('merges the steps of two requests and keeps the later cursor, and has none where one is too large', () => {
        const next = {start: 1, end: 2};
        const merged = mergeFootprints(
            {spatialReference: 'EPSG:4326', steps: [data], next: null, tooLarge: false},
            {spatialReference: 'EPSG:4326', steps: [empty], next, tooLarge: false},
        );
        expect(merged.steps).toEqual([data, empty]);
        expect(merged.next).toBe(next);
        expect(merged.tooLarge).toBe(false);

        const tooLarge = mergeFootprints(merged, {spatialReference: 'EPSG:4326', steps: [], next: null, tooLarge: true});
        expect(tooLarge.steps).toEqual([]);
        expect(tooLarge.tooLarge).toBe(true);
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

describe('windowSteps', () => {
    it('spans about three months of days, three years of months and thirty years of years', () => {
        expect(windowSteps('days', 1)).toBe(100);
        expect(windowSteps('hours', 6)).toBe(100);
        expect(windowSteps('months', 1)).toBe(36);
        expect(windowSteps('months', 3)).toBe(12);
        expect(windowSteps('years', 1)).toBe(30);
        expect(windowSteps('years', 50)).toBe(1);
    });
});

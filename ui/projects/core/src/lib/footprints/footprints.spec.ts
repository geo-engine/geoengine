import {describe, expect, it} from 'vitest';
import {TimeStepFootprint, TimeStepFootprintStatus} from '@geoengine/api-client';
import {
    boxRing,
    calendarStartView,
    cellHasData,
    cellHasStepAfter,
    cellHasStepStart,
    mergeFootprints,
    monthStartUtc,
    stepAt,
    windowRequests,
} from './footprints';

function step(startIso: string, endIso: string, status: TimeStepFootprintStatus): TimeStepFootprint {
    return {time: {start: Date.parse(startIso), end: Date.parse(endIso)}, status, bounds: []};
}

describe('footprints', () => {
    const data = step('2026-03-15T00:00:00Z', '2026-03-16T00:00:00Z', TimeStepFootprintStatus.Data);
    const empty = step('2026-04-01T00:00:00Z', '2026-04-02T00:00:00Z', TimeStepFootprintStatus.Empty);
    const unknown = step('2027-01-01T00:00:00Z', '2027-01-02T00:00:00Z', TimeStepFootprintStatus.Unknown);

    it('marks a time step with data in all calendar views', () => {
        expect(cellHasData([data], new Date(2026, 2, 15), 'month')).toBe(true);
        expect(cellHasData([data], new Date(2026, 2, 1), 'year')).toBe(true);
        expect(cellHasData([data], new Date(2026, 0, 1), 'multi-year')).toBe(true);
    });

    it('does not mark other days', () => {
        expect(cellHasData([data], new Date(2026, 2, 14), 'month')).toBe(false);
        expect(cellHasData([data], new Date(2026, 2, 16), 'month')).toBe(false);
        expect(cellHasData([data], new Date(2026, 3, 15), 'month')).toBe(false);
    });

    it('marks every day, month and year a long time step covers', () => {
        const eightDays = step('2026-03-30T00:00:00Z', '2026-04-07T00:00:00Z', TimeStepFootprintStatus.Data);
        const days = [29, 30, 31, 32, 37, 38].map((day) => cellHasData([eightDays], new Date(2026, 2, day), 'month'));
        expect(days).toEqual([false, true, true, true, true, false]);

        const month = step('2026-01-01T00:00:00Z', '2026-03-01T00:00:00Z', TimeStepFootprintStatus.Data);
        expect([0, 1, 2].map((m) => cellHasData([month], new Date(2026, m, 1), 'year'))).toEqual([true, true, false]);
        expect(cellHasData([month], new Date(2025, 0, 1), 'multi-year')).toBe(false);
    });

    it('tells the day a long time step starts in from the days it only continues in', () => {
        const eightDays = step('2026-03-30T00:00:00Z', '2026-04-07T00:00:00Z', TimeStepFootprintStatus.Data);
        const starts = [29, 30, 31, 32].map((day) => cellHasStepStart([eightDays], new Date(2026, 2, day), 'month'));
        expect(starts).toEqual([false, true, false, false]);
        expect(cellHasStepStart([empty], new Date(2026, 3, 1), 'month')).toBe(false);
    });

    it('tells the days a long time step goes on after from its last day', () => {
        const eightDays = step('2026-03-30T00:00:00Z', '2026-04-07T00:00:00Z', TimeStepFootprintStatus.Data);
        const after = [29, 30, 31, 37, 38].map((day) => cellHasStepAfter([eightDays], new Date(2026, 2, day), 'month'));
        expect(after).toEqual([false, true, true, false, false]);
        expect(cellHasStepAfter([data], new Date(2026, 2, 15), 'month')).toBe(false);
    });

    it('marks neither empty nor unknown time steps', () => {
        expect(cellHasData([empty, unknown], new Date(2026, 3, 1), 'month')).toBe(false);
        expect(cellHasData([empty, unknown], new Date(2027, 0, 1), 'multi-year')).toBe(false);
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

    it('takes the month start from the local fields of a calendar date', () => {
        expect(monthStartUtc(new Date(2026, 2, 15))).toBe(Date.UTC(2026, 2, 1));
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

    it('puts points along the edges of a box, from its corner around back to it', () => {
        const ring = boxRing([0, 0, 16, 8]);

        expect(ring.length).toBe(4 * 16 + 1);
        expect(ring[0]).toEqual([0, 0]);
        expect(ring[16]).toEqual([16, 0]);
        expect(ring[32]).toEqual([16, 8]);
        expect(ring[48]).toEqual([0, 8]);
        expect(ring[64]).toEqual(ring[0]);
        expect(ring.every(([x, y]) => x >= 0 && x <= 16 && y >= 0 && y <= 8)).toBe(true);
    });
});

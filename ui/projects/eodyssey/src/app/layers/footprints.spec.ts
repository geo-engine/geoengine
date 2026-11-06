import {describe, expect, it} from 'vitest';
import {TimeStepFootprint, TimeStepFootprintStatus} from '@geoengine/api-client';
import {calendarCellKey, calendarKeysWithData, mergeFootprints, monthStartUtc, stepAt, windowRequests} from './footprints';

function step(startIso: string, endIso: string, status: TimeStepFootprintStatus): TimeStepFootprint {
    return {time: {start: Date.parse(startIso), end: Date.parse(endIso)}, status, bounds: []};
}

describe('footprints', () => {
    const data = step('2026-03-15T00:00:00Z', '2026-03-16T00:00:00Z', TimeStepFootprintStatus.Data);
    const empty = step('2026-04-01T00:00:00Z', '2026-04-02T00:00:00Z', TimeStepFootprintStatus.Empty);
    const unknown = step('2027-01-01T00:00:00Z', '2027-01-02T00:00:00Z', TimeStepFootprintStatus.Unknown);

    it('marks a time step with data in all calendar views', () => {
        const keys = calendarKeysWithData([data]);

        expect(keys.has(calendarCellKey(new Date(2026, 2, 15), 'month'))).toBe(true);
        expect(keys.has(calendarCellKey(new Date(2026, 2, 1), 'year'))).toBe(true);
        expect(keys.has(calendarCellKey(new Date(2026, 0, 1), 'multi-year'))).toBe(true);
    });

    it('does not mark other days', () => {
        const keys = calendarKeysWithData([data]);

        expect(keys.has(calendarCellKey(new Date(2026, 2, 14), 'month'))).toBe(false);
        expect(keys.has(calendarCellKey(new Date(2026, 3, 15), 'month'))).toBe(false);
    });

    it('marks neither empty nor unknown time steps', () => {
        const keys = calendarKeysWithData([empty, unknown]);

        expect(keys.size).toBe(0);
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
});

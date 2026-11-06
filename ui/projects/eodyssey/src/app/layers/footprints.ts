import {MatCalendarView} from '@angular/material/datepicker';
import {RasterFootprints, TimeStepFootprint, TimeStepFootprintStatus} from '@geoengine/api-client';

/**
 * The calendar's dates are UTC dates written into the local fields of a `Date` (see `setDate` in the
 * layers component), so a cell's key comes from its local fields and a time step's from its UTC fields.
 */
function key(year: number, month: number, day: number, view: MatCalendarView): string {
    switch (view) {
        case 'month':
            return `${year}-${month}-${day}`;
        case 'year':
            return `${year}-${month}`;
        case 'multi-year':
            return `${year}`;
    }
}

export function calendarCellKey(date: Date, view: MatCalendarView): string {
    return key(date.getFullYear(), date.getMonth(), date.getDate(), view);
}

/** The keys of all calendar cells, in all calendar views, that have a time step with data. */
export function calendarKeysWithData(steps: ReadonlyArray<TimeStepFootprint>): Set<string> {
    const keys = new Set<string>();

    for (const step of steps) {
        if (step.status !== TimeStepFootprintStatus.Data) continue;

        const start = new Date(step.time.start);
        for (const view of ['month', 'year', 'multi-year'] as const) {
            keys.add(key(start.getUTCFullYear(), start.getUTCMonth(), start.getUTCDate(), view));
        }
    }

    return keys;
}

/** The time step containing `timeMs`, or starting there for instants. */
export function stepAt(steps: ReadonlyArray<TimeStepFootprint>, timeMs: number): TimeStepFootprint | undefined {
    return steps.find((step) => step.time.start <= timeMs && timeMs < step.time.end);
}

/** The first of the month of `date`, which is a calendar date, as a UTC time. */
export function monthStartUtc(date: Date): number {
    return Date.UTC(date.getFullYear(), date.getMonth(), 1);
}

export interface TimeRange {
    start: number;
    end: number;
}

/** The requests of a window: the month of its start first, so the visible month shows soonest, then the rest. */
export function windowRequests(windowStartMs: number, windowEndMs: number): TimeRange[] {
    const start = new Date(windowStartMs);
    const monthEnd = Date.UTC(start.getUTCFullYear(), start.getUTCMonth() + 1, 1);
    if (monthEnd >= windowEndMs) return [{start: windowStartMs, end: windowEndMs}];
    return [
        {start: windowStartMs, end: monthEnd},
        {start: monthEnd, end: windowEndMs},
    ];
}

/** The footprints of two consecutive requests of one window. */
export function mergeFootprints(first: RasterFootprints, second: RasterFootprints): RasterFootprints {
    return {...first, steps: [...first.steps, ...second.steps], next: second.next, tooLarge: first.tooLarge || second.tooLarge};
}

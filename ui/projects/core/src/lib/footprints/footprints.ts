import {MatCalendarView} from '@angular/material/datepicker';
import {RasterFootprints, TimeStepFootprint, TimeStepFootprintStatus} from '@geoengine/api-client';

export interface TimeRange {
    start: number;
    end: number;
}

/** The calendar's dates are UTC dates in the local fields of a `Date` (see `setDate` in the layers component). */
export function calendarCellRange(date: Date, view: MatCalendarView): TimeRange {
    const [year, month, day] = [date.getFullYear(), date.getMonth(), date.getDate()];
    switch (view) {
        case 'month':
            return {start: Date.UTC(year, month, day), end: Date.UTC(year, month, day + 1)};
        case 'year':
            return {start: Date.UTC(year, month, 1), end: Date.UTC(year, month + 1, 1)};
        case 'multi-year':
            return {start: Date.UTC(year, 0, 1), end: Date.UTC(year + 1, 0, 1)};
    }
}

const CELL_WITH_DATA_CLASS = 'geoengine-calendar-cell-with-data';
const CELL_WITH_DATA_CONTINUED_CLASS = 'geoengine-calendar-cell-with-data-continued';
const CELL_BAND_AFTER_CLASS = 'geoengine-calendar-cell-band-after';
const CELL_FEATURED_CLASS = 'geoengine-calendar-cell-featured';

/**
 * The classes of a calendar cell, which the styles of the calendar header turn into a disc on the day a time
 * step starts and a band over the days it goes on. A step can be valid for many days, and then marks every
 * day, month or year cell it overlaps. The cell of the featured time gets a star.
 */
export function cellClasses(steps: ReadonlyArray<TimeStepFootprint>, date: Date, view: MatCalendarView, featuredMs?: number): string[] {
    const cell = calendarCellRange(date, view);
    const featured = featuredMs !== undefined && cell.start <= featuredMs && featuredMs < cell.end ? [CELL_FEATURED_CLASS] : [];
    let hasData = false;
    let starts = false;
    let goesOn = false;
    for (const step of steps) {
        if (step.status !== TimeStepFootprintStatus.Data || step.time.start >= cell.end || step.time.end <= cell.start) continue;
        hasData = true;
        starts ||= step.time.start >= cell.start;
        goesOn ||= step.time.end > cell.end;
    }
    if (!hasData) return featured;
    return [starts ? CELL_WITH_DATA_CLASS : CELL_WITH_DATA_CONTINUED_CLASS, ...(goesOn ? [CELL_BAND_AFTER_CLASS] : []), ...featured];
}

/** The calendar view with one cell per time step: days for steps of a day or less, months, or years. */
export function calendarStartView(unit: string | undefined): MatCalendarView {
    if (unit === 'month' || unit === 'months') return 'year';
    if (unit === 'year' || unit === 'years') return 'multi-year';
    return 'month';
}

/** The time step containing `timeMs`, or starting there for instants. */
export function stepAt(steps: ReadonlyArray<TimeStepFootprint>, timeMs: number): TimeStepFootprint | undefined {
    return steps.find((step) => step.time.start <= timeMs && timeMs < step.time.end);
}

/** The first of the month of a UTC time. */
export function monthStartOfTime(timeMs: number): number {
    const date = new Date(timeMs);
    return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1);
}

/** The first of the month of `date`, which is a calendar date, as a UTC time. */
export function monthStartUtc(date: Date): number {
    return Date.UTC(date.getFullYear(), date.getMonth(), 1);
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

import {MatCalendarView} from '@angular/material/datepicker';
import {RasterFootprints, TimeStepFootprint, TimeStepFootprintStatus} from '@geoengine/api-client';

export interface TimeRange {
    start: number;
    end: number;
}

/**
 * The calendar's dates are UTC dates written into the local fields of a `Date` (see `setDate` in the
 * layers component), so the UTC range of a cell comes from the local fields of its date.
 */
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

/**
 * Whether a time step with data overlaps the calendar cell. A step can be valid for many days, and then
 * marks every day it covers, as a day, month or year cell.
 */
export function cellHasData(steps: ReadonlyArray<TimeStepFootprint>, date: Date, view: MatCalendarView): boolean {
    const cell = calendarCellRange(date, view);
    return steps.some((step) => step.status === TimeStepFootprintStatus.Data && step.time.start < cell.end && step.time.end > cell.start);
}

/**
 * The calendar view that shows one cell per time step: days for steps of a day or less, because a
 * calendar does not go below days, months for steps of months and years for steps of years.
 */
export function calendarStartView(unit: string | undefined): MatCalendarView {
    if (unit === 'month' || unit === 'months') return 'year';
    if (unit === 'year' || unit === 'years') return 'multi-year';
    return 'month';
}

const BOX_EDGE_STEPS = 16;

/**
 * The outline of a box with points along its edges. A box of one projection is no rectangle in another,
 * and the outline would otherwise be straight between the corners, leaving gaps at the edges of the map.
 */
export function boxRing([minX, minY, maxX, maxY]: [number, number, number, number]): [number, number][] {
    const along = (from: number, to: number, i: number): number => from + ((to - from) * i) / BOX_EDGE_STEPS;
    const steps = Array.from({length: BOX_EDGE_STEPS}, (_, i) => i);
    const ring: [number, number][] = [
        ...steps.map((i): [number, number] => [along(minX, maxX, i), minY]),
        ...steps.map((i): [number, number] => [maxX, along(minY, maxY, i)]),
        ...steps.map((i): [number, number] => [along(maxX, minX, i), maxY]),
        ...steps.map((i): [number, number] => [minX, along(maxY, minY, i)]),
    ];
    return [...ring, ring[0]];
}

/** The time step containing `timeMs`, or starting there for instants. */
export function stepAt(steps: ReadonlyArray<TimeStepFootprint>, timeMs: number): TimeStepFootprint | undefined {
    return steps.find((step) => step.time.start <= timeMs && timeMs < step.time.end);
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

import {MatCalendarView, yearsPerPage} from '@angular/material/datepicker';
import {RasterFootprints, ResponseError, TimeInterval, TimeStepFootprint, TimeStepFootprintStatus} from '@geoengine/api-client';

/** The calendar's dates are UTC dates in the local fields of a `Date` (see `setDate` in the layers component). */
export function calendarCellRange(date: Date, view: MatCalendarView): TimeInterval {
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

/**
 * The classes of a calendar cell, which the styles of the calendar header turn into a disc on the day a time
 * step starts and a band over the days it goes on. A step can be valid for many days, and then marks every
 * day, month or year cell it overlaps.
 */
export function cellClasses(steps: ReadonlyArray<TimeStepFootprint>, date: Date, view: MatCalendarView): string[] {
    const cell = calendarCellRange(date, view);
    let hasData = false;
    let starts = false;
    let goesOn = false;
    for (const step of steps) {
        if (step.status !== TimeStepFootprintStatus.Data || step.time.start >= cell.end || step.time.end <= cell.start) continue;
        hasData = true;
        starts ||= step.time.start >= cell.start;
        goesOn ||= step.time.end > cell.end;
    }
    if (!hasData) return [];
    return [starts ? CELL_WITH_DATA_CLASS : CELL_WITH_DATA_CONTINUED_CLASS, ...(goesOn ? [CELL_BAND_AFTER_CLASS] : [])];
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

/**
 * What the calendar shows in `view` around `date`, a calendar date, as UTC times: its month, its year or its page of
 * years. Without a minimum or maximum date, the calendar starts a page of years at a multiple of `yearsPerPage`.
 */
export function visiblePeriod(date: Date, view: MatCalendarView): TimeInterval {
    const [year, month] = [date.getFullYear(), date.getMonth()];
    switch (view) {
        case 'month':
            return {start: Date.UTC(year, month, 1), end: Date.UTC(year, month + 1, 1)};
        case 'year':
            return {start: Date.UTC(year, 0, 1), end: Date.UTC(year + 1, 0, 1)};
        case 'multi-year': {
            const pageYear = year - (((year % yearsPerPage) + yearsPerPage) % yearsPerPage);
            return {start: Date.UTC(pageYear, 0, 1), end: Date.UTC(pageYear + yearsPerPage, 0, 1)};
        }
    }
}

/** The cell size of a time step as a level: 0 for days, 1 for months, 2 for years, like the views. Steps below a day are -1. */
function stepLevel(unit: string | undefined): number {
    if (unit === 'year' || unit === 'years') return 2;
    if (unit === 'month' || unit === 'months') return 1;
    if (unit === 'day' || unit === 'days' || unit === undefined) return 0;
    return -1;
}

/**
 * What to tell the user in a view whose cells are more than one level coarser than the time step, e.g. years for daily
 * data: such a view has too many time steps to ask for, so it shows no dates with data. Undefined where it shows them.
 */
export function calendarHint(unit: string | undefined, view: MatCalendarView): string | undefined {
    const level = stepLevel(unit);
    if (view === 'month' || (view === 'year' ? 1 : 2) - level <= 1) return undefined;
    if (view === 'year') return 'Select a month to see the days with data.';
    return level < 0 ? 'Select a year and a month to see the days with data.' : 'Select a year to see the months with data.';
}

/** The footprints of consecutive requests of one window. */
export function mergeFootprints(first: Footprints, second: Footprints): Footprints {
    return {...first, steps: [...first.steps, ...second.steps], next: second.next, tooLarge: first.tooLarge || second.tooLarge};
}

/** Footprints, and whether answering them needs too many catalog searches; then they have no steps. */
export type Footprints = RasterFootprints & {tooLarge: boolean};

/** The answer of a footprints request, with the error `FootprintQueryTooLarge` of the server as `tooLarge`. */
export async function footprintsOrTooLarge(request: Promise<RasterFootprints>): Promise<Footprints> {
    try {
        return {...(await request), tooLarge: false};
    } catch (error) {
        const body: unknown = error instanceof ResponseError ? await error.response.json().catch(() => undefined) : undefined;
        if (typeof body === 'object' && body !== null && 'error' in body && body.error === 'FootprintQueryTooLarge') {
            return {spatialReference: '', steps: [], next: null, tooLarge: true};
        }
        throw error;
    }
}

import {ChangeDetectionStrategy, Component, DestroyRef, effect, inject, ViewEncapsulation} from '@angular/core';
import {MatCalendar, MatCalendarHeader} from '@angular/material/datepicker';
import {MatProgressBarModule} from '@angular/material/progress-bar';
import {FootprintsService} from './footprints.service';

/**
 * The default calendar header. A header is the one part of a date picker with access to its calendar,
 * which is needed to follow the visible month and to redraw the highlights when data arrives.
 */
@Component({
    selector: 'geoengine-footprint-calendar-header',
    changeDetection: ChangeDetectionStrategy.OnPush,
    template: `
        <mat-calendar-header></mat-calendar-header>
        @if (footprintsService.isLoading()) {
            <mat-progress-bar mode="indeterminate" aria-label="Loading the days with data"></mat-progress-bar>
        }
    `,
    imports: [MatCalendarHeader, MatProgressBarModule],
    // The cells are drawn by the calendar body, outside this component, so the styles have to be global.
    // They use the theme variables every app defines. Only calendars with this header are touched.
    encapsulation: ViewEncapsulation.None,
    styleUrl: './footprint-calendar-header.component.scss',
})
export class FootprintCalendarHeaderComponent {
    readonly footprintsService = inject(FootprintsService);

    constructor() {
        const calendar = inject<MatCalendar<Date>>(MatCalendar);
        const footprintsService = this.footprintsService;

        footprintsService.calendarOpen.set(true);
        footprintsService.showDate(calendar.activeDate);

        const subscription = calendar.stateChanges.subscribe(() => footprintsService.showDate(calendar.activeDate));

        inject(DestroyRef).onDestroy(() => {
            subscription.unsubscribe();
            footprintsService.calendarOpen.set(false);
        });

        effect(() => {
            footprintsService.steps();
            calendar.updateTodaysDate();
        });
    }
}

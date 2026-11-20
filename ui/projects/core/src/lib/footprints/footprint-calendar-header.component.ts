import {ChangeDetectionStrategy, Component, DestroyRef, effect, inject, signal, ViewEncapsulation} from '@angular/core';
import {MatCalendar, MatCalendarHeader} from '@angular/material/datepicker';
import {MatProgressBarModule} from '@angular/material/progress-bar';
import {FootprintsService} from './footprints.service';

/** The calendar header, the one part of a date picker with access to its calendar: it follows the month and redraws the highlights. */
@Component({
    selector: 'geoengine-footprint-calendar-header',
    changeDetection: ChangeDetectionStrategy.OnPush,
    templateUrl: './footprint-calendar-header.component.html',
    imports: [MatCalendarHeader, MatProgressBarModule],
    // the cells are drawn by the calendar body, so the styles are global; only calendars with this header are touched
    encapsulation: ViewEncapsulation.None,
    styleUrl: './footprint-calendar-header.component.scss',
})
export class FootprintCalendarHeaderComponent {
    readonly footprintsService = inject(FootprintsService);
    /** Shown instead of the dates with data in a view that is too coarse for them. */
    readonly hint = signal<string | undefined>(undefined);

    constructor() {
        const calendar = inject<MatCalendar<Date>>(MatCalendar);
        const footprintsService = this.footprintsService;

        footprintsService.calendarOpen.set(true);
        const show = (): void => {
            footprintsService.showDate(calendar.activeDate, calendar.currentView);
            this.hint.set(footprintsService.calendarHint(calendar.currentView));
        };
        show();

        const subscription = calendar.stateChanges.subscribe(show);

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

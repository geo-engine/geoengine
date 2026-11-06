import {ChangeDetectionStrategy, Component, DestroyRef, effect, inject} from '@angular/core';
import {MatCalendar, MatCalendarHeader} from '@angular/material/datepicker';
import {FootprintsService} from './footprints.service';

/**
 * The default calendar header. A header is the one part of a date picker with access to its calendar,
 * which is needed to follow the visible month and to redraw the highlights when data arrives.
 */
@Component({
    selector: 'geoengine-footprint-calendar-header',
    changeDetection: ChangeDetectionStrategy.OnPush,
    template: `<mat-calendar-header></mat-calendar-header>`,
    imports: [MatCalendarHeader],
})
export class FootprintCalendarHeaderComponent {
    constructor() {
        const calendar = inject<MatCalendar<Date>>(MatCalendar);
        const footprintsService = inject(FootprintsService);

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

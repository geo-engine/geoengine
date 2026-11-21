import {Injectable} from '@angular/core';
import {NativeDateAdapter} from '@angular/material/core';

/** Weeks start on Monday, as in the European date format (dd.MM.yyyy) the app shows. */
@Injectable()
export class MondayDateAdapter extends NativeDateAdapter {
    override getFirstDayOfWeek(): number {
        return 1;
    }
}

import {
    Component,
    ChangeDetectionStrategy,
    ChangeDetectorRef,
    AfterViewInit,
    forwardRef,
    OnChanges,
    SimpleChange,
    inject,
    input,
} from '@angular/core';

import {ControlValueAccessor, NG_VALUE_ACCESSOR, FormsModule} from '@angular/forms';
import moment, {Moment, unitOfTime} from 'moment';
import {
    FxLayoutDirective,
    FxLayoutGapDirective,
    FxLayoutAlignDirective,
    FxFlexDirective,
} from '../../util/directives/flexbox-legacy.directive';
import {MatFormField, MatLabel, MatInput} from '@angular/material/input';

@Component({
    selector: 'geoengine-time-input',
    templateUrl: './time-input.component.html',
    styleUrls: ['./time-input.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    providers: [{provide: NG_VALUE_ACCESSOR, useExisting: forwardRef(() => TimeInputComponent), multi: true}],
    imports: [
        FxLayoutDirective,
        FxLayoutGapDirective,
        FxLayoutAlignDirective,
        MatFormField,
        FxFlexDirective,
        MatLabel,
        MatInput,
        FormsModule,
    ],
})
export class TimeInputComponent implements ControlValueAccessor, AfterViewInit, OnChanges {
    private changeDetectorRef = inject(ChangeDetectorRef);

    // TODO: also react on disabled state in `ControlValueAccessor`
    readonly isDisabled = input(false);

    onTouched?: () => void;
    onChange?: (_: Moment) => void = undefined;

    private _time: Moment = moment.utc();

    get time(): Moment {
        return this._time;
    }

    // set accessor including call the onchange callback
    set time(time: Moment) {
        if (time !== this._time) {
            this._time = time;
            if (this.onChange) {
                this.onChange(time);
            }
        }
    }

    ngAfterViewInit(): void {
        setTimeout(() => this.changeDetectorRef.markForCheck(), 0);
    }

    ngOnChanges(_changes: Record<string, SimpleChange>): void {
        this.changeDetectorRef.markForCheck();
    }

    // Set touched on blur
    onBlur(): void {
        if (this.onTouched) {
            this.onTouched();
        }
    }

    writeValue(time: Moment): void {
        this.time = time;
        this.changeDetectorRef.markForCheck();
    }

    registerOnChange(fn: (_: Moment) => void): void {
        this.onChange = fn;
        this.propagateChange();
    }

    registerOnTouched(fn: () => void): void {
        this.onTouched = fn;
    }

    update(timeUnit: unitOfTime.Base, value?: number): void {
        if (typeof value !== 'number') {
            return;
        }

        if (this.time.get(timeUnit) === value) {
            return; // don't update if there is nothing to update
        }

        this.time.set(timeUnit, value);
        this.propagateChange();
    }

    updateDate(value?: number): void {
        if (typeof value !== 'number') {
            return;
        }

        if (this.time.date() === value) {
            return; // don't update if there is nothing to update
        }

        this.time.date(value);
        this.propagateChange();
    }

    private propagateChange(): void {
        if (this.onChange) {
            this.onChange(this.time);
        }
    }
}

import {AfterViewInit, ChangeDetectionStrategy, ChangeDetectorRef, Component, forwardRef, inject, input} from '@angular/core';
import {Subscription} from 'rxjs';
import moment, {Moment} from 'moment';
import {
    AbstractControl,
    ControlValueAccessor,
    FormControl,
    FormGroup,
    NG_VALIDATORS,
    NG_VALUE_ACCESSOR,
    UntypedFormBuilder,
    UntypedFormGroup,
    ValidationErrors,
    Validator,
    Validators,
    FormsModule,
    ReactiveFormsModule,
} from '@angular/forms';
import {Time, TimeStepDuration} from '../time.model';
import {TimeInputComponent} from '../time-input/time-input.component';
import {FxLayoutDirective, FxLayoutAlignDirective} from '../../util/directives/flexbox-legacy.directive';
import {MatSlideToggle} from '@angular/material/slide-toggle';

const startBeforeEndValidator = (control: AbstractControl): ValidationErrors | null => {
    if (!(control instanceof UntypedFormGroup)) {
        return null;
    }

    const start = control.controls.start.value as Moment;
    const end = control.controls.end.value as Moment;
    const timeAsPoint = control.controls.timeAsPoint.value as boolean;

    if (start && end && (timeAsPoint || start.isSameOrBefore(end))) {
        return null;
    } else {
        return {valid: false};
    }
};

export interface TimeIntervalForm {
    start: FormControl<Moment>;
    timeAsPoint: FormControl<boolean>;
    end: FormControl<Moment>;
}

export interface TimeInterval {
    start: Moment;
    timeAsPoint: boolean;
    end: Moment;
}

@Component({
    selector: 'geoengine-time-interval-input',
    templateUrl: './time-interval-input.component.html',
    styleUrls: ['./time-interval-input.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    providers: [
        {provide: NG_VALUE_ACCESSOR, useExisting: forwardRef(() => TimeIntervalInputComponent), multi: true},
        {
            provide: NG_VALIDATORS,
            multi: true,
            useExisting: TimeIntervalInputComponent,
        },
    ],
    imports: [FormsModule, ReactiveFormsModule, TimeInputComponent, FxLayoutDirective, FxLayoutAlignDirective, MatSlideToggle],
})
export class TimeIntervalInputComponent implements ControlValueAccessor, Validator, AfterViewInit {
    private changeDetectorRef = inject(ChangeDetectorRef);
    private formBuilder = inject(UntypedFormBuilder);

    readonly allowRanges = input(true);

    onTouched?: () => void;
    onChange?: (_: Moment) => void = undefined;

    form: FormGroup<TimeIntervalForm>;

    onChangeSub?: Subscription = undefined;

    constructor() {
        // initialize with the current time to have a defined value
        const time = new Time(moment.utc(), moment.utc());

        this.form = this.formBuilder.group({
            start: [time.start, [Validators.required]],
            timeAsPoint: [this.allowRanges(), Validators.required],
            end: [time.end, [Validators.required]],
        });
        this.form.setValidators(startBeforeEndValidator);
    }

    timeStepComparator(option: TimeStepDuration, selectedElement: TimeStepDuration): boolean {
        const equalAmount = option.durationAmount === selectedElement.durationAmount;
        const equalUnit = option.durationUnit === selectedElement.durationUnit;
        return equalAmount && equalUnit;
    }

    ngAfterViewInit(): void {
        setTimeout(() => this.changeDetectorRef.markForCheck());
    }

    // Set touched on blur
    onBlur(): void {
        if (this.onTouched) {
            this.onTouched();
        }
    }

    writeValue(value: TimeInterval): void {
        if (value) {
            this.form.setValue(value);
        }
    }

    registerOnChange(
        onChange: (
            _: Partial<{
                start: moment.Moment;
                timeAsPoint: boolean;
                end: moment.Moment;
            }>,
        ) => void,
    ): void {
        if (this.onChangeSub) {
            this.onChangeSub.unsubscribe();
        }
        this.onChangeSub = this.form.valueChanges.subscribe((a) => onChange(a));
    }

    registerOnTouched(fn: () => void): void {
        this.onTouched = fn;
    }

    validate(_control: AbstractControl): ValidationErrors | null {
        return this.form.valid ? null : {valid: false};
    }
}

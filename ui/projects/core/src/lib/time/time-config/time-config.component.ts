import {AfterViewInit, ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit, inject} from '@angular/core';
import {ProjectService} from '../../project/project.service';
import {Observable, Subscription} from 'rxjs';
import {FormControl, FormGroup, NonNullableFormBuilder, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {CoreConfig} from '../../config.service';
import moment from 'moment';
import {Time, TimeInterval, TimeStepDuration, CommonModule} from '@geoengine/common';
import {SidenavHeaderComponent} from '../../sidenav/sidenav-header/sidenav-header.component';
import {MatCard, MatCardHeader, MatCardTitle, MatCardSubtitle, MatCardContent, MatCardActions} from '@angular/material/card';
import {MatButton} from '@angular/material/button';
import {MatFormField} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {AsyncPipe} from '@angular/common';

export interface TimeConfigForm {
    timeInterval: FormControl<TimeInterval>;
}

@Component({
    selector: 'geoengine-time-config',
    templateUrl: './time-config.component.html',
    styleUrls: ['./time-config.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        SidenavHeaderComponent,
        MatCard,
        MatCardHeader,
        MatCardTitle,
        MatCardSubtitle,
        MatCardContent,
        FormsModule,
        ReactiveFormsModule,
        CommonModule,
        MatCardActions,
        MatButton,
        MatFormField,
        MatSelect,
        MatOption,
        AsyncPipe,
    ],
})
export class TimeConfigComponent implements OnInit, OnDestroy, AfterViewInit {
    private projectService = inject(ProjectService);
    private changeDetectorRef = inject(ChangeDetectorRef);
    private formBuilder = inject(NonNullableFormBuilder);
    config = inject(CoreConfig);

    form: FormGroup<TimeConfigForm>;

    timeStepDuration$: Observable<TimeStepDuration>;
    timeStepDurations: Array<TimeStepDuration> = [
        {durationAmount: 1, durationUnit: 'minute'},
        {durationAmount: 15, durationUnit: 'minutes'},
        {durationAmount: 1, durationUnit: 'hour'},
        {durationAmount: 1, durationUnit: 'day'},
        {durationAmount: 1, durationUnit: 'month'},
        {durationAmount: 6, durationUnit: 'months'},
        {durationAmount: 1, durationUnit: 'year'},
    ];

    protected time: Time;

    private projectTimeSubscription?: Subscription;

    constructor() {
        // initialize with the current time to have a defined value
        this.time = new Time(moment.utc(), moment.utc());

        this.form = this.formBuilder.group({
            timeInterval: [{start: this.time.start, timeAsPoint: true, end: this.time.end}, [Validators.required]],
        });

        this.timeStepDuration$ = this.projectService.getTimeStepDurationStream();
    }

    timeStepComparator(option: TimeStepDuration, selectedElement: TimeStepDuration): boolean {
        const equalAmount = option.durationAmount === selectedElement.durationAmount;
        const equalUnit = option.durationUnit === selectedElement.durationUnit;
        return equalAmount && equalUnit;
    }

    ngOnInit(): void {
        this.projectTimeSubscription = this.projectService.getTimeStream().subscribe((time) => {
            this.time = time.clone();
            this.reset();
        });
    }

    ngAfterViewInit(): void {
        setTimeout(() => this.changeDetectorRef.markForCheck());
    }

    ngOnDestroy(): void {
        this.projectTimeSubscription?.unsubscribe();
    }

    applyTime(): void {
        if (!this.form.valid) {
            return;
        }

        const time = this.formToTime();

        this.updateTime(time);
    }

    reset(): void {
        const reset = this.time.clone();

        this.form.controls['timeInterval'].setValue({
            start: reset.start,
            end: reset.end,
            timeAsPoint: reset.start.isSame(reset.end),
        });
    }

    isNotResettable(): boolean {
        return this.formToTime().isSame(this.time);
    }

    updateTimeStepDuration(timeStep: TimeStepDuration): void {
        this.projectService.setTimeStepDuration(timeStep);
    }

    protected formToTime(): Time {
        const timeInterval = this.form.get('timeInterval')!.value;

        const start = timeInterval.start;
        const timeAsPoint = timeInterval.timeAsPoint;
        let end = timeInterval.end;

        if (timeAsPoint) {
            end = start;
        }

        return new Time(start, end);
    }

    protected updateTime(time: Time): void {
        if (!time.isValid) {
            return;
        }
        this.projectService.setTime(time);
    }
}

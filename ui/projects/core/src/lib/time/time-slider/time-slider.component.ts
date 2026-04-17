import {Component, OnInit, ChangeDetectionStrategy, ChangeDetectorRef, OnDestroy, inject, input, viewChild} from '@angular/core';
import {DataSet} from 'vis-data/peer';
import {DateType, Timeline} from 'vis-timeline/peer';
import {ElementRef} from '@angular/core';
import {LayoutService} from '../../layout.service';
import {ProjectService} from '../../project/project.service';
import moment, {DurationInputArg2, Moment} from 'moment';
import {Subscription} from 'rxjs';
import {Layer, Time, FxLayoutDirective, FxLayoutGapDirective, FxLayoutAlignDirective} from '@geoengine/common';
import {MatButton} from '@angular/material/button';
import {MatSelect} from '@angular/material/select';
import {FormsModule} from '@angular/forms';
import {MatOption} from '@angular/material/autocomplete';

@Component({
    selector: 'geoengine-time-slider',
    templateUrl: './time-slider.component.html',
    styleUrls: ['./time-slider.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [FxLayoutDirective, FxLayoutGapDirective, FxLayoutAlignDirective, MatButton, MatSelect, FormsModule, MatOption],
})
export class TimeSliderComponent implements OnInit, OnDestroy {
    protected readonly projectService = inject(ProjectService);
    protected readonly layoutService = inject(LayoutService);
    private changeDetectorRef = inject(ChangeDetectorRef);

    //Timeline Data for vis-timeline
    timeline: Timeline | undefined;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    options: any;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    data: any;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    groups: any;

    //Time Data
    startTime: DateType = new Date();
    endTime: DateType = new Date();
    timeScale: {value: DurationInputArg2; text: string}[] = [
        {value: 'hour', text: 'Hours'},
        {value: 'day', text: 'Days'},
        {value: 'month', text: 'Months'},
        {value: 'year', text: 'Years'},
    ];
    selectedScale: DurationInputArg2 = 'year';
    isRange = true;

    readonly height = input(150);

    screenWidth = 0;

    //Layer Data
    layerList: Array<Layer> = [];

    readonly timelineContainer = viewChild.required<ElementRef>('timeline');

    // inventory of used subscriptions
    private subscriptions: Array<Subscription> = [];

    constructor() {
        this.subscriptions.push(
            this.projectService.getLayerStream().subscribe((layerList) => {
                if (layerList !== this.layerList) {
                    this.layerList = layerList;
                }
                this.updateTimeline();
            }),
        );
    }

    ngOnInit(): void {
        this.screenWidth = window.innerWidth;

        this.getTimelineData();
        this.getTimelineGroups();
        this.getOptions();

        this.timeline = new Timeline(this.timelineContainer().nativeElement, this.data, this.options);
        this.timeline.setGroups(this.groups);
        this.timeline.setItems(this.data);

        this.timeline.addCustomTime(this.startTime, 'start');
        this.timeline.addCustomTime(this.endTime, 'end');

        //listens to events of the custom timebars
        this.timeline.on('timechanged', (properties) => {
            if (properties.id === 'start') {
                this.startTime = this.timeline!.getCustomTime(properties.id);
            }
            if (properties.id === 'end') {
                this.endTime = this.timeline!.getCustomTime(properties.id);
            }
            if (!this.isRange) {
                this.endTime = this.startTime;
            }
            this.changeTime();
        });

        this.timeline.on('rangechanged', (properties) => {
            const startWindow = properties.start;
            const endWindow = properties.end;
            const windowSize = endWindow - startWindow;
            this.changeSelectedScale(windowSize);
        });

        //Connection to the timestream of the project
        //changes the position of the Timebars according to the Timestream of the project
        this.subscriptions.push(
            this.projectService.getTimeStream().subscribe((t) => {
                this.startTime = t.start.toDate();
                this.endTime = t.end.toDate();
                this.changeDetectorRef.markForCheck();
                if (this.isRange) {
                    this.timeline?.removeCustomTime('end');
                    this.isRange = false;
                }
                if (t.end.isAfter(t.start)) {
                    this.timeline?.addCustomTime(t.end.toDate(), 'end');
                    this.isRange = true;
                }
                this.timeline?.setCustomTime(t.start.toDate(), 'start');
            }),
        );
    }

    updateTimeline(): void {
        if (this.timeline) {
            this.getTimelineData();
            this.getTimelineGroups();
            this.timeline.setGroups(this.groups);
            this.timeline.setItems(this.data);
        }
    }

    changeSelectedScale(windowSize: number): void {
        this.changeDetectorRef.detectChanges();
        //these scales are fit to a screen size of 1280 pixels and then resized with the screenwidth
        const yearScale = 1000 * 60 * 60 * 24 * 31 * 17; //17 months in milliseconds
        const monthScale = 1000 * 60 * 60 * 24 * 12; //12 days in milliseconds
        const dayScale = 1000 * 60 * 60 * 23; //23 hours in milliseconds
        if (windowSize > (yearScale * this.screenWidth) / 1280) this.selectedScale = 'year';
        else if (windowSize > (monthScale * this.screenWidth) / 1280) this.selectedScale = 'month';
        else if (windowSize > (dayScale * this.screenWidth) / 1280) this.selectedScale = 'day';
        else this.selectedScale = 'hour';
    }

    //changes the Timestream of the project according to the Timebars
    async changeTime(): Promise<void> {
        // TODO: Angular recognizes inifinite loop here --> FIX
        await this.projectService.getTimeOnce();

        const updatedTime = new Time(moment(this.startTime), moment(this.endTime));
        this.projectService.setTime(updatedTime);
    }

    ngOnDestroy(): void {
        this.subscriptions.forEach((s) => s.unsubscribe());
        this.timeline?.off('timechanged');
    }

    centerTimeline(): void {
        this.timeline?.moveTo(this.startTime);
    }

    async changeScale(selectedScale: DurationInputArg2): Promise<void> {
        const time = await this.projectService.getTimeOnce();
        const steps = 8;
        const startWindow = time.start.clone().subtract(steps / 2, selectedScale);
        const endWindow = time.start.clone().add(steps / 2, selectedScale);
        this.timeline?.setWindow(startWindow, endWindow);
    }

    //original snap-function from vis-timeline https://github.com/visjs/vis-timeline/blob/master/lib/timeline/TimeStep.js
    //only change: clone is in utc
    snapFunction(date: Date, scale: string, step: number): Moment {
        const clone = moment(date).utc();

        if (scale === 'year') {
            const year = clone.year() + Math.round(clone.month() / 12);
            clone.year(Math.round(year / step) * step);
            clone.month(0);
            clone.date(0);
            clone.hours(0);
            clone.minutes(0);
            clone.seconds(0);
            clone.milliseconds(0);
        } else if (scale === 'month') {
            if (clone.date() > 15) {
                clone.date(1);
                clone.add(1, 'month');
                // important: first set Date to 1, after that change the month.
            } else {
                clone.date(1);
            }

            clone.hours(0);
            clone.minutes(0);
            clone.seconds(0);
            clone.milliseconds(0);
        } else if (scale === 'week') {
            if (clone.weekday() > 2) {
                // doing it the momentjs locale aware way
                clone.weekday(0);
                clone.add(1, 'week');
            } else {
                clone.weekday(0);
            }

            clone.hours(0);
            clone.minutes(0);
            clone.seconds(0);
            clone.milliseconds(0);
        } else if (scale === 'day') {
            //noinspection FallthroughInSwitchStatementJS
            switch (step) {
                case 5:
                case 2:
                    clone.hours(Math.round(clone.hours() / 24) * 24);
                    break;
                default:
                    clone.hours(Math.round(clone.hours() / 12) * 12);
                    break;
            }
            clone.minutes(0);
            clone.seconds(0);
            clone.milliseconds(0);
        } else if (scale === 'weekday') {
            //noinspection FallthroughInSwitchStatementJS
            switch (step) {
                case 5:
                case 2:
                    clone.hours(Math.round(clone.hours() / 12) * 12);
                    break;
                default:
                    clone.hours(Math.round(clone.hours() / 6) * 6);
                    break;
            }
            clone.minutes(0);
            clone.seconds(0);
            clone.milliseconds(0);
        } else if (scale === 'hour') {
            switch (step) {
                case 4:
                    clone.minutes(Math.round(clone.minutes() / 60) * 60);
                    break;
                default:
                    clone.minutes(Math.round(clone.minutes() / 30) * 30);
                    break;
            }
            clone.seconds(0);
            clone.milliseconds(0);
        }
        return clone;
    }

    //every group represents one layer
    getTimelineGroups(): void {
        this.groups = new DataSet([]);
        if (this.layerList.length > 0) {
            for (const layer of this.layerList) {
                this.groups.add({
                    id: layer.id,
                    content: layer.name,
                });
            }
        }
    }

    //every group (layer) can hav multiple data points
    //ToDo: implement the availability of the layers
    getTimelineData(): void {
        this.data = new DataSet();
        for (const layer of this.layerList) {
            this.data.add({
                id: layer.id,
                group: layer.id,
                content: '',
                //     type: 'point' or 'background',
                start: '2012-01',
                end: '2025-01',
            });
        }
    }

    //options for the timeline
    getOptions(): void {
        this.options = {
            moment: (date: Date): moment.Moment => moment(date).utc(), //use utc
            snap: (date: Date, scale: string, step: number): Moment => this.snapFunction(date, scale, step),
            start: '2012-01',
            end: '2020-01',
            orientation: 'top',
            height: this.height(),
            itemsAlwaysDraggable: false,
            editable: true,
            selectable: false,
            margin: {
                axis: 5,
            },
            showMajorLabels: true,
            showMinorLabels: true,
            showCurrentTime: false,
            zoomMin: 1000 * 60 * 60 * 12, // one day in milliseconds
            zoomMax: 1000 * 60 * 60 * 24 * 31 * 12 * 20, // twenty years in milliseconds
        };
    }
}

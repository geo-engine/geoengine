import {AfterViewInit, ChangeDetectionStrategy, Component, inject} from '@angular/core';
import {FormControl, FormGroup, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {ProjectService} from '../../../project/project.service';
import {map, mergeMap} from 'rxjs/operators';
import {TimeStepGranularityDict} from '../../../backend/backend.model';
import {BehaviorSubject, Observable} from 'rxjs';
import moment from 'moment';
import {
    AbsoluteTimeShiftDictParams,
    Layer,
    NotificationService,
    RasterLayer,
    RasterSymbology,
    RelativeTimeShiftDictParams,
    ResultTypes,
    Time,
    TimeShiftDict,
    VectorLayer,
    VectorSymbology,
    geoengineValidators,
    timeStepGranularityOptions,
    CommonModule,
    AsyncValueDefault,
} from '@geoengine/common';
import {Workflow as WorkflowDict} from '@geoengine/api-client';
import {SidenavHeaderComponent} from '../../../sidenav/sidenav-header/sidenav-header.component';
import {OperatorDialogContainerComponent} from '../helpers/operator-dialog-container/operator-dialog-container.component';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {LayerSelectionComponent} from '../helpers/layer-selection/layer-selection.component';
import {MatFormField, MatLabel, MatInput, MatHint} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {MatSlideToggle} from '@angular/material/slide-toggle';
import {OperatorOutputNameComponent} from '../helpers/operator-output-name/operator-output-name.component';
import {AsyncPipe} from '@angular/common';

type TimeShiftFormType = 'relative' | 'absolute';

interface TimeShiftForm {
    name: FormControl<string>;
    source: FormControl<Layer | undefined>;
    type: FormControl<TimeShiftFormType>;
    // for absolute
    timeInterval: FormGroup<{start: FormControl<moment.Moment>; end: FormControl<moment.Moment>; timeAsPoint: FormControl<boolean>}>;
    // for relative
    granularity: FormControl<TimeStepGranularityDict>;
    value: FormControl<number>;
}

@Component({
    selector: 'geoengine-time-shift',
    templateUrl: './time-shift.component.html',
    styleUrls: ['./time-shift.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        SidenavHeaderComponent,
        FormsModule,
        ReactiveFormsModule,
        OperatorDialogContainerComponent,
        MatIconButton,
        MatIcon,
        LayerSelectionComponent,
        MatFormField,
        MatLabel,
        MatSelect,
        MatOption,
        MatInput,
        MatHint,
        CommonModule,
        MatSlideToggle,
        OperatorOutputNameComponent,
        MatButton,
        AsyncPipe,
        AsyncValueDefault,
    ],
})
export class TimeShiftComponent implements AfterViewInit {
    private readonly projectService = inject(ProjectService);
    private readonly notificationService = inject(NotificationService);

    readonly inputTypes = [ResultTypes.RASTER, ...ResultTypes.VECTOR_TYPES];

    readonly timeGranularityOptions: Array<TimeStepGranularityDict> = timeStepGranularityOptions;
    readonly defaultTimeGranularity: TimeStepGranularityDict = 'months';

    readonly defaultTimeShiftType: TimeShiftFormType = 'relative';

    readonly loading$ = new BehaviorSubject<boolean>(false);

    form: FormGroup<TimeShiftForm>;
    disallowSubmit: Observable<boolean>;

    constructor() {
        const form = new FormGroup<TimeShiftForm>({
            name: new FormControl('Time Shift', {
                validators: [Validators.required, geoengineValidators.notOnlyWhitespace],
                nonNullable: true,
            }),
            source: new FormControl<Layer | undefined>(undefined, {validators: Validators.required, nonNullable: true}),
            type: new FormControl<TimeShiftFormType>('relative', {validators: Validators.required, nonNullable: true}),
            granularity: new FormControl(this.defaultTimeGranularity, {validators: Validators.required, nonNullable: true}),
            value: new FormControl(-1, {
                validators: [Validators.required, geoengineValidators.notZero, Validators.pattern(/^-?\d+$/)],
                nonNullable: true,
            }),
            timeInterval: new FormGroup(
                {
                    start: new FormControl(moment.utc('2014-01-01'), {validators: Validators.required, nonNullable: true}),
                    end: new FormControl(moment.utc('2014-01-01'), {validators: Validators.required, nonNullable: true}),
                    timeAsPoint: new FormControl(true, {validators: Validators.required, nonNullable: true}),
                },
                {validators: geoengineValidators.startBeforeEndValidator},
            ),
        });
        this.form = form;
        this.disallowSubmit = this.form.statusChanges.pipe(map((status) => status !== 'VALID'));
    }

    ngAfterViewInit(): void {
        setTimeout(() => {
            this.form.updateValueAndValidity();
            this.form.controls['source'].updateValueAndValidity();
        });
    }

    changeShiftType(type: TimeShiftFormType): void {
        if (type === 'relative') {
            this.changeToRelative();
        } else if (type === 'absolute') {
            this.changeToAbsolute();
        }
    }

    changeToRelative(): void {
        this.form.controls['type'].setValue('relative');

        this.form.controls['granularity'].enable();
        this.form.controls['value'].enable();

        this.form.controls['timeInterval'].disable();
    }

    changeToAbsolute(): void {
        this.form.controls['type'].setValue('absolute');

        this.form.controls['granularity'].disable();
        this.form.controls['value'].disable();

        this.form.controls['timeInterval'].enable();
    }

    add(): void {
        if (this.loading$.value) {
            return; // don't add while loading
        }

        const sourceLayer: Layer | undefined = this.form.controls['source'].value;

        if (!sourceLayer) {
            return; // should be captured by form validation
        }

        const outputName: string = this.form.controls['name'].value;
        const type: TimeShiftFormType = this.form.controls['type'].value;

        let params: AbsoluteTimeShiftDictParams | RelativeTimeShiftDictParams;

        if (type === 'absolute') {
            const timeInput = this.form.controls['timeInterval'].value;

            let time: Time;
            if (timeInput.timeAsPoint) {
                time = new Time(timeInput.start);
            } else {
                time = new Time(timeInput.start, timeInput.end);
            }

            params = {
                type,
                timeInterval: time.toDict(),
            } as AbsoluteTimeShiftDictParams;
        } else if (type === 'relative') {
            params = {
                type,
                granularity: this.form.controls['granularity'].value,
                value: this.form.controls['value'].value,
            } as RelativeTimeShiftDictParams;
        } else {
            // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
            throw Error(`Invalid time shift type ${type}`);
        }

        let layerType: 'Vector' | 'Raster';
        if (sourceLayer.layerType === 'raster') {
            layerType = 'Raster';
        } else if (sourceLayer.layerType === 'vector') {
            layerType = 'Vector';
        } else {
            // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
            throw Error(`Invalid layer type ${sourceLayer.layerType}`);
        }

        this.loading$.next(true);

        this.projectService
            .getWorkflow(sourceLayer.workflowId)
            .pipe(
                mergeMap((inputWorkflow: WorkflowDict) =>
                    this.projectService.registerWorkflow({
                        type: layerType,
                        operator: {
                            type: 'TimeShift',
                            params,
                            sources: {
                                source: inputWorkflow.operator,
                            },
                        } as TimeShiftDict,
                    }),
                ),
                mergeMap((workflowId) => {
                    if (layerType === 'Vector') {
                        return this.projectService.addLayer(
                            new VectorLayer({
                                workflowId,
                                name: outputName,
                                symbology: sourceLayer.symbology as VectorSymbology,
                                isLegendVisible: false,
                                isVisible: true,
                            }),
                        );
                    } else if (layerType === 'Raster') {
                        return this.projectService.addLayer(
                            new RasterLayer({
                                workflowId,
                                name: outputName,
                                symbology: sourceLayer.symbology as RasterSymbology,
                                isLegendVisible: false,
                                isVisible: true,
                            }),
                        );
                    } else {
                        // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
                        throw Error(`Invalid layer type ${layerType}`);
                    }
                }),
            )
            .subscribe({
                next: () => {
                    // success
                    this.loading$.next(false);
                },
                error: (error) => {
                    this.notificationService.error(error);
                    this.loading$.next(false);
                },
            });
    }
}

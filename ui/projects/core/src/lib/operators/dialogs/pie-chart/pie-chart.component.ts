import {AfterViewInit, ChangeDetectionStrategy, Component, OnDestroy, inject} from '@angular/core';
import {FormControl, FormGroup, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {of, ReplaySubject, Subscription} from 'rxjs';
import {ProjectService} from '../../../project/project.service';
import {map, mergeMap, tap} from 'rxjs/operators';
import {
    NotificationService,
    PieChartCountParams,
    PieChartDict,
    Plot,
    ResultTypes,
    VectorLayer,
    VectorLayerMetadata,
    geoengineValidators,
    FxLayoutDirective,
} from '@geoengine/common';
import {Workflow as WorkflowDict} from '@geoengine/api-client';
import {SidenavHeaderComponent} from '../../../sidenav/sidenav-header/sidenav-header.component';
import {OperatorDialogContainerComponent} from '../helpers/operator-dialog-container/operator-dialog-container.component';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {LayerSelectionComponent} from '../helpers/layer-selection/layer-selection.component';
import {MatFormField, MatLabel, MatHint} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {MatCheckbox} from '@angular/material/checkbox';
import {OperatorOutputNameComponent} from '../helpers/operator-output-name/operator-output-name.component';
import {AsyncPipe} from '@angular/common';

interface PieChartForm {
    name: FormControl<string>;
    type: FormControl<'count'>;
    layer: FormControl<VectorLayer | undefined>;
    attribute: FormControl<string | undefined>;
    donut: FormControl<boolean>;
}

/**
 * This dialog allows creating a histogram plot of a layer's values.
 */
@Component({
    selector: 'geoengine-pie-chart',
    templateUrl: './pie-chart.component.html',
    styleUrls: ['./pie-chart.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        SidenavHeaderComponent,
        FormsModule,
        FxLayoutDirective,
        ReactiveFormsModule,
        OperatorDialogContainerComponent,
        MatIconButton,
        MatIcon,
        LayerSelectionComponent,
        MatFormField,
        MatLabel,
        MatSelect,
        MatOption,
        MatCheckbox,
        OperatorOutputNameComponent,
        MatHint,
        MatButton,
        AsyncPipe,
    ],
})
export class PieChartComponent implements AfterViewInit, OnDestroy {
    private readonly projectService = inject(ProjectService);
    private readonly notificationService = inject(NotificationService);

    minNumberOfBuckets = 1;
    maxNumberOfBuckets = 100;

    inputTypes = ResultTypes.INPUT_TYPES;

    form: FormGroup<PieChartForm>;

    attributes$ = new ReplaySubject<Array<string>>(1);

    private subscriptions: Array<Subscription> = [];

    /**
     * DI for services
     */
    constructor() {
        this.form = new FormGroup({
            name: new FormControl('Filtered Values', {
                validators: [Validators.required, geoengineValidators.notOnlyWhitespace],
                nonNullable: true,
            }),
            layer: new FormControl<VectorLayer | undefined>(undefined, {
                validators: [Validators.required],
                nonNullable: true,
            }),
            type: new FormControl('count', {
                validators: [Validators.required, geoengineValidators.notOnlyWhitespace],
                nonNullable: true,
            }),
            attribute: new FormControl<string | undefined>(undefined, {
                validators: [Validators.required, geoengineValidators.notOnlyWhitespace],
                nonNullable: true,
            }),
            donut: new FormControl(false, {
                validators: Validators.required,
                nonNullable: true,
            }),
        });

        this.form.controls['type'].disable(); // TODO: remove when other options are available

        this.subscriptions.push(
            this.form.controls.layer.valueChanges
                .pipe(
                    tap({
                        next: () => this.form.controls.attribute.setValue(undefined),
                    }),
                    mergeMap((layer: VectorLayer | undefined) => {
                        if (!layer || !(layer instanceof VectorLayer)) {
                            return of([]);
                        }

                        return this.projectService
                            .getVectorLayerMetadata(layer)
                            .pipe(map((metadata: VectorLayerMetadata) => metadata.dataTypes.keySeq().toArray()));
                    }),
                )
                .subscribe((attributes) => this.attributes$.next(attributes)),
        );
    }

    ngAfterViewInit(): void {
        setTimeout(() => {
            this.form.updateValueAndValidity();
            this.form.controls['layer'].updateValueAndValidity();
        });
    }

    ngOnDestroy(): void {
        this.subscriptions.forEach((subscription) => subscription.unsubscribe());
    }

    /**
     * Uses the user input to create a histogram plot.
     * The plot is added to the plot view.
     */
    add(): void {
        const inputLayer = this.form.controls['layer'].value;
        const columnName = this.form.controls['attribute'].value;

        if (!inputLayer || !columnName) {
            return;
        }

        const pieChartType = this.form.controls['type'].value;
        const donut = this.form.controls['donut'].value;
        const outputName: string = this.form.controls['name'].value;

        this.projectService
            .getWorkflow(inputLayer.workflowId)
            .pipe(
                mergeMap((inputWorkflow: WorkflowDict) =>
                    this.projectService.registerWorkflow({
                        type: 'Plot',
                        operator: {
                            type: 'PieChart',
                            params: {
                                type: pieChartType,
                                columnName,
                                donut,
                            } as PieChartCountParams,
                            sources: {
                                vector: inputWorkflow.operator,
                            },
                        } as PieChartDict,
                    }),
                ),
                mergeMap((workflowId) =>
                    this.projectService.addPlot(
                        new Plot({
                            workflowId,
                            name: outputName,
                        }),
                    ),
                ),
            )
            .subscribe(
                () => {
                    // success
                },
                (error) => this.notificationService.error(error),
            );
    }
}

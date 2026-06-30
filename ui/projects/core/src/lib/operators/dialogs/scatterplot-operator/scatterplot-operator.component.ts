import {AfterViewInit, ChangeDetectionStrategy, Component, OnDestroy, inject} from '@angular/core';
import {UntypedFormBuilder, UntypedFormGroup, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {ReplaySubject, Subscription} from 'rxjs';
import {ProjectService} from '../../../project/project.service';

import {map, mergeMap} from 'rxjs/operators';
import {
    Layer,
    NotificationService,
    Plot,
    ResultTypes,
    ScatterPlotDict,
    VectorColumnDataTypes,
    VectorLayer,
    VectorLayerMetadata,
    geoengineValidators,
} from '@geoengine/common';
import {Workflow as WorkflowDict} from '@geoengine/api-client';
import {SidenavHeaderComponent} from '../../../sidenav/sidenav-header/sidenav-header.component';
import {OperatorDialogContainerComponent} from '../helpers/operator-dialog-container/operator-dialog-container.component';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {LayerSelectionComponent} from '../helpers/layer-selection/layer-selection.component';
import {MatFormField, MatHint} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {OperatorOutputNameComponent} from '../helpers/operator-output-name/operator-output-name.component';
import {AsyncPipe} from '@angular/common';

/**
 * This dialog allows creating a box plot of a layer's values.
 */
@Component({
    selector: 'geoengine-boxplot-operator',
    templateUrl: './scatterplot-operator.component.html',
    styleUrls: ['./scatterplot-operator.component.scss'],
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
        MatSelect,
        MatOption,
        OperatorOutputNameComponent,
        MatHint,
        MatButton,
        AsyncPipe,
    ],
})
export class ScatterplotOperatorComponent implements AfterViewInit, OnDestroy {
    private readonly projectService = inject(ProjectService);
    private readonly notificationService = inject(NotificationService);
    private readonly formBuilder = inject(UntypedFormBuilder);

    inputTypes = ResultTypes.VECTOR_TYPES;

    form: UntypedFormGroup;

    attributes$ = new ReplaySubject<Array<string>>(1);

    private subscriptions: Array<Subscription> = [];

    /**
     * DI for services
     */
    constructor() {
        const layerControl = this.formBuilder.control(undefined, Validators.required);
        this.form = this.formBuilder.group({
            name: ['Filtered Values', [Validators.required, geoengineValidators.notOnlyWhitespace]],
            layer: layerControl,
            columnX: [undefined, Validators.required],
            columnY: [undefined, Validators.required],
        });

        this.subscriptions.push(
            this.form.controls['layer'].valueChanges
                .pipe(
                    mergeMap((layer: Layer) =>
                        this.projectService.getVectorLayerMetadata(layer as VectorLayer).pipe(
                            map((metadata: VectorLayerMetadata) =>
                                metadata.dataTypes
                                    .filter(
                                        (columnType) =>
                                            columnType === VectorColumnDataTypes.Float || columnType === VectorColumnDataTypes.Int,
                                    )
                                    .keySeq()
                                    .toArray(),
                            ),
                        ),
                    ),
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
     * Uses the user input to create a box plot.
     * The plot is added to the plot view.
     */
    add(): void {
        const inputLayer = this.form.controls['layer'].value as Layer;

        const columnX = this.form.controls['columnX'].value as string;

        const columnY = this.form.controls['columnY'].value as string;

        const outputName: string = this.form.controls['name'].value;

        this.projectService
            .getWorkflow(inputLayer.workflowId)
            .pipe(
                mergeMap((inputWorkflow: WorkflowDict) =>
                    this.projectService.registerWorkflow({
                        type: 'Plot',
                        operator: {
                            type: 'ScatterPlot',
                            params: {
                                columnX,
                                columnY,
                            },
                            sources: {
                                vector: inputWorkflow.operator,
                            },
                        } as ScatterPlotDict,
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

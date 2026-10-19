import {AsyncPipe} from '@angular/common';
import {AfterViewInit, ChangeDetectionStrategy, Component, OnDestroy, inject} from '@angular/core';
import {FormArray, FormBuilder, FormControl, FormGroup, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {MatOption} from '@angular/material/autocomplete';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {MatFormField, MatHint} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {from, Observable, of, ReplaySubject, Subscription} from 'rxjs';
import {map, mergeMap, tap} from 'rxjs/operators';
import {AttributeSelection, BoxPlot, ProcessingGraph, RasterOperator, VectorOperator} from '@geoengine/api-client';
import {
    Layer,
    NotificationService,
    Plot,
    ResultTypes,
    VectorColumnDataTypes,
    VectorLayer,
    VectorLayerMetadata,
    geoengineValidators,
    FxLayoutDirective,
    FxFlexDirective,
    FxLayoutAlignDirective,
} from '@geoengine/common';
import {DialogSectionHeadingComponent} from '../../../dialogs/dialog-section-heading/dialog-section-heading.component';
import {ProjectService} from '../../../project/project.service';
import {SidenavHeaderComponent} from '../../../sidenav/sidenav-header/sidenav-header.component';
import {LayerSelectionComponent} from '../helpers/layer-selection/layer-selection.component';
import {OperatorDialogContainerComponent} from '../helpers/operator-dialog-container/operator-dialog-container.component';
import {OperatorOutputNameComponent} from '../helpers/operator-output-name/operator-output-name.component';

interface BoxPlotForm {
    name: FormControl<string>;
    layer: FormControl<Layer | null>;
    columnNames: FormArray<FormControl<string | null>>;
}

/**
 * This dialog allows creating a box plot of a layer's values.
 */
@Component({
    selector: 'geoengine-boxplot-operator',
    templateUrl: './boxplot-operator.component.html',
    styleUrls: ['./boxplot-operator.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        SidenavHeaderComponent,
        FormsModule,
        ReactiveFormsModule,
        OperatorDialogContainerComponent,
        MatIconButton,
        MatIcon,
        LayerSelectionComponent,
        FxLayoutDirective,
        DialogSectionHeadingComponent,
        FxFlexDirective,
        FxLayoutAlignDirective,
        MatButton,
        MatFormField,
        MatSelect,
        MatOption,
        OperatorOutputNameComponent,
        MatHint,
        AsyncPipe,
    ],
})
export class BoxPlotOperatorComponent implements AfterViewInit, OnDestroy {
    private readonly projectService = inject(ProjectService);
    private readonly notificationService = inject(NotificationService);
    private readonly formBuilder = inject(FormBuilder);

    readonly inputTypes = ResultTypes.INPUT_TYPES;

    form: FormGroup<BoxPlotForm>;

    attributes$ = new ReplaySubject<Array<string>>(1);

    isVectorLayer$: Observable<boolean>;

    private subscriptions: Array<Subscription> = [];

    /**
     * DI for services
     */
    constructor() {
        const layerControl = this.formBuilder.control<Layer | null>(null, Validators.required);
        this.form = this.formBuilder.group({
            name: this.formBuilder.nonNullable.control('Filtered Values', [Validators.required, geoengineValidators.notOnlyWhitespace]),
            layer: layerControl,
            columnNames: this.formBuilder.array<FormControl<string | null>>(
                [],
                geoengineValidators.conditionalValidator(Validators.required, () => layerControl.value?.isVectorLayer ?? false),
            ),
        });

        this.subscriptions.push(
            this.form.controls.layer.valueChanges
                .pipe(
                    tap(() => {
                        this.columnNames.clear();
                        if (layerControl.value?.isVectorLayer) {
                            this.addColumn();
                        }
                    }),
                    mergeMap((layer: Layer | null) => {
                        if (layer instanceof VectorLayer) {
                            return this.projectService.getVectorLayerMetadata(layer).pipe(
                                map((metadata: VectorLayerMetadata) =>
                                    metadata.dataTypes
                                        .filter(
                                            (columnType) =>
                                                columnType === VectorColumnDataTypes.Float || columnType === VectorColumnDataTypes.Int,
                                        )
                                        .keySeq()
                                        .toArray(),
                                ),
                            );
                        } else {
                            return of([]);
                        }
                    }),
                )
                .subscribe((attributes) => this.attributes$.next(attributes)),
        );
        this.isVectorLayer$ = this.form.controls.layer.valueChanges.pipe(map((layer) => layer?.isVectorLayer ?? false));
    }

    ngAfterViewInit(): void {
        setTimeout(() => {
            this.form.updateValueAndValidity();
            this.form.controls.layer.updateValueAndValidity();
        });
    }

    ngOnDestroy(): void {
        this.subscriptions.forEach((subscription) => subscription.unsubscribe());
    }

    get columnNames(): FormArray<FormControl<string | null>> {
        return this.form.controls.columnNames;
    }

    addColumn(): void {
        this.columnNames.push(this.formBuilder.control<string | null>(null, Validators.required));
    }

    removeColumn(i: number): void {
        this.columnNames.removeAt(i);
    }

    /**
     * Uses the user input to create a box plot.
     * The plot is added to the plot view.
     */
    add(): void {
        const inputLayer = this.form.controls.layer.value!;

        // for rasters, select all bands
        const attributes: AttributeSelection = inputLayer.isVectorLayer
            ? {type: 'names', values: [...new Set(this.columnNames.controls.map((fc) => fc.value?.toString() ?? ''))]}
            : {type: 'all'};

        const outputName: string = this.form.controls.name.value;

        from(this.projectService.getProcessingGraph(inputLayer.workflowId))
            .pipe(
                mergeMap((inputWorkflow: ProcessingGraph) => {
                    if (inputWorkflow.type !== 'Raster' && inputWorkflow.type !== 'Vector') {
                        throw new Error(`Invalid workflow type ${inputWorkflow.type}.`);
                    }

                    const source: RasterOperator | VectorOperator = inputWorkflow.operator;

                    return this.projectService.registerProcessingGraph({
                        type: 'Plot',
                        operator: {
                            type: 'BoxPlot',
                            params: {
                                attributes,
                            },
                            sources: {
                                source,
                            },
                        } as BoxPlot,
                    });
                }),
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
                (error: unknown) => this.notificationService.error(error instanceof Error ? error.message : String(error)),
            );
    }
}

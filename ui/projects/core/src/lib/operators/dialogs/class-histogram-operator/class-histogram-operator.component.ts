import {AfterViewInit, ChangeDetectionStrategy, Component, OnDestroy, inject} from '@angular/core';
import {
    AbstractControl,
    AsyncValidatorFn,
    FormControl,
    FormGroup,
    FormsModule,
    ReactiveFormsModule,
    Validators,
    NonNullableFormBuilder,
} from '@angular/forms';
import {Observable, of, ReplaySubject, Subscription, first} from 'rxjs';
import {ProjectService} from '../../../project/project.service';
import {map, mergeMap, tap} from 'rxjs/operators';
import {
    ClassHistogramDict,
    ClassificationMeasurement,
    Layer,
    NotificationService,
    Plot,
    RasterLayer,
    ResultTypes,
    VectorColumnDataTypes,
    VectorLayer,
    VectorLayerMetadata,
    geoengineValidators,
} from '@geoengine/common';
import {Workflow as WorkflowDict} from '@geoengine/api-client';
import {OperatorDialogContainerComponent} from '../helpers/operator-dialog-container/operator-dialog-container.component';
import {MatIconModule} from '@angular/material/icon';
import {MatButtonModule} from '@angular/material/button';
import {SidenavHeaderComponent} from '../../../sidenav/sidenav-header/sidenav-header.component';
import {AsyncPipe, CommonModule} from '@angular/common';
import {MatFormFieldModule} from '@angular/material/form-field';
import {MatSelectModule} from '@angular/material/select';
import {MatInputModule} from '@angular/material/input';
import {OperatorOutputNameComponent} from '../helpers/operator-output-name/operator-output-name.component';
import {LayerSelectionComponent} from '../helpers/layer-selection/layer-selection.component';

interface ClassHistogramForm {
    name: FormControl<string>;
    layer: FormControl<Layer | undefined>;
    attribute: FormControl<string | undefined>;
}

/**
 * This dialog allows creating a class histogram plot of a layer's values.
 */
@Component({
    selector: 'geoengine-class-histogram-operator',
    templateUrl: './class-histogram-operator.component.html',
    styleUrls: ['./class-histogram-operator.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        AsyncPipe,
        CommonModule,
        FormsModule,
        LayerSelectionComponent,
        MatButtonModule,
        MatFormFieldModule,
        MatIconModule,
        MatInputModule,
        MatSelectModule,
        OperatorDialogContainerComponent,
        OperatorOutputNameComponent,
        ReactiveFormsModule,
        SidenavHeaderComponent,
    ],
})
export class ClassHistogramOperatorComponent implements AfterViewInit, OnDestroy {
    private readonly projectService = inject(ProjectService);
    private readonly notificationService = inject(NotificationService);
    private readonly formBuilder = inject(NonNullableFormBuilder);

    inputTypes = ResultTypes.INPUT_TYPES;

    form: FormGroup<ClassHistogramForm>;

    attributes$ = new ReplaySubject<Array<string>>(1);

    isVectorLayer$: Observable<boolean>;

    private subscriptions: Array<Subscription> = [];

    /**
     * DI for services
     */
    constructor() {
        const projectService = this.projectService;

        const layerControl = this.formBuilder.control<Layer | undefined>(undefined, Validators.required);
        this.form = this.formBuilder.group<ClassHistogramForm>({
            name: this.formBuilder.control<string>('Filtered Values', [Validators.required, geoengineValidators.notOnlyWhitespace]),
            layer: layerControl,
            attribute: this.formBuilder.control<string | undefined>(
                undefined,
                geoengineValidators.conditionalValidator(Validators.required, () => isVectorLayer(layerControl.value)),
            ),
        });

        layerControl.addAsyncValidators(categoricalInputValidator(projectService, this.form.controls['attribute']));

        this.subscriptions.push(
            this.form.controls['layer'].valueChanges
                .pipe(
                    tap(() => this.form.controls.attribute.setValue(undefined)),
                    mergeMap((layer: Layer | undefined) => {
                        if (layer instanceof VectorLayer) {
                            return this.projectService.getVectorLayerMetadata(layer).pipe(
                                map((metadata: VectorLayerMetadata) =>
                                    metadata.dataTypes
                                        .filter(
                                            (columnType) =>
                                                columnType === VectorColumnDataTypes.Float ||
                                                columnType === VectorColumnDataTypes.Int ||
                                                columnType === VectorColumnDataTypes.Category,
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

        this.subscriptions.push(
            this.form.controls['attribute'].valueChanges.subscribe({
                next: (value) => {
                    if (!value) {
                        return;
                    }

                    // trigger `categoricalInputValidator`
                    layerControl.updateValueAndValidity({
                        // don't traverse tree
                        onlySelf: true,
                        // don't trigger resetting attributes on layer change
                        emitEvent: false,
                    });
                },
            }),
        );

        this.isVectorLayer$ = this.form.controls['layer'].valueChanges.pipe(map((layer) => isVectorLayer(layer)));
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

        if (!inputLayer) {
            return;
        }

        const attributeName = this.form.controls['attribute'].value;

        const outputName: string = this.form.controls['name'].value;

        this.projectService
            .getWorkflow(inputLayer.workflowId)
            .pipe(
                mergeMap((inputWorkflow: WorkflowDict) =>
                    this.projectService.registerWorkflow({
                        type: 'Plot',
                        operator: {
                            type: 'ClassHistogram',
                            params: {
                                columnName: attributeName,
                            },
                            sources: {
                                source: inputWorkflow.operator,
                            },
                        } as ClassHistogramDict,
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
            .subscribe({
                next: () => {
                    // success
                },
                error: (error) => {
                    this.notificationService.error(error);
                },
            });
    }
}

/**
 * Checks whether the layer is a vector layer (points, lines, polygons).
 */
const isVectorLayer = (layer: Layer | undefined): boolean => {
    if (!layer) {
        return false;
    }
    return layer.layerType === 'vector';
};

/**
 * Checks whether the input is categorical
 */
const categoricalInputValidator =
    (projectService: ProjectService, attributeControl: FormControl<string | undefined>): AsyncValidatorFn =>
    (control: AbstractControl): Observable<{nonCategorical: true} | {onlyWhitespace: true} | null> => {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        const layer = control.value;

        if (!layer) {
            return of(null);
        }

        if (layer instanceof RasterLayer) {
            return projectService.getRasterLayerMetadata(layer).pipe(
                first(),
                map((metadata) => {
                    const allBandsAreClassification = metadata.bands.every((band) => band.measurement.type === 'classification');
                    if (allBandsAreClassification) {
                        return null;
                    } else {
                        return {nonCategorical: true};
                    }
                }),
            );
        } else if (layer instanceof VectorLayer) {
            const attributeName: string | undefined = attributeControl.value;

            if (!attributeName) {
                return of({onlyWhitespace: true});
            }

            return projectService.getVectorLayerMetadata(layer).pipe(
                first(),
                map((metadata) => {
                    const measurement = metadata.measurements.get(attributeName);

                    if (!measurement) {
                        return {onlyWhitespace: true};
                    }

                    if (measurement instanceof ClassificationMeasurement) {
                        return null;
                    } else {
                        return {nonCategorical: true};
                    }
                }),
            );
        } else {
            throw Error('unexpected input layer variant');
        }
    };

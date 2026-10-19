import {AsyncPipe, CommonModule} from '@angular/common';
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
import {MatButtonModule} from '@angular/material/button';
import {MatFormFieldModule} from '@angular/material/form-field';
import {MatIconModule} from '@angular/material/icon';
import {MatInputModule} from '@angular/material/input';
import {MatSelectModule} from '@angular/material/select';
import {from, Observable, of, ReplaySubject, Subscription, first} from 'rxjs';
import {map, mergeMap, tap} from 'rxjs/operators';
import {ProcessingGraph} from '@geoengine/api-client';
import {
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
import {ProjectService} from '../../../project/project.service';
import {SidenavHeaderComponent} from '../../../sidenav/sidenav-header/sidenav-header.component';
import {LayerSelectionComponent} from '../helpers/layer-selection/layer-selection.component';
import {OperatorDialogContainerComponent} from '../helpers/operator-dialog-container/operator-dialog-container.component';
import {OperatorOutputNameComponent} from '../helpers/operator-output-name/operator-output-name.component';

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
            // the vector attribute or raster band
            attribute: this.formBuilder.control<string | undefined>(undefined, Validators.required),
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
                        } else if (layer instanceof RasterLayer) {
                            return this.projectService
                                .getRasterLayerMetadata(layer)
                                .pipe(
                                    map((metadata) =>
                                        metadata.bands
                                            .filter((band) => band.measurement.type === 'classification')
                                            .map((band) => band.name),
                                    ),
                                );
                        } else {
                            return of([]);
                        }
                    }),
                )
                .subscribe((attributes) => {
                    this.attributes$.next(attributes);

                    // preselect the first band, so that single-band rasters need no selection
                    if (layerControl.value instanceof RasterLayer && attributes.length > 0) {
                        this.form.controls.attribute.setValue(attributes[0]);
                    }
                }),
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

        from(this.projectService.getProcessingGraph(inputLayer.workflowId))
            .pipe(
                mergeMap((inputWorkflow: ProcessingGraph) => {
                    if (inputWorkflow.type === 'Plot') {
                        throw new Error('Expected a raster or vector processing graph for class histogram.');
                    }

                    return this.projectService.registerProcessingGraph({
                        type: 'Plot',
                        operator: {
                            type: 'ClassHistogram',
                            params: {
                                columnName: attributeName,
                            },
                            sources: {
                                source: inputWorkflow.operator,
                            },
                        },
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
            .subscribe({
                next: () => {
                    // success
                },
                error: (error: unknown) => {
                    this.notificationService.error(error instanceof Error ? error.message : String(error));
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
            const bandName: string | undefined = attributeControl.value;

            return projectService.getRasterLayerMetadata(layer).pipe(
                first(),
                map((metadata) => {
                    // without a selected band, check whether any band is categorical
                    const isCategorical = bandName
                        ? metadata.bands.some((band) => band.name === bandName && band.measurement.type === 'classification')
                        : metadata.bands.some((band) => band.measurement.type === 'classification');
                    if (isCategorical) {
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

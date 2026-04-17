import {map, mergeMap} from 'rxjs/operators';
import {BehaviorSubject, combineLatest, Observable, of, Subscription} from 'rxjs';
import {AfterViewInit, ChangeDetectionStrategy, Component, OnDestroy, inject, input, viewChild} from '@angular/core';
import {
    AbstractControl,
    FormArray,
    FormBuilder,
    FormControl,
    FormGroup,
    Validators,
    FormsModule,
    ReactiveFormsModule,
} from '@angular/forms';
import {ProjectService} from '../../../project/project.service';
import {UUID} from '../../../backend/backend.model';
import {LayoutService, SidenavConfig} from '../../../layout.service';
import {SymbologyCreatorComponent} from '../../../layers/symbology/symbology-creator/symbology-creator.component';
import {
    Layer,
    NeighborhoodAggregateDict,
    RasterLayer,
    RasterSymbology,
    ResultTypes,
    geoengineValidators,
    FxLayoutDirective,
    FxLayoutAlignDirective,
    FxLayoutGapDirective,
    FxFlexDirective,
    AsyncValueDefault,
} from '@geoengine/common';
import {Workflow as WorkflowDict} from '@geoengine/api-client';
import {SidenavHeaderComponent} from '../../../sidenav/sidenav-header/sidenav-header.component';
import {OperatorDialogContainerComponent} from '../helpers/operator-dialog-container/operator-dialog-container.component';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {LayerSelectionComponent} from '../helpers/layer-selection/layer-selection.component';
import {MatFormField, MatLabel, MatInput, MatError} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {MatMenuTrigger, MatMenu, MatMenuItem} from '@angular/material/menu';
import {OperatorOutputNameComponent} from '../helpers/operator-output-name/operator-output-name.component';
import {AsyncPipe} from '@angular/common';

interface NeighborhoodAggregateForm {
    rasterLayer: FormControl<RasterLayer | undefined>;
    neighborhood: FormGroup<WeightsMatrixNeighborhoodForm> | FormGroup<RectangleNeighborhoodForm>;
    aggregateFunction: FormControl<'sum' | 'standardDeviation'>;
    name: FormControl<string>;
}

type NeighborhoodType = 'weightsMatrix' | 'rectangle';

interface NeighborhoodForm {
    type: FormControl<NeighborhoodType>;
}

interface WeightsMatrixNeighborhoodForm extends NeighborhoodForm {
    type: FormControl<'weightsMatrix'>;
    weights: FormArray<FormArray<FormControl<number>>>;
}

interface RectangleNeighborhoodForm extends NeighborhoodForm {
    type: FormControl<'rectangle'>;
    dimensions: FormArray<FormControl<number>>;
}

/**
 * The dialog for applying a `NeighborhoodAggregate` operator.
 */
@Component({
    selector: 'geoengine-neighborhood-aggregate',
    templateUrl: './neighborhood-aggregate.component.html',
    styleUrls: ['./neighborhood-aggregate.component.scss'],
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
        MatButton,
        MatMenuTrigger,
        MatMenu,
        MatMenuItem,
        MatInput,
        FxLayoutDirective,
        FxLayoutAlignDirective,
        FxLayoutGapDirective,
        FxFlexDirective,
        OperatorOutputNameComponent,
        MatError,
        SymbologyCreatorComponent,
        AsyncPipe,
        AsyncValueDefault,
    ],
})
export class NeighborhoodAggregateComponent implements AfterViewInit, OnDestroy {
    protected readonly projectService = inject(ProjectService);
    protected readonly layoutService = inject(LayoutService);
    protected readonly formBuilder = inject(FormBuilder);

    /**
     * If the inputs are empty, show the following button.
     */
    readonly dataListConfig = input<SidenavConfig>();

    readonly RASTER_TYPE = [ResultTypes.RASTER];
    readonly form: FormGroup<NeighborhoodAggregateForm>;

    readonly lastError$ = new BehaviorSubject<string | undefined>(undefined);

    readonly projectHasRasterLayers$: Observable<boolean>;

    readonly loading$ = new BehaviorSubject<boolean>(false);

    readonly symbologyCreator = viewChild.required(SymbologyCreatorComponent);

    readonly subscriptions: Array<Subscription> = [];

    /**
     * DI of services and setup of observables for the template
     */
    constructor() {
        this.form = new FormGroup<NeighborhoodAggregateForm>({
            rasterLayer: new FormControl<RasterLayer | undefined>(undefined, {
                nonNullable: true,
                validators: [Validators.required],
            }),
            neighborhood: this.defaultWeightsMatrixNeighborhood(),
            aggregateFunction: new FormControl<'sum' | 'standardDeviation'>('sum', {
                nonNullable: true,
                validators: [Validators.required],
            }),
            name: new FormControl('Neighborhood Aggregate', {
                nonNullable: true,
                validators: [Validators.required, geoengineValidators.notOnlyWhitespace],
            }),
        });

        this.projectHasRasterLayers$ = this.projectService
            .getLayerStream()
            .pipe(map((layers: Array<Layer>) => layers.filter((layer) => layer.layerType === 'raster').length > 0));
    }

    ngAfterViewInit(): void {
        setTimeout(() =>
            this.form.controls['rasterLayer'].updateValueAndValidity({
                onlySelf: false,
                emitEvent: true,
            }),
        );
    }

    ngOnDestroy(): void {
        for (const subscription of this.subscriptions) {
            subscription.unsubscribe();
        }
    }

    changeNeighborhood(neighborhood: string): void {
        if (neighborhood === 'weightsMatrix') {
            this.form.setControl('neighborhood', this.defaultWeightsMatrixNeighborhood());
        } else if (neighborhood === 'rectangle') {
            this.form.setControl('neighborhood', this.defaultRectangleNeighborhood());
        }
    }

    get weightsMatrixControls(): FormArray<FormArray<FormControl<number>>> {
        const neighborhood = this.form.controls.neighborhood as FormGroup<WeightsMatrixNeighborhoodForm>;
        return neighborhood.controls.weights;
    }

    enlargeMatrix(): void {
        this.setMatrix(increaseMatrix(this.getMatrix()));
    }

    smallenMatrix(): void {
        this.setMatrix(decreaseMatrix(this.getMatrix()));
    }

    rotate90(): void {
        this.setMatrix(rotateMatrixClockwise(this.getMatrix()));
    }

    presetDerivativeForSobel(): void {
        this.setMatrix([
            [1.0, 0.0, -1.0],
            [2.0, 0.0, -2.0],
            [1.0, 0.0, -1.0],
        ]);
    }

    presetMean(): void {
        this.setMatrix([
            [1 / 9, 1 / 9, 1 / 9],
            [1 / 9, 1 / 9, 1 / 9],
            [1 / 9, 1 / 9, 1 / 9],
        ]);
    }

    presetGaussianBlur(): void {
        this.setMatrix([
            [0.003, 0.0133, 0.0219, 0.0133, 0.003],
            [0.0133, 0.0596, 0.0983, 0.0596, 0.0133],
            [0.0219, 0.0983, 0.1621, 0.0983, 0.0219],
            [0.0133, 0.0596, 0.0983, 0.0596, 0.0133],
            [0.003, 0.0133, 0.0219, 0.0133, 0.003],
        ]);
    }

    /**
     * Uses the user input and creates a new expression operator.
     * The resulting layer is added to the map.
     */
    add(): void {
        if (this.loading$.value) {
            return; // don't add while loading
        }

        const name: string = this.form.controls['name'].value;
        const rasterLayer: RasterLayer | undefined = this.form.controls['rasterLayer'].value;
        const neighborhood = this.form.controls.neighborhood.value;
        const aggregateFunction: 'sum' | 'standardDeviation' = this.form.controls.aggregateFunction.value;

        if (!rasterLayer) {
            return; // checked by form validator
        }

        this.loading$.next(true);

        this.projectService
            .getAutomaticallyProjectedOperatorsFromLayers([rasterLayer])
            .pipe(
                mergeMap(([raster]) => {
                    const workflow: WorkflowDict = {
                        type: 'Raster',
                        operator: {
                            type: 'NeighborhoodAggregate',
                            params: {
                                neighborhood,
                                aggregateFunction,
                            },
                            sources: {
                                raster,
                            },
                        } as NeighborhoodAggregateDict,
                    };

                    return this.projectService.registerWorkflow(workflow);
                }),
                mergeMap((workflowId: UUID) => {
                    const symbology$: Observable<RasterSymbology> = this.symbologyCreator().symbologyForRasterLayer(
                        workflowId,
                        rasterLayer,
                    );
                    return combineLatest([of(workflowId), symbology$]);
                }),
                mergeMap(([workflowId, symbology]: [UUID, RasterSymbology]) =>
                    this.projectService.addLayer(
                        new RasterLayer({
                            workflowId,
                            name,
                            symbology,
                            isLegendVisible: false,
                            isVisible: true,
                        }),
                    ),
                ),
            )
            .subscribe({
                next: () => {
                    // everything worked well

                    this.lastError$.next(undefined);

                    this.loading$.next(false);
                },
                error: (error) => {
                    const errorMsg = error.error.message;

                    this.lastError$.next(errorMsg);

                    this.loading$.next(false);
                },
            });
    }

    goToAddDataTab(): void {
        const dataListConfig = this.dataListConfig();
        if (!dataListConfig) {
            return;
        }

        this.layoutService.setSidenavContentComponent(dataListConfig);
    }

    protected setMatrix(matrix: number[][]): void {
        const matrixForm = this.matrixToFormArray(matrix);

        const neighborhood = this.form.controls.neighborhood as FormGroup<WeightsMatrixNeighborhoodForm>;
        neighborhood.setControl('weights', matrixForm);
    }

    protected getMatrix(): Array<Array<number>> {
        const neighborhood = this.form.controls.neighborhood as FormGroup<WeightsMatrixNeighborhoodForm>;
        const value = neighborhood.value;

        if (value.type !== 'weightsMatrix' || !value.weights) {
            return [];
        }

        return value.weights;
    }

    protected defaultWeightsMatrixNeighborhood(): FormGroup<WeightsMatrixNeighborhoodForm> {
        const fb = this.formBuilder.nonNullable;
        return fb.group<WeightsMatrixNeighborhoodForm>(
            {
                type: this.createNeighborhoodType('weightsMatrix'),
                weights: this.matrixToFormArray([
                    [1.0, 0.0, -1.0],
                    [2.0, 0.0, -2.0],
                    [1.0, 0.0, -1.0],
                ]),
            },
            {validators: [correctNeighborhoodDimensions]},
        );
    }

    protected defaultRectangleNeighborhood(): FormGroup<RectangleNeighborhoodForm> {
        const fb = this.formBuilder.nonNullable;
        return fb.group<RectangleNeighborhoodForm>(
            {
                type: this.createNeighborhoodType('rectangle'),
                dimensions: fb.array([3, 3]),
            },
            {validators: [correctNeighborhoodDimensions]},
        );
    }

    protected createNeighborhoodType<T extends NeighborhoodType>(name: T): FormControl<T> {
        const fb = this.formBuilder.nonNullable;
        const neighborhoodType = fb.control<T>(name, Validators.required);
        neighborhoodType.valueChanges.subscribe((value) => this.changeNeighborhood(value));
        return neighborhoodType;
    }

    protected matrixToFormArray(matrix: Array<Array<number>>): FormArray<FormArray<FormControl<number>>> {
        const fb = this.formBuilder.nonNullable;

        const rows = matrix.map((row) => fb.array(row));

        return fb.array(rows);
    }
}

/**
 * Increase matrix by one column and row.
 * Copy the previous matrix into the center, fill the rest with zeros.
 */
export function increaseMatrix(matrix: Array<Array<number>>): Array<Array<number>> {
    const halfPadding = 1;

    const rows = matrix.length;
    const cols = rows > 0 ? matrix[0].length : 0;

    const newMatrix: Array<Array<number>> = [];

    for (let r = 0; r < rows + 2 * halfPadding; ++r) {
        const newRow: Array<number> = [];

        for (let c = 0; c < cols + 2 * halfPadding; ++c) {
            if (r < halfPadding || r > rows || c < halfPadding || c > cols) {
                newRow.push(0);
            } else {
                newRow.push(matrix[r - halfPadding][c - halfPadding]);
            }
        }

        newMatrix.push(newRow);
    }
    return newMatrix;
}

/**
 * Decrease matrix by one column and row.
 * Copy the previous matrix into the center, omit the outer ring.
 *
 * Do nothing if matrix is already 1x1.
 */
export function decreaseMatrix(matrix: Array<Array<number>>): Array<Array<number>> {
    const halfPadding = 1;

    const rows = matrix.length;
    const cols = rows > 0 ? matrix[0].length : 0;

    if (rows <= 1 || cols <= 1) {
        return matrix;
    }

    const newMatrix: Array<Array<number>> = [];

    for (let r = halfPadding; r < rows - halfPadding; ++r) {
        newMatrix.push([]);
        for (let c = halfPadding; c < cols - halfPadding; ++c) {
            newMatrix[r - 1].push(matrix[r][c]);
        }
    }
    return newMatrix;
}

/**
 * Rotate the matrix by 90 degrees.
 */
export function rotateMatrixClockwise(matrix: Array<Array<number>>): Array<Array<number>> {
    const rows = matrix.length;
    const cols = rows > 0 ? matrix[0].length : 0;

    if (rows === 0 || cols === 0) {
        return matrix;
    }

    const newMatrix: Array<Array<number>> = [];

    for (let c = 0; c < cols; ++c) {
        newMatrix.push([]);
        for (let r = 0; r < rows; ++r) {
            newMatrix[c].push(matrix[rows - r - 1][c]);
        }
    }

    return newMatrix;
}

const correctNeighborhoodDimensions = (
    control: AbstractControl,
): {
    emptyDimensions?: true;
    dimensionsNotOdd?: true;
    dimensionsNegative?: true;
    dimensionsNotInteger?: true;
    valuesMissing?: true;
} | null => {
    const neighborhoodType: AbstractControl<NeighborhoodType> | null = control.get('type');

    if (!neighborhoodType) {
        return null;
    }

    let rows: number;
    let cols: number;
    let values: Array<number> = [];

    switch (neighborhoodType.value) {
        case 'weightsMatrix': {
            const weightsMatrixForm: AbstractControl<FormArray<FormControl<number>>> | null = control.get('weights');

            if (!weightsMatrixForm || !(weightsMatrixForm instanceof FormArray)) {
                rows = cols = 0;
                break;
            }

            const weights: Array<Array<number>> = weightsMatrixForm.value;
            rows = weights.length;
            cols = rows > 0 ? weights[0].length : 0;

            values = weights.flat();

            break;
        }
        case 'rectangle': {
            const dimensionsForm: AbstractControl<FormControl<number>> | null = control.get('dimensions');

            if (!dimensionsForm || !(dimensionsForm instanceof FormArray)) {
                rows = cols = 0;
                break;
            }

            const dimensions: [number, number] = dimensionsForm.value;
            rows = dimensions[0];
            cols = dimensions[1];

            break;
        }
        default: {
            rows = cols = 0;
        }
    }

    if (!rows || !cols) {
        return {emptyDimensions: true};
    }

    if (rows < 0 || cols < 0) {
        return {dimensionsNegative: true};
    }

    if (!Number.isInteger(rows) || !Number.isInteger(cols)) {
        return {dimensionsNotInteger: true};
    }

    if (rows % 2 === 0 || cols % 2 === 0) {
        return {dimensionsNotOdd: true};
    }

    if (values.some((value) => !Number.isFinite(value))) {
        return {valuesMissing: true};
    }

    return null;
};

import {Component, ChangeDetectionStrategy, AfterViewInit, OnDestroy, inject} from '@angular/core';
import {
    Validators,
    FormBuilder,
    FormControl,
    FormArray,
    FormGroup,
    AsyncValidatorFn,
    AbstractControl,
    FormsModule,
    ReactiveFormsModule,
} from '@angular/forms';
import {ProjectService} from '../../../project/project.service';
import {BehaviorSubject, combineLatest, firstValueFrom, Observable, of, ReplaySubject, Subscription} from 'rxjs';
import {map, mergeMap, startWith} from 'rxjs/operators';
import {
    ColumnOutputColumn,
    GeometryOutputColumn,
    Measurement,
    RandomColorService,
    ResultTypes,
    SymbologyType,
    UnitlessMeasurement,
    VectorColumnDataType,
    VectorColumnDataTypes,
    VectorExpressionDict,
    VectorExpressionParams,
    VectorLayer,
    VectorLayerMetadata,
    VectorSymbology,
    createVectorSymbology,
    geoengineValidators,
    FxLayoutDirective,
    FxFlexDirective,
    FxLayoutAlignDirective,
    CommonModule,
    AsyncStringSanitizer,
    AsyncValueDefault,
} from '@geoengine/common';

import {Workflow as WorkflowDict} from '@geoengine/api-client';
import {SidenavHeaderComponent} from '../../../sidenav/sidenav-header/sidenav-header.component';
import {OperatorDialogContainerComponent} from '../helpers/operator-dialog-container/operator-dialog-container.component';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {LayerSelectionComponent} from '../helpers/layer-selection/layer-selection.component';
import {DialogSectionHeadingComponent} from '../../../dialogs/dialog-section-heading/dialog-section-heading.component';
import {MatFormField, MatLabel, MatInput, MatHint} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {MatRadioGroup, MatRadioButton} from '@angular/material/radio';
import {OperatorOutputNameComponent} from '../helpers/operator-output-name/operator-output-name.component';
import {AsyncPipe} from '@angular/common';

const MAX_NUMBER_OF_COLUMNS = 8;
const ALLOWED_EXPRESSION_COLUMN_TYPES = [VectorColumnDataTypes.Float, VectorColumnDataTypes.Int];

interface VectorExpressionForm {
    source: FormControl<VectorLayer | null>;
    inputColumns: FormArray<FormControl<string | null>>;
    outputColumnType: FormControl<OutputColumnType>;
    outputColumnName: FormControl<string>;
    outputGeometryType: FormControl<GeometryType>;
    expression: FormControl<string>;
    geometryColumnName: FormControl<string>;
    outputMeasurement: FormControl<Measurement>;
    layerName: FormControl<string>;
}

type OutputColumnType = 'column' | 'geometry';
type GeometryType = 'MultiPoint' | 'MultiLineString' | 'MultiPolygon';

interface VectorColumn {
    name: string;
    datatype: VectorColumnDataType;
}

@Component({
    selector: 'geoengine-vector-expression',
    templateUrl: './vector-expression.component.html',
    styleUrls: ['./vector-expression.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        SidenavHeaderComponent,
        FormsModule,
        ReactiveFormsModule,
        OperatorDialogContainerComponent,
        MatIconButton,
        MatIcon,
        LayerSelectionComponent,
        DialogSectionHeadingComponent,
        MatFormField,
        MatLabel,
        MatInput,
        MatHint,
        FxLayoutDirective,
        FxFlexDirective,
        FxLayoutAlignDirective,
        MatButton,
        MatSelect,
        MatOption,
        MatRadioGroup,
        MatRadioButton,
        CommonModule,
        OperatorOutputNameComponent,
        AsyncPipe,
        AsyncStringSanitizer,
        AsyncValueDefault,
    ],
})
export class VectorExpressionComponent implements AfterViewInit, OnDestroy {
    private readonly formBuilder = inject(FormBuilder);
    private readonly projectService = inject(ProjectService);
    private readonly randomColorService = inject(RandomColorService);

    readonly allowedLayerTypes = ResultTypes.VECTOR_TYPES;

    readonly inputGeometryType = new BehaviorSubject<GeometryType | undefined>(undefined);

    readonly availableAttributes$ = new ReplaySubject<Array<VectorColumn>>(1);

    readonly form: FormGroup<VectorExpressionForm>;

    readonly columnNames: FormArray<FormControl<string | null>>;
    readonly outputColumnType: FormControl<OutputColumnType>;
    readonly outputColumnName: FormControl<string>;
    readonly layerName: FormControl<string>;

    readonly expression: FormControl<string>;
    readonly fnSignature: Observable<string>;
    readonly lastError$ = new BehaviorSubject<string | undefined>(undefined);

    readonly loading$ = new BehaviorSubject<boolean>(false);

    protected readonly allAttributes$ = new ReplaySubject<Immutable.Map<string, VectorColumnDataType>>(1);

    protected readonly subscriptions: Array<Subscription> = [];

    constructor() {
        const layerControl = this.formBuilder.control<VectorLayer | null>(null, Validators.required);
        this.columnNames = this.formBuilder.nonNullable.array<string | null>([], [Validators.maxLength(MAX_NUMBER_OF_COLUMNS)]);
        this.outputColumnType = this.formBuilder.nonNullable.control<OutputColumnType>('column', Validators.required);
        this.outputColumnName = this.formBuilder.nonNullable.control<string>(
            '',
            [
                geoengineValidators.conditionalValidator(Validators.required, () => this.outputColumnType.value === 'column'),

                geoengineValidators.notOnlyWhitespace,
            ],
            geoengineValidators.conditionalAsyncValidator(
                attributeNameCollision(this.allAttributes$.pipe(map((attributes) => attributes.keySeq().toArray()))),
                // eslint-disable-next-line @typescript-eslint/require-await
                async () => this.outputColumnType.value === 'column',
            ),
        );
        this.layerName = this.formBuilder.nonNullable.control<string>('VectorExpression', [
            Validators.required,
            geoengineValidators.notOnlyWhitespace,
        ]);
        this.expression = this.formBuilder.nonNullable.control<string>('1', [Validators.required, geoengineValidators.notOnlyWhitespace]);

        this.form = this.formBuilder.group({
            source: layerControl,

            inputColumns: this.columnNames,

            outputColumnType: this.outputColumnType,
            outputColumnName: this.outputColumnName,
            outputGeometryType: this.formBuilder.nonNullable.control<GeometryType>(
                'MultiPoint',
                geoengineValidators.conditionalValidator(Validators.required, () => this.outputColumnType.value === 'geometry'),
            ),

            expression: this.expression,

            geometryColumnName: this.formBuilder.nonNullable.control<string>({value: 'geom', disabled: true}, [
                Validators.required,
                geoengineValidators.notOnlyWhitespace,
            ]),

            // TODO: add form component (build generic one)
            outputMeasurement: this.formBuilder.nonNullable.control<Measurement>(new UnitlessMeasurement(), Validators.required),

            layerName: this.layerName,
        });

        const source$: Observable<VectorLayer | null> = this.form.controls.source.valueChanges;
        const inputColumns$: Observable<Array<string | null>> = this.columnNames.valueChanges.pipe(startWith(this.columnNames.value));

        this.subscriptions.push(
            source$.subscribe((source) => {
                // reset
                this.columnNames.clear();

                if (!source) {
                    this.inputGeometryType.next(undefined);
                    return;
                }

                switch (source.symbology.getSymbologyType()) {
                    case SymbologyType.POINT:
                        this.inputGeometryType.next('MultiPoint');
                        break;
                    case SymbologyType.LINE:
                        this.inputGeometryType.next('MultiLineString');
                        break;
                    case SymbologyType.POLYGON:
                        this.inputGeometryType.next('MultiPolygon');
                        break;
                    default:
                        this.inputGeometryType.next(undefined);
                }
            }),
        );

        this.subscriptions.push(
            combineLatest({
                source: source$,
                inputColumns: inputColumns$,
            })
                .pipe(
                    mergeMap(({source, inputColumns}) => {
                        if (!source) {
                            return of([[], []]);
                        }

                        const usedColumns = inputColumns.filter((column) => column !== null);

                        return this.projectService.getVectorLayerMetadata(source).pipe(
                            map<VectorLayerMetadata, [Array<VectorColumn>, Immutable.Map<string, VectorColumnDataType>]>(
                                (metadata: VectorLayerMetadata) => [
                                    metadata.dataTypes
                                        .filter((columnType) => ALLOWED_EXPRESSION_COLUMN_TYPES.includes(columnType))
                                        .entrySeq()
                                        .filter(([columnName, _columnType]) => !usedColumns.includes(columnName))
                                        .map(([columnName, columnType]) => ({name: columnName, datatype: columnType}))
                                        .toArray(),
                                    metadata.dataTypes,
                                ],
                            ),
                        );
                    }),
                )
                .subscribe(([availableAttributes, allAttributes]) => {
                    this.availableAttributes$.next(availableAttributes);
                    this.allAttributes$.next(allAttributes as Immutable.Map<string, VectorColumnDataType>);
                }),
        );

        this.fnSignature = combineLatest({
            columns: this.columnNames.valueChanges,
            geometryName: this.form.controls.geometryColumnName.valueChanges,
            outputGeometryType: this.form.controls.outputGeometryType.valueChanges,
        }).pipe(
            map(({columns, geometryName, outputGeometryType}) => {
                const variables = columns.filter((c) => c !== null).map((c) => canonicalizeVariableName(c));
                const geometryComma = variables.length > 0 ? ', ' : '';
                const returnType = this.outputColumnType.value === 'column' ? VectorColumnDataTypes.Float : outputGeometryType;
                // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
                return `fn(${geometryName}${geometryComma}${variables.join(', ')}) -> ${returnType}:`;
            }),
        );

        // re-trigger validation after type change
        this.subscriptions.push(
            this.outputColumnType.valueChanges.subscribe(() => {
                this.outputColumnName.updateValueAndValidity();
                this.form.controls.outputGeometryType.updateValueAndValidity();
            }),
        );

        // trigger `geometryColumnName`, `outputGeometryType` & `columnNames` to start submitting `valueChanges
        setTimeout(() => {
            this.form.controls.geometryColumnName.updateValueAndValidity();
            this.columnNames.updateValueAndValidity();
            this.form.controls.outputGeometryType.updateValueAndValidity();
        });
    }

    get addColumnDisabled(): boolean {
        return this.columnNames.length >= MAX_NUMBER_OF_COLUMNS - 1;
    }

    addColumn(): void {
        if (this.columnNames.length >= MAX_NUMBER_OF_COLUMNS) {
            return;
        }

        const newControl = this.formBuilder.control<string | null>(null, Validators.required);
        this.columnNames.push(newControl);
    }

    removeColumn(i: number): void {
        this.columnNames.removeAt(i);
    }

    typeOfColumn(column: string): Observable<VectorColumnDataType | undefined> {
        return this.allAttributes$.pipe(map((attributes) => attributes.get(column) ?? undefined));
    }

    add(): void {
        if (this.loading$.value) {
            return; // don't add while loading
        }
        this.loading$.next(true);

        const sourceLayer = this.form.controls.source.value!;

        const inputColumns = this.columnNames.controls.map((fc) => (fc ? fc.value?.toString() : ''));

        const outputColumnType = this.form.controls.outputColumnType.value;
        const outputGeometryType = this.form.controls.outputGeometryType.value;
        let outputColumn: ColumnOutputColumn | GeometryOutputColumn;
        if (outputColumnType === 'column') {
            outputColumn = {
                type: 'column',
                value: this.form.controls.outputColumnName.value,
            } as ColumnOutputColumn;
        } else if (outputColumnType === 'geometry') {
            outputColumn = {
                type: 'geometry',
                value: outputGeometryType,
            } as GeometryOutputColumn;
        }

        const expression = this.form.controls.expression.value;
        const geometryColumnName = this.form.controls.geometryColumnName.value;
        const outputMeasurement = this.form.controls.outputMeasurement.value.toDict();

        const layerName = this.form.controls.layerName.value;

        this.projectService
            .getWorkflow(sourceLayer.workflowId)
            .pipe(
                mergeMap(({operator: vector}: WorkflowDict) =>
                    this.projectService.registerWorkflow({
                        type: 'Vector',
                        operator: {
                            type: 'VectorExpression',
                            params: {
                                inputColumns,
                                outputColumn,
                                expression,
                                geometryColumnName,
                                outputMeasurement,
                            } as VectorExpressionParams,
                            sources: {
                                vector,
                            },
                        } as VectorExpressionDict,
                    }),
                ),
                mergeMap((workflowId) =>
                    this.projectService.addLayer(
                        new VectorLayer({
                            workflowId,
                            name: layerName,
                            symbology: createSymbology(
                                this.randomColorService,
                                sourceLayer.symbology,
                                outputColumnType,
                                outputGeometryType,
                            ),
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

    ngOnDestroy(): void {
        this.subscriptions.forEach((subscription) => subscription.unsubscribe());
    }

    ngAfterViewInit(): void {
        setTimeout(() => {
            this.form.updateValueAndValidity({
                onlySelf: false,
                emitEvent: true,
            });
            this.form.controls.source.updateValueAndValidity();
        });
    }
}

/**
 * Canonicalizes a column name to a variable name.
 */
function canonicalizeVariableName(name: string): string {
    // if starts with number
    const additionalPrefix = /^\d/.exec(name) ? '_' : '';

    // replace all non-alphanumeric characters with _
    const canonicalName = name.replace(/[^a-zA-Z0-9_]/g, '_');

    return `${additionalPrefix}${canonicalName}`;
}

function createSymbology(
    randomColorService: RandomColorService,
    oldSymbology: VectorSymbology,
    newOutputType: OutputColumnType,
    newGeometryType: GeometryType,
): VectorSymbology {
    if (newOutputType === 'column') {
        return oldSymbology.clone();
    }

    let newSymbologyType: SymbologyType;
    switch (newGeometryType) {
        case 'MultiPoint':
            newSymbologyType = SymbologyType.POINT;
            break;
        case 'MultiLineString':
            newSymbologyType = SymbologyType.LINE;
            break;
        case 'MultiPolygon':
            newSymbologyType = SymbologyType.POLYGON;
            break;
    }

    if (oldSymbology.getSymbologyType() === newSymbologyType) {
        return oldSymbology.clone();
    }
    return createVectorSymbology(newGeometryType, randomColorService.getRandomColorRgba());
}

/**
 * Checks for collisions of attribute names.
 */
const attributeNameCollision =
    (attributes$: Observable<Array<string>>): AsyncValidatorFn =>
    async (control: AbstractControl<string>): Promise<{duplicateName?: boolean} | null> => {
        const attributes = await firstValueFrom(attributes$);
        const attribute = control.value;

        if (attributes.includes(attribute)) {
            return {duplicateName: true};
        }

        return null;
    };

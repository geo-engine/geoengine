import {AfterViewInit, ChangeDetectionStrategy, Component, computed, effect, inject, input, signal, viewChild} from '@angular/core';
import {toSignal} from '@angular/core/rxjs-interop';
import {FormControl, FormGroup, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {MatOption} from '@angular/material/autocomplete';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatCheckbox} from '@angular/material/checkbox';
import {MatIcon} from '@angular/material/icon';
import {MatFormField, MatLabel, MatInput, MatError} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {Completion} from '@codemirror/autocomplete';
import {combineLatest, from, Observable, of} from 'rxjs';
import {map, mergeMap, switchMap} from 'rxjs/operators';
import {Measurement, ProcessingGraph, RasterDataType as ApiRasterDataType} from '@geoengine/api-client';
import {
    GeoEngineError,
    Layer,
    MeasurementComponent,
    RasterDataType,
    RasterDataTypes,
    RasterLayer,
    RasterSymbology,
    ResultTypes,
    SingleBandRasterColorizer,
    geoengineValidators,
    CommonModule,
    errorToText,
    errorMessage,
    expressionBandVariables,
    EXPRESSION_FUNCTIONS,
    EXPRESSION_KEYWORDS,
} from '@geoengine/common';
import {UUID} from '../../../backend/backend.model';
import {SymbologyCreationType, SymbologyCreatorComponent} from '../../../layers/symbology/symbology-creator/symbology-creator.component';
import {LayoutService, SidenavConfig} from '../../../layout.service';
import {ProjectService} from '../../../project/project.service';
import {SidenavHeaderComponent} from '../../../sidenav/sidenav-header/sidenav-header.component';
import {LayerSelectionComponent} from '../helpers/layer-selection/layer-selection.component';
import {OperatorDialogContainerComponent} from '../helpers/operator-dialog-container/operator-dialog-container.component';
import {OperatorOutputNameComponent} from '../helpers/operator-output-name/operator-output-name.component';

/**
 * A raster band and the variable that refers to it in the expression.
 */
interface BandVariable {
    variable: string;
    bandName: string;
    index: number;
}

interface ExpressionForm {
    rasterLayer: FormControl<RasterLayer | undefined>;
    expression: FormControl<string>;
    dataType: FormControl<RasterDataType | undefined>;
    name: FormControl<string>;
    mapNoData: FormControl<boolean>;
    outputBandName: FormControl<string>;
    outputMeasurement: FormControl<Measurement | undefined>;
    symbologyCreation: FormControl<SymbologyCreationType>;
}

/**
 * This dialog allows calculations on (one or more) raster layers.
 */
@Component({
    selector: 'geoengine-expression-operator',
    templateUrl: './expression-operator.component.html',
    styleUrls: ['./expression-operator.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        SidenavHeaderComponent,
        FormsModule,
        ReactiveFormsModule,
        OperatorDialogContainerComponent,
        MatIconButton,
        MatIcon,
        LayerSelectionComponent,
        CommonModule,
        MatCheckbox,
        MatFormField,
        MatLabel,
        MatSelect,
        MatOption,
        MatInput,
        OperatorOutputNameComponent,
        MatError,
        SymbologyCreatorComponent,
        MatButton,
    ],
})
export class ExpressionOperatorComponent implements AfterViewInit {
    protected readonly projectService = inject(ProjectService);
    protected readonly layoutService = inject(LayoutService);

    /**
     * If the list is empty, show the following button.
     */
    readonly dataListConfig = input<SidenavConfig>();

    readonly measurementComponent = viewChild(MeasurementComponent);
    readonly symbologyCreator = viewChild.required(SymbologyCreatorComponent);

    readonly RASTER_TYPE = [ResultTypes.RASTER];

    readonly form = new FormGroup<ExpressionForm>({
        rasterLayer: new FormControl<RasterLayer | undefined>(undefined, {
            nonNullable: true,
            validators: [Validators.required],
        }),
        expression: new FormControl<string>('', {
            nonNullable: true,
            validators: [Validators.required],
        }),
        dataType: new FormControl(undefined, {
            nonNullable: true,
            validators: [Validators.required],
        }),
        mapNoData: new FormControl(false, {
            nonNullable: true,
            validators: [Validators.required],
        }),
        name: new FormControl('Expression', {
            nonNullable: true,
            validators: [Validators.required, geoengineValidators.notOnlyWhitespace],
        }),
        outputBandName: new FormControl('band', {
            nonNullable: true,
            validators: [Validators.required, geoengineValidators.notOnlyWhitespace],
        }),
        outputMeasurement: new FormControl<Measurement | undefined>(undefined, {
            nonNullable: true,
        }),
        symbologyCreation: new FormControl(SymbologyCreationType.AS_INPUT, {
            nonNullable: true,
            validators: [Validators.required],
        }),
    });

    readonly projectHasRasterLayers = toSignal(
        this.projectService.getLayerStream().pipe(map((layers: Array<Layer>) => layers.some((layer) => layer.layerType === 'raster'))),
        {initialValue: false},
    );

    /** The metadata of the selected raster layer. */
    private readonly rasterLayerMetadata = toSignal(
        this.form.controls.rasterLayer.valueChanges.pipe(
            switchMap((rasterLayer: RasterLayer | undefined) =>
                rasterLayer ? this.projectService.getRasterLayerMetadata(rasterLayer) : of(undefined),
            ),
        ),
        {initialValue: undefined},
    );

    readonly outputDataTypes = computed<Array<[RasterDataType, string]>>(() => {
        const inputDataType = this.rasterLayerMetadata()?.dataType;
        return RasterDataTypes.ALL_DATATYPES.map((dataType) => [dataType, dataType === inputDataType ? '(like input)' : '']);
    });

    private readonly bandVariables = computed<Array<BandVariable>>(() => {
        const bandNames = this.rasterLayerMetadata()?.bands.map((band) => band.name) ?? [];
        return expressionBandVariables(bandNames).map((variable, index) => ({
            variable,
            bandName: bandNames[index],
            index,
        }));
    });

    readonly fnSignature = computed(
        () =>
            `fn(${this.bandVariables()
                .map((band) => band.variable)
                .join(', ')}):`,
    );

    readonly completions = computed(() => expressionCompletions(this.bandVariables()));

    readonly lastError = signal<string | undefined>(undefined);
    readonly loading = signal(false);

    /** The expression that was suggested last. It is only replaced as long as the user does not change it. */
    private suggestedExpression = '';

    constructor() {
        // use the data type of the input as the default output data type
        effect(() => {
            const inputDataType = this.rasterLayerMetadata()?.dataType;
            if (inputDataType) {
                this.form.controls.dataType.setValue(inputDataType);
            }
        });

        // suggest an expression with the first band, unless the user has changed the expression
        effect(() => {
            const firstBand = this.bandVariables().at(0);
            const expressionControl = this.form.controls.expression;
            if (!firstBand || expressionControl.value !== this.suggestedExpression) {
                return;
            }
            this.suggestedExpression = `1 * ${firstBand.variable}`;
            expressionControl.setValue(this.suggestedExpression);
        });
    }

    ngAfterViewInit(): void {
        setTimeout(() =>
            this.form.controls.rasterLayer.updateValueAndValidity({
                onlySelf: false,
                emitEvent: true,
            }),
        );
    }

    /**
     * Uses the user input and creates a new expression operator.
     * The resulting layer is added to the map.
     */
    add(): void {
        if (this.loading()) {
            return; // don't add while loading
        }

        const {name, dataType, expression, rasterLayer, mapNoData, outputBandName} = this.form.getRawValue();

        if (!dataType || !rasterLayer) {
            return; // checked by form validator
        }

        const outputBand = {
            name: outputBandName,
            measurement: this.measurementComponent()?.measurement() ?? {type: 'unitless'},
        };

        this.loading.set(true);

        from(this.projectService.getProcessingGraph(rasterLayer.workflowId))
            .pipe(
                mergeMap((inputWorkflow: ProcessingGraph) => {
                    if (inputWorkflow.type !== 'Raster') {
                        throw new Error('Expected a raster processing graph for expression operator.');
                    }

                    const workflow: ProcessingGraph = {
                        type: 'Raster',
                        operator: {
                            type: 'Expression',
                            params: {
                                expression,
                                outputType: dataType.getCode() as unknown as ApiRasterDataType,
                                outputBand,
                                mapNoData,
                            },
                            sources: {
                                raster: inputWorkflow.operator,
                            },
                        },
                    };

                    return this.projectService.registerProcessingGraph(workflow);
                }),
                mergeMap((workflowId: UUID) => {
                    const symbology$: Observable<RasterSymbology> = this.symbologyCreator().symbologyForRasterLayer(
                        workflowId,
                        rasterLayer,
                    );
                    return combineLatest([of(workflowId), symbology$]);
                }),
                mergeMap(([workflowId, symbology]: [UUID, RasterSymbology]) => {
                    if (symbology.rasterColorizer instanceof SingleBandRasterColorizer) {
                        const outSymbology = new RasterSymbology(symbology.opacity, symbology.rasterColorizer.replaceBand(0));
                        return this.projectService.addLayer(
                            new RasterLayer({
                                processingGraphId: workflowId,
                                name,
                                symbology: outSymbology,
                                isLegendVisible: false,
                                isVisible: true,
                            }),
                        );
                    } else {
                        throw new GeoEngineError('SymbologyError', 'The input Symbology must be a single band colorizer.');
                    }
                }),
            )
            .subscribe({
                next: () => {
                    // everything worked well
                    this.lastError.set(undefined);
                    this.loading.set(false);
                },
                error: (error) => {
                    void errorToText(error, errorMessage(error)).then((errorMsg) => {
                        this.lastError.set(errorMsg);
                        this.loading.set(false);
                    });
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
}

/**
 * Suggestions for the expression editor: the band variables, the functions and the keywords.
 */
function expressionCompletions(bands: ReadonlyArray<BandVariable>): Array<Completion> {
    const variables: Array<Completion> = bands.map((band) => ({
        label: band.variable,
        type: 'variable',
        detail: `band ${band.index + 1}`,
        info: band.bandName !== band.variable ? `band name: ${band.bandName}` : undefined,
        boost: 2,
    }));

    const functions: Array<Completion> = EXPRESSION_FUNCTIONS.map((fn) => ({
        label: fn.name,
        type: 'function',
        detail: fn.signature,
        info: fn.description,
        apply: `${fn.name}(`,
        boost: 1,
    }));

    const keywords: Array<Completion> = EXPRESSION_KEYWORDS.map((keyword) => ({
        label: keyword === 'nodata' ? 'NODATA' : keyword === 'is' ? 'IS NODATA' : keyword,
        type: 'keyword',
    }));

    return [...variables, ...functions, ...keywords];
}

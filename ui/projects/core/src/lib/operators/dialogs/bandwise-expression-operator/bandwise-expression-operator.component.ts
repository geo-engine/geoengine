import {map, mergeMap} from 'rxjs/operators';
import {BehaviorSubject, combineLatest, Observable, of} from 'rxjs';
import {AfterViewInit, ChangeDetectionStrategy, Component, inject, input, viewChild} from '@angular/core';
import {AbstractControl, FormControl, FormGroup, ValidationErrors, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';

import {ProjectService} from '../../../project/project.service';
import {UUID} from '../../../backend/backend.model';
import {LayoutService, SidenavConfig} from '../../../layout.service';
import {
    BandwiseExpressionDict,
    GeoEngineError,
    Layer,
    MeasurementComponent,
    RasterDataType,
    RasterDataTypes,
    RasterLayer,
    RasterLayerMetadata,
    RasterSymbology,
    ResultTypes,
    SingleBandRasterColorizer,
    geoengineValidators,
    CommonModule,
    AsyncValueDefault,
} from '@geoengine/common';
import {SymbologyCreationType, SymbologyCreatorComponent} from '../../../layers/symbology/symbology-creator/symbology-creator.component';
import {Workflow as WorkflowDict} from '@geoengine/api-client';
import {SidenavHeaderComponent} from '../../../sidenav/sidenav-header/sidenav-header.component';
import {OperatorDialogContainerComponent} from '../helpers/operator-dialog-container/operator-dialog-container.component';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {LayerSelectionComponent} from '../helpers/layer-selection/layer-selection.component';
import {MatCheckbox} from '@angular/material/checkbox';
import {MatFormField, MatLabel, MatError} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {OperatorOutputNameComponent} from '../helpers/operator-output-name/operator-output-name.component';
import {AsyncPipe} from '@angular/common';

interface ExpressionForm {
    rasterLayer: FormControl<RasterLayer | undefined>;
    expression: FormControl<string>;
    dataType: FormControl<RasterDataType | undefined>;
    name: FormControl<string>;
    mapNoData: FormControl<boolean>;
    symbologyCreation: FormControl<SymbologyCreationType>;
}

/**
 * This dialog allows calculations on all bands of a raster layer.
 */
@Component({
    selector: 'geoengine-bandwise-expression-operator',
    templateUrl: './bandwise-expression-operator.component.html',
    styleUrls: ['./bandwise-expression-operator.component.scss'],
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
        OperatorOutputNameComponent,
        MatError,
        SymbologyCreatorComponent,
        MatButton,
        AsyncPipe,
        AsyncValueDefault,
    ],
})
export class BandwiseExpressionOperatorComponent implements AfterViewInit {
    protected readonly projectService = inject(ProjectService);
    protected readonly layoutService = inject(LayoutService);

    /**
     * If the list is empty, show the following button.
     */
    readonly dataListConfig = input<SidenavConfig>();

    readonly measurementComponent = viewChild(MeasurementComponent);

    readonly RASTER_TYPE = [ResultTypes.RASTER];
    readonly form: FormGroup<ExpressionForm>;

    readonly outputDataTypes$: Observable<Array<[RasterDataType, string]>>;

    readonly lastError$ = new BehaviorSubject<string | undefined>(undefined);
    readonly projectHasRasterLayers$: Observable<boolean>;

    readonly loading$ = new BehaviorSubject<boolean>(false);

    readonly symbologyCreator = viewChild.required(SymbologyCreatorComponent);

    /**
     * DI of services and setup of observables for the template
     */
    constructor() {
        this.form = new FormGroup<ExpressionForm>({
            rasterLayer: new FormControl<RasterLayer | undefined>(undefined, {
                nonNullable: true,
                validators: [Validators.required],
            }),
            expression: new FormControl<string>('1 * x', {
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
            symbologyCreation: new FormControl(SymbologyCreationType.AS_INPUT, {
                nonNullable: true,
                validators: [Validators.required],
            }),
            // TODO: add unit related inputs
        });

        // TODO on selected raster update the data type (like input)
        this.outputDataTypes$ = this.form.controls.rasterLayer.valueChanges.pipe(
            mergeMap((rasterLayer: RasterLayer | undefined) => {
                if (!rasterLayer) {
                    return of(undefined);
                }

                return this.projectService.getRasterLayerMetadata(rasterLayer);
            }),
            map((rasterLayer: RasterLayerMetadata | undefined) => {
                const outputDataTypes: Array<[RasterDataType, string]> = RasterDataTypes.ALL_DATATYPES.map((dataType: RasterDataType) => [
                    dataType,
                    '',
                ]);

                if (!rasterLayer) {
                    return outputDataTypes;
                }

                for (const output of outputDataTypes) {
                    const outputDataType = output[0];

                    if (outputDataType === rasterLayer?.dataType) {
                        output[1] = '(like input)';

                        const dataTypeControl = this.form.controls.dataType;
                        setTimeout(() => {
                            dataTypeControl.setValue(outputDataType);
                        });
                    }
                }

                return outputDataTypes;
            }),
        );

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

    validateNoData(control: AbstractControl): ValidationErrors | null {
        if (!isNaN(parseFloat(control.value)) || control.value.toLowerCase() === 'nan') {
            return null;
        } else {
            return {
                error: true,
            };
        }
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
        const dataType: RasterDataType | undefined = this.form.controls['dataType'].value;
        const expression: string = this.form.controls['expression'].value;
        const rasterLayer: RasterLayer | undefined = this.form.controls['rasterLayer'].value;
        const mapNoData: boolean = this.form.controls['mapNoData'].value;

        if (!dataType || !rasterLayer) {
            return; // checked by form validator
        }

        this.projectService
            .getWorkflow(rasterLayer.workflowId)
            .pipe(
                mergeMap((inputWorkflow) => {
                    const workflow: WorkflowDict = {
                        type: 'Raster',
                        operator: {
                            type: 'BandwiseExpression',
                            params: {
                                expression,
                                outputType: dataType.getCode(),
                                mapNoData,
                            },
                            sources: {
                                raster: inputWorkflow.operator,
                            },
                        } as BandwiseExpressionDict,
                    } as WorkflowDict;

                    return this.projectService.registerWorkflow(workflow);
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
                                workflowId,
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
}

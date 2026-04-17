import {ChangeDetectionStrategy, Component, OnDestroy, inject} from '@angular/core';
import {
    AbstractControl,
    FormArray,
    FormBuilder,
    FormControl,
    FormGroup,
    ValidationErrors,
    ValidatorFn,
    Validators,
    FormsModule,
    ReactiveFormsModule,
} from '@angular/forms';
import {EMPTY, Subscription, combineLatest} from 'rxjs';
import {ProjectService} from '../../../project/project.service';

import {filter, map, mergeMap} from 'rxjs/operators';
import {LetterNumberConverter, MultiLayerSelectionComponent} from '../helpers/multi-layer-selection/multi-layer-selection.component';
import {
    ColumnNamesDict,
    NotificationService,
    PointSymbology,
    RandomColorService,
    RasterLayer,
    RasterLayerMetadata,
    RasterVectorJoinDict,
    RasterVectorJoinParams,
    ResultTypes,
    StaticColor,
    VectorLayer,
    VectorLayerMetadata,
    geoengineValidators,
    FxLayoutDirective,
} from '@geoengine/common';
import {SidenavHeaderComponent} from '../../../sidenav/sidenav-header/sidenav-header.component';
import {OperatorDialogContainerComponent} from '../helpers/operator-dialog-container/operator-dialog-container.component';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {LayerSelectionComponent} from '../helpers/layer-selection/layer-selection.component';
import {MatButtonToggleGroup, MatButtonToggle} from '@angular/material/button-toggle';
import {MatFormField, MatLabel, MatInput, MatError, MatHint} from '@angular/material/input';
import {MatRadioGroup, MatRadioButton} from '@angular/material/radio';
import {MatCheckbox} from '@angular/material/checkbox';
import {OperatorOutputNameComponent} from '../helpers/operator-output-name/operator-output-name.component';

type TemporalAggregation = 'none' | 'first' | 'mean';
type FeatureAggregation = 'first' | 'mean';

interface RasterVectorJoinForm {
    vectorLayer: FormControl<VectorLayer | undefined>;
    rasterLayers: FormControl<Array<RasterLayer>>;
    columnNamesType: FormControl<ColumnNames>;
    columnNamesValues: FormArray<FormControl<string>>;
    temporalAggregation: FormControl<TemporalAggregation>;
    temporalAggregationIgnoreNodata: FormControl<boolean>;
    featureAggregation: FormControl<FeatureAggregation>;
    featureAggregationIgnoreNodata: FormControl<boolean>;
    name: FormControl<string>;
}

enum ColumnNames {
    Default,
    Suffix,
    Names,
}

@Component({
    selector: 'geoengine-raster-vector-join',
    templateUrl: './raster-vector-join.component.html',
    styleUrls: ['./raster-vector-join.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        SidenavHeaderComponent,
        FormsModule,
        ReactiveFormsModule,
        OperatorDialogContainerComponent,
        MatIconButton,
        MatIcon,
        LayerSelectionComponent,
        MultiLayerSelectionComponent,
        MatButtonToggleGroup,
        MatButtonToggle,
        MatFormField,
        MatLabel,
        MatInput,
        MatError,
        MatRadioGroup,
        FxLayoutDirective,
        MatRadioButton,
        MatCheckbox,
        OperatorOutputNameComponent,
        MatHint,
        MatButton,
    ],
})
export class RasterVectorJoinComponent implements OnDestroy {
    private readonly projectService = inject(ProjectService);
    private readonly randomColorService = inject(RandomColorService);
    private readonly notificationService = inject(NotificationService);
    private readonly formBuilder = inject(FormBuilder);

    minNumberOfRasterInputs = 1;
    maxNumberOfRasterInputs = 8;
    allowedVectorTypes = [ResultTypes.POINTS, ResultTypes.POLYGONS];
    allowedRasterTypes = [ResultTypes.RASTER];

    ColumnNames = ColumnNames;

    form: FormGroup<RasterVectorJoinForm>;

    private rasterLayerMetadata: Array<RasterLayerMetadata> = [];
    private vectorColumns: string[] = [];

    private subscriptions: Array<Subscription> = [];

    constructor() {
        this.form = this.formBuilder.nonNullable.group({
            vectorLayer: new FormControl<VectorLayer | undefined>(undefined, {
                nonNullable: true,
                validators: [Validators.required],
            }),
            rasterLayers: new FormControl<Array<RasterLayer>>([], {
                nonNullable: true,
                validators: [Validators.required],
            }),
            columnNamesType: new FormControl(ColumnNames.Default, {
                nonNullable: true,
                validators: [Validators.required],
            }),
            columnNamesValues: new FormArray<FormControl<string>>([], {validators: geoengineValidators.duplicateInFormArrayValidator()}),
            temporalAggregation: ['none' as TemporalAggregation, Validators.required],
            temporalAggregationIgnoreNodata: [false, Validators.required],
            featureAggregation: ['first' as FeatureAggregation, Validators.required],
            featureAggregationIgnoreNodata: [false, Validators.required],
            name: ['Vectors With Raster Values', [Validators.required, geoengineValidators.notOnlyWhitespace]],
        });

        const rasterLayerSub = this.form.controls.rasterLayers.valueChanges
            .pipe(
                mergeMap((rasterLayers: Array<RasterLayer>) => {
                    if (!rasterLayers) {
                        return EMPTY;
                    }

                    const metaData = rasterLayers.map((l) => this.projectService.getRasterLayerMetadata(l));
                    return combineLatest(metaData);
                }),
            )
            .subscribe((rasterLayers: Array<RasterLayerMetadata>) => {
                this.rasterLayerMetadata = rasterLayers;
                this.updateColumnNamesType();
            });

        this.subscriptions.push(rasterLayerSub);

        const vectorLayerSubscription = this.form.controls['vectorLayer'].valueChanges
            .pipe(
                filter((vectorLayer): vectorLayer is VectorLayer => !!vectorLayer),
                mergeMap((vectorLayer: VectorLayer) => this.projectService.getLayerMetadata(vectorLayer)),
                map((metadata) => {
                    if (!(metadata instanceof VectorLayerMetadata)) {
                        throw Error('expected to get vector metadata');
                    }

                    return metadata.dataTypes.keySeq().toArray();
                }),
            )
            .subscribe((columns) => {
                this.vectorColumns = columns;
                this.updateColumnNamesType();
            });
        this.subscriptions.push(vectorLayerSubscription);
    }

    ngOnDestroy(): void {
        this.subscriptions.forEach((subscription) => subscription.unsubscribe());
    }

    get columnNameValues(): FormArray {
        return this.form.get('columnNamesValues') as FormArray;
    }

    columnNameHint(i: number): string {
        switch (this.form.controls.columnNamesType.value) {
            case ColumnNames.Default:
                return '';
            case ColumnNames.Suffix:
                return `Suffix for input ${i}`;
            case ColumnNames.Names:
                return `New name for column ${i}`;
        }
    }

    updateColumnNamesType(): void {
        if (!this.form.controls.rasterLayers.value) {
            return;
        }

        const columnNamesValuesControl = this.form.controls.columnNamesValues;
        columnNamesValuesControl.clear();

        const columnNamesType = this.form.controls.columnNamesType.value;

        this.rasterLayerMetadata.forEach((layer, layerIndex) => {
            if (columnNamesType === ColumnNames.Suffix) {
                columnNamesValuesControl.push(
                    new FormControl(`_${layerIndex}`, {
                        nonNullable: true,
                        validators: [
                            forbiddenValueValidator(
                                this.vectorColumns,
                                layer.bands.map((b) => b.name),
                            ),
                        ],
                    }),
                );
            } else if (columnNamesType === ColumnNames.Names) {
                layer.bands.forEach((band) => {
                    columnNamesValuesControl.push(
                        new FormControl(band.name, {
                            nonNullable: true,
                            validators: [
                                Validators.required,
                                geoengineValidators.notOnlyWhitespace,
                                forbiddenValueValidator(this.vectorColumns),
                            ],
                        }),
                    );
                });
            }
        });
    }

    getValueNameControls(): Array<FormControl> {
        const valueNames = this.form.get('valueNames');

        if (!valueNames || !(valueNames instanceof FormArray)) {
            return [];
        }

        return valueNames.controls as Array<FormControl>;
    }

    add(): void {
        const vectorLayer: VectorLayer | undefined = this.form.controls.vectorLayer.value;
        const rasterLayers: Array<RasterLayer> = this.form.controls.rasterLayers.value;
        if (!vectorLayer || !rasterLayers) {
            return;
        }
        const names = this.getColumnNames();
        const temporalAggregation: TemporalAggregation = this.form.controls.temporalAggregation.value;
        const temporalAggregationIgnoreNoData = this.form.controls.temporalAggregationIgnoreNodata.value;
        const featureAggregation: FeatureAggregation = this.form.controls.featureAggregation.value;
        const featureAggregationIgnoreNoData = this.form.controls.featureAggregationIgnoreNodata.value;
        const outputLayerName: string = this.form.controls['name'].value;
        const params: RasterVectorJoinParams = {
            names,
            temporalAggregation,
            temporalAggregationIgnoreNoData,
            featureAggregation,
            featureAggregationIgnoreNoData,
        };
        const sourceOperators = this.projectService.getAutomaticallyProjectedOperatorsFromLayers([vectorLayer, ...rasterLayers]);
        sourceOperators
            .pipe(
                mergeMap(([vectorOperator, ...rasterOperators]) =>
                    this.projectService.registerWorkflow({
                        type: 'Vector',
                        operator: {
                            type: 'RasterVectorJoin',
                            params,
                            sources: {
                                vector: vectorOperator,
                                rasters: rasterOperators,
                            },
                        } as RasterVectorJoinDict,
                    }),
                ),
                mergeMap((workflowId) =>
                    this.projectService.addLayer(
                        new VectorLayer({
                            workflowId,
                            name: outputLayerName,
                            symbology: this.symbologyWithNewColor(vectorLayer.symbology as PointSymbology),
                            isLegendVisible: false,
                            isVisible: true,
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

    toLetters(number: number): string {
        return LetterNumberConverter.toLetters(number);
    }

    private getColumnNames(): ColumnNamesDict {
        switch (this.form.controls.columnNamesType.value) {
            case ColumnNames.Default:
                return {
                    type: 'default',
                };
            case ColumnNames.Suffix:
                return {
                    type: 'suffix',
                    values: this.form.controls.columnNamesValues.value,
                };
            case ColumnNames.Names:
                return {
                    type: 'names',
                    values: this.form.controls.columnNamesValues.value,
                };
        }
    }

    private symbologyWithNewColor(inputSymbology: PointSymbology): PointSymbology {
        const symbology = inputSymbology.clone();

        // TODO: more sophisticated update method that makes sense for non-points
        symbology.fillColor = new StaticColor(this.randomColorService.getRandomColorRgba());

        return symbology;
    }
}

/**
 * check if the current value is contained in the list of forbidden values.
 * you can additionaly give a list of prefixes that will be prepended to the value before checking.
 */
export const forbiddenValueValidator =
    (forbiddenValues: string[], prefixes: string[] = ['']): ValidatorFn =>
    (control: AbstractControl): ValidationErrors | null => {
        const text = control.value as string;

        for (const prefix of prefixes) {
            const value = `${prefix}${text}`;
            if (forbiddenValues.includes(value)) {
                return forbiddenValues.includes(text) ? {forbiddenValue: {value}} : null;
            }
        }

        return null;
    };

import {KeyValuePipe} from '@angular/common';
import {ChangeDetectionStrategy, Component, computed, inject, signal, viewChild} from '@angular/core';
import {takeUntilDestroyed, toObservable, toSignal} from '@angular/core/rxjs-interop';
import {FormBuilder, FormControl, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {MatOption} from '@angular/material/autocomplete';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatCheckbox} from '@angular/material/checkbox';
import {MatIcon} from '@angular/material/icon';
import {MatFormField, MatLabel, MatInput, MatHint} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import moment, {Moment} from 'moment';
import {firstValueFrom, of} from 'rxjs';
import {map, switchMap} from 'rxjs/operators';
import {
    PolygonRasterizationAggregation,
    PolygonRasterizationParameters,
    PolygonRasterizationTime,
    PolygonRasterizationValue,
    RasterDataType as ApiRasterDataType,
    VectorOperator,
} from '@geoengine/api-client';
import {
    Layer,
    NotificationService,
    RasterDataType,
    RasterDataTypes,
    RasterLayer,
    RasterLayerMetadata,
    ResultTypes,
    SpatialReference,
    VectorColumnDataTypes,
    VectorLayer,
    geoengineValidators,
    timeStepGranularityOptions,
    CommonModule,
    errorMessage,
} from '@geoengine/common';
import {TimeStepGranularityDict} from '../../../backend/backend.model';
import {SymbologyCreatorComponent} from '../../../layers/symbology/symbology-creator/symbology-creator.component';
import {ProjectService} from '../../../project/project.service';
import {SidenavHeaderComponent} from '../../../sidenav/sidenav-header/sidenav-header.component';
import {LayerSelectionComponent} from '../helpers/layer-selection/layer-selection.component';
import {OperatorDialogContainerComponent} from '../helpers/operator-dialog-container/operator-dialog-container.component';
import {OperatorOutputNameComponent} from '../helpers/operator-output-name/operator-output-name.component';

export type ValueMode = PolygonRasterizationValue['type'];
export type TimeMode = PolygonRasterizationTime['type'];

/**
 * The raw values of the dialog's form that define the operator parameters.
 */
export interface PolygonRasterizationFormValues {
    resolutionX: number;
    resolutionY: number;
    originX: number;
    originY: number;
    valueMode: ValueMode;
    constant: number;
    column: string;
    aggregation: PolygonRasterizationAggregation;
    ignoreNoData: boolean;
    outputType: RasterDataType | undefined;
    timeMode: TimeMode;
    granularity: TimeStepGranularityDict;
    step: number;
    referenceChecked: boolean;
    reference: Moment;
}

/**
 * Creates the operator parameters from the form values, omitting what the backend derives by default.
 */
export function polygonRasterizationParams(values: PolygonRasterizationFormValues): PolygonRasterizationParameters {
    let value: PolygonRasterizationValue;
    switch (values.valueMode) {
        case 'constant':
            value = {type: 'constant', value: values.constant};
            break;
        case 'count':
            value = {type: 'count'};
            break;
        case 'attribute':
            value = {type: 'attribute', column: values.column, aggregation: values.aggregation, ignoreNoData: values.ignoreNoData};
            break;
    }

    let time: PolygonRasterizationTime;
    switch (values.timeMode) {
        case 'featureTimes':
            time = {type: 'featureTimes'};
            break;
        case 'regular':
            time = {
                type: 'regular',
                step: {granularity: values.granularity, step: values.step},
                reference: values.referenceChecked ? values.reference.valueOf() : undefined,
            };
            break;
        case 'ignore':
            time = {type: 'ignore'};
            break;
    }

    return {
        spatialResolution: {x: values.resolutionX, y: values.resolutionY},
        originCoordinate: {x: values.originX, y: values.originY},
        value,
        outputType: values.outputType ? (values.outputType.getCode() as ApiRasterDataType) : undefined,
        time,
    };
}

/**
 * The grid values of the form that align the output with the grid of a raster.
 */
export function gridOfRaster(
    metadata: RasterLayerMetadata,
): Pick<PolygonRasterizationFormValues, 'resolutionX' | 'resolutionY' | 'originX' | 'originY' | 'outputType'> {
    const grid = metadata.spatialGrid.spatialGrid;
    return {
        resolutionX: Math.abs(grid.pixelSizeX),
        resolutionY: Math.abs(grid.pixelSizeY),
        originX: grid.originCoordinate.x,
        originY: grid.originCoordinate.y,
        outputType: metadata.dataType,
    };
}

/**
 * Reprojects the vector operator if its spatial reference differs from the target.
 */
export function reprojectedVectorOperator(
    operator: VectorOperator,
    spatialReference: SpatialReference,
    targetSpatialReference: SpatialReference | undefined,
): VectorOperator {
    if (!targetSpatialReference || spatialReference.equals(targetSpatialReference)) {
        return operator;
    }

    return {
        type: 'Reprojection',
        params: {targetSpatialReference: targetSpatialReference.srsString},
        sources: {source: operator},
    };
}

/**
 * This component allows creating the polygon rasterization operator.
 */
@Component({
    selector: 'geoengine-polygon-rasterization',
    templateUrl: './polygon-rasterization.component.html',
    styleUrls: ['./polygon-rasterization.component.scss'],
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
        MatInput,
        MatHint,
        MatCheckbox,
        CommonModule,
        OperatorOutputNameComponent,
        SymbologyCreatorComponent,
        MatButton,
        KeyValuePipe,
    ],
})
export class PolygonRasterizationComponent {
    private readonly projectService = inject(ProjectService);
    private readonly notificationService = inject(NotificationService);
    private readonly formBuilder = inject(FormBuilder);

    readonly inputTypes = [ResultTypes.POLYGONS];
    readonly rasterDataTypes = RasterDataTypes.ALL_DATATYPES;
    readonly timeGranularityOptions: Array<TimeStepGranularityDict> = timeStepGranularityOptions;

    readonly valueModes: Record<ValueMode, string> = {
        constant: 'Constant value',
        count: 'Number of overlapping polygons',
        attribute: 'Value of a column',
    };
    readonly defaultOutputTypes: Record<ValueMode, string> = {
        constant: 'U8',
        count: 'U32',
        attribute: 'F64',
    };
    readonly aggregations: Record<PolygonRasterizationAggregation, string> = {
        first: 'First',
        last: 'Last',
        min: 'Minimum',
        max: 'Maximum',
        sum: 'Sum',
        mean: 'Mean',
        medianEstimate: 'Median (estimate)',
    };
    readonly timeModes: Record<TimeMode, string> = {
        featureTimes: 'From the feature times',
        regular: 'Regular time steps',
        ignore: 'Ignore time',
    };

    readonly form = this.formBuilder.nonNullable.group({
        name: ['', [Validators.required, geoengineValidators.notOnlyWhitespace]],
        layer: new FormControl<Layer | undefined>(undefined, {nonNullable: true, validators: [Validators.required]}),
        gridLayer: new FormControl<RasterLayer | undefined>(undefined, {nonNullable: true}),
        resolutionX: [0.1, [Validators.required, geoengineValidators.largerThan(0)]],
        resolutionY: [0.1, [Validators.required, geoengineValidators.largerThan(0)]],
        originX: [0, [Validators.required]],
        originY: [0, [Validators.required]],
        valueMode: ['constant' as ValueMode, [Validators.required]],
        constant: [1, [Validators.required]],
        column: [''],
        aggregation: ['max' as PolygonRasterizationAggregation, [Validators.required]],
        ignoreNoData: [true],
        outputType: new FormControl<RasterDataType | undefined>(undefined, {nonNullable: true}),
        timeMode: ['featureTimes' as TimeMode, [Validators.required]],
        granularity: ['months' as TimeStepGranularityDict, [Validators.required]],
        step: [1, [Validators.required, Validators.min(1)]],
        referenceChecked: [false],
        reference: [moment.utc(0), [Validators.required]],
    });

    readonly valueMode = toSignal(this.form.controls.valueMode.valueChanges, {initialValue: this.form.controls.valueMode.value});
    readonly timeMode = toSignal(this.form.controls.timeMode.valueChanges, {initialValue: this.form.controls.timeMode.value});
    readonly referenceChecked = toSignal(this.form.controls.referenceChecked.valueChanges, {
        initialValue: this.form.controls.referenceChecked.value,
    });

    readonly layer = toSignal(this.form.controls.layer.valueChanges, {initialValue: this.form.controls.layer.value});

    readonly layerMetadata = toSignal(
        toObservable(this.layer).pipe(
            switchMap((layer) => (layer instanceof VectorLayer ? this.projectService.getVectorLayerMetadata(layer) : of(undefined))),
        ),
        {initialValue: undefined},
    );

    /** The numeric and categorical columns of the selected layer, which can be burned into the raster */
    readonly columns = computed(
        () =>
            this.layerMetadata()
                ?.dataTypes.filter((columnType) => columnType.isNumeric || columnType === VectorColumnDataTypes.Category)
                .keySeq()
                .toArray() ?? [],
    );

    /** The raster layers of the project, whose grid the output can be aligned with */
    readonly rasterLayers = toSignal(
        this.projectService.getLayerStream().pipe(map((layers) => layers.filter((layer) => layer instanceof RasterLayer))),
        {initialValue: []},
    );

    readonly gridLayer = toSignal(this.form.controls.gridLayer.valueChanges, {initialValue: this.form.controls.gridLayer.value});

    readonly gridMetadata = toSignal(
        toObservable(this.gridLayer).pipe(
            switchMap((layer) => (layer ? this.projectService.getRasterLayerMetadata(layer) : of(undefined))),
        ),
        {initialValue: undefined},
    );

    /** The spatial reference the polygons are reprojected to, if it differs from theirs */
    readonly reprojectionTarget = computed(() => {
        const polygonSpatialReference = this.layerMetadata()?.spatialReference;
        const gridSpatialReference = this.gridMetadata()?.spatialReference;
        if (!polygonSpatialReference || !gridSpatialReference || polygonSpatialReference.equals(gridSpatialReference)) {
            return undefined;
        }
        return gridSpatialReference;
    });

    readonly loading = signal(false);

    readonly symbologyCreator = viewChild.required(SymbologyCreatorComponent);

    constructor() {
        // the column is only required when the value is taken from it
        this.form.controls.valueMode.valueChanges.pipe(takeUntilDestroyed()).subscribe((valueMode) => {
            const column = this.form.controls.column;
            column.setValidators(valueMode === 'attribute' ? [Validators.required] : []);
            column.updateValueAndValidity();
        });

        // reset the column when the layer changes, since it may not exist anymore
        this.form.controls.layer.valueChanges.pipe(takeUntilDestroyed()).subscribe(() => this.form.controls.column.setValue(''));

        // align the output with the grid of the selected raster
        toObservable(this.gridMetadata)
            .pipe(takeUntilDestroyed())
            .subscribe((metadata) => {
                if (metadata) {
                    this.form.patchValue(gridOfRaster(metadata));
                }
            });
    }

    /** Keeps the declaration order of the options for the `keyvalue` pipe */
    keepOrder(): number {
        return 0;
    }

    async add(): Promise<void> {
        if (this.loading()) return; // don't add while loading

        const inputLayer = this.form.controls.layer.value;
        if (!(inputLayer instanceof VectorLayer)) return;

        const layerName = this.form.controls.name.value;
        const params = polygonRasterizationParams(this.form.getRawValue());

        this.loading.set(true);

        try {
            const polygons = await this.projectService.getProcessingGraph(inputLayer.workflowId);
            if (polygons.type !== 'Vector') {
                throw new Error('Expected a vector processing graph for polygon rasterization.');
            }
            const polygonMetadata = await firstValueFrom(this.projectService.getVectorLayerMetadata(inputLayer));
            const gridMetadata = this.gridMetadata();

            const workflowId = await this.projectService.registerProcessingGraph({
                type: 'Raster',
                operator: {
                    type: 'PolygonRasterization',
                    params,
                    sources: {
                        vector: reprojectedVectorOperator(
                            polygons.operator,
                            polygonMetadata.spatialReference,
                            gridMetadata?.spatialReference,
                        ),
                    },
                },
            });

            const symbology = await firstValueFrom(this.symbologyCreator().symbologyForRasterLayer(workflowId));

            await firstValueFrom(
                this.projectService.addLayer(
                    new RasterLayer({
                        processingGraphId: workflowId,
                        name: layerName,
                        symbology,
                        isLegendVisible: false,
                        isVisible: true,
                    }),
                ),
            );
        } catch (error) {
            this.notificationService.error(errorMessage(error));
        } finally {
            this.loading.set(false);
        }
    }
}

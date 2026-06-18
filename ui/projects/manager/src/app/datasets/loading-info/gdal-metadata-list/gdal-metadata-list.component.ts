import {ChangeDetectionStrategy, ChangeDetectorRef, Component, Input, OnChanges, SimpleChanges, inject, input} from '@angular/core';
import {
    AbstractControl,
    FormArray,
    FormControl,
    FormGroup,
    ValidationErrors,
    ValidatorFn,
    Validators,
    FormsModule,
    ReactiveFormsModule,
} from '@angular/forms';
import {GdalDatasetParametersComponent, GdalDatasetParametersForm} from '../gdal-dataset-parameters/gdal-dataset-parameters.component';
import {
    CommonModule,
    DatasetsService,
    GeoTransform,
    GridBoundingBox2D,
    GridIdx2D,
    TimeInterval,
    errorToText,
    BoundingBox2D,
    SpatialGridDefinition,
    SpatialGridDescriptor,
} from '@geoengine/common';
import moment from 'moment';
import {
    DataPath,
    GdalDatasetParameters,
    GdalMetaDataList,
    MetaDataDefinition,
    RasterDataType,
    RasterResultDescriptor,
} from '@geoengine/api-client';
import {MatSnackBar} from '@angular/material/snack-bar';
import {MatFormField, MatLabel, MatInput, MatError} from '@angular/material/input';
import {MatButton} from '@angular/material/button';
import {MatDivider, MatNavList, MatListItem, MatListItemTitle, MatListItemLine} from '@angular/material/list';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';

export interface GdalMetadataListForm {
    timeSlices: FormArray<FormGroup<TimeSliceForm>>;
    rasterResultDescriptor: FormGroup<RasterResultDescriptorForm>;
}

export interface TimeSliceForm {
    time: FormControl<TimeInterval>;
    gdalParameters: FormGroup<GdalDatasetParametersForm>;
    cacheTtl: FormControl<number>;
}

export interface RasterResultDescriptorForm {
    bandName: FormControl<string>;
    dataType: FormControl<RasterDataType>;
    spatialReference: FormControl<string>;
}

@Component({
    selector: 'geoengine-manager-gdal-metadata-list',
    templateUrl: './gdal-metadata-list.component.html',
    styleUrl: './gdal-metadata-list.component.scss',
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        FormsModule,
        ReactiveFormsModule,
        MatFormField,
        MatLabel,
        MatInput,
        MatButton,
        MatDivider,
        MatSelect,
        MatOption,
        MatNavList,
        MatListItem,
        MatListItemTitle,
        MatListItemLine,
        MatError,
        CommonModule,
        GdalDatasetParametersComponent,
    ],
})
export class GdalMetadataListComponent implements OnChanges {
    private readonly datasetsService = inject(DatasetsService);
    private readonly snackBar = inject(MatSnackBar);
    private readonly changeDetectorRef = inject(ChangeDetectorRef);

    RasterDataTypes = Object.values(RasterDataType);

    form: FormGroup<GdalMetadataListForm> = this.setUpForm();

    mainFile = '';

    @Input() dataPath?: DataPath;

    readonly metaData = input<GdalMetaDataList>();

    selectedTimeSlice = 0;

    // eslint-disable-next-line @typescript-eslint/no-misused-promises, @typescript-eslint/require-await
    async ngOnChanges(changes: SimpleChanges): Promise<void> {
        const metaData = this.metaData();
        if (changes.metaData && metaData) {
            this.setUpFormFromMetaData(metaData);
        }
    }

    addTimeSlicePlaceholder(): void {
        this.addTimeSlice(
            {
                start: moment.utc(),
                timeAsPoint: false,
                end: moment.utc().add(1, 'days'),
            },
            GdalDatasetParametersComponent.placeHolderGdalParams(),
            0,
        );
    }

    addTimeSlice(time: TimeInterval, gdalParams: GdalDatasetParameters, cacheTtl: number): void {
        this.form.controls.timeSlices.push(
            new FormGroup<TimeSliceForm>({
                time: new FormControl(time, {
                    nonNullable: true,
                    validators: [Validators.required],
                }),
                gdalParameters: GdalDatasetParametersComponent.setUpForm(gdalParams),
                cacheTtl: new FormControl<number>(cacheTtl, {
                    nonNullable: true,
                    validators: [Validators.required],
                }),
            }),
        );
        this.selectTimeSlice(this.form.controls.timeSlices.length - 1);
    }

    removeTimeSlice(): void {
        this.form.controls.timeSlices.removeAt(this.selectedTimeSlice);
        if (this.selectedTimeSlice >= this.form.controls.timeSlices.length) {
            this.selectedTimeSlice = this.form.controls.timeSlices.length - 1;
        }
        this.selectTimeSlice(this.selectedTimeSlice);
    }

    selectTimeSlice(index: number): void {
        this.selectedTimeSlice = index;
        this.changeDetectorRef.detectChanges();
    }

    getTime(i: number): string {
        const start = this.form.controls.timeSlices.at(i).controls.time.value.start.format('YYYY-MM-DD HH:mm');
        const end = this.form.controls.timeSlices.at(i).controls.time.value.end.format('YYYY-MM-DD HH:mm');
        return `${start} - ${end}`;
    }

    getGdalParameters(form: FormGroup<GdalDatasetParametersForm>): GdalDatasetParameters {
        return {
            allowAlphabandAsMask: form.controls.allowAlphabandAsMask.value,
            fileNotFoundHandling: form.controls.fileNotFoundHandling.value,
            filePath: form.controls.filePath.value,
            gdalConfigOptions: form.controls.gdalConfigOptions.value,
            gdalOpenOptions: form.controls.gdalOpenOptions.value,
            geoTransform: {
                originCoordinate: {
                    x: form.controls.geoTransform.controls.originCoordinate.controls.x.value,
                    y: form.controls.geoTransform.controls.originCoordinate.controls.y.value,
                },
                xPixelSize: form.controls.geoTransform.controls.xPixelSize.value,
                yPixelSize: form.controls.geoTransform.controls.yPixelSize.value,
            },
            height: form.controls.height.value,
            width: form.controls.width.value,
            noDataValue: form.controls.noDataValue.value,
            propertiesMapping: form.controls.propertiesMapping.controls.map((control) => {
                return {
                    sourceKey: {
                        domain: control.controls.sourceKey.controls.domain.value,
                        key: control.controls.sourceKey.controls.key.value,
                    },
                    targetKey: {
                        domain: control.controls.targetKey.controls.domain.value,
                        key: control.controls.targetKey.controls.key.value,
                    },
                    targetType: control.controls.targetType.value,
                };
            }),
            rasterbandChannel: form.controls.rasterbandChannel.value,
        };
    }

    getMetaData(): MetaDataDefinition | undefined {
        const params = this.form.controls.timeSlices.controls.map((control) => {
            return {
                time: {
                    start: control.controls.time.value.start.valueOf(),
                    end: control.controls.time.value.end.valueOf(),
                },
                params: this.getGdalParameters(control.controls.gdalParameters),

                cacheTtl: control.controls.cacheTtl.value,
            };
        });

        const resultDescriptorControl = this.form.controls.rasterResultDescriptor.controls;

        const boundsIter = params
            .filter((p) => !!p.params)
            .map((p) => {
                const gt = GeoTransform.fromDict(p.params.geoTransform);
                const bounds = gt.gridBoundsToSpatialBounds(
                    new GridBoundingBox2D(new GridIdx2D(0, 0), new GridIdx2D(p.params.width - 1, p.params.height - 1)),
                );
                return bounds;
            });
        const bounds = BoundingBox2D.unionFold(boundsIter);
        if (!bounds) {
            return undefined;
        }
        const gt = GeoTransform.fromDict(params.filter((p) => !!p.params).map((p) => p.params.geoTransform)[0]);
        const pxBounds = gt.spatialToGridBounds(bounds);

        const spatialGrid = new SpatialGridDefinition(gt, pxBounds);
        const spatialGridDesc = new SpatialGridDescriptor(spatialGrid, 'source');

        const resultDescriptor: RasterResultDescriptor = {
            bands: [
                {
                    name: resultDescriptorControl.bandName.value,
                    measurement: {
                        type: 'unitless',
                    },
                },
            ],
            spatialReference: resultDescriptorControl.spatialReference.value,
            dataType: resultDescriptorControl.dataType.value,
            spatialGrid: spatialGridDesc.toDict(),
            time: {
                bounds: null,
                dimension: {
                    type: 'irregular',
                },
            },
        };

        return {
            type: 'GdalMetaDataList',
            params,
            resultDescriptor,
        };
    }

    async suggest(): Promise<void> {
        if (!this.dataPath) {
            this.snackBar.open('No data path selected.', 'Close', {panelClass: ['error-snackbar']});
            return;
        }

        try {
            const suggestion = await this.datasetsService.suggestMetaData({
                suggestMetaData: {
                    dataPath: this.dataPath,
                    mainFile: this.mainFile,
                },
            });

            if (suggestion.metaData.type !== 'GdalMetaDataList') {
                this.snackBar.open(`Metadata suggestion is not of type "GdalMetaDataList" but ${suggestion.metaData.type}`, 'Close', {
                    panelClass: ['error-snackbar'],
                });
                return;
            }

            const gdalMetaDataList = suggestion.metaData;
            const slices = gdalMetaDataList.params;

            if (slices.length === 0) {
                this.snackBar.open('No time slices found in metadata suggestion.', 'Close', {panelClass: ['error-snackbar']});
                return;
            }

            const firstSlice = slices[0];
            const gdalParams = firstSlice.params;

            if (!gdalParams) {
                this.snackBar.open('No gdal parameters found in metadata suggestion.', 'Close', {panelClass: ['error-snackbar']});
                return;
            }

            if (this.form.controls.timeSlices.length === 0) {
                this.addTimeSlice(
                    {
                        start: moment.utc(),
                        timeAsPoint: false,
                        end: moment.utc().add(1, 'days'),
                    },
                    gdalParams,
                    0,
                );
            } else {
                this.form.controls.timeSlices
                    .at(this.selectedTimeSlice)
                    .setControl('gdalParameters', GdalDatasetParametersComponent.setUpForm(gdalParams));
            }
            this.setResultDescriptor(gdalMetaDataList.resultDescriptor);
        } catch (error) {
            const errorMessage = await errorToText(error, 'Metadata suggestion failed.');
            this.snackBar.open(errorMessage, 'Close', {panelClass: ['error-snackbar']});
        }
    }

    setResultDescriptor(resultDescriptor: RasterResultDescriptor): void {
        this.form.controls.rasterResultDescriptor.controls.bandName.setValue(resultDescriptor.bands[0].name);
        this.form.controls.rasterResultDescriptor.controls.dataType.setValue(resultDescriptor.dataType);
        this.form.controls.rasterResultDescriptor.controls.spatialReference.setValue(resultDescriptor.spatialReference);
    }

    private setUpFormFromMetaData(metaData: GdalMetaDataList): void {
        this.form = new FormGroup<GdalMetadataListForm>({
            timeSlices: new FormArray<FormGroup<TimeSliceForm>>([], {validators: overlappingTimeIntervalsValidator()}),
            rasterResultDescriptor: new FormGroup<RasterResultDescriptorForm>({
                bandName: new FormControl(metaData.resultDescriptor.bands[0].name, {
                    nonNullable: true,
                    validators: [Validators.required],
                }),
                dataType: new FormControl(metaData.resultDescriptor.dataType, {
                    nonNullable: true,
                    validators: [Validators.required],
                }),
                spatialReference: new FormControl(metaData.resultDescriptor.spatialReference, {
                    nonNullable: true,
                    validators: [Validators.required],
                }),
            }),
        });

        for (const slice of metaData.params) {
            if (slice.params) {
                this.addTimeSlice(
                    {
                        start: moment.unix(slice.time.start / 1000),
                        timeAsPoint: slice.time.start === slice.time.end,
                        end: moment.unix(slice.time.end / 1000),
                    },
                    slice.params,
                    slice.cacheTtl ?? 0,
                );
            }
        }
        this.form.markAsPristine();
    }

    private setUpForm(): FormGroup<GdalMetadataListForm> {
        const form = new FormGroup<GdalMetadataListForm>({
            timeSlices: new FormArray<FormGroup<TimeSliceForm>>(
                [
                    new FormGroup<TimeSliceForm>({
                        time: new FormControl(
                            {
                                start: moment.utc(),
                                timeAsPoint: false,
                                end: moment.utc().add(1, 'days'),
                            },
                            {
                                nonNullable: true,
                                validators: [Validators.required],
                            },
                        ),
                        gdalParameters: GdalDatasetParametersComponent.setUpPlaceholderForm(),
                        cacheTtl: new FormControl<number>(86400 /* 1 day */, {
                            nonNullable: true,
                            validators: [Validators.required],
                        }),
                    }),
                ],
                {validators: overlappingTimeIntervalsValidator()},
            ),
            rasterResultDescriptor: new FormGroup<RasterResultDescriptorForm>({
                bandName: new FormControl('', {
                    nonNullable: true,
                    validators: [Validators.required],
                }),
                dataType: new FormControl(RasterDataType.U8, {
                    nonNullable: true,
                    validators: [Validators.required],
                }),
                spatialReference: new FormControl('', {
                    nonNullable: true,
                    validators: [Validators.required],
                }),
            }),
        });

        return form;
    }
}

export const overlappingTimeIntervalsValidator =
    (): ValidatorFn =>
    (control: AbstractControl): ValidationErrors | null => {
        const timeIntervalIntersects = (a: TimeInterval, b: TimeInterval): boolean => {
            // instants must be distinct
            if (a.start == a.end || b.start == b.end) {
                return a.start == b.start;
            }
            // touching intervals are not overlapping
            if (a.start == b.end || b.start == a.end) {
                return false;
            }
            // check if start of one interval is within the other
            return (a.start >= b.start && a.start < b.end) || (b.start >= a.start && b.start < a.end);
        };

        if (!(control instanceof FormArray)) {
            return null;
        }

        const formArray = control;

        const controls = formArray.controls;
        const values: TimeInterval[] = controls.map((c) => c.value.time);

        for (let i = 0; i < values.length; i++) {
            for (let j = i + 1; j < values.length; j++) {
                if (timeIntervalIntersects(values[i], values[j])) {
                    return {overlappingTimeInterval: true};
                }
            }
        }

        return null;
    };

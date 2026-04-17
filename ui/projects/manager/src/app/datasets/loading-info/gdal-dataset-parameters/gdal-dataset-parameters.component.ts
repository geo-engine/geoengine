import {ChangeDetectionStrategy, Component, input} from '@angular/core';
import {FormArray, FormControl, FormGroup, NG_VALUE_ACCESSOR, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {DataPath, FileNotFoundHandling, GdalDatasetParameters, RasterPropertiesEntryType} from '@geoengine/api-client';
import {MatFormField, MatLabel, MatInput} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {MatSlideToggle} from '@angular/material/slide-toggle';

export interface GdalMetadataMapping {
    sourceKey: RasterPropertiesKey;
    targetKey: RasterPropertiesKey;
    targetType: RasterPropertiesEntryType;
}

export interface RasterPropertiesKey {
    domain: string;
    key: string;
}

export interface GdalDatasetParametersForm {
    filePath: FormControl<string>;
    rasterbandChannel: FormControl<number>;
    geoTransform: FormGroup<GdalDatasetGeoTransformForm>;
    width: FormControl<number>;
    height: FormControl<number>;
    fileNotFoundHandling: FormControl<FileNotFoundHandling>;
    noDataValue: FormControl<number>;
    propertiesMapping: FormArray<FormGroup<GdalMetadataMappingForm>>;
    gdalOpenOptions: FormArray<FormControl<string>>;
    gdalConfigOptions: FormArray<FormArray<FormControl<string>>>;
    allowAlphabandAsMask: FormControl<boolean>;
}

export interface GdalDatasetGeoTransformForm {
    originCoordinate: FormGroup<OriginCoordinateForm>;
    xPixelSize: FormControl<number>;
    yPixelSize: FormControl<number>;
}

export interface OriginCoordinateForm {
    x: FormControl<number>;
    y: FormControl<number>;
}

export interface GdalMetadataMappingForm {
    sourceKey: FormGroup<RasterPropertiesKeyForm>;
    targetKey: FormGroup<RasterPropertiesKeyForm>;
    targetType: FormControl<RasterPropertiesEntryType>;
}

export interface RasterPropertiesKeyForm {
    domain: FormControl<string>;
    key: FormControl<string>;
}

@Component({
    selector: 'geoengine-manager-gdal-dataset-parameters',
    templateUrl: './gdal-dataset-parameters.component.html',
    styleUrl: './gdal-dataset-parameters.component.scss',
    providers: [
        {
            provide: NG_VALUE_ACCESSOR,
            multi: true,
            useExisting: GdalDatasetParametersComponent,
        },
    ],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        FormsModule,
        ReactiveFormsModule,
        MatFormField,
        MatLabel,
        MatInput,
        MatSelect,
        MatOption,
        MatIconButton,
        MatIcon,
        MatButton,
        MatSlideToggle,
    ],
})
export class GdalDatasetParametersComponent {
    readonly form = input<FormGroup<GdalDatasetParametersForm>>(GdalDatasetParametersComponent.setUpPlaceholderForm());

    readonly dataPath = input<DataPath>();

    FileNotFoundHandling = Object.values(FileNotFoundHandling);
    RasterPropertiesEntryType = Object.values(RasterPropertiesEntryType);

    removePropertyMapping(i: number): void {
        this.form().controls.propertiesMapping.removeAt(i);
        this.form().markAsDirty();
    }

    addPropertyMapping(): void {
        this.form().controls.propertiesMapping.push(
            new FormGroup<GdalMetadataMappingForm>({
                sourceKey: new FormGroup<RasterPropertiesKeyForm>({
                    domain: new FormControl('newSourceDomain', {
                        nonNullable: true,
                        validators: [Validators.required],
                    }),
                    key: new FormControl('newSourceKey', {
                        nonNullable: true,
                        validators: [Validators.required],
                    }),
                }),
                targetKey: new FormGroup<RasterPropertiesKeyForm>({
                    domain: new FormControl('newTargetDomain', {
                        nonNullable: true,
                        validators: [Validators.required],
                    }),
                    key: new FormControl('newTargetKey', {
                        nonNullable: true,
                        validators: [Validators.required],
                    }),
                }),
                targetType: new FormControl(RasterPropertiesEntryType.String, {
                    nonNullable: true,
                    validators: [Validators.required],
                }),
            }),
        );
    }

    removeOpenOption(i: number): void {
        this.form().controls.gdalOpenOptions.removeAt(i);
        this.form().markAsDirty();
    }

    addOpenOption(): void {
        this.form().controls.gdalOpenOptions.push(
            new FormControl('newOption', {
                nonNullable: true,
                validators: [Validators.required],
            }),
        );
    }

    removeConfigOption(i: number): void {
        this.form().controls.gdalConfigOptions.removeAt(i);
        this.form().markAsDirty();
    }

    addConfigOption(): void {
        this.form().controls.gdalConfigOptions.push(
            new FormArray<FormControl<string>>([
                new FormControl('newKey', {
                    nonNullable: true,
                    validators: [Validators.required],
                }),
                new FormControl('newValue', {
                    nonNullable: true,
                    validators: [Validators.required],
                }),
            ]),
        );
    }

    static setUpPlaceholderForm(): FormGroup<GdalDatasetParametersForm> {
        return this.setUpForm(this.placeHolderGdalParams());
    }

    static placeHolderGdalParams(): GdalDatasetParameters {
        return {
            filePath: 'path/to/file',
            rasterbandChannel: 1,
            geoTransform: {
                originCoordinate: {x: 0, y: 0},
                xPixelSize: 1,
                yPixelSize: -1,
            },
            width: 1000,
            height: 1000,
            fileNotFoundHandling: FileNotFoundHandling.NoData,
            noDataValue: 0,
            propertiesMapping: [],
            gdalOpenOptions: [],
            gdalConfigOptions: [],
            allowAlphabandAsMask: false,
        };
    }

    static setUpForm(gdalParams: GdalDatasetParameters): FormGroup<GdalDatasetParametersForm> {
        return new FormGroup<GdalDatasetParametersForm>({
            filePath: new FormControl(gdalParams.filePath, {
                nonNullable: true,
                validators: [Validators.required], // TODO: check is relative file path
            }),
            rasterbandChannel: new FormControl(gdalParams.rasterbandChannel, {
                nonNullable: true,
                validators: [Validators.required], // TODO: check > 0
            }),
            geoTransform: new FormGroup<GdalDatasetGeoTransformForm>({
                originCoordinate: new FormGroup<OriginCoordinateForm>({
                    x: new FormControl(gdalParams.geoTransform.originCoordinate.x, {
                        nonNullable: true,
                        validators: [Validators.required],
                    }),
                    y: new FormControl(gdalParams.geoTransform.originCoordinate.y, {
                        nonNullable: true,
                        validators: [Validators.required],
                    }),
                }),
                xPixelSize: new FormControl(gdalParams.geoTransform.xPixelSize, {
                    nonNullable: true,
                    validators: [Validators.required],
                }),
                yPixelSize: new FormControl(gdalParams.geoTransform.yPixelSize, {
                    nonNullable: true,
                    validators: [Validators.required],
                }),
            }),
            width: new FormControl(gdalParams.width, {
                nonNullable: true,
                validators: [Validators.required],
            }),
            height: new FormControl(gdalParams.height, {
                nonNullable: true,
                validators: [Validators.required],
            }),
            fileNotFoundHandling: new FormControl(gdalParams.fileNotFoundHandling, {
                nonNullable: true,
                validators: [Validators.required],
            }),
            noDataValue: new FormControl(gdalParams.noDataValue ?? 0, {
                nonNullable: true,
                validators: [Validators.required],
            }),
            propertiesMapping: new FormArray<FormGroup<GdalMetadataMappingForm>>(
                gdalParams.propertiesMapping?.map((m) => {
                    return new FormGroup<GdalMetadataMappingForm>({
                        sourceKey: new FormGroup<RasterPropertiesKeyForm>({
                            domain: new FormControl(m.sourceKey.domain ?? '', {
                                nonNullable: true,
                                validators: [Validators.required],
                            }),
                            key: new FormControl(m.sourceKey.key, {
                                nonNullable: true,
                                validators: [Validators.required],
                            }),
                        }),
                        targetKey: new FormGroup<RasterPropertiesKeyForm>({
                            domain: new FormControl(m.targetKey.domain ?? '', {
                                nonNullable: true,
                                validators: [Validators.required],
                            }),
                            key: new FormControl(m.targetKey.key, {
                                nonNullable: true,
                                validators: [Validators.required],
                            }),
                        }),
                        targetType: new FormControl(RasterPropertiesEntryType.String, {
                            nonNullable: true,
                            validators: [Validators.required],
                        }),
                    });
                }) ?? [],
            ),
            gdalOpenOptions: new FormArray<FormControl<string>>(
                gdalParams.gdalOpenOptions?.map(
                    (o) =>
                        new FormControl(o, {
                            nonNullable: true,
                            validators: [Validators.required],
                        }),
                ) ?? [],
            ),
            gdalConfigOptions: new FormArray<FormArray<FormControl<string>>>(
                gdalParams.gdalConfigOptions?.map((o) => {
                    return new FormArray<FormControl<string>>([
                        new FormControl(o[0], {
                            nonNullable: true,
                            validators: [Validators.required],
                        }),
                        new FormControl(o[1], {
                            nonNullable: true,
                            validators: [Validators.required],
                        }),
                    ]);
                }) ?? [],
            ),
            allowAlphabandAsMask: new FormControl(gdalParams.allowAlphabandAsMask ?? false, {
                nonNullable: true,
                validators: [Validators.required],
            }),
        });
    }
}

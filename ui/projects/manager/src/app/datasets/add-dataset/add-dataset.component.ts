import {Component, inject, viewChild} from '@angular/core';
import {FormControl, FormGroup, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {ConfirmationComponent, DatasetsService, errorToText, OgrDatasetComponent} from '@geoengine/common';
import {DataPath, MetaDataDefinition, Volume as VolumeDict} from '@geoengine/api-client';
import {MatSnackBar} from '@angular/material/snack-bar';
import {MatDialog, MatDialogRef, MatDialogTitle} from '@angular/material/dialog';
import {BehaviorSubject, filter, firstValueFrom, merge} from 'rxjs';
import {GdalMetadataListComponent} from '../loading-info/gdal-metadata-list/gdal-metadata-list.component';
import {MatCard, MatCardHeader, MatCardTitle, MatCardContent} from '@angular/material/card';
import {MatFormField, MatLabel, MatInput} from '@angular/material/input';
import {MatButtonToggleGroup, MatButtonToggle} from '@angular/material/button-toggle';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {MatButton} from '@angular/material/button';
import {AsyncPipe} from '@angular/common';

enum DataPaths {
    Upload,
    Volume,
}

enum DataTypes {
    Raster,
    Vector,
}

export interface AddDatasetForm {
    name: FormControl<string>;
    displayName: FormControl<string>;
    dataPathType: FormControl<DataPaths>;
    uploadId: FormControl<string>;
    volumeName: FormControl<string>;
    dataType: FormControl<DataTypes>;
    rawLoadingInfo: FormControl<string>;
}

@Component({
    selector: 'geoengine-manager-add-dataset',
    templateUrl: './add-dataset.component.html',
    styleUrl: './add-dataset.component.scss',
    imports: [
        MatDialogTitle,
        FormsModule,
        ReactiveFormsModule,
        MatCard,
        MatCardHeader,
        MatCardTitle,
        MatCardContent,
        MatFormField,
        MatLabel,
        MatInput,
        MatButtonToggleGroup,
        MatButtonToggle,
        MatSelect,
        MatOption,
        GdalMetadataListComponent,
        OgrDatasetComponent,
        MatButton,
        AsyncPipe,
    ],
})
export class AddDatasetComponent {
    private readonly datasetsService = inject(DatasetsService);
    private readonly snackBar = inject(MatSnackBar);
    private readonly dialogRef = inject<MatDialogRef<AddDatasetComponent>>(MatDialogRef);
    private readonly dialog = inject(MatDialog);

    DataPaths = DataPaths;
    DataTypes = DataTypes;

    readonly gdalMetaDataList = viewChild(GdalMetadataListComponent);
    readonly ogrDatasetComponent = viewChild(OgrDatasetComponent);

    volumes$ = new BehaviorSubject<VolumeDict[]>([]);

    form: FormGroup<AddDatasetForm> = new FormGroup<AddDatasetForm>({
        name: new FormControl('', {
            nonNullable: true,
            validators: [Validators.required, Validators.pattern(/^([a-zA-Z0-9_-]+:)?[a-zA-Z0-9_-]+$/), Validators.minLength(3)],
        }),
        displayName: new FormControl('', {
            nonNullable: true,
            validators: [Validators.required],
        }),
        dataPathType: new FormControl(DataPaths.Volume, {
            nonNullable: true,
            validators: [Validators.required],
        }),
        uploadId: new FormControl('', {
            nonNullable: true,
            validators: [],
        }),
        volumeName: new FormControl('', {
            nonNullable: true,
            validators: [Validators.required],
        }),
        dataType: new FormControl(DataTypes.Raster, {
            nonNullable: true,
            validators: [Validators.required],
        }),
        rawLoadingInfo: new FormControl('', {
            nonNullable: true,
        }),
    });

    constructor() {
        merge(this.dialogRef.backdropClick(), this.dialogRef.keydownEvents().pipe(filter((event) => event.key === 'Escape'))).subscribe(
            // eslint-disable-next-line @typescript-eslint/no-misused-promises
            async (event) => {
                event.stopPropagation();

                if (this.form.pristine) {
                    this.dialogRef.close();
                    return;
                }

                const confirmDialogRef = this.dialog.open(ConfirmationComponent, {
                    data: {message: 'Do you really want to stop creating the dataset? All changes will be lost.'},
                });

                const confirm = await firstValueFrom(confirmDialogRef.afterClosed());

                if (confirm) {
                    this.dialogRef.close();
                }
            },
        );

        this.datasetsService.getVolumes().then((volumes) => {
            this.volumes$.next(volumes);
        });
    }

    dataPath(): DataPath {
        switch (this.form.controls.dataPathType.value) {
            case DataPaths.Upload:
                return {upload: this.form.controls.uploadId.value};
            case DataPaths.Volume:
                return {volume: this.form.controls.volumeName.value};
        }
    }

    updateDataPathType(): void {
        switch (this.form.controls.dataPathType.value) {
            case DataPaths.Upload:
                this.form.controls.uploadId.setValidators([Validators.required]);
                this.form.controls.uploadId.updateValueAndValidity();
                this.form.controls.volumeName.clearValidators();
                this.form.controls.volumeName.updateValueAndValidity();
                break;
            case DataPaths.Volume:
                this.form.controls.volumeName.setValidators([Validators.required]);
                this.form.controls.volumeName.updateValueAndValidity();
                this.form.controls.uploadId.clearValidators();
                this.form.controls.uploadId.updateValueAndValidity();
                break;
        }
    }

    updateDataType(): void {
        if (this.form.controls.dataType.value === DataTypes.Raster) {
            this.form.controls.rawLoadingInfo.clearValidators();
        } else {
            this.form.controls.rawLoadingInfo.setValidators([Validators.required]);
        }
    }

    isCreateDisabled(): boolean {
        const general = this.form.pristine || this.form.invalid;

        const raster = this.form.controls.dataType.value === DataTypes.Raster && (this.gdalMetaDataList()?.form?.invalid ?? false);

        const vector =
            this.form.controls.dataType.value === DataTypes.Vector && (this.ogrDatasetComponent()?.formMetaData?.invalid ?? false);

        return general || raster || vector;
    }

    async createDataset(): Promise<void> {
        if (!this.form.valid) {
            return;
        }

        let sourceOperator = undefined;
        let metaData: MetaDataDefinition | undefined = undefined;

        if (this.form.controls.dataType.value === DataTypes.Raster) {
            const gdalMetaDataList = this.gdalMetaDataList();
            if (!gdalMetaDataList) {
                return;
            }

            metaData = gdalMetaDataList.getMetaData();

            sourceOperator = 'GdalSource';
        } else if (this.form.controls.dataType.value === DataTypes.Vector) {
            const ogrDatasetComponent = this.ogrDatasetComponent();
            if (!ogrDatasetComponent) {
                return;
            }

            metaData = ogrDatasetComponent.getMetaData();

            sourceOperator = 'OgrSource';
        } else {
            return;
        }

        if (!metaData) {
            this.snackBar.open('Invalid loading information.', 'Close', {panelClass: ['error-snackbar']});
            return;
        }

        const definition = {
            metaData,
            properties: {
                name: this.form.controls.name.value,
                displayName: this.form.controls.displayName.value,
                description: '',
                sourceOperator,
            },
        };

        try {
            const datasetName = await this.datasetsService.createDataset(this.dataPath(), definition);
            this.dialogRef.close(datasetName);
        } catch (error) {
            const errorMessage = await errorToText(error, 'Creating dataset failed.');
            this.snackBar.open(errorMessage, 'Close', {panelClass: ['error-snackbar']});
        }
    }
}

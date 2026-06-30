import {ChangeDetectorRef, Component, effect, inject, input, linkedSignal, output, viewChild} from '@angular/core';
import {
    AbstractControl,
    FormControl,
    FormGroup,
    ValidationErrors,
    ValidatorFn,
    Validators,
    FormsModule,
    ReactiveFormsModule,
} from '@angular/forms';
import {MatChipInput, MatChipGrid, MatChipRow, MatChipRemove} from '@angular/material/chips';
import {MatDialog} from '@angular/material/dialog';
import {MatSnackBar} from '@angular/material/snack-bar';
import {
    ConfirmationComponent,
    DatasetsService,
    OgrDatasetComponent,
    RasterSymbology,
    Symbology,
    UUID,
    VectorSymbology,
    WHITE,
    WorkflowsService,
    createVectorSymbology as createDefaultVectorSymbology,
    errorToText,
    geoengineValidators,
    AsyncValueDefault,
    CodeEditorComponent,
} from '@geoengine/common';
import {Dataset, DatasetListing, GdalMetaDataList, MetaDataDefinition, OgrMetaData, TypedResultDescriptor} from '@geoengine/api-client';
import {BehaviorSubject, firstValueFrom} from 'rxjs';
import {ProvenanceComponent} from '../../provenance/provenance.component';
import {AppConfig} from '../../app-config.service';
import {GdalMetadataListComponent} from '../loading-info/gdal-metadata-list/gdal-metadata-list.component';
import {MatCard, MatCardHeader, MatCardTitle, MatCardContent} from '@angular/material/card';
import {MatFormField, MatLabel, MatInput, MatError} from '@angular/material/input';
import {MatIcon} from '@angular/material/icon';
import {MatButton} from '@angular/material/button';
import {SymbologyEditorComponent} from '../../symbology/symbology-editor/symbology-editor.component';
import {RasterResultDescriptorComponent} from '../../result-descriptors/raster-result-descriptor/raster-result-descriptor.component';
import {VectorResultDescriptorComponent} from '../../result-descriptors/vector-result-descriptor/vector-result-descriptor.component';
import {PermissionsComponent} from '../../permissions/permissions.component';
import {AsyncPipe} from '@angular/common';

export interface DatasetForm {
    layerType: FormControl<'plot' | 'raster' | 'vector'>;
    dataType: FormControl<string>;
    name: FormControl<string>;
    displayName: FormControl<string>;
    description: FormControl<string>;
    tags: FormControl<string[]>;
    newTag: FormControl<string>;
}

@Component({
    selector: 'geoengine-manager-dataset-editor',
    templateUrl: './dataset-editor.component.html',
    styleUrl: './dataset-editor.component.scss',
    imports: [
        MatCard,
        MatCardHeader,
        MatCardTitle,
        MatCardContent,
        FormsModule,
        ReactiveFormsModule,
        MatFormField,
        MatLabel,
        MatInput,
        MatChipGrid,
        MatChipRow,
        MatIcon,
        MatChipRemove,
        MatChipInput,
        MatError,
        MatButton,
        ProvenanceComponent,
        SymbologyEditorComponent,
        GdalMetadataListComponent,
        OgrDatasetComponent,
        RasterResultDescriptorComponent,
        VectorResultDescriptorComponent,
        PermissionsComponent,
        AsyncPipe,
        AsyncValueDefault,
        CodeEditorComponent,
    ],
})
export class DatasetEditorComponent {
    private readonly datasetsService = inject(DatasetsService);
    private readonly workflowsService = inject(WorkflowsService);
    private readonly snackBar = inject(MatSnackBar);
    private readonly dialog = inject(MatDialog);
    private readonly config = inject(AppConfig);
    private readonly changeDetectorRef = inject(ChangeDetectorRef);

    readonly _datasetListing = input.required<DatasetListing>({
        // eslint-disable-next-line @angular-eslint/no-input-rename
        alias: 'datasetListing',
    });

    readonly datasetListing = linkedSignal(this._datasetListing);

    readonly datasetDeleted = output<void>();

    readonly tagInput = viewChild.required(MatChipInput);
    readonly provenanceComponent = viewChild.required(ProvenanceComponent);
    readonly gdalMetadataListComponent = viewChild(GdalMetadataListComponent);
    readonly ogrDatasetComponent = viewChild(OgrDatasetComponent);

    dataset?: Dataset;
    form: FormGroup<DatasetForm> = this.placeholderForm();

    datasetWorkflowId$ = new BehaviorSubject<UUID | undefined>(undefined);

    rasterSymbology?: RasterSymbology = undefined;
    vectorSymbology?: VectorSymbology = undefined;

    rawLoadingInfo = '';
    rawLoadingInfoPristine = true;
    gdalMetaDataList?: GdalMetaDataList;
    ogrMetaData?: OgrMetaData;

    constructor() {
        effect(() => {
            void this.loadDataset(this._datasetListing());
        });
    }

    private async loadDataset(datasetListing: DatasetListing): Promise<void> {
        this.dataset = await this.datasetsService.getDataset(datasetListing.name);
        this.setUpForm(this.dataset);
        const workflowId = await this.getWorkflowId(this.dataset);
        this.datasetWorkflowId$.next(workflowId);
        this.setUpColorizer(this.dataset);

        const loadingInfo = await this.datasetsService.getLoadingInfo(this.dataset.name);
        if (loadingInfo.type === 'GdalMetaDataList') {
            this.gdalMetaDataList = loadingInfo;
            this.ogrMetaData = undefined;
            this.rawLoadingInfo = '';
        } else if (loadingInfo.type === 'OgrMetaData') {
            this.ogrMetaData = loadingInfo;
            this.gdalMetaDataList = undefined;
            this.rawLoadingInfo = '';
        } else {
            this.gdalMetaDataList = undefined;
            this.ogrMetaData = undefined;
            this.rawLoadingInfo = JSON.stringify(loadingInfo, null, 2);
            this.rawLoadingInfoPristine = true;
        }
        this.changeDetectorRef.detectChanges();
    }

    async applyChanges(): Promise<void> {
        if (!this.form.valid) {
            return;
        }

        const name = this.form.controls.name.value;
        const displayName = this.form.controls.displayName.value;
        const description = this.form.controls.description.value;
        const tags = this.form.controls.tags.value;

        try {
            const datasetListing = this.datasetListing();
            await this.datasetsService.updateDataset(datasetListing.name, {name, displayName, description, tags});
            datasetListing.name = name;
            datasetListing.displayName = displayName;
            datasetListing.description = description;
            this.datasetListing.set(datasetListing);
            this.snackBar.open('Dataset successfully updated.', 'Close', {duration: this.config.DEFAULTS.SNACKBAR_DURATION});
            this.form.markAsPristine();
        } catch (error) {
            const errorMessage = await errorToText(error, 'Updating dataset failed.');
            this.snackBar.open(errorMessage, 'Close', {panelClass: ['error-snackbar']});
        }
    }

    removeTag(tag: string): void {
        const tags: Array<string> = this.form.controls.tags.value;

        const index = tags.indexOf(tag);
        if (index > -1) {
            tags.splice(index, 1);
        }

        this.form.markAsDirty();
    }

    addTag(): void {
        const tags: Array<string> = this.form.controls.tags.value;

        const tag = this.tagInput().inputElement.value;

        if (!isValidTag(tag)) {
            return;
        }

        this.tagInput().inputElement.value = '';

        tags.push(tag);

        this.form.markAsDirty();
    }

    async saveProvenance(): Promise<void> {
        const provenanceComponent = this.provenanceComponent();
        if (!provenanceComponent.form.valid) {
            return;
        }

        const provenance = provenanceComponent.getProvenance();

        try {
            await this.datasetsService.updateProvenance(this.datasetListing().name, provenance);
            this.snackBar.open('Dataset provenance successfully updated.', 'Close', {duration: this.config.DEFAULTS.SNACKBAR_DURATION});
            provenanceComponent.form.markAsPristine();
        } catch (error) {
            const errorMessage = await errorToText(error, 'Updating dataset provenance failed.');
            this.snackBar.open(errorMessage, 'Close', {panelClass: ['error-snackbar']});
        }
    }

    get tagInputControl(): FormControl {
        return this.form.get('newTag') as FormControl;
    }

    get tagsControl(): FormControl {
        return this.form.get('tags') as FormControl;
    }

    createSymbology(dataset: Dataset): void {
        if (dataset.resultDescriptor.type === 'vector') {
            this.createVectorSymbology(dataset);
        }
        if (dataset.resultDescriptor.type === 'raster') {
            this.createRasterSymbology();
        }
    }

    async deleteDataset(): Promise<void> {
        const dialogRef = this.dialog.open(ConfirmationComponent, {
            data: {message: 'Confirm the deletion of the dataset. This cannot be undone.'},
        });

        const confirm = await firstValueFrom(dialogRef.afterClosed());

        if (!confirm) {
            return;
        }

        try {
            await this.datasetsService.deleteDataset(this.datasetListing().name);
            this.snackBar.open('Dataset successfully deleted.', 'Close', {duration: this.config.DEFAULTS.SNACKBAR_DURATION});
            this.datasetDeleted.emit(undefined);
        } catch (error) {
            const errorMessage = await errorToText(error, 'Deleting dataset failed.');
            this.snackBar.open(errorMessage, 'Close', {panelClass: ['error-snackbar']});
        }
    }

    getMetaDataDefinition(): MetaDataDefinition | undefined {
        const gdalMetadataListComponent = this.gdalMetadataListComponent();
        const ogrDatasetComponent = this.ogrDatasetComponent();
        if (gdalMetadataListComponent) {
            return gdalMetadataListComponent.getMetaData();
        } else if (ogrDatasetComponent) {
            return ogrDatasetComponent.getMetaData();
        } else {
            try {
                return JSON.parse(this.rawLoadingInfo) as MetaDataDefinition;
            } catch (_e) {
                this.snackBar.open('Invalid loading information.', 'Close', {panelClass: ['error-snackbar']});
                return undefined;
            }
        }
    }

    isSaveLoadingInfoDisabled(): boolean {
        const gdalMetadataListComponent = this.gdalMetadataListComponent();
        const ogrDatasetComponent = this.ogrDatasetComponent();
        if (gdalMetadataListComponent) {
            return gdalMetadataListComponent.form.pristine || gdalMetadataListComponent.form.invalid;
        } else if (ogrDatasetComponent) {
            return ogrDatasetComponent.formMetaData.pristine || ogrDatasetComponent.formMetaData.invalid;
        } else {
            return this.rawLoadingInfo === '' || this.rawLoadingInfoPristine;
        }
    }

    async saveLoadingInfo(): Promise<void> {
        const metaData = this.getMetaDataDefinition();

        if (!metaData) {
            return;
        }

        try {
            await this.datasetsService.updateLoadingInfo(this.datasetListing().name, metaData);
            this.snackBar.open('Dataset loading information successfully updated.', 'Close', {
                duration: this.config.DEFAULTS.SNACKBAR_DURATION,
            });

            this.rawLoadingInfoPristine = true;
            const gdalMetadataListComponent = this.gdalMetadataListComponent();
            if (gdalMetadataListComponent) {
                gdalMetadataListComponent.form.markAsPristine();
            }
        } catch (error) {
            const errorMessage = await errorToText(error, 'Updating dataset loading information failed.');
            this.snackBar.open(errorMessage, 'Close', {panelClass: ['error-snackbar']});
        }
    }

    updateLoadingInfo(spec: string): void {
        if (this.rawLoadingInfo === spec) {
            return;
        }
        this.rawLoadingInfo = spec;
        this.rawLoadingInfoPristine = false;
    }

    private setUpColorizer(dataset: Dataset): void {
        if (dataset.symbology) {
            const symbology = Symbology.fromDict(dataset.symbology);

            if (symbology instanceof RasterSymbology) {
                this.rasterSymbology = symbology;
            } else {
                this.rasterSymbology = undefined;
            }

            if (symbology instanceof VectorSymbology) {
                this.vectorSymbology = symbology;
            } else {
                this.vectorSymbology = undefined;
            }
        } else {
            this.rasterSymbology = undefined;
            this.vectorSymbology = undefined;
        }
    }

    private getWorkflowId(dataset: Dataset): Promise<UUID> {
        if (dataset.resultDescriptor.type === 'raster') {
            return this.workflowsService.registerWorkflow({
                type: 'Raster',
                operator: {
                    type: 'GdalSource',
                    params: {
                        data: dataset.name,
                    },
                },
            });
        }

        if (dataset.resultDescriptor.type === 'vector') {
            return this.workflowsService.registerWorkflow({
                type: 'Vector',
                operator: {
                    type: 'OgrSource',
                    params: {
                        data: dataset.name,
                    },
                },
            });
        }

        throw new Error('Unknown dataset type');
    }

    private createVectorSymbology(dataset: Dataset): void {
        if (!(dataset.resultDescriptor.type === 'vector')) {
            return;
        }

        this.vectorSymbology = createDefaultVectorSymbology(dataset.resultDescriptor.dataType, WHITE);
    }

    private createRasterSymbology(): void {
        this.rasterSymbology = RasterSymbology.fromRasterSymbologyDict({
            type: 'raster',
            opacity: 1.0,
            rasterColorizer: {
                type: 'singleBand',
                band: 0,
                bandColorizer: {
                    type: 'linearGradient',
                    breakpoints: [
                        {value: 1, color: [0, 0, 0, 255]},
                        {value: 255, color: [255, 255, 255, 255]},
                    ],
                    overColor: [255, 255, 255, 127],
                    underColor: [0, 0, 0, 127],
                    noDataColor: [0, 0, 0, 0],
                },
            },
        });
    }

    private dataTypeFromResultDescriptor(rd: TypedResultDescriptor): string {
        if (rd.type === 'raster') {
            return rd.dataType;
        }

        if (rd.type === 'vector') {
            return rd.dataType;
        }

        // There are no plot datasets so this should never happen
        return '';
    }

    private setUpForm(dataset: Dataset): void {
        this.form = new FormGroup<DatasetForm>({
            layerType: new FormControl(dataset.resultDescriptor.type, {
                nonNullable: true,
                validators: [Validators.required],
            }),
            dataType: new FormControl(this.dataTypeFromResultDescriptor(dataset.resultDescriptor), {
                nonNullable: true,
                validators: [Validators.required],
            }),
            name: new FormControl(dataset.name, {
                nonNullable: true,
                validators: [Validators.required, Validators.pattern(/^([a-zA-Z0-9_-]+:)?[a-zA-Z0-9_-]+$/), Validators.minLength(1)],
            }),
            displayName: new FormControl(dataset.displayName, {
                nonNullable: true,
                validators: [Validators.required],
            }),
            description: new FormControl(dataset.description, {
                nonNullable: true,
            }),
            tags: new FormControl<string[]>(dataset.tags ?? [], {
                nonNullable: true,
                validators: [geoengineValidators.duplicateValidator()],
            }),
            newTag: new FormControl('', {nonNullable: true, validators: [tagValidator()]}),
        });
    }

    private placeholderForm(): FormGroup<DatasetForm> {
        return new FormGroup<DatasetForm>({
            layerType: new FormControl('raster', {
                nonNullable: true,
                validators: [Validators.required],
            }),
            dataType: new FormControl('U8', {
                nonNullable: true,
                validators: [Validators.required],
            }),
            name: new FormControl('name', {
                nonNullable: true,
                validators: [Validators.required, Validators.pattern(/^[a-zA-Z0-9_]+$/), Validators.minLength(1)],
            }),
            displayName: new FormControl('displayName', {
                nonNullable: true,
                validators: [Validators.required],
            }),
            description: new FormControl('description', {
                nonNullable: true,
            }),
            tags: new FormControl<string[]>([], {nonNullable: true, validators: [geoengineValidators.duplicateValidator()]}),
            newTag: new FormControl('', {nonNullable: true, validators: [tagValidator()]}),
        });
    }
}

export const isValidTag = (tag: string): boolean => {
    const illegalChars = [' ', '/', '..'];
    return tag.length > 0 && !illegalChars.some((char) => tag.includes(char));
};

export const tagValidator =
    (): ValidatorFn =>
    (control: AbstractControl): ValidationErrors | null => {
        const text = control.value as string;
        if (!text) {
            return null;
        }

        if (isValidTag(text)) {
            return null;
        }
        return {invalidTag: true};
    };

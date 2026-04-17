import {ChangeDetectorRef, Component, OnChanges, SimpleChanges, inject, input} from '@angular/core';
import {FormsModule, ReactiveFormsModule, UntypedFormControl, UntypedFormGroup, Validators} from '@angular/forms';
import {DatasetsService} from '../datasets.service';
import {UploadsService} from '../../uploads/uploads.service';
import {UserService} from '../../user/user.service';
import {MatChipInputEvent, MatChipsModule} from '@angular/material/chips';
import {
    MetaDataDefinition,
    MetaDataSuggestion,
    OgrMetaData,
    OgrSourceDatasetTimeType,
    OgrSourceDurationSpec,
    OgrSourceTimeFormat,
    TimeGranularity,
    VectorColumnInfo,
} from '@geoengine/api-client';
import {UUID} from '../dataset.model';
import {CommonModule as AngularCommonModule} from '@angular/common';
import {MatInputModule} from '@angular/material/input';
import {MatFormFieldModule} from '@angular/material/form-field';
import {MatSelectModule} from '@angular/material/select';
import {timeStepGranularityOptions} from '../../time/time.model';
import {MatButtonModule} from '@angular/material/button';
import {MatIconModule} from '@angular/material/icon';
import {FxLayoutDirective} from '../../util/directives/flexbox-legacy.directive';

@Component({
    selector: 'geoengine-ogr-dataset',
    imports: [
        AngularCommonModule,
        MatInputModule,
        MatFormFieldModule,
        FormsModule,
        ReactiveFormsModule,
        MatSelectModule,
        MatChipsModule,
        MatButtonModule,
        MatIconModule,
        FxLayoutDirective,
    ],
    templateUrl: './ogr-dataset.component.html',
    styleUrl: './ogr-dataset.component.css',
})
export class OgrDatasetComponent implements OnChanges {
    protected datasetsService = inject(DatasetsService);
    protected uploadsService = inject(UploadsService);
    protected userService = inject(UserService);
    protected changeDetectorRef = inject(ChangeDetectorRef);

    vectorDataTypes = ['Data', 'MultiPoint', 'MultiLineString', 'MultiPolygon'];
    timeDurationValueTypes = ['infinite', 'value', 'zero'];
    timeTypes = ['None', 'Start', 'Start/End', 'Start/Duration'];
    timeFormats = ['auto', 'unixTimeStamp', 'custom'];
    timestampTypes = ['epochSeconds', 'epochMilliseconds'];
    errorHandlings = ['ignore', 'abort'];
    readonly timeGranularityOptions: Array<TimeGranularity> = timeStepGranularityOptions;

    readonly uploadId = input<UUID>();
    readonly volumeName = input<string>();
    readonly metaData = input<OgrMetaData>();

    formMetaData: UntypedFormGroup;

    uploadFiles?: Array<string>;
    uploadFileLayers: Array<string> = [];

    readonly defaultTimeGranularity: TimeGranularity = 'seconds';

    constructor() {
        this.formMetaData = new UntypedFormGroup({
            mainFile: new UntypedFormControl('', Validators.required),
            layerName: new UntypedFormControl('', Validators.required),
            dataType: new UntypedFormControl('', Validators.required),
            timeType: new UntypedFormControl('', Validators.required),
            timeStartColumn: new UntypedFormControl(''),
            timeStartFormat: new UntypedFormControl(''),
            timeStartFormatCustom: new UntypedFormControl(''), // TODO: validate format
            timeStartFormatUnix: new UntypedFormControl(''),
            timeDurationColumn: new UntypedFormControl(''),
            timeDurationValue: new UntypedFormControl(1), // TODO: validate is positive integer
            timeDurationValueType: new UntypedFormControl('infinite'),
            timeDurationGranularity: new UntypedFormControl(this.defaultTimeGranularity),
            timeEndColumn: new UntypedFormControl(''),
            timeEndFormat: new UntypedFormControl(''),
            timeEndFormatCustom: new UntypedFormControl(''), // TODO: validate format
            timeEndFormatUnix: new UntypedFormControl(''),
            columnsX: new UntypedFormControl(''),
            columnsY: new UntypedFormControl(''),
            columnsText: new UntypedFormControl(''),
            columnsFloat: new UntypedFormControl(''),
            columnsInt: new UntypedFormControl(''),
            errorHandling: new UntypedFormControl('skip', Validators.required),
            spatialReference: new UntypedFormControl('EPSG:4326', Validators.required), // TODO: validate sref string
        });
    }
    ngOnChanges(changes: SimpleChanges): void {
        if (changes.uploadId?.currentValue) {
            this.setUpMetadataSpecification(changes.uploadId.currentValue);
            return;
        }

        if (changes.metaData?.currentValue) {
            const metaData = changes.metaData.currentValue as OgrMetaData;
            this.fillMetaDataForm({
                mainFile: metaData.loadingInfo.fileName,
                layerName: metaData.loadingInfo.layerName,
                metaData: metaData,
            });
            return;
        }
    }

    changeTimeType(): void {
        const form = this.formMetaData.controls;
        const timeType = form.timeType.value;

        form.timeStartColumn.clearValidators();
        form.timeStartFormat.clearValidators();
        form.timeStartFormatCustom.clearValidators();
        form.timeStartFormatUnix.clearValidators();
        form.timeDurationColumn.clearValidators();
        form.timeDurationValue.clearValidators();
        form.timeDurationValueType.clearValidators();
        form.timeDurationGranularity.clearValidators();
        form.timeEndColumn.clearValidators();
        form.timeEndFormat.clearValidators();
        form.timeEndFormatCustom.clearValidators();
        form.timeEndFormatUnix.clearValidators();

        if (timeType === 'Start') {
            form.timeStartColumn.setValidators(Validators.required);
            form.timeStartFormat.setValidators(Validators.required);
            form.timeDurationValueType.setValidators(Validators.required);
        } else if (timeType === 'Start/Duration') {
            form.timeStartColumn.setValidators(Validators.required);
            form.timeStartFormat.setValidators(Validators.required);
            form.timeDurationColumn.setValidators(Validators.required);
        } else if (timeType === 'Start/End') {
            form.timeStartColumn.setValidators(Validators.required);
            form.timeStartFormat.setValidators(Validators.required);
            form.timeEndColumn.setValidators(Validators.required);
            form.timeEndFormat.setValidators(Validators.required);
        }

        form.timeStartColumn.updateValueAndValidity();
        form.timeStartFormat.updateValueAndValidity();
        form.timeStartFormatCustom.updateValueAndValidity();
        form.timeStartFormatUnix.updateValueAndValidity();
        form.timeDurationColumn.updateValueAndValidity();
        form.timeDurationValueType.updateValueAndValidity();
        form.timeDurationGranularity.updateValueAndValidity();
        form.timeDurationValue.updateValueAndValidity();
        form.timeEndColumn.updateValueAndValidity();
        form.timeEndFormat.updateValueAndValidity();
        form.timeEndFormatCustom.updateValueAndValidity();
        form.timeEndFormatUnix.updateValueAndValidity();
    }

    changeTimeStartFormat(): void {
        const form = this.formMetaData.controls;

        if (form.timeStartFormat.value === 'custom') {
            form.timeStartFormatCustom.setValidators(Validators.required);
        } else {
            form.timeStartFormatCustom.clearValidators();
        }
        form.timeStartFormatCustom.updateValueAndValidity();

        if (form.timeStartFormat.value === 'unixTimeStamp') {
            form.timeStartFormatUnix.setValidators(Validators.required);
        } else {
            form.timeStartFormatUnix.clearValidators();
        }
        form.timeStartFormatUnix.updateValueAndValidity();
    }

    changeTimeEndFormat(): void {
        const form = this.formMetaData.controls;

        if (form.timeEndFormat.value === 'custom') {
            form.timeEndFormatCustom.setValidators(Validators.required);
        } else {
            form.timeEndFormatCustom.clearValidators();
        }
        form.timeEndFormatCustom.updateValueAndValidity();

        if (form.timeEndFormat.value === 'unixTimeStamp') {
            form.timeEndFormatUnix.setValidators(Validators.required);
        } else {
            form.timeEndFormatUnix.clearValidators();
        }
        form.timeEndFormatUnix.updateValueAndValidity();
    }

    changeTimeDurationValueType(): void {
        const form = this.formMetaData.controls;
        if (form.timeDurationValueType.value === 'value') {
            form.timeDurationValue.setValidators(Validators.required);
            form.timeDurationGranularity.setValidators(Validators.required);
        } else {
            form.timeDurationValue.clearValidators();
            form.timeDurationGranularity.clearValidators();
        }
        form.timeDurationValue.updateValueAndValidity();
        form.timeDurationGranularity.updateValueAndValidity();
    }

    async changeMainFile(): Promise<void> {
        const uploadId = this.uploadId();
        if (!uploadId) {
            return;
        }

        const form = this.formMetaData.controls;
        const mainFile = form.mainFile.value;
        const layer = form.layerName.value;

        const layers = await this.uploadsService.getUploadFileLayers(uploadId, mainFile);
        this.uploadFileLayers = layers.layers;

        if (this.uploadFileLayers.length > 0 && !this.uploadFileLayers.includes(layer)) {
            form.layerName.setValue(this.uploadFileLayers[0]);
        }

        this.changeDetectorRef.markForCheck();
    }

    removeText(column: string): void {
        const columns: Array<string> = this.formMetaData.controls.columnsText.value;

        const index = columns.indexOf(column);
        if (index > -1) {
            columns.splice(index, 1);
        }
    }

    addText(event: MatChipInputEvent): void {
        const columns: Array<string> = this.formMetaData.controls.columnsText.value;
        const column = event.value;
        const eventInput = event.input;

        if (columns.indexOf(column)) {
            columns.push(column);
        }

        if (eventInput) {
            eventInput.value = '';
        }
    }

    removeInt(column: string): void {
        const columns: Array<string> = this.formMetaData.controls.columnsInt.value;

        const index = columns.indexOf(column);
        if (index > -1) {
            columns.splice(index, 1);
        }
    }

    addInt(event: MatChipInputEvent): void {
        const columns: Array<string> = this.formMetaData.controls.columnsInt.value;
        const column = event.value;
        const eventInput = event.input;

        if (columns.indexOf(column)) {
            columns.push(column);
        }

        if (eventInput) {
            eventInput.value = '';
        }
    }

    removeFloat(column: string): void {
        const columns: Array<string> = this.formMetaData.controls.columnsFloat.value;

        const index = columns.indexOf(column);
        if (index > -1) {
            columns.splice(index, 1);
        }
    }

    addFloat(event: MatChipInputEvent): void {
        const columns: Array<string> = this.formMetaData.controls.columnsFloat.value;
        const column = event.value;
        const eventInput = event.input;

        if (columns.indexOf(column)) {
            columns.push(column);
        }

        if (eventInput) {
            eventInput.value = '';
        }
    }

    reloadSuggestion(): void {
        this.suggest(this.formMetaData.controls.mainFile.value, this.formMetaData.controls.layerName.value);
    }

    fillMetaDataForm(suggest: MetaDataSuggestion): void {
        if (suggest.metaData.type !== 'OgrMetaData') {
            return;
        }

        const info = suggest.metaData.loadingInfo;

        const start = this.getStartTime(info?.time);
        const end = this.getEndTime(info?.time);
        this.formMetaData.patchValue({
            mainFile: suggest.mainFile,
            layerName: info?.layerName,
            dataType: info?.dataType,
            timeType: info.time ? this.getTimeType(info.time) : 'None',
            timeStartColumn: start ? start.startField : '',
            timeStartFormat: start ? start.startFormat.format : '',
            timeStartFormatCustom: start ? (start.startFormat.format == 'custom' ? start.startFormat.customFormat : '') : '',
            timeStartFormatUnix: start ? (start.startFormat.format == 'unixTimeStamp' ? start.startFormat.timestampType : '') : '',
            timeDurationColumn: info?.time?.type === 'start+duration' ? info?.time.durationField : '',
            timeDurationValue: info?.time?.type === 'start' ? info?.time.duration : 1,
            timeDurationValueType: info?.time?.type === 'start' ? info?.time.duration.type : 'infinite',
            timeEndColumn: end ? end.endField : '',
            timeEndFormat: end ? end.endFormat.format : '',
            timeEndFormatCustom: end ? (end.endFormat.format == 'custom' ? end.endFormat.customFormat : '') : '',
            timeEndFormatUnix: end ? (end.endFormat.format == 'unixTimeStamp' ? end.endFormat.timestampType : '') : '',
            columnsX: info?.columns?.x,
            columnsY: info?.columns?.y,
            columnsText: info?.columns?.text,
            columnsFloat: info?.columns?._float,
            columnsInt: info?.columns?._int,
            errorHandling: info?.onError,
            spatialReference: suggest.metaData.resultDescriptor.spatialReference,
        });
    }

    async loadLayers(): Promise<void> {
        let layers = undefined;

        const uploadId = this.uploadId();
        const volumeName = this.volumeName();
        if (volumeName) {
            layers = await this.datasetsService.getVolumeFileLayers(volumeName, this.formMetaData.controls.mainFile.value);
        } else if (uploadId) {
            layers = await this.uploadsService.getUploadFileLayers(uploadId, this.formMetaData.controls.mainFile.value);
        }

        if (!layers) {
            return;
        }

        this.uploadFileLayers = layers.layers;

        const form = this.formMetaData.controls;
        const layer = form.layerName.value;

        if (this.uploadFileLayers.length > 0 && !this.uploadFileLayers.includes(layer)) {
            form.layerName.setValue(this.uploadFileLayers[0]);
        }

        this.changeDetectorRef.markForCheck();
    }

    private async setUpMetadataSpecification(uploadId: string): Promise<void> {
        let uploadFiles = this.uploadFiles;

        if (!uploadFiles) {
            const res = await this.uploadsService.getUploadFiles(uploadId);
            uploadFiles = res.files;
        }

        const suggest = await this.datasetsService.suggestMetaData({suggestMetaData: {dataPath: {upload: uploadId}}});

        const layers = await this.uploadsService.getUploadFileLayers(uploadId, suggest.mainFile);

        this.uploadFiles = uploadFiles;
        this.uploadFileLayers = layers.layers;
        this.fillMetaDataForm(suggest);
        this.changeDetectorRef.markForCheck();
    }

    async suggest(mainFile: string | undefined = undefined, layerName: string | undefined = undefined): Promise<void> {
        let dataPath = undefined;

        const uploadId = this.uploadId();
        const volumeName = this.volumeName();
        if (uploadId) {
            dataPath = {upload: uploadId};
        } else if (volumeName) {
            dataPath = {volume: volumeName};
        } else {
            return;
        }

        // TODO: error handling
        const suggest = await this.datasetsService.suggestMetaData({
            suggestMetaData: {dataPath, mainFile, layerName},
        });

        this.fillMetaDataForm(suggest);
        this.changeDetectorRef.markForCheck();
    }

    private getStartTime(
        time: OgrSourceDatasetTimeType | undefined,
    ): undefined | {startField: string; startFormat: OgrSourceTimeFormat; custom?: string} {
        if (!time || time.type === 'none') {
            return undefined;
        }

        return time;
    }

    private getEndTime(
        time: OgrSourceDatasetTimeType | undefined,
    ): undefined | {endField: string; endFormat: OgrSourceTimeFormat; custom?: string} {
        if (!time || time.type === 'none') {
            return undefined;
        }

        if (time.type === 'start+end') {
            return time;
        }

        return undefined;
    }

    private getColumnsAsMap(): Record<string, VectorColumnInfo> {
        const formMeta = this.formMetaData.controls;
        const columns: Record<string, VectorColumnInfo> = {};

        for (const column of formMeta.columnsText.value as Array<string>) {
            columns[column] = {
                dataType: 'text',
                measurement: {
                    // TODO: incorporate in selection
                    type: 'unitless',
                },
            };
        }

        for (const column of formMeta.columnsInt.value as Array<string>) {
            columns[column] = {
                dataType: 'int',
                measurement: {
                    // TODO: incorporate in selection
                    type: 'unitless',
                },
            };
        }

        for (const column of formMeta.columnsFloat.value as Array<string>) {
            columns[column] = {
                dataType: 'float',
                measurement: {
                    // TODO: incorporate in selection
                    type: 'unitless',
                },
            };
        }
        return columns;
    }

    private getDuration(): OgrSourceDurationSpec {
        const formMeta = this.formMetaData.controls;

        if (formMeta.timeDurationValueType.value === 'zero') {
            return {
                type: 'zero',
            };
        } else if (formMeta.timeDurationValueType.value === 'infinite') {
            return {
                type: 'infinite',
            };
        } else if (formMeta.timeDurationValueType.value === 'value') {
            return {
                type: 'value',
                granularity: formMeta.timeDurationGranularity.value,
                step: formMeta.timeDurationValue.value,
            };
        }

        throw Error('Invalid time duration type');
    }

    private getTime(): OgrSourceDatasetTimeType {
        const formMeta = this.formMetaData.controls;
        let time: OgrSourceDatasetTimeType = {
            type: 'none',
        };

        if (formMeta.timeType.value === 'Start') {
            time = {
                type: 'start',
                startField: formMeta.timeStartColumn.value,
                startFormat: this.getStartTimeFormat(),
                duration: this.getDuration(),
            };
        } else if (formMeta.timeType.value === 'Start/End') {
            time = {
                type: 'start+end',
                startField: formMeta.timeStartColumn.value,
                startFormat: this.getStartTimeFormat(),
                endField: formMeta.timeEndColumn.value,
                endFormat: this.getEndTimeFormat(),
            };
        } else if (formMeta.timeType.value === 'Start/Duration') {
            time = {
                type: 'start+duration',
                startField: formMeta.timeStartColumn.value,
                startFormat: this.getStartTimeFormat(),
                durationField: formMeta.timeDurationColumn.value,
            };
        }
        return time;
    }

    private getStartTimeFormat(): OgrSourceTimeFormat {
        const formMeta = this.formMetaData.controls;

        if (formMeta.timeStartFormat.value === 'custom') {
            return {
                format: 'custom',
                customFormat: formMeta.timeStartFormatCustom.value,
            };
        } else if (formMeta.timeStartFormat.value === 'unixTimeStamp') {
            return {
                format: 'unixTimeStamp',
                timestampType: formMeta.timeStartFormatUnix.value,
            };
        }
        return {
            format: formMeta.timeStartFormat.value,
        };
    }

    private getEndTimeFormat(): OgrSourceTimeFormat {
        const formMeta = this.formMetaData.controls;

        if (formMeta.timeEndFormat.value === 'custom') {
            return {
                format: 'custom',
                customFormat: formMeta.timeEndFormatCustom.value,
            };
        } else if (formMeta.timeEndFormat.value === 'unixTimeStamp') {
            return {
                format: 'unixTimeStamp',
                timestampType: formMeta.timeEndFormatUnix.value,
            };
        }
        return {
            format: formMeta.timeEndFormat.value,
        };
    }

    private getTimeType(time: OgrSourceDatasetTimeType): string {
        if (time.type === 'none') {
            return 'None';
        }
        if (time.type === 'start') {
            return 'Start';
        } else if (time.type === 'start+duration') {
            return 'Start/Duration';
        } else if (time.type === 'start+end') {
            return 'Start/End';
        }
        return 'None';
    }

    getMetaData(): MetaDataDefinition {
        const formMeta = this.formMetaData.controls;

        return {
            type: 'OgrMetaData',
            loadingInfo: {
                fileName: formMeta.mainFile.value,
                layerName: formMeta.layerName.value,
                dataType: formMeta.dataType.value,
                time: this.getTime(),
                columns: {
                    x: formMeta.columnsX.value,
                    y: formMeta.columnsY.value,
                    text: formMeta.columnsText.value,
                    _float: formMeta.columnsFloat.value,
                    _int: formMeta.columnsInt.value,
                },
                forceOgrTimeFilter: false,
                onError: formMeta.errorHandling.value,
            },
            resultDescriptor: {
                dataType: formMeta.dataType.value,
                spatialReference: formMeta.spatialReference.value,
                columns: this.getColumnsAsMap(),
            },
        };
    }
}

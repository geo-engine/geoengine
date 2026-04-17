import {HttpEventType} from '@angular/common/http';
import {Component, ChangeDetectionStrategy, ChangeDetectorRef, OnDestroy, inject, viewChild} from '@angular/core';
import {FormControl, FormGroup, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {MatStepper, MatStep, MatStepLabel} from '@angular/material/stepper';
import {Subject, Subscription} from 'rxjs';
import {mergeMap} from 'rxjs/operators';
import {UUID} from '../../backend/backend.model';
import {ProjectService} from '../../project/project.service';
import {
    DatasetsService,
    NotificationService,
    OgrDatasetComponent,
    UploadsService,
    UserService,
    timeStepGranularityOptions,
} from '@geoengine/common';
import {DatasetService} from '../dataset.service';
import {AddDataset, DatasetDefinition, MetaDataDefinition, MetaDataSuggestion, TimeGranularity} from '@geoengine/api-client';
import {SidenavHeaderComponent} from '../../sidenav/sidenav-header/sidenav-header.component';
import {DragAndDropComponent} from '../drag-and-drop/drag-and-drop.component';
import {MatButton} from '@angular/material/button';
import {MatProgressBar} from '@angular/material/progress-bar';
import {MatFormField, MatLabel, MatInput, MatPrefix} from '@angular/material/input';
import {AsyncPipe} from '@angular/common';

interface NameDescription {
    name: FormControl<string>;
    displayName: FormControl<string>;
    description: FormControl<string>;
}

@Component({
    selector: 'geoengine-upload',
    templateUrl: './upload.component.html',
    styleUrls: ['./upload.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        SidenavHeaderComponent,
        MatStepper,
        MatStep,
        MatStepLabel,
        DragAndDropComponent,
        MatButton,
        MatProgressBar,
        OgrDatasetComponent,
        FormsModule,
        ReactiveFormsModule,
        MatFormField,
        MatLabel,
        MatInput,
        MatPrefix,
        AsyncPipe,
    ],
})
export class UploadComponent implements OnDestroy {
    protected datasetsService = inject(DatasetsService);
    protected uploadsService = inject(UploadsService);
    protected notificationService = inject(NotificationService);
    protected projectService = inject(ProjectService);
    protected userService = inject(UserService);
    protected changeDetectorRef = inject(ChangeDetectorRef);
    protected datasetService = inject(DatasetService);

    vectorDataTypes = ['Data', 'MultiPoint', 'MultiLineString', 'MultiPolygon'];
    timeDurationValueTypes = ['infinite', 'value', 'zero'];
    timeTypes = ['None', 'Start', 'Start/End', 'Start/Duration'];
    timeFormats = ['auto', 'unixTimeStamp', 'custom'];
    timestampTypes = ['epochSeconds', 'epochMilliseconds'];
    errorHandlings = ['ignore', 'abort'];
    readonly timeGranularityOptions: Array<TimeGranularity> = timeStepGranularityOptions;

    readonly stepper = viewChild.required(MatStepper);
    readonly ogrDatasetComponent = viewChild.required(OgrDatasetComponent);

    progress$ = new Subject<number>();
    metaDataSuggestion$ = new Subject<MetaDataSuggestion>();

    uploadId?: UUID;
    datasetName?: UUID;
    selectedFiles?: Array<File>;
    selectedTimeType?: string;

    uploadFiles?: Array<string>;

    formNameDescription: FormGroup<NameDescription>;

    userNamePrefix = '_';

    uploadFileLayers: Array<string> = [];

    private displayNameChangeSubscription: Subscription;

    constructor() {
        this.formNameDescription = new FormGroup<NameDescription>({
            name: new FormControl('', {
                nonNullable: true,
                validators: [Validators.required, Validators.pattern(/^[a-zA-Z0-9_]+$/), Validators.minLength(1)],
            }),
            displayName: new FormControl('', {
                nonNullable: true,
                validators: [Validators.required],
            }),
            description: new FormControl('', {
                nonNullable: true,
            }),
        });

        this.userService.getSessionOnce().subscribe((session) => {
            if (session.user) {
                this.userNamePrefix = session.user.id;
            }
        });

        /**
         * Suggest a name based on the display name
         */
        this.displayNameChangeSubscription = this.formNameDescription.controls.displayName.valueChanges.subscribe((value) => {
            const nameControl = this.formNameDescription.controls.name;

            if (nameControl.dirty) {
                return;
            }

            const src = /[^a-zA-Z0-9_]/g;
            const target = '_';

            const name = value.replace(src, target);

            nameControl.setValue(name);
        });
    }

    ngOnDestroy(): void {
        this.displayNameChangeSubscription?.unsubscribe();
    }

    upload(): void {
        if (!this.selectedFiles) {
            return;
        }

        const form = new FormData();

        for (const file of this.selectedFiles) {
            form.append('files[]', file, file.name);
        }

        this.datasetService.upload(form).subscribe(
            (event) => {
                if (event.type === HttpEventType.UploadProgress) {
                    const fraction = event.total ? event.loaded / event.total : 1;
                    this.progress$.next(Math.round(100 * fraction));
                } else if (event.type === HttpEventType.Response) {
                    const uploadId = event.body?.id;
                    this.uploadId = uploadId;
                    const stepper = this.stepper();
                    if (stepper.selected) {
                        stepper.selected.completed = true;
                        stepper.selected.editable = false;
                    }
                    stepper.next();
                }
            },
            (err) => {
                this.notificationService.error('File upload failed: ' + err.message);
            },
        );
    }

    addToMap(): void {
        if (!this.datasetName) {
            return;
        }

        this.datasetService
            .getDataset(this.datasetName)
            .pipe(mergeMap((dataset) => this.datasetService.addDatasetToMap(dataset)))
            .subscribe();
    }

    async submitCreate(): Promise<void> {
        if (!this.uploadId) {
            return;
        }

        const formDataset = this.formNameDescription.controls;

        const metaData: MetaDataDefinition = this.ogrDatasetComponent().getMetaData();

        const addData: AddDataset = {
            name: this.userNamePrefix + ':' + formDataset.name.value,
            displayName: formDataset.displayName.value,
            description: formDataset.description.value,
            sourceOperator: 'OgrSource',
        };

        const definition: DatasetDefinition = {
            properties: addData,
            metaData,
        };

        try {
            const datasetName = await this.datasetsService.createDataset(
                {
                    upload: this.uploadId,
                },
                definition,
            );

            this.datasetName = datasetName;
            const stepper = this.stepper();
            if (stepper.selected) {
                stepper.selected.completed = true;
                stepper.selected.editable = false;
            }
            const prevStep = stepper.steps.get(stepper.selectedIndex - 1);
            if (prevStep) {
                prevStep.completed = true;
                prevStep.editable = false;
            }

            stepper.next();
        } catch (err) {
            let errorMessage;
            if (err instanceof Error) {
                errorMessage = err.message;
            } else if (typeof err === 'string') {
                errorMessage = err;
            } else if (err && typeof err === 'object' && 'message' in err) {
                errorMessage = (err as {message: string}).message;
            } else {
                throw new Error('Unknown error occurred while creating dataset');
            }

            this.notificationService.error('Create dataset failed: ' + errorMessage);
            return;
        }
    }

    get formMetaData(): FormGroup {
        const ogrDatasetComponent = this.ogrDatasetComponent();
        if (!ogrDatasetComponent) {
            return new FormGroup({});
        }
        return ogrDatasetComponent.formMetaData;
    }
}

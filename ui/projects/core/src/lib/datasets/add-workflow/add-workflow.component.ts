import {Component, ChangeDetectionStrategy, inject} from '@angular/core';
import {UntypedFormGroup, UntypedFormControl, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {GeoEngineErrorDict, RasterResultDescriptorDict, UUID, VectorResultDescriptorDict} from '../../backend/backend.model';
import {ProjectService} from '../../project/project.service';
import {
    NotificationService,
    RandomColorService,
    RasterLayer,
    RasterSymbology,
    VectorLayer,
    createVectorSymbology,
    isValidUuid,
} from '@geoengine/common';
import {SidenavHeaderComponent} from '../../sidenav/sidenav-header/sidenav-header.component';
import {DialogHelpComponent} from '../../dialogs/dialog-help/dialog-help.component';
import {MatFormField, MatLabel, MatInput, MatHint} from '@angular/material/input';
import {MatButton} from '@angular/material/button';
import {AsyncPipe} from '@angular/common';

@Component({
    selector: 'geoengine-add-workflow',
    templateUrl: './add-workflow.component.html',
    styleUrls: ['./add-workflow.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        SidenavHeaderComponent,
        DialogHelpComponent,
        FormsModule,
        ReactiveFormsModule,
        MatFormField,
        MatLabel,
        MatInput,
        MatHint,
        MatButton,
        AsyncPipe,
    ],
})
export class AddWorkflowComponent {
    protected readonly projectService = inject(ProjectService);
    protected readonly notificationService = inject(NotificationService);
    protected readonly randomColorService = inject(RandomColorService);

    readonly form: UntypedFormGroup;

    constructor() {
        this.form = new UntypedFormGroup({
            layerName: new UntypedFormControl('New Layer', Validators.required),
            workflowId: new UntypedFormControl('', [Validators.required, isValidUuid]),
        });
    }

    add(): void {
        const layerName: string = this.form.controls.layerName.value;
        const workflowId: UUID = this.form.controls.workflowId.value;

        this.projectService.getWorkflowMetaData(workflowId).subscribe(
            (resultDescriptorDict) => {
                const keys = Object.keys(resultDescriptorDict);

                if (keys.includes('columns')) {
                    this.addVectorLayer(layerName, workflowId, resultDescriptorDict as VectorResultDescriptorDict);
                } else if (keys.includes('bands')) {
                    this.addRasterLayer(layerName, workflowId, resultDescriptorDict as RasterResultDescriptorDict);
                } else {
                    // TODO: implement plots, etc.
                    this.notificationService.error('Adding this workflow type is unimplemented, yet');
                }
            },
            (requestError) => this.handleError(requestError.error, workflowId),
        );
    }

    private addVectorLayer(layerName: string, workflowId: UUID, resultDescriptor: VectorResultDescriptorDict): void {
        const layer = new VectorLayer({
            name: layerName,
            workflowId,
            isVisible: true,
            isLegendVisible: false,
            symbology: createVectorSymbology(resultDescriptor.dataType, this.randomColorService.getRandomColorRgba()),
        });

        this.projectService.addLayer(layer);
    }

    private addRasterLayer(layerName: string, workflowId: UUID, _resultDescriptor: RasterResultDescriptorDict): void {
        const layer = new RasterLayer({
            name: layerName,
            workflowId,
            isVisible: true,
            isLegendVisible: false,
            symbology: RasterSymbology.fromRasterSymbologyDict({
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
            }),
        });

        this.projectService.addLayer(layer);
    }

    private handleError(error: GeoEngineErrorDict, workflowId: UUID): void {
        let errorMessage = `No workflow found for id: ${workflowId}`;

        if (error.error !== 'NoWorkflowForGivenId') {
            errorMessage = `Unknown error -> ${error.error}: ${error.message}`;
        }

        this.notificationService.error(errorMessage);
    }
}

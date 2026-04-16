import {AfterViewInit, ChangeDetectionStrategy, Component, inject} from '@angular/core';
import {FormControl, FormBuilder, FormGroup, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {ProjectService} from '../../../project/project.service';

import {map, mergeMap} from 'rxjs/operators';
import {Observable} from 'rxjs';
import {Layer} from 'ol/layer';
import {
    NotificationService,
    RasterDataType,
    RasterDataTypes,
    RasterLayer,
    RasterTypeConversionDict,
    ResultTypes,
    geoengineValidators,
} from '@geoengine/common';
import {Workflow as WorkflowDict} from '@geoengine/api-client';
import {SidenavHeaderComponent} from '../../../sidenav/sidenav-header/sidenav-header.component';
import {OperatorDialogContainerComponent} from '../helpers/operator-dialog-container/operator-dialog-container.component';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {LayerSelectionComponent} from '../helpers/layer-selection/layer-selection.component';
import {MatFormField, MatLabel, MatHint} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {OperatorOutputNameComponent} from '../helpers/operator-output-name/operator-output-name.component';
import {AsyncPipe} from '@angular/common';

@Component({
    selector: 'geoengine-raster-type-conversion',
    templateUrl: './raster-type-conversion.component.html',
    styleUrls: ['./raster-type-conversion.component.scss'],
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
        OperatorOutputNameComponent,
        MatHint,
        MatButton,
        AsyncPipe,
    ],
})
export class RasterTypeConversionComponent implements AfterViewInit {
    private readonly projectService = inject(ProjectService);
    private readonly notificationService = inject(NotificationService);
    private readonly formBuilder = inject(FormBuilder);

    readonly inputTypes = [ResultTypes.RASTER];
    readonly rasterDataTypes = RasterDataTypes.ALL_DATATYPES;

    form: FormGroup;
    disallowSubmit: Observable<boolean>;

    constructor() {
        this.form = this.formBuilder.group({
            name: ['', [Validators.required, geoengineValidators.notOnlyWhitespace]],
            layer: new FormControl<Layer | null>(null, {validators: Validators.required}),
            dataType: new FormControl(this.rasterDataTypes[0], {
                nonNullable: true,
                validators: [Validators.required],
            }),
        });
        this.disallowSubmit = this.form.statusChanges.pipe(map((status) => status !== 'VALID'));
    }

    ngAfterViewInit(): void {
        setTimeout(() => {
            this.form.updateValueAndValidity();
            this.form.controls['layer'].updateValueAndValidity();
            this.form.controls['dataType'].updateValueAndValidity();
        });
    }

    add(): void {
        const inputLayer: RasterLayer = this.form.controls['layer'].value;
        const outputName: string = this.form.controls['name'].value;

        const outputDataType: RasterDataType = this.form.controls['dataType'].value;

        this.projectService
            .getWorkflow(inputLayer.workflowId)
            .pipe(
                mergeMap((inputWorkflow: WorkflowDict) =>
                    this.projectService.registerWorkflow({
                        type: 'Raster',
                        operator: {
                            type: 'RasterTypeConversion',
                            params: {
                                outputDataType: outputDataType.getCode(),
                            },
                            sources: {
                                raster: inputWorkflow.operator,
                            },
                        } as RasterTypeConversionDict,
                    }),
                ),
                mergeMap((workflowId) =>
                    this.projectService.addLayer(
                        new RasterLayer({
                            workflowId,
                            name: outputName,
                            symbology: inputLayer.symbology.clone(),
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
}

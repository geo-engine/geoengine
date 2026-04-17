import {AfterViewInit, ChangeDetectionStrategy, Component, inject} from '@angular/core';
import {UntypedFormBuilder, UntypedFormGroup, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {ProjectService} from '../../../project/project.service';

import {map, mergeMap} from 'rxjs/operators';
import {Observable} from 'rxjs';
import {
    MeanRasterPixelValuesOverTimeDict,
    MeanRasterPixelValuesOverTimeParams,
    NotificationService,
    Plot,
    RasterLayer,
    ResultTypes,
    geoengineValidators,
} from '@geoengine/common';
import {Workflow as WorkflowDict} from '@geoengine/api-client';
import {SidenavHeaderComponent} from '../../../sidenav/sidenav-header/sidenav-header.component';
import {OperatorDialogContainerComponent} from '../helpers/operator-dialog-container/operator-dialog-container.component';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {LayerSelectionComponent} from '../helpers/layer-selection/layer-selection.component';
import {MatFormField, MatHint} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {MatCheckbox} from '@angular/material/checkbox';
import {OperatorOutputNameComponent} from '../helpers/operator-output-name/operator-output-name.component';
import {AsyncPipe} from '@angular/common';

type TimePosition = 'start' | 'center' | 'end';

interface TimePositionOption {
    value: TimePosition;
    label: string;
}

/**
 * This dialog allows creating a histogram plot of a layer's values.
 */
@Component({
    selector: 'geoengine-mean-raster-pixel-values-over-time-dialog',
    templateUrl: './mean-raster-pixel-values-over-time-dialog.component.html',
    styleUrls: ['./mean-raster-pixel-values-over-time-dialog.component.scss'],
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
        MatSelect,
        MatOption,
        MatCheckbox,
        OperatorOutputNameComponent,
        MatHint,
        MatButton,
        AsyncPipe,
    ],
})
export class MeanRasterPixelValuesOverTimeDialogComponent implements AfterViewInit {
    private readonly projectService = inject(ProjectService);
    private readonly notificationService = inject(NotificationService);
    private readonly formBuilder = inject(UntypedFormBuilder);

    readonly inputTypes = [ResultTypes.RASTER];

    readonly timePositionOptions: Array<TimePositionOption> = [
        {
            value: 'start',
            label: 'Time start',
        },
        {
            value: 'center',
            label: 'In the center between start and end',
        },
        {
            value: 'end',
            label: 'Time end',
        },
    ];

    form: UntypedFormGroup;
    disallowSubmit: Observable<boolean>;

    /**
     * DI for services
     */
    constructor() {
        this.form = this.formBuilder.group({
            name: ['', [Validators.required, geoengineValidators.notOnlyWhitespace]],
            layer: [undefined, Validators.required],
            timePosition: [this.timePositionOptions[0].value, Validators.required],
            area: [true, Validators.required],
        });
        this.disallowSubmit = this.form.statusChanges.pipe(map((status) => status !== 'VALID'));
    }

    ngAfterViewInit(): void {
        setTimeout(() => {
            this.form.updateValueAndValidity();
            this.form.controls['layer'].updateValueAndValidity();
        });
    }

    /**
     * Uses the user input to create the plot.
     * The plot is added to the plot view.
     */
    add(): void {
        const inputLayer: RasterLayer = this.form.controls['layer'].value;
        const outputName: string = this.form.controls['name'].value;

        const timePosition: TimePosition = this.form.controls['timePosition'].value;
        const area: boolean = this.form.controls['area'].value;

        const params: MeanRasterPixelValuesOverTimeParams = {
            timePosition,
            area,
        };

        this.projectService
            .getWorkflow(inputLayer.workflowId)
            .pipe(
                mergeMap((inputWorkflow: WorkflowDict) =>
                    this.projectService.registerWorkflow({
                        type: 'Plot',
                        operator: {
                            type: 'MeanRasterPixelValuesOverTime',
                            params,
                            sources: {
                                raster: inputWorkflow.operator,
                            },
                        } as MeanRasterPixelValuesOverTimeDict,
                    }),
                ),
                mergeMap((workflowId) =>
                    this.projectService.addPlot(
                        new Plot({
                            workflowId,
                            name: outputName,
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

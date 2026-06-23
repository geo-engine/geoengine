import {ChangeDetectionStrategy, Component, OnInit, inject} from '@angular/core';
import {FormBuilder, FormControl, FormGroup, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {ProjectService} from '../../../project/project.service';
import {mergeMap} from 'rxjs/operators';
import {BehaviorSubject} from 'rxjs';
import {
    Layer,
    LineSimplificationDict,
    NotificationService,
    ResultTypes,
    VectorLayer,
    geoengineValidators,
    AsyncValueDefault,
} from '@geoengine/common';
import {Workflow as WorkflowDict} from '@geoengine/api-client';
import {SidenavHeaderComponent} from '../../../sidenav/sidenav-header/sidenav-header.component';
import {OperatorDialogContainerComponent} from '../helpers/operator-dialog-container/operator-dialog-container.component';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {LayerSelectionComponent} from '../helpers/layer-selection/layer-selection.component';
import {MatFormField, MatLabel, MatInput, MatHint} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {MatCheckbox} from '@angular/material/checkbox';
import {OperatorOutputNameComponent} from '../helpers/operator-output-name/operator-output-name.component';
import {AsyncPipe} from '@angular/common';

interface LineSimplificationForm {
    name: FormControl<string>;
    layer: FormControl<Layer | undefined>;
    algorithm: FormControl<'douglasPeucker' | 'visvalingam'>;
    epsilon: FormControl<number | undefined>;
}

/**
 * This component allows creating the line simplification operator.
 */
@Component({
    selector: 'geoengine-line-simplification',
    templateUrl: './line-simplification.component.html',
    styleUrls: ['./line-simplification.component.scss'],
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
        MatCheckbox,
        MatInput,
        MatHint,
        OperatorOutputNameComponent,
        MatButton,
        AsyncPipe,
        AsyncValueDefault,
    ],
})
export class LineSimplificationComponent implements OnInit {
    private readonly projectService = inject(ProjectService);
    private readonly notificationService = inject(NotificationService);
    private readonly formBuilder = inject(FormBuilder);

    selected = new FormControl(0, {validators: [Validators.required], nonNullable: true});

    readonly inputTypes = [ResultTypes.LINES, ResultTypes.POLYGONS];

    readonly form: FormGroup<LineSimplificationForm>;

    readonly loading$ = new BehaviorSubject<boolean>(false);

    constructor() {
        this.form = new FormGroup<LineSimplificationForm>({
            name: this.formBuilder.nonNullable.control<string>('', [Validators.required, geoengineValidators.notOnlyWhitespace]),
            layer: this.formBuilder.nonNullable.control<Layer | undefined>(undefined, [Validators.required]),
            algorithm: this.formBuilder.nonNullable.control<'douglasPeucker' | 'visvalingam'>('douglasPeucker', [Validators.required]),
            epsilon: this.formBuilder.nonNullable.control<number | undefined>(1.0, [geoengineValidators.largerThan(0)]),
        });
    }

    ngOnInit(): void {
        // Necessary for having an emitted value for the layer, which is used for the name suggestion in the template
        setTimeout(() => {
            this.form.controls.layer.updateValueAndValidity();
        });
    }

    epsilonChecked(autoEpsilon: boolean): void {
        if (autoEpsilon) {
            this.form.controls.epsilon.setValue(undefined);
        } else {
            this.form.controls.epsilon.setValue(1.0);
        }
    }

    add(): void {
        if (this.loading$.value) {
            return; // don't add while loading
        }

        const vectorLayer = this.form.controls.layer.value as VectorLayer;
        const layerName: string = this.form.controls.name.value;
        const algorithm: 'douglasPeucker' | 'visvalingam' = this.form.controls.algorithm.value;
        const epsilon: number | undefined = this.form.controls.epsilon.value;

        this.loading$.next(true);

        this.projectService
            .getWorkflow(vectorLayer.workflowId)
            .pipe(
                mergeMap((sourceWorkflow) => {
                    const workflow: WorkflowDict = {
                        type: 'Vector',
                        operator: {
                            type: 'LineSimplification',
                            params: {
                                algorithm,
                                epsilon,
                            },
                            sources: {
                                vector: sourceWorkflow.operator,
                            },
                        } as LineSimplificationDict,
                    };
                    return this.projectService.registerWorkflow(workflow);
                }),
                mergeMap((workflowId) =>
                    this.projectService.addLayer(
                        new VectorLayer({
                            workflowId,
                            name: layerName,
                            symbology: vectorLayer.symbology,
                            isLegendVisible: false,
                            isVisible: true,
                        }),
                    ),
                ),
            )
            .subscribe({
                next: () => {
                    // success
                    this.loading$.next(false);
                },
                error: (error) => {
                    this.notificationService.error(error);
                    this.loading$.next(false);
                },
            });
    }
}

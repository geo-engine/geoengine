import {Component, ChangeDetectionStrategy, AfterViewInit, OnDestroy, inject} from '@angular/core';
import {UntypedFormGroup, UntypedFormBuilder, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';

import {ProjectService} from '../../../project/project.service';
import {of, ReplaySubject, Subscription} from 'rxjs';
import {map, mergeMap, tap} from 'rxjs/operators';

import {
    FeatureAttributeOverTimeDict,
    geoengineValidators,
    Layer,
    NotificationService,
    Plot,
    ResultTypes,
    VectorColumnDataTypes,
    VectorLayer,
    VectorLayerMetadata,
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
import {OperatorOutputNameComponent} from '../helpers/operator-output-name/operator-output-name.component';
import {AsyncPipe} from '@angular/common';

interface AttributeCandidates {
    id: Array<string>;
    value: Array<string>;
}

@Component({
    selector: 'geoengine-feature-attribute-over-time',
    templateUrl: './feature-attribute-over-time.component.html',
    styleUrls: ['./feature-attribute-over-time.component.scss'],
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
        OperatorOutputNameComponent,
        MatHint,
        MatButton,
        AsyncPipe,
    ],
})
export class FeatureAttributeOvertimeComponent implements AfterViewInit, OnDestroy {
    private formBuilder = inject(UntypedFormBuilder);
    private projectService = inject(ProjectService);
    private notificationService = inject(NotificationService);

    inputTypes = ResultTypes.VECTOR_TYPES;

    attributes$ = new ReplaySubject<AttributeCandidates>(1);

    readonly subscriptions: Array<Subscription> = [];

    form: UntypedFormGroup;

    constructor() {
        this.form = this.formBuilder.group({
            name: ['', [Validators.required, geoengineValidators.notOnlyWhitespace]],
            layer: [undefined, Validators.required],
            idAttribute: [undefined, Validators.required],
            valueAttribute: [undefined, Validators.required],
        });

        this.subscriptions.push(
            this.form.controls['layer'].valueChanges
                .pipe(
                    tap(() => {
                        this.form.controls['idAttribute'].setValue(undefined);
                        this.form.controls['valueAttribute'].setValue(undefined);
                    }),
                    mergeMap((layer: Layer) => {
                        if (layer instanceof VectorLayer) {
                            return this.projectService.getVectorLayerMetadata(layer).pipe(
                                map((metadata: VectorLayerMetadata) => {
                                    const candidates: AttributeCandidates = {
                                        id: [],
                                        value: [],
                                    };
                                    for (const [candidate, columnType] of metadata.dataTypes) {
                                        if (columnType === VectorColumnDataTypes.Int) {
                                            candidates.id.push(candidate);
                                            candidates.value.push(candidate);
                                        } else if (columnType === VectorColumnDataTypes.Float) {
                                            candidates.value.push(candidate);
                                        } else if (columnType === VectorColumnDataTypes.Text) {
                                            candidates.id.push(candidate);
                                        }
                                    }
                                    return candidates;
                                }),
                            );
                        } else {
                            return of({
                                id: [],
                                value: [],
                            });
                        }
                    }),
                )
                .subscribe((candidates: AttributeCandidates) => {
                    this.attributes$.next(candidates);
                }),
        );
    }

    add(): void {
        const inputLayer = this.form.controls['layer'].value as Layer;

        const idAttribute = this.form.controls['idAttribute'].value as string;
        const valueAttribute = this.form.controls['valueAttribute'].value as string;

        const outputName: string = this.form.controls['name'].value;

        this.projectService
            .getWorkflow(inputLayer.workflowId)
            .pipe(
                mergeMap((inputWorkflow: WorkflowDict) =>
                    this.projectService.registerWorkflow({
                        type: 'Plot',
                        operator: {
                            type: 'FeatureAttributeValuesOverTime',
                            params: {
                                idColumn: idAttribute,
                                valueColumn: valueAttribute,
                            },
                            sources: {
                                vector: inputWorkflow.operator,
                            },
                        } as FeatureAttributeOverTimeDict,
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

    ngOnDestroy(): void {
        this.subscriptions.forEach((subscription) => subscription.unsubscribe());
    }

    ngAfterViewInit(): void {
        setTimeout(() => {
            this.form.updateValueAndValidity();
            this.form.controls['layer'].updateValueAndValidity();
        });
    }
}

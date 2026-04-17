import {AfterViewInit, ChangeDetectionStrategy, Component, OnDestroy, inject} from '@angular/core';
import {UntypedFormBuilder, UntypedFormGroup, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {Observable, of, ReplaySubject, Subscription} from 'rxjs';
import {ProjectService} from '../../../project/project.service';

import {map, mergeMap, tap} from 'rxjs/operators';
import {
    HistogramDict,
    HistogramParams,
    Layer,
    NotificationService,
    Plot,
    RasterLayer,
    RasterLayerMetadata,
    ResultTypes,
    VectorColumnDataTypes,
    VectorLayer,
    VectorLayerMetadata,
    geoengineValidators,
    FxLayoutDirective,
    FxFlexDirective,
} from '@geoengine/common';
import {Workflow as WorkflowDict} from '@geoengine/api-client';
import {SidenavHeaderComponent} from '../../../sidenav/sidenav-header/sidenav-header.component';
import {OperatorDialogContainerComponent} from '../helpers/operator-dialog-container/operator-dialog-container.component';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {LayerSelectionComponent} from '../helpers/layer-selection/layer-selection.component';
import {MatFormField, MatInput, MatHint} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {MatCheckbox} from '@angular/material/checkbox';
import {OperatorOutputNameComponent} from '../helpers/operator-output-name/operator-output-name.component';
import {AsyncPipe} from '@angular/common';

/**
 * Checks whether the layer is a vector layer (points, lines, polygons).
 */
const isVectorLayer = (layer: Layer): boolean => {
    if (!layer) {
        return false;
    }
    return layer.layerType === 'vector';
};

/**
 * This dialog allows creating a histogram plot of a layer's values.
 */
@Component({
    selector: 'geoengine-histogram-operator',
    templateUrl: './histogram-operator.component.html',
    styleUrls: ['./histogram-operator.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        SidenavHeaderComponent,
        FormsModule,
        FxLayoutDirective,
        ReactiveFormsModule,
        OperatorDialogContainerComponent,
        MatIconButton,
        MatIcon,
        LayerSelectionComponent,
        MatFormField,
        MatSelect,
        MatOption,
        FxFlexDirective,
        MatInput,
        MatHint,
        MatCheckbox,
        OperatorOutputNameComponent,
        MatButton,
        AsyncPipe,
    ],
})
export class HistogramOperatorComponent implements AfterViewInit, OnDestroy {
    private readonly projectService = inject(ProjectService);
    private readonly notificationService = inject(NotificationService);
    private readonly formBuilder = inject(UntypedFormBuilder);

    minNumberOfBuckets = 1;
    maxNumberOfBuckets = 100;

    inputTypes = ResultTypes.INPUT_TYPES;

    form: UntypedFormGroup;

    attributes$ = new ReplaySubject<Array<string>>(1);

    isVectorLayer$: Observable<boolean>;

    private subscriptions: Array<Subscription> = [];

    /**
     * DI for services
     */
    constructor() {
        const layerControl = this.formBuilder.control(undefined, Validators.required);
        const rangeTypeControl = this.formBuilder.control('data', Validators.required);
        this.form = this.formBuilder.group({
            name: ['Filtered Values', [Validators.required, geoengineValidators.notOnlyWhitespace]],
            layer: layerControl,
            attribute: [undefined, Validators.required],
            rangeType: rangeTypeControl,
            range: this.formBuilder.group(
                {
                    min: [undefined],
                    max: [undefined],
                },
                {
                    validator: geoengineValidators.conditionalValidator(
                        geoengineValidators.minAndMax('min', 'max', {checkBothExist: true}),
                        () => rangeTypeControl.value === 'custom',
                    ),
                },
            ),
            autoBuckets: [true, Validators.required],
            numberOfBuckets: [20, [Validators.required, Validators.min(this.minNumberOfBuckets), Validators.max(this.maxNumberOfBuckets)]],
        });

        this.subscriptions.push(
            this.form.controls['layer'].valueChanges
                .pipe(
                    tap(() => this.form.controls['attribute'].setValue(undefined)),
                    mergeMap((layer: Layer) => {
                        if (layer instanceof VectorLayer) {
                            return this.projectService.getVectorLayerMetadata(layer).pipe(
                                map((metadata: VectorLayerMetadata) =>
                                    metadata.dataTypes
                                        .filter(
                                            (columnType) =>
                                                columnType === VectorColumnDataTypes.Float || columnType === VectorColumnDataTypes.Int,
                                        )
                                        .keySeq()
                                        .toArray(),
                                ),
                            );
                        } else if (layer instanceof RasterLayer) {
                            return this.projectService
                                .getRasterLayerMetadata(layer)
                                .pipe(map((metadata: RasterLayerMetadata) => metadata.bands.map((band) => band.name)));
                        } else {
                            return of([]);
                        }
                    }),
                )
                .subscribe((attributes) => this.attributes$.next(attributes)),
        );

        this.subscriptions.push(
            this.form.controls['rangeType'].valueChanges.subscribe(() => this.form.controls['range'].updateValueAndValidity()),
        );

        this.isVectorLayer$ = this.form.controls['layer'].valueChanges.pipe(map((layer) => isVectorLayer(layer)));
    }

    ngAfterViewInit(): void {
        setTimeout(() => {
            this.form.updateValueAndValidity();
            this.form.controls['layer'].updateValueAndValidity();
        });
    }

    ngOnDestroy(): void {
        this.subscriptions.forEach((subscription) => subscription.unsubscribe());
    }

    /**
     * Uses the user input to create a histogram plot.
     * The plot is added to the plot view.
     */
    add(): void {
        const inputLayer = this.form.controls['layer'].value as Layer;

        const attributeName = this.form.controls['attribute'].value as string;

        let range: {min: number; max: number} | string = this.form.controls['rangeType'].value as string;
        if (range === 'custom') {
            range = this.form.controls['range'].value as {min: number; max: number};
        }

        let buckets:
            | {
                  type: 'number';
                  value: number;
              }
            | {
                  type: 'squareRootChoiceRule';
                  maxNumberOfBuckets: number;
              };
        if (!this.form.controls['autoBuckets'].value) {
            buckets = {
                type: 'number',
                value: this.form.controls['numberOfBuckets'].value as number,
            };
        } else {
            const MAX_NUMBER_OF_AUTO_BUCKETS = 100;
            buckets = {
                type: 'squareRootChoiceRule',
                maxNumberOfBuckets: MAX_NUMBER_OF_AUTO_BUCKETS,
            };
        }

        const outputName: string = this.form.controls['name'].value;

        this.projectService
            .getWorkflow(inputLayer.workflowId)
            .pipe(
                mergeMap((inputWorkflow: WorkflowDict) =>
                    this.projectService.registerWorkflow({
                        type: 'Plot',
                        operator: {
                            type: 'Histogram',
                            params: {
                                attributeName: attributeName,
                                buckets,
                                bounds: range,
                            } as HistogramParams,
                            sources: {
                                source: inputWorkflow.operator,
                            },
                        } as HistogramDict,
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

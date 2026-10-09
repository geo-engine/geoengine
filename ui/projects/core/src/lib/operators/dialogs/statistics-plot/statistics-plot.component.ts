import {Component, ChangeDetectionStrategy, AfterViewInit, OnDestroy, inject} from '@angular/core';
import {Validators, FormBuilder, FormControl, FormArray, FormGroup, FormsModule, ReactiveFormsModule} from '@angular/forms';

import {ProjectService} from '../../../project/project.service';

import {from, Observable, of, ReplaySubject, Subscription} from 'rxjs';
import {map, mergeMap, tap} from 'rxjs/operators';
import {
    geoengineValidators,
    Layer,
    Plot,
    ResultTypes,
    VectorColumnDataTypes,
    VectorLayer,
    VectorLayerMetadata,
    FxLayoutDirective,
    FxFlexDirective,
    FxLayoutAlignDirective,
} from '@geoengine/common';
import {ProcessingGraph, RasterOperator, Statistics, StatisticsParameters, VectorOperator} from '@geoengine/api-client';
import {SidenavHeaderComponent} from '../../../sidenav/sidenav-header/sidenav-header.component';
import {OperatorDialogContainerComponent} from '../helpers/operator-dialog-container/operator-dialog-container.component';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {LayerSelectionComponent} from '../helpers/layer-selection/layer-selection.component';
import {DialogSectionHeadingComponent} from '../../../dialogs/dialog-section-heading/dialog-section-heading.component';
import {MatFormField, MatHint} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {OperatorOutputNameComponent} from '../helpers/operator-output-name/operator-output-name.component';
import {AsyncPipe} from '@angular/common';

interface StatisticsPlotForm {
    layer: FormControl<Layer | null>;
    name: FormControl<string>;
    columnNames: FormArray<FormControl<string | null>>;
}

/**
 * Checks whether the layer is a vector layer (points, lines, polygons).
 */
const isVectorLayer = (layer: Layer | null): boolean => {
    if (!layer) {
        return false;
    }
    return layer.layerType === 'vector';
};

@Component({
    selector: 'geoengine-statistics-plot',
    templateUrl: './statistics-plot.component.html',
    styleUrls: ['./statistics-plot.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        SidenavHeaderComponent,
        FormsModule,
        ReactiveFormsModule,
        OperatorDialogContainerComponent,
        MatIconButton,
        MatIcon,
        LayerSelectionComponent,
        FxLayoutDirective,
        DialogSectionHeadingComponent,
        FxFlexDirective,
        FxLayoutAlignDirective,
        MatButton,
        MatFormField,
        MatSelect,
        MatOption,
        OperatorOutputNameComponent,
        MatHint,
        AsyncPipe,
    ],
})
export class StatisticsPlotComponent implements AfterViewInit, OnDestroy {
    private formBuilder = inject(FormBuilder);
    private projectService = inject(ProjectService);

    readonly allowedLayerTypes = ResultTypes.LAYER_TYPES;

    attributes$ = new ReplaySubject<Array<string>>(1);

    isVectorLayer$: Observable<boolean>;

    form: FormGroup<StatisticsPlotForm>;

    private subscriptions: Array<Subscription> = [];

    constructor() {
        const layerControl = this.formBuilder.control<Layer | null>(null, Validators.required);
        this.form = this.formBuilder.group({
            layer: layerControl,
            name: this.formBuilder.nonNullable.control<string>('Statistics', [Validators.required, geoengineValidators.notOnlyWhitespace]),
            columnNames: this.formBuilder.array<string>(
                [],
                geoengineValidators.conditionalValidator(Validators.required, () => isVectorLayer(layerControl.value)),
            ),
        });
        this.subscriptions.push(
            this.form.controls['layer'].valueChanges
                .pipe(
                    // reset
                    tap(() => {
                        this.columnNames.clear();
                        if (isVectorLayer(layerControl.value)) {
                            this.addColumn();
                        }
                    }),
                    // get vector attributes or []
                    mergeMap((layer: Layer | null) => {
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
                        } else {
                            return of([]);
                        }
                    }),
                )
                .subscribe((attributes) => {
                    this.attributes$.next(attributes);
                }),
        );
        this.isVectorLayer$ = this.form.controls['layer']?.valueChanges.pipe(map((layer) => isVectorLayer(layer)));
    }

    get columnNames(): FormArray<FormControl<string | null>> {
        return this.form.get('columnNames') as FormArray<FormControl<string | null>>;
    }

    addColumn(): void {
        this.columnNames.push(this.formBuilder.control<string | null>(null, Validators.required));
    }

    removeColumn(i: number): void {
        this.columnNames.removeAt(i);
    }

    add(): void {
        const inputLayer = this.form.controls['layer'].value!;

        // for rasters, an empty list selects all bands
        const columnNames = isVectorLayer(inputLayer) ? this.columnNames.controls.map((fc) => (fc ? fc.value?.toString() : '')) : [];

        from(this.projectService.getWorkflow(inputLayer.workflowId))
            .pipe(
                mergeMap((inputWorkflow: ProcessingGraph) => {
                    if (inputWorkflow.type !== 'Raster' && inputWorkflow.type !== 'Vector') {
                        throw new Error(`Invalid workflow type ${inputWorkflow.type}.`);
                    }

                    const source: RasterOperator | VectorOperator = inputWorkflow.operator;

                    return this.projectService.registerWorkflow({
                        type: 'Plot',
                        operator: {
                            type: 'Statistics',
                            params: {
                                columnNames,
                            } as StatisticsParameters,
                            sources: {
                                source,
                            },
                        } as Statistics,
                    });
                }),
                mergeMap((workflowId) =>
                    this.projectService.addPlot(
                        new Plot({
                            workflowId,
                            name: this.form.controls['name'].value.toString(),
                        }),
                    ),
                ),
            )
            .subscribe();
    }

    ngOnDestroy(): void {
        this.subscriptions.forEach((subscription) => subscription.unsubscribe());
    }

    ngAfterViewInit(): void {
        setTimeout(() => {
            this.form.updateValueAndValidity({
                onlySelf: false,
                emitEvent: true,
            });
            this.form.controls['layer'].updateValueAndValidity();
        });
    }
}

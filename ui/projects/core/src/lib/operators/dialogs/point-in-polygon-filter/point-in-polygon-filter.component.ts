import {Component, ChangeDetectionStrategy, inject} from '@angular/core';
import {UntypedFormGroup, UntypedFormBuilder, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';

import {ProjectService} from '../../../project/project.service';
import {from} from 'rxjs';
import {mergeMap} from 'rxjs/operators';
import {
    ClusteredPointSymbology,
    Layer,
    NotificationService,
    PointSymbology,
    RandomColorService,
    ResultTypes,
    VectorLayer,
    colorToDict,
    geoengineValidators,
    FxLayoutDirective,
} from '@geoengine/common';
import {ProcessingGraph} from '@geoengine/api-client';
import {SidenavHeaderComponent} from '../../../sidenav/sidenav-header/sidenav-header.component';
import {OperatorDialogContainerComponent} from '../helpers/operator-dialog-container/operator-dialog-container.component';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {LayerSelectionComponent} from '../helpers/layer-selection/layer-selection.component';
import {OperatorOutputNameComponent} from '../helpers/operator-output-name/operator-output-name.component';
import {MatHint} from '@angular/material/input';

/**
 * This component allows creating the point in polygon filter operator.
 */
@Component({
    selector: 'geoengine-point-in-polygon-filter',
    templateUrl: './point-in-polygon-filter.component.html',
    styleUrls: ['./point-in-polygon-filter.component.scss'],
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
        OperatorOutputNameComponent,
        MatHint,
        MatButton,
    ],
})
export class PointInPolygonFilterOperatorComponent {
    private randomColorService = inject(RandomColorService);
    private projectService = inject(ProjectService);
    private notificationService = inject(NotificationService);
    private formBuilder = inject(UntypedFormBuilder);

    ResultTypes = ResultTypes;

    form: UntypedFormGroup;

    constructor() {
        const formBuilder = this.formBuilder;

        this.form = formBuilder.group({
            name: ['Filtered Values', [Validators.required, geoengineValidators.notOnlyWhitespace]],
            pointLayer: [undefined, Validators.required],
            polygonLayer: [undefined, Validators.required],
        });
    }

    add(): void {
        const pointsLayer = this.form.controls['pointLayer'].value as Layer;
        const polygonsLayer = this.form.controls['polygonLayer'].value as Layer;

        const name: string = this.form.controls['name'].value;

        const sourceOperators = this.projectService.getAutomaticallyProjectedOperatorsFromLayers([pointsLayer, polygonsLayer]);

        sourceOperators
            .pipe(
                mergeMap(([points, polygons]) => {
                    if (points.type !== 'Vector' || polygons.type !== 'Vector') {
                        throw new Error('Expected vector workflows for point-in-polygon filter.');
                    }

                    const workflow: ProcessingGraph = {
                        type: 'Vector',
                        operator: {
                            type: 'PointInPolygonFilter',
                            params: {},
                            sources: {
                                points: points.operator,
                                polygons: polygons.operator,
                            },
                        },
                    };

                    return from(this.projectService.registerWorkflow(workflow)).pipe(
                        mergeMap((workflowId) =>
                            this.projectService.addLayer(
                                new VectorLayer({
                                    workflowId,
                                    name,
                                    symbology: ClusteredPointSymbology.fromPointSymbologyDict({
                                        type: 'point',
                                        radius: {
                                            type: 'static',
                                            value: PointSymbology.DEFAULT_POINT_RADIUS,
                                        },
                                        stroke: {
                                            width: {
                                                type: 'static',
                                                value: 1,
                                            },
                                            color: {
                                                type: 'static',
                                                color: [0, 0, 0, 255],
                                            },
                                        },
                                        fillColor: {
                                            type: 'static',
                                            color: colorToDict(this.randomColorService.getRandomColorRgba()),
                                        },
                                    }),
                                    isLegendVisible: false,
                                    isVisible: true,
                                }),
                            ),
                        ),
                    );
                }),
            )
            .subscribe({
                error: (error) => this.notificationService.error(error),
            });
    }
}

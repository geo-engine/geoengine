import {Component, ChangeDetectionStrategy, inject, computed, effect, signal, untracked} from '@angular/core';
import {rxResource} from '@angular/core/rxjs-interop';
import {applyEach, form, FormField, FormRoot, minLength, required} from '@angular/forms/signals';
import {ProjectService} from '../../../project/project.service';
import {Observable, of} from 'rxjs';
import {map} from 'rxjs/operators';
import {
    geoengineValidators,
    Layer,
    NotificationService,
    Plot,
    ResultTypes,
    VectorColumnDataTypes,
    VectorLayer,
    VectorLayerMetadata,
    FxLayoutDirective,
    FxFlexDirective,
    FxLayoutAlignDirective,
} from '@geoengine/common';
import {Statistics, StatisticsParameters} from '@geoengine/api-client';
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

interface StatisticsPlotModel {
    layer: Layer | null;
    name: string;
    columnNames: Array<string | null>;
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
        FormRoot,
        FormField,
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
    ],
})
export class StatisticsPlotComponent {
    private readonly projectService = inject(ProjectService);
    private readonly notificationService = inject(NotificationService);

    readonly allowedLayerTypes = ResultTypes.LAYER_TYPES;

    readonly model = signal<StatisticsPlotModel>({
        layer: null,
        name: 'Statistics',
        columnNames: [],
    });

    readonly form = form(
        this.model,
        (schemaPath) => {
            required(schemaPath.layer);
            required(schemaPath.name);
            geoengineValidators.notOnlyWhitespace2(schemaPath.name);
            minLength(schemaPath.columnNames, 1, {when: (ctx) => isVectorLayer(ctx.valueOf(schemaPath.layer))});
            applyEach(schemaPath.columnNames, (column) => required(column));
        },
        {
            submission: {
                action: async (): Promise<void> => this.add(),
            },
        },
    );

    readonly layer = computed(() => this.model().layer);
    readonly isVectorLayer = computed(() => isVectorLayer(this.layer()));
    readonly nameSuggestion = computed(() => {
        const layer = this.layer();
        return layer ? `Statistics of ${layer.name}` : 'Statistics';
    });
    readonly isFormInvalid = computed(() => this.form().invalid());
    readonly hasInvalidName = computed(() => this.form.name().invalid());

    readonly attributes = rxResource({
        defaultValue: [] as Array<string>,
        params: () => ({layer: this.layer()}),
        stream: ({params}): Observable<Array<string>> => {
            if (!(params.layer instanceof VectorLayer)) {
                return of([]);
            }
            return this.projectService.getVectorLayerMetadata(params.layer).pipe(
                map((metadata: VectorLayerMetadata) =>
                    metadata.dataTypes
                        .filter((columnType) => columnType === VectorColumnDataTypes.Float || columnType === VectorColumnDataTypes.Int)
                        .keySeq()
                        .toArray(),
                ),
            );
        },
    });

    constructor() {
        // reset the column selection whenever the layer changes
        effect(() => {
            const layer = this.layer();

            untracked(() => {
                this.form.columnNames().value.set(isVectorLayer(layer) ? [null] : []);
            });
        });
    }

    addColumn(): void {
        this.form.columnNames().value.update((columnNames) => [...columnNames, null]);
    }

    removeColumn(i: number): void {
        this.form.columnNames().value.update((columnNames) => columnNames.filter((_, index) => index !== i));
    }

    async add(): Promise<void> {
        const {layer, name, columnNames} = this.model();
        if (!layer) {
            return;
        }

        // for rasters, an empty list selects all bands
        const statisticsColumnNames = isVectorLayer(layer) ? columnNames.map((columnName) => columnName ?? '') : [];

        try {
            const inputWorkflow = await this.projectService.getProcessingGraph(layer.workflowId);
            if (inputWorkflow.type !== 'Raster' && inputWorkflow.type !== 'Vector') {
                throw new Error(`Invalid processing graph type ${inputWorkflow.type}.`);
            }

            const workflowId = await this.projectService.registerProcessingGraph({
                type: 'Plot',
                operator: {
                    type: 'Statistics',
                    params: {
                        columnNames: statisticsColumnNames,
                    } satisfies StatisticsParameters,
                    sources: {
                        source: inputWorkflow.operator,
                    },
                } as Statistics,
            });

            await this.projectService.addPlot(new Plot({workflowId, name}));
        } catch (error) {
            this.notificationService.error(error instanceof Error ? error.message : String(error));
        }
    }
}

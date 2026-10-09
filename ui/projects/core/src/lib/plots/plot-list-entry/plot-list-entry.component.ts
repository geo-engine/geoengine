import {ChangeDetectionStrategy, Component, computed, effect, inject, input, signal} from '@angular/core';
import {Clipboard} from '@angular/cdk/clipboard';
import {PlotDataDict} from '../../backend/backend.model';
import {LoadingState} from '../../project/loading-state.model';
import {ProjectService} from '../../project/project.service';
import {PlotDetailViewComponent} from '../plot-detail-view/plot-detail-view.component';
import {MatDialog} from '@angular/material/dialog';
import {
    createIconDataUrl,
    GeoEngineError,
    Plot,
    CommonModule,
    FxLayoutDirective,
    FxFlexDirective,
    NotificationService,
    PlotDataFormat,
    plotDataToText,
    statisticsFromPlotData,
    statisticsTable,
    vegaDataTable,
    vegaDataValues,
    VegaChartData,
} from '@geoengine/common';
import {MatCard, MatCardHeader, MatCardAvatar, MatCardTitle, MatCardSubtitle, MatCardContent, MatCardActions} from '@angular/material/card';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {MatIconButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {MatMenu, MatMenuItem, MatMenuTrigger} from '@angular/material/menu';
import {JsonPipe} from '@angular/common';

@Component({
    selector: 'geoengine-plot-list-entry',
    templateUrl: './plot-list-entry.component.html',
    styleUrls: ['./plot-list-entry.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        MatCard,
        MatCardHeader,
        MatCardAvatar,
        MatCardTitle,
        MatCardSubtitle,
        MatCardContent,
        MatProgressSpinner,
        CommonModule,
        MatCardActions,
        FxLayoutDirective,
        MatIconButton,
        MatIcon,
        MatMenu,
        MatMenuItem,
        MatMenuTrigger,
        FxFlexDirective,
        JsonPipe,
    ],
})
export class PlotListEntryComponent {
    private readonly projectService = inject(ProjectService);
    private readonly dialog = inject(MatDialog);
    private readonly clipboard = inject(Clipboard);
    private readonly notificationService = inject(NotificationService);

    readonly plot = input.required<Plot>();

    readonly plotStatus = input<LoadingState>();

    readonly plotData = input<PlotDataDict>();

    readonly plotError = input<GeoEngineError>();

    readonly width = input<number>();

    readonly plotIcon = signal<string | undefined>(undefined);

    readonly isLoading = signal(true);
    readonly isOk = signal(false);
    readonly isError = signal(false);

    /**
     * Only Vega plots carry their data, which can be exported
     */
    readonly canExportData = computed(() => this.plotData()?.outputFormat === 'JsonVega');

    constructor() {
        effect(() => {
            const plotData = this.plotData();
            if (!plotData) return;

            this.plotIcon.set(createIconDataUrl(plotData.outputFormat));
        });

        effect(() => {
            const plotStatus = this.plotStatus();

            this.isLoading.set(plotStatus === LoadingState.LOADING);
            this.isOk.set(plotStatus === LoadingState.OK);
            this.isError.set(plotStatus === LoadingState.ERROR);
        });
    }

    /**
     * Show a plot as a fullscreen modal dialog
     */
    showFullscreen(): void {
        this.dialog.open(PlotDetailViewComponent, {
            data: this.plot(),
            maxHeight: '100vh',
            maxWidth: '100vw',
        });
    }

    /**
     * Copy the data of the plot in the given `format` with exact values, e.g., for pasting into a spreadsheet
     */
    copyData(format: PlotDataFormat): void {
        const text = this.exportData(format);
        if (text === undefined) {
            this.notificationService.error(`The plot ${this.plot().name} contains no data to copy`);
            return;
        }

        if (this.clipboard.copy(text)) {
            this.notificationService.info(`Copied the data of ${this.plot().name} as ${format.toUpperCase()} to the clipboard`);
        } else {
            this.notificationService.error('Could not copy the data to the clipboard');
        }
    }

    /**
     * The data of a Vega plot in the given `format`, or `undefined` if it has no inline data
     */
    private exportData(format: PlotDataFormat): string | undefined {
        const plotData = this.plotData();
        if (plotData?.outputFormat !== 'JsonVega') return undefined;

        const data = plotData.data as VegaChartData;
        try {
            const values = vegaDataValues(data);
            // statistics get one column per percentile instead of a JSON array
            const table = plotData.plotType === 'Statistics' ? statisticsTable(statisticsFromPlotData(data)) : vegaDataTable(data);
            return values && table ? plotDataToText(format, table, values) : undefined;
        } catch {
            return undefined;
        }
    }

    removePlot(): void {
        this.projectService.removePlot(this.plot());
    }

    reloadPlot(): void {
        this.projectService.reloadPlot(this.plot());
    }
}

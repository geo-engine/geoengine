import {ChangeDetectionStrategy, Component, effect, inject, input, signal} from '@angular/core';
import {PlotDataDict} from '../../backend/backend.model';
import {LoadingState} from '../../project/loading-state.model';
import {ProjectService} from '../../project/project.service';
import {PlotDetailViewComponent} from '../plot-detail-view/plot-detail-view.component';
import {MatDialog} from '@angular/material/dialog';
import {createIconDataUrl, GeoEngineError, Plot, CommonModule, FxLayoutDirective, FxFlexDirective} from '@geoengine/common';
import {MatCard, MatCardHeader, MatCardAvatar, MatCardTitle, MatCardSubtitle, MatCardContent, MatCardActions} from '@angular/material/card';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {MatIconButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
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
        FxFlexDirective,
        JsonPipe,
    ],
})
export class PlotListEntryComponent {
    private readonly projectService = inject(ProjectService);
    private readonly dialog = inject(MatDialog);

    readonly plot = input.required<Plot>();

    readonly plotStatus = input<LoadingState>();

    readonly plotData = input<PlotDataDict>();

    readonly plotError = input<GeoEngineError>();

    readonly width = input<number>();

    readonly plotIcon = signal<string | undefined>(undefined);

    readonly isLoading = signal(true);
    readonly isOk = signal(false);
    readonly isError = signal(false);

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

    removePlot(): void {
        this.projectService.removePlot(this.plot());
    }

    reloadPlot(): void {
        this.projectService.reloadPlot(this.plot());
    }
}

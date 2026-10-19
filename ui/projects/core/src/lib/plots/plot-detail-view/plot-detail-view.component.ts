import {CdkScrollable} from '@angular/cdk/scrolling';
import {JsonPipe} from '@angular/common';
import {ChangeDetectionStrategy, Component, ElementRef, afterNextRender, inject, signal, viewChild} from '@angular/core';
import {rxResource} from '@angular/core/rxjs-interop';
import {MAT_DIALOG_DATA, MatDialogContent} from '@angular/material/dialog';
import {MatProgressBar} from '@angular/material/progress-bar';
import {Plot, CommonModule} from '@geoengine/common';
import {DialogHeaderComponent} from '../../dialogs/dialog-header/dialog-header.component';
import {LayoutService} from '../../layout.service';
import {ProjectService} from '../../project/project.service';

@Component({
    selector: 'geoengine-plot-detail-view',
    templateUrl: './plot-detail-view.component.html',
    styleUrls: ['./plot-detail-view.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [DialogHeaderComponent, CdkScrollable, MatDialogContent, MatProgressBar, CommonModule, JsonPipe],
})
export class PlotDetailViewComponent {
    projectService = inject(ProjectService);
    plot = inject<Plot>(MAT_DIALOG_DATA);

    readonly matDialogContent = viewChild.required<MatDialogContent, ElementRef<HTMLElement>>(MatDialogContent, {read: ElementRef});

    // TODO: implement strategy for PNGs

    readonly maxWidth = signal(1);
    readonly maxHeight = signal(1);

    // initially blank pixel
    // imagePlotData$ = new BehaviorSubject('data:image/gif;base64,R0lGODlhAQABAIAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==');

    plotData = rxResource({
        params: () => ({
            plot: this.plot,
        }),
        stream: ({params}) => this.projectService.getPlotDataStream(params.plot),
    });

    constructor() {
        afterNextRender({
            read: () => {
                this.onResize();
                const topToolbarObserver = new ResizeObserver(() => this.onResize());
                topToolbarObserver.observe(this.matDialogContent().nativeElement);
            },
        });
    }

    private onResize(): void {
        this.maxWidth.set(window.innerWidth - 2 * LayoutService.remInPx);
        this.maxHeight.set(window.innerHeight - 2 * LayoutService.remInPx - LayoutService.getToolbarHeightPx());
    }
}

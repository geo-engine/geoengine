import {AfterViewInit, ChangeDetectionStrategy, Component, OnDestroy, OnInit, inject} from '@angular/core';
import {MAT_DIALOG_DATA, MatDialogContent} from '@angular/material/dialog';
import {BehaviorSubject, ReplaySubject, Subscription} from 'rxjs';
import {ProjectService} from '../../project/project.service';
import {PlotDataDict} from '../../backend/backend.model';
import {LayoutService} from '../../layout.service';
import {Plot, CommonModule} from '@geoengine/common';
import {DialogHeaderComponent} from '../../dialogs/dialog-header/dialog-header.component';
import {CdkScrollable} from '@angular/cdk/scrolling';
import {MatProgressBar} from '@angular/material/progress-bar';
import {AsyncPipe, JsonPipe} from '@angular/common';

@Component({
    selector: 'geoengine-plot-detail-view',
    templateUrl: './plot-detail-view.component.html',
    styleUrls: ['./plot-detail-view.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [DialogHeaderComponent, CdkScrollable, MatDialogContent, MatProgressBar, CommonModule, AsyncPipe, JsonPipe],
})
export class PlotDetailViewComponent implements OnInit, AfterViewInit, OnDestroy {
    projectService = inject(ProjectService);
    plot = inject<Plot>(MAT_DIALOG_DATA);

    // TODO: implement strategy for PNGs

    maxWidth$ = new ReplaySubject<number>(1);
    maxHeight$ = new ReplaySubject<number>(1);

    // initially blank pixel
    // imagePlotData$ = new BehaviorSubject('data:image/gif;base64,R0lGODlhAQABAIAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==');

    plotLoading$ = new BehaviorSubject(true);
    plotData?: PlotDataDict;

    private dataSubscription?: Subscription;

    ngOnInit(): void {
        this.dataSubscription = this.projectService.getPlotDataStream(this.plot).subscribe((plotData) => {
            this.plotLoading$.next(false);
            this.plotData = plotData;
        });

        // combineLatest([
        //     this.projectService.getPlotDataStream(this.plot),
        //     this.projectService.getTimeStream(),
        //     this.mapService.getViewportSizeStream(),
        //     this.maxWidth$,
        //     this.maxHeight$,
        // ]).pipe(
        //     first())
        //     .subscribe(([plotData, time, projection, viewport, width, height]) => {
        //         // set data uri for png type and load full screen image
        //         // if (plotData.type === 'png') {
        //             // this.imagePlotData$.next(`data:image/png;base64,${plotData.data}`);
        //
        //             this.backend
        //                 .getPlot(plot.workflow, {
        //                     operator: this.plot.operator,
        //                     time: time,
        //                     extent: viewport.extent,
        //                     projection: projection,
        //                     plotWidth: width - LayoutService.remInPx,
        //                     plotHeight: height,
        //                 }).pipe(
        //                 first())
        //                 .subscribe(newPlotData => {
        //                     this.imagePlotData$.next(`data:image/png;base64,${newPlotData.data}`);
        //
        //                     this.imagePlotLoading$.next(false);
        //                 });
        //         }
        //     });
    }

    ngAfterViewInit(): void {
        setTimeout(() => {
            this.maxWidth$.next(window.innerWidth - 2 * LayoutService.remInPx);
            this.maxHeight$.next(window.innerHeight - 2 * LayoutService.remInPx - LayoutService.getToolbarHeightPx());
        });
    }

    ngOnDestroy(): void {
        this.dataSubscription?.unsubscribe();
    }
}

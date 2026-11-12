import {AsyncPipe} from '@angular/common';
import {ChangeDetectionStrategy, Component, OnDestroy, OnInit, inject, input, viewChild} from '@angular/core';
import {MatButton} from '@angular/material/button';
import {BehaviorSubject, Subject, Subscription, combineLatest} from 'rxjs';
import {debounceTime} from 'rxjs/operators';
import {
    Layer,
    RasterLayer,
    RasterSymbology,
    RasterSymbologyEditorComponent,
    SymbologyQueryParams,
    SymbologyWorkflow,
    VectorLayer,
    VectorSymbology,
    extentToBboxDict,
    CommonModule,
    AsyncValueDefault,
} from '@geoengine/common';
import {CoreConfig} from '../../../config.service';
import {DialogHelpComponent} from '../../../dialogs/dialog-help/dialog-help.component';
import {MapService} from '../../../map/map.service';
import {ProjectService} from '../../../project/project.service';
import {SidenavHeaderComponent} from '../../../sidenav/sidenav-header/sidenav-header.component';

@Component({
    selector: 'geoengine-symbology-editor',
    templateUrl: './symbology-editor.component.html',
    styleUrl: './symbology-editor.component.scss',
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [SidenavHeaderComponent, DialogHelpComponent, CommonModule, MatButton, AsyncPipe, AsyncValueDefault],
})
export class SymbologyEditorComponent implements OnInit, OnDestroy {
    private readonly projectService = inject(ProjectService);
    private readonly mapService = inject(MapService);
    private readonly config = inject(CoreConfig);

    readonly layer = input.required<Layer>();

    readonly rasterSymbologyEditorComponent = viewChild(RasterSymbologyEditorComponent);

    rasterSymbologyWorkflow$ = new BehaviorSubject<SymbologyWorkflow<RasterSymbology> | undefined>(undefined);
    vectorSymbologyWorkflow$ = new BehaviorSubject<SymbologyWorkflow<VectorSymbology> | undefined>(undefined);

    queryParams$ = new BehaviorSubject<SymbologyQueryParams | undefined>(undefined);

    queryParamsSubscription?: Subscription = undefined;

    unappliedRasterChanges = false;

    rasterSymbology?: RasterSymbology = undefined;

    /**
     * Vector symbology changes are applied while editing. Every change stores the project and
     * reloads the layer, so they are debounced.
     */
    private readonly vectorSymbologyChanges$ = new Subject<VectorSymbology>();
    private readonly vectorSymbologySubscription = this.vectorSymbologyChanges$
        .pipe(debounceTime(this.config.DELAYS.DEBOUNCE))
        .subscribe((vectorSymbology) => this.projectService.changeLayer(this.layer(), {symbology: vectorSymbology}));

    ngOnInit(): void {
        this.setUp();
        this.createHistogramParamsSubscription();
    }

    ngOnDestroy(): void {
        if (this.queryParamsSubscription) {
            this.queryParamsSubscription.unsubscribe();
        }
        this.vectorSymbologySubscription.unsubscribe();
    }

    applyRasterChanges(): void {
        if (!this.rasterSymbology) {
            return;
        }
        const layer = this.layer();
        this.projectService.changeLayer(layer, {symbology: this.rasterSymbology});
        this.rasterSymbologyWorkflow$.next({workflowId: layer.workflowId, symbology: this.rasterSymbology});
        this.unappliedRasterChanges = false;
    }

    resetRasterChanges(): void {
        const rasterSymbologyEditorComponent = this.rasterSymbologyEditorComponent();
        if (rasterSymbologyEditorComponent) {
            rasterSymbologyEditorComponent.resetChanges();
            this.unappliedRasterChanges = false;
        }
    }

    changeRasterSymbology(rasterSymbology: RasterSymbology): void {
        this.rasterSymbology = rasterSymbology;
        this.unappliedRasterChanges = true;
    }

    changeVectorSymbology(vectorSymbology: VectorSymbology): void {
        this.vectorSymbologyChanges$.next(vectorSymbology);
    }

    private createHistogramParamsSubscription(): void {
        this.queryParamsSubscription = combineLatest([
            this.projectService.getTimeStream(),
            this.mapService.getViewportSizeStream(),
            this.projectService.getSpatialReferenceStream(),
        ]).subscribe(([time, viewport, spatialReference]) => {
            this.queryParams$.next({
                time,
                bbox: extentToBboxDict(viewport.extent),
                resolution: {x: viewport.resolution, y: viewport.resolution},
                spatialReference,
            } as SymbologyQueryParams);
        });
    }

    private setUp(): void {
        const layer = this.layer();
        if (layer instanceof RasterLayer) {
            this.rasterSymbologyWorkflow$.next({
                symbology: layer.symbology,
                workflowId: layer.workflowId,
            });
        }

        if (layer instanceof VectorLayer) {
            this.vectorSymbologyWorkflow$.next({
                symbology: layer.symbology,
                workflowId: layer.workflowId,
            });
        }
    }
}

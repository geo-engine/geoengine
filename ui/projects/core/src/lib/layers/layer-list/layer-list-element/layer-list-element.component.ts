import {Component, ChangeDetectionStrategy, ChangeDetectorRef, inject, input} from '@angular/core';
import {Clipboard} from '@angular/cdk/clipboard';
import {MatDialog} from '@angular/material/dialog';
import {TabsService} from '../../../tabs/tabs.service';
import {CoreConfig} from '../../../config.service';
import {MapService} from '../../../map/map.service';
import {ProjectService} from '../../../project/project.service';
import {LayoutService} from '../../../layout.service';
import {last, map, mergeMap, Observable, startWith, tap} from 'rxjs';
import {ProvenanceTableComponent} from '../../../provenance/table/provenance-table.component';
import {DataTableComponent} from '../../../datatable/table/table.component';
import {RenameLayerComponent} from '../../rename-layer/rename-layer.component';
import {LineageGraphComponent} from '../../../provenance/lineage-graph/lineage-graph.component';
import {LoadingState} from '../../../project/loading-state.model';
import {BackendService} from '../../../backend/backend.service';
import {HttpEventType} from '@angular/common/http';
import {filenameFromHttpHeaders} from '../../../util/http';
import {
    IconStyle,
    Layer,
    NotificationService,
    RasterLayerMetadata,
    RasterSymbology,
    Symbology,
    SymbologyType,
    UserService,
    FxLayoutDirective,
    FxLayoutAlignDirective,
    PointIconComponent,
    LineIconComponent,
    PolygonIconComponent,
    RasterIconComponent,
    FxFlexDirective,
} from '@geoengine/common';
import {RasterBandDescriptor} from '@geoengine/api-client';
import {SymbologyEditorComponent} from '../../symbology/symbology-editor/symbology-editor.component';
import {DownloadLayerComponent} from '../../../download-layer/download-layer.component';
import {MatMenu, MatMenuItem, MatMenuTrigger} from '@angular/material/menu';
import {AsyncPipe} from '@angular/common';
import {MatIcon} from '@angular/material/icon';
import {MatIconButton} from '@angular/material/button';
import {MatTooltip} from '@angular/material/tooltip';
import {CdkDragHandle} from '@angular/cdk/drag-drop';
import {VectorLegendComponent} from '../../legend/legend-vector/vector-legend.component';
import {RasterLegendComponent} from '../../legend/legend-raster/raster-legend.component';
import {MatProgressBar} from '@angular/material/progress-bar';
/**
 * The layer list component displays active layers, legends and other controlls.
 */
@Component({
    selector: 'geoengine-layer-list-element',
    templateUrl: './layer-list-element.component.html',
    styleUrls: ['./layer-list-element.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        MatMenu,
        MatMenuItem,
        MatIcon,
        FxLayoutDirective,
        FxLayoutAlignDirective,
        MatIconButton,
        MatTooltip,
        PointIconComponent,
        LineIconComponent,
        PolygonIconComponent,
        RasterIconComponent,
        CdkDragHandle,
        FxFlexDirective,
        MatMenuTrigger,
        VectorLegendComponent,
        RasterLegendComponent,
        MatProgressBar,
        AsyncPipe,
    ],
})
export class LayerListElementComponent {
    readonly dialog = inject(MatDialog);
    readonly layoutService = inject(LayoutService);
    readonly projectService = inject(ProjectService);
    readonly mapService = inject(MapService);
    readonly config = inject(CoreConfig);
    readonly changeDetectorRef = inject(ChangeDetectorRef);
    protected readonly backend = inject(BackendService);
    protected readonly userService = inject(UserService);
    protected readonly tabsService = inject(TabsService);
    protected readonly clipboard = inject(Clipboard);
    protected readonly notificationService = inject(NotificationService);

    readonly layer = input.required<Layer>();

    readonly menu = input(true);

    readonly LayoutService = LayoutService;
    readonly ST = SymbologyType;
    readonly LoadingState = LoadingState;
    readonly RenameLayerComponent = RenameLayerComponent;
    readonly LineageGraphComponent = LineageGraphComponent;

    /**
     * select a layer
     */
    toggleLegend(layer: Layer): void {
        this.projectService.toggleLegend(layer);
    }

    /**
     * method to get the symbology stream of a layer. This is used by the icons and legend components.
     */
    getLayerSymbologyStream(layer: Layer): Observable<Symbology> {
        return this.projectService.getLayerChangesStream(layer).pipe(map(() => layer.symbology));
    }

    getIconStyleStream(layer: Layer): Observable<IconStyle> {
        return this.projectService.getLayerChangesStream(layer).pipe(
            startWith(layer),
            map((l) => l.symbology.getIconStyle()),
        );
    }

    /**
     * helper method to cast AbstractSymbology to VectorSymbology
     */
    vectorLayerCast(layer: Layer): Layer {
        return layer;
    }

    showChannelParameterSlider(_layer: Layer): boolean {
        // return layer.operator.operatorType.toString() === 'GDAL Source'
        //     && !!layer.operator.operatorTypeParameterOptions
        //     && layer.operator.operatorTypeParameterOptions.getParameterOption('channelConfig').hasTicks();

        // TODO: re-implement
        return false;
    }

    showProvenance(layer: Layer): void {
        const name = this.projectService.getLayerChangesStream(layer).pipe(map((l) => 'Provenance of ' + l.name));
        const removeTrigger = this.projectService.getLayerChangesStream(layer).pipe(
            last(),
            map(() => undefined),
        );
        this.tabsService.addComponent({
            name,
            component: ProvenanceTableComponent,
            inputs: {layer},
            equals: (a, b): boolean => a.layer.id === b.layer.id,
            removeTrigger,
        });
    }

    showDatatable(layer: Layer): void {
        const name = this.projectService.getLayerChangesStream(layer).pipe(map((l) => l.name));
        const removeTrigger = this.projectService.getLayerChangesStream(layer).pipe(
            last(),
            map(() => undefined),
        );
        this.tabsService.addComponent({
            name,
            component: DataTableComponent,
            inputs: {layer},
            equals: (a, b): boolean => a.layer.id === b.layer.id,
            removeTrigger,
        });
    }

    showSymbologyEditor(layer: Layer): void {
        this.layoutService.setSidenavContentComponent({component: SymbologyEditorComponent, config: {layer}});
    }

    getBands(layer: Layer): Observable<Array<RasterBandDescriptor>> {
        return this.projectService.getLayerMetadata(layer).pipe(
            map((metaData) => metaData as RasterLayerMetadata),
            map((metaData) => metaData.bands),
        );
    }

    copyWorkflowIdToClipboard(layer: Layer): void {
        this.clipboard.copy(layer.workflowId);
        this.notificationService.info('Copied workflow id to clipboard');
    }

    downloadMetadata(layer: Layer): void {
        this.notificationService.info(`Downloading metadata for layer ${layer.name}`);

        this.userService
            .getSessionTokenForRequest()
            .pipe(
                mergeMap((token) => this.backend.downloadWorkflowMetadata(layer.workflowId, token)),
                tap((event) => {
                    if (event.type !== HttpEventType.DownloadProgress) {
                        return;
                    }

                    const fraction = event.total ? event.loaded / event.total : 1;
                    this.notificationService.info(`Metadata download: ${100 * fraction}%`);
                }),
                last(),
            )
            .subscribe({
                next: (event) => {
                    if (event.type !== HttpEventType.Response || event.body === null) {
                        return;
                    }

                    const zipArchive = new File([event.body], filenameFromHttpHeaders(event.headers) ?? 'metadata.zip');
                    const url = window.URL.createObjectURL(zipArchive);

                    // trigger download
                    const anchor = document.createElement('a');
                    anchor.href = url;
                    anchor.download = zipArchive.name;
                    anchor.click();
                },
                error: (error) => {
                    this.notificationService.error(`File download failed: ${error.message}`);
                },
            });
    }

    showDownload(layer: Layer): void {
        this.layoutService.setSidenavContentComponent({component: DownloadLayerComponent, config: {layer}});
    }

    rasterSymbology(layer: Layer): RasterSymbology {
        return layer.symbology as RasterSymbology;
    }
}

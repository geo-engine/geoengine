import {Observable, Subscription} from 'rxjs';
import {Component, OnDestroy, Input, ChangeDetectionStrategy, ChangeDetectorRef, OnChanges, inject, input} from '@angular/core';
import {CdkDragDrop, moveItemInArray, CdkDropList, CdkDrag, CdkDragPlaceholder} from '@angular/cdk/drag-drop';
import {Clipboard} from '@angular/cdk/clipboard';
import {MatDialog} from '@angular/material/dialog';
import {LayoutService, SidenavConfig} from '../../layout.service';
import {MapService} from '../../map/map.service';
import {ProjectService} from '../../project/project.service';
import {CoreConfig} from '../../config.service';
import {AddDataComponent} from '../../datasets/add-data/add-data.component';
import {TabsService} from '../../tabs/tabs.service';
import {SimpleChanges} from '@angular/core';
import {Layer, NotificationService} from '@geoengine/common';
import {MatButton} from '@angular/material/button';
import {MatTooltip} from '@angular/material/tooltip';
import {MatIcon} from '@angular/material/icon';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {LayerListElementComponent} from './layer-list-element/layer-list-element.component';
import {AsyncPipe} from '@angular/common';

/**
 * The layer list component displays active layers, legends and other controlls.
 */
@Component({
    selector: 'geoengine-layer-list',
    templateUrl: './layer-list.component.html',
    styleUrls: ['./layer-list.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        CdkDropList,
        MatButton,
        MatTooltip,
        MatIcon,
        MatProgressSpinner,
        CdkDrag,
        CdkDragPlaceholder,
        LayerListElementComponent,
        AsyncPipe,
    ],
})
export class LayerListComponent implements OnDestroy, OnChanges {
    dialog = inject(MatDialog);
    layoutService = inject(LayoutService);
    projectService = inject(ProjectService);
    mapService = inject(MapService);
    config = inject(CoreConfig);
    changeDetectorRef = inject(ChangeDetectorRef);
    protected readonly tabsService = inject(TabsService);
    protected readonly clipboard = inject(Clipboard);
    protected readonly notificationService = inject(NotificationService);

    /**
     * The desired height of the list
     */
    readonly height = input<number>();

    /**
     * The empty list shows a button to trigger the generation of a first layer.
     * This sidenav config is called to present a date listing or a similar dialog in the sidenav.
     */
    @Input() addAFirstLayerSidenavConfig?: SidenavConfig = {component: AddDataComponent};

    /**
     * sends if the layerlist should be visible
     */
    readonly layerListVisibility$: Observable<boolean>;

    /**
     * sends if the map should be a grid (or else a single map)
     */
    readonly mapIsGrid$: Observable<boolean>;

    maxHeight = 0;

    /**
     * The list of layers displayed in the layer list
     */
    layerList: Array<Layer> = [];

    // inventory of used subscriptions
    private subscriptions: Array<Subscription> = [];

    /**
     * The component constructor. It injects angular and geoengine services.
     */
    constructor() {
        this.layerListVisibility$ = this.layoutService.getLayerListVisibilityStream();

        this.subscriptions.push(
            this.projectService.getLayerStream().subscribe((layerList) => {
                if (layerList !== this.layerList) {
                    this.layerList = layerList;
                }
                this.changeDetectorRef.markForCheck();
            }),
        );

        this.mapIsGrid$ = this.mapService.isGrid$;
    }

    ngOnChanges(changes: SimpleChanges): void {
        const height = this.height();
        if (changes.height && height) {
            this.maxHeight = height - LayoutService.getToolbarHeightPx();
        }
    }

    ngOnDestroy(): void {
        this.subscriptions.forEach((s) => s.unsubscribe());
    }

    /**
     * the drop method is used by the dran and drop feature of the list
     */
    drop(event: CdkDragDrop<string[]>): void {
        const layerList = this.layerList.slice(); // make a copy to not modify the current list
        moveItemInArray(layerList, event.previousIndex, event.currentIndex);

        this.layerList = layerList; // change in advance to remove flickering
        this.projectService.setLayers(layerList);
    }

    /**
     * helper method to cast AbstractSymbology to VectorSymbology
     */
    vectorLayerCast(layer: Layer): Layer {
        return layer;
    }
}

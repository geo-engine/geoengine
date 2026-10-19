import {Component, ChangeDetectionStrategy, inject} from '@angular/core';
import {toSignal} from '@angular/core/rxjs-interop';
import {MatIconButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {MatMenuTrigger, MatMenu, MatMenuItem} from '@angular/material/menu';
import {MatTooltip} from '@angular/material/tooltip';
import {CoreConfig} from '../../../config.service';
import {LayoutService} from '../../../layout.service';
import {MapService} from '../../../map/map.service';
import {ProjectService} from '../../../project/project.service';

@Component({
    selector: 'geoengine-layer-list-menu',
    templateUrl: './layer-list-menu.component.html',
    styleUrls: ['./layer-list-menu.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [MatIconButton, MatTooltip, MatMenuTrigger, MatIcon, MatMenu, MatMenuItem],
})
export class LayerListMenuComponent {
    layoutService = inject(LayoutService);
    mapService = inject(MapService);
    projectService = inject(ProjectService);
    config = inject(CoreConfig);

    /**
     * sends if the layerlist should be visible
     */
    readonly layerListIsVisibile = toSignal(this.layoutService.getLayerListVisibilityStream(), {
        initialValue: true,
    });

    /**
     * sends if the map should be a grid (or else a single map)
     */
    readonly mapIsGrid = toSignal(this.mapService.isGrid$, {
        initialValue: false,
    });
}

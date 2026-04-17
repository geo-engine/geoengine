import {Component, ChangeDetectionStrategy, inject, input} from '@angular/core';
import {LayerListing, ProviderLayerCollectionId} from '@geoengine/api-client';
import {ProjectService} from '../project/project.service';
import {SidenavHeaderComponent} from '../sidenav/sidenav-header/sidenav-header.component';
import {CommonModule} from '@geoengine/common';

@Component({
    selector: 'geoengine-collection-layer-selection',
    templateUrl: './layer-collection-selection.component.html',
    styleUrls: ['./layer-collection-selection.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [SidenavHeaderComponent, CommonModule],
})
export class LayerCollectionSelectionComponent {
    private readonly projectService = inject(ProjectService);

    readonly collectionId = input.required<ProviderLayerCollectionId>();
    readonly collectionName = input('Layer Collection');

    selectLayer(layer: LayerListing): void {
        this.projectService.addLayerbyId(layer.id);
    }
}

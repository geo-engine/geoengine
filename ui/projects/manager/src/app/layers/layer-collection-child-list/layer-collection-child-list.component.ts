import {Component, inject, input, output, viewChild} from '@angular/core';
import {FormArray, FormControl} from '@angular/forms';
import {MatDialog} from '@angular/material/dialog';
import {CollectionNavigation, ConfirmationComponent, LayerCollectionListComponent, LayersService, CommonModule} from '@geoengine/common';
import {LayerCollectionListing, LayerListing, ProviderLayerCollectionId} from '@geoengine/api-client';
import {firstValueFrom} from 'rxjs';
import {AddLayerItemComponent} from '../add-layer-item/add-layer-item.component';
import {ItemId} from '../layers.component';
import {MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';

export interface CollectionForm {
    name: FormControl<string>;
    description: FormControl<string>;
    properties: FormArray<FormArray<FormControl<string>>>;
}

@Component({
    selector: 'geoengine-manager-layer-collection-child-list',
    templateUrl: './layer-collection-child-list.component.html',
    styleUrl: './layer-collection-child-list.component.scss',
    imports: [CommonModule, MatButton, MatIcon],
})
export class LayerCollectionChildListComponent {
    private readonly layersService = inject(LayersService);
    private readonly dialog = inject(MatDialog);

    readonly CollectionNavigation = CollectionNavigation;

    readonly collectionId = input.required<ProviderLayerCollectionId>();
    readonly modifiedChildren = output<LayerListing>();

    readonly layerCollectionListComponent = viewChild.required(LayerCollectionListComponent);

    selectedLayer?: LayerListing;
    selectedCollection?: LayerCollectionListing;

    async removeChild(): Promise<void> {
        const dialogRef = this.dialog.open(ConfirmationComponent, {
            data: {message: 'Confirm the deletion of the child from this collection.'},
        });

        const confirm = await firstValueFrom(dialogRef.afterClosed());

        if (!confirm) {
            return;
        }

        if (this.selectedLayer) {
            await this.layersService.removeLayerFromCollection(this.selectedLayer.id.layerId, this.collectionId().collectionId);
            this.selectedLayer = undefined;
        } else if (this.selectedCollection) {
            await this.layersService.removeCollectionFromCollection(
                this.selectedCollection.id.collectionId,
                this.collectionId().collectionId,
            );
            this.selectedCollection = undefined;
        }
        this.layerCollectionListComponent().refreshCollection();
    }

    selectCollection(collection: LayerCollectionListing): void {
        this.selectedCollection = collection;
        this.selectedLayer = undefined;
    }

    selectLayer(layer: LayerListing): void {
        this.selectedLayer = layer;
        this.selectedCollection = undefined;
    }

    async createChild(): Promise<void> {
        const dialogRef = this.dialog.open(AddLayerItemComponent, {
            width: '50%',
            height: 'calc(60%)',
            autoFocus: false,
            disableClose: true,
            data: {
                parent: this.collectionId(),
            },
        });

        const itemId: ItemId = await firstValueFrom(dialogRef.afterClosed());

        if (!itemId) {
            return;
        }

        this.layerCollectionListComponent().refreshCollection();
    }
}

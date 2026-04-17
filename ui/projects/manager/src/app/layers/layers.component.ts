import {Component, signal, WritableSignal, inject, viewChild} from '@angular/core';
import {
    CollectionNavigation,
    LAYER_DB_PROVIDER_ID,
    LAYER_DB_ROOT_COLLECTION_ID,
    LayerCollectionNavigationComponent,
    LayersService,
    UUID,
    CommonModule,
} from '@geoengine/common';
import {LayerCollectionListing, LayerListing, ProviderLayerCollectionId} from '@geoengine/api-client';
import {AddLayerItemComponent} from './add-layer-item/add-layer-item.component';
import {firstValueFrom} from 'rxjs';
import {MatDialog} from '@angular/material/dialog';
import {MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {MatNavList, MatListItem, MatListItemTitle, MatListItemLine} from '@angular/material/list';
import {LayerCollectionEditorComponent} from './layer-collection-editor/layer-collection-editor.component';
import {LayerEditorComponent} from './layer-editor/layer-editor.component';

export enum ItemType {
    Layer,
    Collection,
}

export type Item = {type: ItemType.Layer; layer: LayerListing} | {type: ItemType.Collection; collection: LayerCollectionListing};
export type ItemId = {type: ItemType.Layer; layer: UUID} | {type: ItemType.Collection; collection: UUID};

@Component({
    selector: 'geoengine-manager-layers',
    templateUrl: './layers.component.html',
    styleUrl: './layers.component.scss',
    imports: [
        MatButton,
        MatIcon,
        MatNavList,
        MatListItem,
        MatListItemTitle,
        MatListItemLine,
        CommonModule,
        LayerCollectionEditorComponent,
        LayerEditorComponent,
    ],
})
export class LayersComponent {
    protected readonly layersService = inject(LayersService);
    protected readonly dialog = inject(MatDialog);

    readonly CollectionNavigation = CollectionNavigation;
    readonly ItemType = ItemType;

    readonly rootCollectionId: ProviderLayerCollectionId = {providerId: LAYER_DB_PROVIDER_ID, collectionId: LAYER_DB_ROOT_COLLECTION_ID};
    readonly currentCollection = signal<ProviderLayerCollectionId>(this.rootCollectionId);

    readonly selectedItem: WritableSignal<Item | undefined> = signal(undefined);

    readonly addedItem = signal<LayerListing | LayerCollectionListing | undefined>(undefined);

    readonly layerCollectionNavigationComponent = viewChild.required(LayerCollectionNavigationComponent);

    selectLayer(layer: LayerListing): void {
        this.selectedItem.set({layer, type: ItemType.Layer});
    }

    selectCollection(collection: LayerCollectionListing): void {
        this.selectedItem.set({collection, type: ItemType.Collection});
    }

    collectionUpdated(): void {
        this.layerCollectionNavigationComponent().refresh();
    }

    itemDeleted(): void {
        if (this.addedItem()) {
            this.addedItem.set(undefined);
        } else {
            this.layerCollectionNavigationComponent().refreshCollection();
        }
        this.selectedItem.set(undefined);
    }

    layerUpdated(): void {
        this.layerCollectionNavigationComponent().refresh();
    }

    async addItem(): Promise<void> {
        const layerCollectionNavigationComponent = this.layerCollectionNavigationComponent();
        if (!layerCollectionNavigationComponent.selectedCollection) {
            return;
        }
        const dialogRef = this.dialog.open(AddLayerItemComponent, {
            width: '50%',
            height: 'calc(66%)',
            autoFocus: false,
            disableClose: true,
            data: {
                parent: layerCollectionNavigationComponent.selectedCollection.id,
            },
        });

        const itemId: ItemId = await firstValueFrom(dialogRef.afterClosed());

        if (!itemId) {
            return;
        }

        if (itemId.type === ItemType.Layer) {
            layerCollectionNavigationComponent.refreshCollection();

            const layer = await this.layersService.getLayer(LAYER_DB_PROVIDER_ID, itemId.layer);

            const listing: LayerListing = {
                description: layer.description,
                id: layer.id,
                name: layer.name,
                type: 'layer',
            };
            this.selectLayer(listing);
            this.addedItem.set(listing);
        } else {
            layerCollectionNavigationComponent.refreshCollection();

            const collection = await this.layersService.getLayerCollectionItems(LAYER_DB_PROVIDER_ID, itemId.collection);

            const listing: LayerCollectionListing = {
                description: collection.description,
                id: collection.id,
                name: collection.name,
                type: 'collection',
            };
            this.selectCollection(listing);
            this.addedItem.set(listing);
        }
    }

    backToAllItems(): void {
        this.addedItem.set(undefined);
    }

    navigateToCollection(collection: LayerCollectionListing): void {
        this.selectedItem.set(undefined);
        this.currentCollection.set(collection.id);
    }
}

import {Component, Inject, inject} from '@angular/core';
import {FormArray, FormControl} from '@angular/forms';
import {MAT_DIALOG_DATA, MatDialogRef, MatDialogTitle} from '@angular/material/dialog';
import {CollectionNavigation, LAYER_DB_PROVIDER_ID, LAYER_DB_ROOT_COLLECTION_ID, CommonModule} from '@geoengine/common';
import {LayerCollectionListing, LayerListing, ProviderLayerCollectionId} from '@geoengine/api-client';
import {ItemType} from '../layers.component';

export interface CollectionForm {
    name: FormControl<string>;
    description: FormControl<string>;
    properties: FormArray<FormArray<FormControl<string>>>;
}

@Component({
    selector: 'geoengine-manager-layer-collection-child-selection',
    templateUrl: './layer-collection-child-selection.component.html',
    styleUrl: './layer-collection-child-selection.component.scss',
    imports: [MatDialogTitle, CommonModule],
})
export class LayerCollectionChildSelectionComponent {
    private dialogRef = inject<MatDialogRef<LayerCollectionChildSelectionComponent>>(MatDialogRef);

    CollectionNavigation = CollectionNavigation;

    rootCollectionId = {providerId: LAYER_DB_PROVIDER_ID, collectionId: LAYER_DB_ROOT_COLLECTION_ID};

    @Inject(MAT_DIALOG_DATA) config!: {collection: ProviderLayerCollectionId};

    selectLayer(layer: LayerListing): void {
        this.dialogRef.close({layer, type: ItemType.Layer});
    }

    selectCollection(collection: LayerCollectionListing): void {
        this.dialogRef.close({collection, type: ItemType.Collection});
    }
}

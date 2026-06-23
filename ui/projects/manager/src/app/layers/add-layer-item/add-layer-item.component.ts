import {Component, signal, inject} from '@angular/core';
import {FormControl, FormGroup, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {
    CollectionNavigation,
    ConfirmationComponent,
    errorToText,
    geoengineValidators,
    LAYER_DB_PROVIDER_ID,
    LAYER_DB_ROOT_COLLECTION_ID,
    LayersService,
    CommonModule,
    FxLayoutDirective,
    FxFlexDirective,
} from '@geoengine/common';
import {LayerCollectionListing, LayerListing, ProviderLayerCollectionId, Workflow} from '@geoengine/api-client';
import {MatSnackBar} from '@angular/material/snack-bar';
import {MAT_DIALOG_DATA, MatDialog, MatDialogRef, MatDialogTitle} from '@angular/material/dialog';
import {filter, firstValueFrom, merge} from 'rxjs';
import {ItemId, ItemType} from '../layers.component';
import {MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {MatCard, MatCardHeader, MatCardTitle, MatCardContent} from '@angular/material/card';
import {MatButtonToggleGroup, MatButtonToggle} from '@angular/material/button-toggle';
import {MatFormField, MatLabel, MatInput, MatError} from '@angular/material/input';

export interface AddLayerItemForm {
    itemType: FormControl<ItemType>;
    name: FormControl<string>;
    description: FormControl<string>;
    workflow: FormControl<string>;
}

enum ChildType {
    Undefined,
    New,
    Existing,
}

@Component({
    selector: 'geoengine-manager-add-layer-item',
    templateUrl: './add-layer-item.component.html',
    styleUrl: './add-layer-item.component.scss',
    imports: [
        MatDialogTitle,
        MatButton,
        MatIcon,
        CommonModule,
        FormsModule,
        ReactiveFormsModule,
        FxLayoutDirective,
        FxFlexDirective,
        MatCard,
        MatCardHeader,
        MatCardTitle,
        MatCardContent,
        MatButtonToggleGroup,
        MatButtonToggle,
        MatFormField,
        MatLabel,
        MatInput,
        MatError,
    ],
})
export class AddLayerItemComponent {
    private readonly layersService = inject(LayersService);
    private readonly snackBar = inject(MatSnackBar);
    private readonly dialogRef = inject<MatDialogRef<AddLayerItemComponent>>(MatDialogRef);
    private readonly dialog = inject(MatDialog);

    ItemType = ItemType;
    ChildType = ChildType;
    CollectionNavigation = CollectionNavigation;

    parentCollectionId: ProviderLayerCollectionId;
    rootCollectionId = {providerId: LAYER_DB_PROVIDER_ID, collectionId: LAYER_DB_ROOT_COLLECTION_ID};

    readonly childType = signal(ChildType.Undefined);

    form: FormGroup<AddLayerItemForm> = new FormGroup<AddLayerItemForm>({
        itemType: new FormControl(ItemType.Collection, {
            nonNullable: true,
            validators: [Validators.required],
        }),
        name: new FormControl('', {
            nonNullable: true,
            validators: [Validators.required],
        }),
        description: new FormControl('', {
            nonNullable: true,
            validators: [],
        }),
        workflow: new FormControl(JSON.stringify({type: 'Raster', operator: {type: 'GdalSource', params: {data: 'example'}}}, null, ' '), {
            nonNullable: true,
            validators: [],
        }),
    });

    constructor() {
        const config = inject<{
            parent: ProviderLayerCollectionId;
        }>(MAT_DIALOG_DATA);

        this.parentCollectionId = config.parent;
        merge(this.dialogRef.backdropClick(), this.dialogRef.keydownEvents().pipe(filter((event) => event.key === 'Escape'))).subscribe(
            // eslint-disable-next-line @typescript-eslint/no-misused-promises
            async (event) => {
                event.stopPropagation();

                if (this.form.pristine) {
                    this.dialogRef.close();
                    return;
                }

                const confirmDialogRef = this.dialog.open(ConfirmationComponent, {
                    data: {message: 'Do you really want to stop creating the item? All changes will be lost.'},
                });

                const confirm = await firstValueFrom(confirmDialogRef.afterClosed());

                if (confirm) {
                    this.dialogRef.close();
                }
            },
        );
    }

    updateItemType(): void {
        if (this.form.controls.itemType.value === ItemType.Layer) {
            this.form.controls.workflow.setValidators([Validators.required, geoengineValidators.validJson]);
            this.form.controls.workflow.updateValueAndValidity();
        } else {
            this.form.controls.workflow.clearValidators();
            this.form.controls.workflow.updateValueAndValidity();
        }
    }

    async createItem(): Promise<void> {
        if (!this.form.valid) {
            return;
        }

        if (this.form.controls.itemType.value === ItemType.Collection) {
            try {
                const collection = await this.layersService.addCollection(this.parentCollectionId.collectionId, {
                    name: this.form.controls.name.value,
                    description: this.form.controls.description.value,
                });

                const res: ItemId = {type: ItemType.Collection, collection: collection};
                this.dialogRef.close(res);
            } catch (error) {
                const errorMessage = await errorToText(error, 'Creating collection failed.');
                this.snackBar.open(errorMessage, 'Close', {panelClass: ['error-snackbar']});
            }
        } else if (this.form.controls.itemType.value === ItemType.Layer) {
            try {
                const layer = await this.layersService.addLayer(this.parentCollectionId.collectionId, {
                    name: this.form.controls.name.value,
                    description: this.form.controls.description.value,
                    workflow: JSON.parse(this.form.controls.workflow.value ?? '{}') as Workflow,
                });

                const res: ItemId = {type: ItemType.Layer, layer: layer};
                this.dialogRef.close(res);
            } catch (error) {
                const errorMessage = await errorToText(error, 'Creating layer failed.');
                this.snackBar.open(errorMessage, 'Close', {panelClass: ['error-snackbar']});
            }
        }
    }

    async addExistingLayer(layer: LayerListing): Promise<void> {
        try {
            await this.layersService.addLayerToCollection(layer.id.layerId, this.parentCollectionId.collectionId);
            const res: ItemId = {type: ItemType.Layer, layer: layer.id.layerId};
            this.dialogRef.close(res);
        } catch (error) {
            const errorMessage = await errorToText(error, 'Adding layer failed.');
            this.snackBar.open(errorMessage, 'Close', {panelClass: ['error-snackbar']});
        }
    }

    async addExistingCollection(collection: LayerCollectionListing): Promise<void> {
        try {
            await this.layersService.addCollectionToCollection(collection.id.collectionId, this.parentCollectionId.collectionId);
            const res: ItemId = {type: ItemType.Collection, collection: collection.id.collectionId};
            this.dialogRef.close(res);
        } catch (error) {
            const errorMessage = await errorToText(error, 'Adding collection failed.');
            this.snackBar.open(errorMessage, 'Close', {panelClass: ['error-snackbar']});
        }
    }
}

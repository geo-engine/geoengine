import {Component, OnChanges, signal, SimpleChanges, WritableSignal, inject, input, output} from '@angular/core';
import {FormArray, FormControl, FormGroup, Validators, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {MatDialog} from '@angular/material/dialog';
import {MatSnackBar} from '@angular/material/snack-bar';
import {CollectionNavigation, ConfirmationComponent, errorToText, LayersService} from '@geoengine/common';
import {LayerCollection, LayerCollectionListing, LayerListing, ProviderLayerCollectionId} from '@geoengine/api-client';
import {firstValueFrom} from 'rxjs';
import {AppConfig} from '../../app-config.service';
import {MatCard, MatCardHeader, MatCardTitle, MatCardContent} from '@angular/material/card';
import {MatFormField, MatLabel, MatInput} from '@angular/material/input';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {LayerCollectionChildListComponent} from '../layer-collection-child-list/layer-collection-child-list.component';
import {PermissionsComponent} from '../../permissions/permissions.component';

export interface CollectionForm {
    name: FormControl<string>;
    description: FormControl<string>;
    properties: FormArray<FormArray<FormControl<string>>>;
}

@Component({
    selector: 'geoengine-manager-layer-collection-editor',
    templateUrl: './layer-collection-editor.component.html',
    styleUrl: './layer-collection-editor.component.scss',
    imports: [
        FormsModule,
        ReactiveFormsModule,
        MatCard,
        MatCardHeader,
        MatCardTitle,
        MatCardContent,
        MatFormField,
        MatLabel,
        MatInput,
        MatIconButton,
        MatIcon,
        MatButton,
        LayerCollectionChildListComponent,
        PermissionsComponent,
    ],
})
export class LayerCollectionEditorComponent implements OnChanges {
    private readonly layersService = inject(LayersService);
    private readonly dialog = inject(MatDialog);
    private readonly snackBar = inject(MatSnackBar);
    private readonly config = inject(AppConfig);

    readonly CollectionNavigation = CollectionNavigation;

    readonly collectionListing = input.required<LayerCollectionListing>();
    readonly parentCollection = input.required<ProviderLayerCollectionId>();

    readonly collectionSelected = output<LayerCollectionListing>();
    readonly layerSelected = output<LayerListing>();
    readonly collectionDeleted = output<void>();

    readonly collectionUpdated = output<void>();

    readonly collection: WritableSignal<LayerCollection | undefined> = signal(undefined);

    form: FormGroup<CollectionForm> = this.placeholderForm();

    selectedLayer?: LayerListing;
    selectedCollection?: LayerCollectionListing;

    // eslint-disable-next-line @typescript-eslint/no-misused-promises
    async ngOnChanges(changes: SimpleChanges): Promise<void> {
        if (changes.collectionListing) {
            const collection = await this.layersService.getLayerCollectionItems(
                this.collectionListing().id.providerId,
                this.collectionListing().id.collectionId,
                0,
                0,
            );
            this.collection.set(collection);
            this.setUpForm(collection);
        }
    }

    private setUpForm(collection: LayerCollection): void {
        this.form = new FormGroup<CollectionForm>({
            name: new FormControl(collection.name, {
                nonNullable: true,
                validators: [Validators.required, Validators.minLength(1)],
            }),
            description: new FormControl(collection.description, {
                nonNullable: true,
            }),
            properties: new FormArray<FormArray<FormControl<string>>>(
                collection.properties?.map((c) => {
                    return new FormArray<FormControl<string>>([
                        new FormControl(c[0], {
                            nonNullable: true,
                            validators: [Validators.required],
                        }),
                        new FormControl(c[1], {
                            nonNullable: true,
                            validators: [Validators.required],
                        }),
                    ]);
                }) ?? [],
            ),
        });
    }

    private placeholderForm(): FormGroup<CollectionForm> {
        return new FormGroup<CollectionForm>({
            name: new FormControl('name', {
                nonNullable: true,
                validators: [Validators.required, Validators.minLength(1)],
            }),
            description: new FormControl('description', {
                nonNullable: true,
            }),
            properties: new FormArray<FormArray<FormControl<string>>>([]),
        });
    }

    addProperty(): void {
        this.form.controls.properties.push(
            new FormArray<FormControl<string>>([
                new FormControl('newKey', {
                    nonNullable: true,
                    validators: [Validators.required],
                }),
                new FormControl('newValue', {
                    nonNullable: true,
                    validators: [Validators.required],
                }),
            ]),
        );
        this.form.markAsDirty();
    }

    removeProperty(i: number): void {
        this.form.controls.properties.removeAt(i);
        this.form.markAsDirty();
    }

    async applyChanges(): Promise<void> {
        if (this.form.invalid) {
            return;
        }

        const name = this.form.controls.name.value;
        const description = this.form.controls.description.value;
        const properties = this.form.controls.properties.value;

        try {
            const collectionListing = this.collectionListing();
            this.layersService.updateLayerCollection(collectionListing.id.collectionId, {
                name,
                description,
                properties,
            });

            this.snackBar.open('Collection successfully updated.', 'Close', {duration: this.config.DEFAULTS.SNACKBAR_DURATION});
            collectionListing.name = name;
            collectionListing.description = description;
            this.form.markAsPristine();

            this.collectionUpdated.emit(undefined);
        } catch (error) {
            const errorMessage = await errorToText(error, 'Updating collection failed.');
            this.snackBar.open(errorMessage, 'Close', {panelClass: ['error-snackbar']});
        }
    }

    async deleteCollection(): Promise<void> {
        const dialogRef = this.dialog.open(ConfirmationComponent, {
            data: {message: 'Confirm the deletion of the collection. It will be removed from ALL collections. This cannot be undone.'},
        });

        const confirm = await firstValueFrom(dialogRef.afterClosed());

        if (!confirm) {
            return;
        }

        try {
            await this.layersService.removeLayerCollection(this.collectionListing().id.collectionId);
            this.snackBar.open('Collection successfully deleted.', 'Close', {duration: this.config.DEFAULTS.SNACKBAR_DURATION});
            this.collectionDeleted.emit(undefined);
        } catch (error) {
            const errorMessage = await errorToText(error, 'Deleting collection failed.');
            this.snackBar.open(errorMessage, 'Close', {panelClass: ['error-snackbar']});
        }
    }

    async deleteCollectionFromParent(): Promise<void> {
        const dialogRef = this.dialog.open(ConfirmationComponent, {
            data: {message: 'Confirm the removal of the collection from the parent collection.'},
        });

        const confirm = await firstValueFrom(dialogRef.afterClosed());

        if (!confirm) {
            return;
        }

        try {
            await this.layersService.removeCollectionFromCollection(
                this.collectionListing().id.collectionId,
                this.parentCollection().collectionId,
            );
            this.snackBar.open('Collection successfully deleted.', 'Close', {duration: this.config.DEFAULTS.SNACKBAR_DURATION});
            this.collectionDeleted.emit(undefined);
        } catch (error) {
            const errorMessage = await errorToText(error, 'Removing collection failed.');
            this.snackBar.open(errorMessage, 'Close', {panelClass: ['error-snackbar']});
        }
    }
}

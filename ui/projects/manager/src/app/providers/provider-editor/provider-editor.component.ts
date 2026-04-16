import {Component, inject, output, input, ChangeDetectionStrategy, Signal, computed, resource, effect, signal} from '@angular/core';
import {
    LayerProviderListing,
    Permission,
    TypedDataProviderDefinition,
    TypedDataProviderDefinitionToJSON,
    WildliveDataConnectorDefinitionTypeEnum,
} from '@geoengine/api-client';
import {ConfirmationComponent, errorToText, LayersService, PermissionsService} from '@geoengine/common';
import {firstValueFrom} from 'rxjs';
import {MatDialog} from '@angular/material/dialog';
import {MatSnackBar} from '@angular/material/snack-bar';
import {AppConfig} from '../../app-config.service';
import {ProviderInputComponent} from '../provider-input/provider-input.component';

import {MatCard, MatCardContent, MatCardHeader, MatCardSubtitle, MatCardTitle} from '@angular/material/card';
import {MatHint} from '@angular/material/form-field';
import {PermissionsComponent} from '../../permissions/permissions.component';
import {MatButton} from '@angular/material/button';

export enum ProviderType {
    ARUNA = 'Aruna',
    WildLIVE = 'WildLIVE',
    OTHER = 'Other / JSON',
}

@Component({
    selector: 'geoengine-manager-provider-editor',
    templateUrl: './provider-editor.component.html',
    styleUrl: './provider-editor.component.scss',
    imports: [
        ProviderInputComponent,
        MatCard,
        MatCardHeader,
        MatCardSubtitle,
        MatCardTitle,
        MatCardContent,
        MatHint,
        PermissionsComponent,
        MatButton,
    ],
    changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ProviderEditorComponent {
    readonly providerListing = input.required<LayerProviderListing>();

    readonly providerUpdated = output<void>();

    readonly providerDeleted = output<void>();

    readonly provider = resource({
        params: () => ({providerId: this.providerListing().id}),
        loader: ({params}): Promise<TypedDataProviderDefinition> => this.layersService.getProviderDefinition(params.providerId),
    });

    readonly updatedDefinition = signal<TypedDataProviderDefinition | undefined>(undefined, {
        equal: (a, b) => JSON.stringify(TypedDataProviderDefinitionToJSON(a)) === JSON.stringify(TypedDataProviderDefinitionToJSON(b)),
    });

    readonly providerType: Signal<ProviderType> = computed(() => {
        const type = this.provider.value()?.type;
        switch (type) {
            case 'Aruna':
                return ProviderType.ARUNA;
            case WildliveDataConnectorDefinitionTypeEnum.WildLive:
                return ProviderType.WildLIVE;
            default:
                return ProviderType.OTHER;
        }
    });

    readonly isReadonly = resource({
        defaultValue: false,
        params: () => ({providerId: this.providerListing().id}),
        loader: async ({params}): Promise<boolean> => {
            const permissions = await this.permissionsService.getPermissions('provider', params.providerId, 0, 1);
            return permissions.length < 1 || permissions[0].permission != Permission.Owner;
        },
    });

    private readonly layersService = inject(LayersService);
    private readonly permissionsService = inject(PermissionsService);
    private readonly dialog = inject(MatDialog);
    private readonly snackBar = inject(MatSnackBar);
    private readonly config = inject(AppConfig);

    constructor() {
        effect(() => {
            // reset updated definition when provider changes
            this.provider.value();
            this.updatedDefinition.set(undefined);
        });
    }

    async submitUpdate(): Promise<void> {
        const provider = this.updatedDefinition();

        if (!provider) {
            return;
        }

        try {
            await this.layersService.updateProviderDefinition(this.providerListing().id, provider);

            this.providerUpdated.emit();
            this.provider.set(provider);
            this.updatedDefinition.set(undefined);
        } catch (e) {
            const error = await errorToText(e, 'Unknown error while updating provider');
            this.snackBar.open(error, 'Close');
        }
    }

    async delete(): Promise<void> {
        const dialogRef = this.dialog.open(ConfirmationComponent, {
            data: {message: 'Confirm the deletion of the provider. This cannot be undone.'},
        });

        const confirm = (await firstValueFrom(dialogRef.afterClosed())) as boolean;

        if (!confirm) {
            return;
        }

        try {
            await this.layersService.deleteProvider(this.providerListing().id);
            this.snackBar.open('Provider successfully deleted.', 'Close', {duration: this.config.DEFAULTS.SNACKBAR_DURATION});
            this.providerDeleted.emit(undefined);
        } catch (error) {
            const errorMessage = await errorToText(error, 'Deleting provider failed.');
            this.snackBar.open(errorMessage, 'Close', {panelClass: ['error-snackbar']});
        }
    }
}

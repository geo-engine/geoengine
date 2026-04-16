import {Component, viewChild} from '@angular/core';
import {ProviderListComponent} from './provider-list/provider-list.component';
import {LayerProviderListing} from '@geoengine/api-client';
import {ProviderEditorComponent} from './provider-editor/provider-editor.component';

@Component({
    selector: 'geoengine-manager-providers',
    templateUrl: './providers.component.html',
    styleUrl: './providers.component.scss',
    imports: [ProviderEditorComponent, ProviderListComponent],
})
export class ProvidersComponent {
    readonly providerList = viewChild.required(ProviderListComponent);

    selectedProvider$: LayerProviderListing | undefined = undefined;

    selectProvider(provider?: LayerProviderListing): void {
        this.selectedProvider$ = provider;
    }

    providerUpdated(): void {
        this.providerList().reload();
    }

    providerDeleted(): void {
        this.providerList().reload();
        this.selectedProvider$ = undefined;
    }
}

import {Component, inject} from '@angular/core';
import {LayersService} from '@geoengine/common';
import {ProviderType} from '../provider-editor/provider-editor.component';
import {TypedDataProviderDefinition} from '@geoengine/api-client';
import {MatDialogRef, MatDialogTitle} from '@angular/material/dialog';
import {MatSnackBar} from '@angular/material/snack-bar';
import {errorToText} from 'projects/common/src/lib/util/conversions';
import {FormsModule} from '@angular/forms';
import {ProviderInputComponent} from '../provider-input/provider-input.component';
import {MatCard, MatCardContent} from '@angular/material/card';
import {MatLabel} from '@angular/material/input';
import {MatOption, MatSelect} from '@angular/material/select';

import {MatButton} from '@angular/material/button';

@Component({
    selector: 'geoengine-manager-add-provider',
    templateUrl: './add-provider.component.html',
    styleUrl: './add-provider.component.scss',
    imports: [FormsModule, ProviderInputComponent, MatDialogTitle, MatCard, MatCardContent, MatLabel, MatSelect, MatOption, MatButton],
})
export class AddProviderComponent {
    providerType: ProviderType = ProviderType.OTHER;
    provider: TypedDataProviderDefinition | undefined;

    protected readonly ProviderType = ProviderType;
    protected readonly Object = Object;

    private readonly layersService = inject(LayersService);
    private readonly dialogRef = inject(MatDialogRef<AddProviderComponent>);
    private readonly matSnackBar = inject(MatSnackBar);

    async createProvider(): Promise<void> {
        try {
            const id = await this.layersService.addProvider(this.provider!);
            this.dialogRef.close(id);
        } catch (e) {
            const error = await errorToText(e, 'Unknown error while creating provider');
            this.matSnackBar.open(error, 'Close');
        }
    }

    setChangedDefinition(definition?: TypedDataProviderDefinition): void {
        this.provider = definition;
    }
}

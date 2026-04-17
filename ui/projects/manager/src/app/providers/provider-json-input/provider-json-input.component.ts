import {Component, effect, input, output, signal} from '@angular/core';
import {TypedDataProviderDefinition, TypedDataProviderDefinitionFromJSON} from '@geoengine/api-client';
import {MatError} from '@angular/material/form-field';
import {CodeEditorComponent} from '@geoengine/common';
import {FormControl, ReactiveFormsModule} from '@angular/forms';

@Component({
    selector: 'geoengine-manager-provider-json-input',
    templateUrl: './provider-json-input.component.html',
    styleUrl: './provider-json-input.component.scss',
    imports: [MatError, CodeEditorComponent, ReactiveFormsModule],
})
export class ProviderJsonInputComponent {
    readonly changed = output<TypedDataProviderDefinition>();
    readonly provider = input<TypedDataProviderDefinition>();
    readonly visible = input<boolean>(false);
    readonly readonly = input<boolean>(false);
    readonly inputInvalid = signal<boolean>(false);
    json = new FormControl<string | null>('');

    constructor() {
        effect(() => {
            if (this.provider()) {
                this.json.setValue(JSON.stringify(this.provider(), undefined, 4), {emitEvent: false});
                this.inputInvalid.set(false);
            } else {
                this.inputInvalid.set(true);
            }
        });

        this.json.valueChanges.subscribe((value) => {
            if (value && this.visible()) {
                try {
                    this.setChangedDefinition(TypedDataProviderDefinitionFromJSON(JSON.parse(value)));
                    this.inputInvalid.set(false);
                } catch (_) {
                    this.inputInvalid.set(true);
                }
            }
        });
    }

    setChangedDefinition(provider: TypedDataProviderDefinition): void {
        this.changed.emit(provider);
    }
}

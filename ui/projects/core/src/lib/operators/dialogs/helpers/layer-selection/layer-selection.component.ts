import {
    ChangeDetectionStrategy,
    Component,
    computed,
    effect,
    forwardRef,
    inject,
    input,
    linkedSignal,
    resource,
    signal,
} from '@angular/core';
import {ControlValueAccessor, FormsModule, NG_VALUE_ACCESSOR} from '@angular/forms';
import {firstValueFrom} from 'rxjs';
import {ProjectService} from '../../../../project/project.service';
import {Layer, LayerCollectionLayerDetailsComponent, LayerMetadata, ResultType, ResultTypes} from '@geoengine/common';
import {MatFormFieldModule} from '@angular/material/form-field';
import {MatButtonModule} from '@angular/material/button';
import {MatIconModule} from '@angular/material/icon';

import {MatSelectModule} from '@angular/material/select';
import {toSignal} from '@angular/core/rxjs-interop';
import {MatInputModule} from '@angular/material/input';

/**
 * This component allows selecting one layer.
 */
@Component({
    selector: 'geoengine-layer-selection',
    templateUrl: './layer-selection.component.html',
    styleUrls: ['./layer-selection.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    providers: [{provide: NG_VALUE_ACCESSOR, useExisting: forwardRef(() => LayerSelectionComponent), multi: true}],
    imports: [
        MatFormFieldModule,
        MatButtonModule,
        MatIconModule,
        LayerCollectionLayerDetailsComponent,
        MatSelectModule,
        FormsModule,
        MatInputModule,
    ],
})
export class LayerSelectionComponent implements ControlValueAccessor {
    private readonly projectService = inject(ProjectService);

    /**
     * An array of possible layers.
     */
    readonly _layers = input<Array<Layer>>(undefined, {
        // eslint-disable-next-line @angular-eslint/no-input-rename
        alias: 'layers',
    });
    readonly defaultLayers = toSignal(this.projectService.getLayerStream(), {initialValue: []});
    layers = resource({
        params: () => ({inputLayers: this._layers(), defaultLayers: this.defaultLayers(), types: this.types()}),
        defaultValue: [],
        loader: async ({params}): Promise<Array<Layer>> => {
            const layers: Array<Layer> = params.inputLayers ?? params.defaultLayers;

            const filteredLayers = [];

            for (const layer of layers) {
                const metadata = await firstValueFrom(this.projectService.getLayerMetadata(layer));
                if (params.types.includes(metadata.resultType)) {
                    filteredLayers.push(layer);
                }
            }

            return filteredLayers;
        },
    });

    /**
     * The type is used as a filter for the layers to choose from.
     */
    readonly types = input<Array<ResultType>>(ResultTypes.ALL_TYPES);

    /**
     * The title of the component (optional).
     */
    readonly _title = input<string>(undefined, {
        // eslint-disable-next-line @angular-eslint/no-input-rename
        alias: 'title',
    });
    readonly title = computed(
        () =>
            this._title() ??
            this.types()
                .map((type) => type.toString())
                .map((name) => (name.endsWith('s') ? name.substr(0, name.length - 1) : name))
                .join(', '),
    );

    onTouched?: () => void;
    onChange?: (_: Layer | undefined) => void = undefined;

    readonly hasLayers = computed<boolean>(() => this.layers.value().length > 0);
    readonly selectedLayer = linkedSignal<Array<Layer>, Layer | undefined>({
        source: () => this.layers.value(),
        computation: (newlayers, previous) => {
            const previousValue = previous?.value;
            const layers = newlayers.length ? newlayers : (previous?.source ?? []); // if newlayers is empty (aka loading), use the previous value

            if (previousValue && layers.includes(previousValue)) {
                return previousValue;
            }

            if (layers.length > 0) {
                return layers[0];
            }

            return undefined;
        },
    });
    readonly expanded = signal<boolean>(false);
    metadata = resource({
        params: () => ({selectedLayer: this.selectedLayer()}),
        defaultValue: undefined,
        loader: async ({params}): Promise<LayerMetadata | undefined> => {
            const selectedLayer = params.selectedLayer;
            if (!selectedLayer) {
                return undefined;
            }
            return await firstValueFrom(this.projectService.getLayerMetadata(selectedLayer));
        },
    });

    constructor() {
        effect(() => {
            const selectedLayer = this.selectedLayer();
            if (this.onChange) {
                this.onChange(selectedLayer);
            }
        });
    }

    onBlur(): void {
        if (this.onTouched) {
            this.onTouched();
        }
    }

    writeValue(layer: Layer): void {
        if (layer !== null) {
            this.selectedLayer.set(layer);
        }
    }

    registerOnChange(fn: (_: Layer | undefined) => void): void {
        this.onChange = fn;

        this.onChange(this.selectedLayer());
    }

    registerOnTouched(fn: () => void): void {
        this.onTouched = fn;
    }

    setSelectedLayer(layer: Layer): void {
        this.selectedLayer.set(layer);
        this.expanded.set(false);
    }

    toggleExpand(): void {
        this.expanded.set(!this.expanded());
    }
}

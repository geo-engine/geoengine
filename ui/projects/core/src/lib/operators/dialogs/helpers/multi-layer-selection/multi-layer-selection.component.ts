import {of, zip, forkJoin, firstValueFrom} from 'rxjs';
import {map, mergeMap} from 'rxjs/operators';
import {Component, ChangeDetectionStrategy, inject, input, computed, model, signal, output, forwardRef, effect} from '@angular/core';
import {ProjectService} from '../../../../project/project.service';
import {
    Layer,
    LayerMetadata,
    ResultType,
    ResultTypes,
    FxLayoutDirective,
    FxFlexDirective,
    FxLayoutAlignDirective,
    LayerCollectionLayerDetailsComponent,
} from '@geoengine/common';
import {MatIconButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {MatFormField, MatLabel} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {rxResource, toSignal} from '@angular/core/rxjs-interop';
import {FormValueControl} from '@angular/forms/signals';
import {ControlValueAccessor, NG_VALUE_ACCESSOR} from '@angular/forms';

/**
 * Singleton for a letter to number converter for ids.
 */
// eslint-disable-next-line @typescript-eslint/naming-convention
export const LetterNumberConverter = {
    /**
     * Convert a numeric id to a alphanumeric one.
     * Starting with `1`.
     */
    toLetters: (num: number): string => {
        const mod = num % 26;
        let pow = (num / 26) | 0; // eslint-disable-line no-bitwise
        // noinspection CommaExpressionJS
        const out = mod ? String.fromCharCode(64 + mod) : (--pow, 'Z');
        return pow ? LetterNumberConverter.toLetters(pow) + out : out;
    },

    /**
     * Convert an alphanumeric id to a numeric one.
     * Starting with `A`.
     */
    fromLetters: (str: string): number => {
        let out = 0;
        const len = str.length;
        let pos = len;
        while (--pos > -1) {
            out += (str.charCodeAt(pos) - 64) * Math.pow(26, len - 1 - pos);
        }
        return out;
    },
};

export interface LayerDetails {
    expanded: boolean;
    description?: string;
    metadata?: LayerMetadata;
}

@Component({
    selector: 'geoengine-multi-layer-selection',
    templateUrl: './multi-layer-selection.component.html',
    styleUrls: ['./multi-layer-selection.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        FxLayoutDirective,
        FxFlexDirective,
        FxLayoutAlignDirective,
        MatIconButton,
        MatIcon,
        MatFormField,
        MatLabel,
        MatSelect,
        MatOption,
        LayerCollectionLayerDetailsComponent,
    ],
    providers: [{provide: NG_VALUE_ACCESSOR, useExisting: forwardRef(() => MultiLayerSelectionComponent), multi: true}],
})
export class MultiLayerSelectionComponent implements FormValueControl<Array<Layer>>, ControlValueAccessor {
    private projectService = inject(ProjectService);

    /**
     * An array of possible layers.
     */
    readonly layers = input<Array<Layer>>();

    /**
     * The minimum number of elements to select.
     */
    readonly min = input<number | undefined>();

    /**
     * The maximum number of elements to select.
     */
    readonly max = input<number | undefined>();

    /**
     * The type is used as a filter for the layers to choose from.
     */
    readonly types = input<Array<ResultType>>(ResultTypes.ALL_TYPES);

    /**
     * A function for naming the individual raster selections
     */
    readonly inputNaming = input<(index: number) => string>((idx) => 'Input ' + this.toLetters(idx));

    readonly touched = output<boolean>();

    /**
     * The title of the component (optional).
     */
    readonly _title = input<string>(undefined, {
        // eslint-disable-next-line @angular-eslint/no-input-rename
        alias: 'title',
    });

    readonly title = computed<string>(() => {
        return (
            this._title() ??
            this.types()
                .map((type) => type.toString())
                .join(', ')
        );
    });

    readonly value = model<Array<Layer>>([]);

    readonly allLayers = toSignal(this.projectService.getLayerStream(), {initialValue: []});

    readonly filteredLayers = rxResource({
        defaultValue: [] as Array<Layer>,
        params: () => ({
            layers: this.layers(),
            types: this.types(),
            allLayers: this.allLayers(),
        }),
        stream: ({params}) => {
            return of(params.layers ?? params.allLayers).pipe(
                mergeMap((layers) => {
                    const layersAndMetadata = layers.map((l) => zip(of(l), this.projectService.getLayerMetadata(l)));
                    return forkJoin(layersAndMetadata);
                }),
                map((layers: Array<[Layer, LayerMetadata]>) =>
                    layers.filter(([_layer, meta]) => params.types.includes(meta.resultType)).map(([layer, _]) => layer),
                ),
            );
        },
    });

    readonly hasLayers = computed(() => this.filteredLayers.value().length > 0);
    readonly layersAtMin = computed(() => !this.hasLayers() || this.value().length <= (this.min() ?? 1));
    readonly layersAtMax = computed(() => !this.hasLayers() || this.value().length >= (this.max() ?? 1));
    readonly minNotEqualMax = computed(() => {
        const min = this.min();
        const max = this.max();
        return min !== undefined && max !== undefined && min !== max;
    });

    readonly layerDetails = signal<LayerDetails[]>([]);

    constructor() {
        // check min and max wrt. value
        effect(() => {
            const selectedLayers = this.value();
            const min = this.min();
            const max = this.max();

            if (max !== undefined && selectedLayers.length > max) {
                this.value.set(selectedLayers.slice(0, max - 1));
                this.layerDetails.set(this.layerDetails().slice(0, max - 1));
            } else if (min !== undefined && selectedLayers.length < min) {
                // add selected layers
                const difference = min - selectedLayers.length;
                const newLayers = this.layersForInitialSelection(this.filteredLayers.value(), selectedLayers, difference);
                if (newLayers.length === 0) return; // cannot add layers if there are no layers available

                this.value.set(selectedLayers.concat(newLayers));
                this.layerDetails.set(
                    this.layerDetails().concat(
                        Array(difference)
                            .fill(null)
                            .map(() => ({expanded: false})),
                    ),
                );
            }
        });

        effect(() => {
            this.onChange(this.value());
        });
        this.touched.subscribe(() => {
            this.onTouched();
        });
    }

    updateLayer(index: number, layer: Layer): void {
        const newSelectedLayers = [...this.value()];
        const newLayerDetails = [...this.layerDetails()];
        newSelectedLayers[index] = layer;
        newLayerDetails[index] = {expanded: false};
        this.value.set(newSelectedLayers);
        this.layerDetails.set(newLayerDetails);
    }

    add(): void {
        const selectedLayers = this.value();
        const max = this.max();
        if (max !== undefined && selectedLayers.length >= max) {
            return;
        }

        this.layerDetails.set([...this.layerDetails(), {expanded: false}]);
        const newLayers = this.layersForInitialSelection(this.filteredLayers.value(), selectedLayers, 1);
        this.value.set(selectedLayers.concat(newLayers));
    }

    remove(): void {
        const selectedLayers = this.value();
        const min = this.min();
        if (min !== undefined && selectedLayers.length <= min) {
            return;
        }

        this.layerDetails.set(this.layerDetails().slice(0, selectedLayers.length - 1));
        this.value.set(selectedLayers.slice(0, selectedLayers.length - 1));
    }

    // noinspection JSMethodCanBeStatic
    toLetters(i: number): string {
        return LetterNumberConverter.toLetters(i + 1);
    }

    async toggleExpand(i: number): Promise<void> {
        const newLayerDetails = [...this.layerDetails()];

        const layer = this.value()[i];
        const details = newLayerDetails[i];

        if (!layer) return;

        if (!details.metadata) {
            const resultDescriptor = await firstValueFrom(this.projectService.getLayerMetadata(layer));
            details.metadata = resultDescriptor;
        }

        details.expanded = !details.expanded;

        this.layerDetails.set(newLayerDetails);
    }

    private layersForInitialSelection(layers: Array<Layer>, blacklist: Array<Layer>, amount: number): Array<Layer> {
        if (layers.length === 0) {
            return [];
        }

        const layersForSelection = [...layers].filter((layer) => !blacklist.includes(layer));

        while (layersForSelection.length < amount) {
            layersForSelection.push(layers[0]);
        }

        return layersForSelection.slice(0, amount);
    }

    // TODO: remove
    writeValue(obj: Layer[]): void {
        if (obj === this.value()) {
            return;
        }
        this.value.set(obj);
    }

    // TODO: remove
    onChange: (_: Array<Layer>) => void = () => ({});
    registerOnChange(fn: (_: Array<Layer>) => void): void {
        this.onChange = fn;
    }

    // TODO: remove
    onTouched: () => void = () => ({});
    registerOnTouched(fn: () => void): void {
        this.onTouched = fn;
    }
}

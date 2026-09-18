import {computed, inject, resource, ResourceRef, Service, signal} from '@angular/core';
import {DATA_SOURCES, DataSourceLayer, PRESET_CATEGORY_LABELS, PresetCategory, VisualizationPreset} from './data-sources';
import {CollectionItem, ProviderLayerId} from '@geoengine/api-client';
import {LayersService} from '@geoengine/common';

/**
 * Service for managing layers in the Enhanced Data Viewer.
 */
@Service()
export class EdvLayersService {
    readonly debug = signal(false);
    readonly layerService = inject(LayersService);

    readonly selectedDataSource = signal(DATA_SOURCES[0]);
    readonly selectedPresetIndex = signal<number>(0);

    readonly currentPresets = computed(() => {
        const dataSource = this.selectedDataSource();
        const presets = dataSource?.presets ?? [];

        // The static and ad-hoc presets are not production-ready yet; they are hidden
        // unless the app is opened with the `debug` query parameter.
        if (this.debug()) return presets;
        return presets.filter((preset) => preset.category === 'harvested');
    });

    readonly presetGroups = computed(() => {
        const presets = this.currentPresets();
        const groups = new Map<PresetCategory, VisualizationPreset[]>();
        for (const preset of presets) {
            const group = groups.get(preset.category) ?? [];
            group.push(preset);
            groups.set(preset.category, group);
        }
        return Array.from(groups.entries()).map(([category, items]) => ({
            category,
            label: PRESET_CATEGORY_LABELS[category],
            presets: items,
        }));
    });

    readonly activePreset = computed(() => {
        const presets = this.currentPresets();
        const index = this.selectedPresetIndex();
        return presets[index] ?? presets[0];
    });

    private readonly presetRequestParams = computed(() => {
        const preset = this.activePreset();
        if (!preset) return undefined;
        return {connectorId: preset.connectorId, collectionId: preset.collectionId, name: preset.name};
    });

    readonly mapTileLayerResource: ResourceRef<DataSourceLayer | undefined> = resource({
        params: () => this.presetRequestParams(),
        loader: async ({params}) => {
            if (!params) return undefined;

            const limit = 20;
            let offset = 0;
            let layer: CollectionItem | undefined;

            while (!layer) {
                const items = await this.layerService.getLayerCollectionItems(params.connectorId, params.collectionId, offset, limit);

                if (items.items.length === 0) break;

                layer = items.items.find((item) => item.name === params.name);

                if (items.items.length < limit) break;

                offset += limit;
            }

            if (!layer) return undefined;

            const id = layer.id as ProviderLayerId;

            return {
                dataConnectorId: id.providerId,
                layerId: id.layerId,
            };
        },
    });

    readonly mapTileLayer = computed(() => this.mapTileLayerResource.value());
}

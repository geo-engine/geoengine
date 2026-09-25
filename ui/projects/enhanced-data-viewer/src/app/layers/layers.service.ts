import {computed, effect, inject, resource, ResourceRef, Service, signal} from '@angular/core';
import {CollectionItem, TimeStepFromJSON} from '@geoengine/api-client';
import {LAYER_DB_PROVIDER_ID, LAYER_DB_ROOT_COLLECTION_ID, LayersService, timeStepDictTotimeStepDuration} from '@geoengine/common';
import {AppConfig} from '../app-config.service';
import {
    DataSourceDefinition,
    DataSourceLayer,
    DataSourceVariant,
    PRESET_CATEGORIES,
    PRESET_CATEGORY_LABELS,
    PresetCategory,
    VisualizationPreset,
} from './data-sources';

@Service()
export class EdvLayersService {
    readonly debug = signal(false);
    readonly layerService = inject(LayersService);
    private readonly appConfig = inject(AppConfig);

    readonly selectedDataSource = signal<DataSourceDefinition | undefined>(undefined);
    readonly selectedVariantKey = signal<string | undefined>(undefined);
    readonly selectedPresetKey = signal<string | undefined>(undefined);
    readonly selectedPresetIndex = signal(0);

    readonly catalogueError = computed(() => {
        const error = this.catalogueResource.error();
        if (error instanceof Error) {
            return error.message;
        }
        return error ? String(error) : undefined;
    });

    private readonly catalogueResource: ResourceRef<DataSourceDefinition[] | undefined> = resource({
        params: () => ({
            root: LAYER_DB_ROOT_COLLECTION_ID,
            category: this.debug() ? '*' : this.appConfig.EDV.CATEGORY,
        }),
        loader: ({params}) => this.loadCatalogue(params.root, params.category),
    });

    readonly dataSources = computed(() => (this.catalogueResource.hasValue() ? this.catalogueResource.value() : []));
    readonly catalogueLoading = computed(() => this.catalogueResource.isLoading());
    readonly currentVariants = computed(() => this.selectedDataSource()?.variants ?? []);
    readonly selectedVariant = computed(() => {
        const variants = this.currentVariants();
        return variants.find((variant) => variant.key === this.selectedVariantKey()) ?? variants[0];
    });
    readonly currentPresets = computed(() => this.selectedVariant()?.presets ?? []);
    readonly presetGroups = computed(() => {
        const groups = new Map<PresetCategory, VisualizationPreset[]>();
        for (const preset of this.currentPresets()) {
            const presets = groups.get(preset.category) ?? [];
            presets.push(preset);
            groups.set(preset.category, presets);
        }
        return [...groups.entries()].map(([category, presets]) => ({
            category,
            label: PRESET_CATEGORY_LABELS[category],
            presets,
        }));
    });
    readonly activePreset = computed(() => {
        const presets = this.currentPresets();
        return presets.find((preset) => preset.key === this.selectedPresetKey()) ?? presets[0];
    });
    readonly mapTileLayer = computed<DataSourceLayer | undefined>(() => {
        const preset = this.activePreset();
        if (!preset) {
            return undefined;
        }
        return {dataConnectorId: preset.connectorId, layerId: preset.layerId};
    });

    constructor() {
        effect(() => {
            const sources = this.dataSources();
            if (!sources.length) {
                this.selectedDataSource.set(undefined);
                this.selectedVariantKey.set(undefined);
                this.selectedPresetKey.set(undefined);
                this.selectedPresetIndex.set(0);
                return;
            }

            const current = this.selectedDataSource();
            const next = current ? sources.find((source) => source.key === current.key) : undefined;
            const selected = next ?? sources[0];
            const sourceChanged = current?.key !== selected.key;
            this.selectedDataSource.set(selected);

            const wantedVariantKey = sourceChanged ? undefined : this.selectedVariantKey();
            const variant = selected.variants.find((candidate) => candidate.key === wantedVariantKey) ?? selected.variants[0];
            this.selectedVariantKey.set(variant?.key);

            const wantedPresetKey = sourceChanged ? undefined : this.selectedPresetKey();
            const preset = variant?.presets.find((candidate) => candidate.key === wantedPresetKey) ?? variant?.presets[0];
            this.selectedPresetKey.set(preset?.key);
            this.selectedPresetIndex.set(preset ? (variant?.presets.indexOf(preset) ?? 0) : 0);
        });
    }

    retryCatalogue(): void {
        this.catalogueResource.reload();
    }

    setSelectedDataSource(key: string): void {
        const dataSource = this.dataSources().find((source) => source.key === key);
        if (!dataSource) {
            return;
        }
        this.selectedDataSource.set(dataSource);
        this.selectedVariantKey.set(undefined);
        this.selectedPresetKey.set(undefined);
        this.selectedPresetIndex.set(0);
    }

    setSelectedVariant(key: string): void {
        const variant = this.currentVariants().find((candidate) => candidate.key === key);
        if (!variant) {
            return;
        }
        this.selectedVariantKey.set(variant.key);
        const currentPreset = variant.presets.find((preset) => preset.key === this.selectedPresetKey());
        const nextPreset = currentPreset ?? variant.presets[0];
        this.selectedPresetKey.set(nextPreset?.key);
        this.selectedPresetIndex.set(nextPreset ? variant.presets.indexOf(nextPreset) : 0);
    }

    setSelectedPreset(key: string): void {
        const index = this.currentPresets().findIndex((preset) => preset.key === key);
        if (index < 0) {
            return;
        }
        this.selectedPresetKey.set(key);
        this.selectedPresetIndex.set(index);
    }

    /** Discover datasets in the EDV -> category -> dataset -> variant -> preset hierarchy. */
    private async loadCatalogue(rootCollectionId: string, category: string): Promise<DataSourceDefinition[]> {
        if (category !== '*' && !(PRESET_CATEGORIES as readonly string[]).includes(category)) {
            throw new Error('Unsupported EDV category: ' + category);
        }
        const categories: readonly PresetCategory[] = category === '*' ? PRESET_CATEGORIES : [category as PresetCategory];

        const edvCollection = await this.findItem(rootCollectionId, (item) => item.type === 'collection' && item.name === 'EDV');
        const edvCollectionId = edvCollection && getCollectionId(edvCollection);
        if (!edvCollectionId) {
            throw new Error('EDV collection was not found under the layer database root');
        }

        const categoryListings = await this.allItems(edvCollectionId);
        const sourcesByCategory = await Promise.all(
            categories.map((presetCategory) => {
                const listing = categoryListings.find((item) => item.type === 'collection' && item.name === presetCategory);
                return this.loadCategory(listing && getCollectionId(listing), presetCategory);
            }),
        );
        return mergeAndSortDataSources(sourcesByCategory.flat());
    }

    private async loadCategory(categoryId: string | undefined, category: PresetCategory): Promise<DataSourceDefinition[]> {
        if (!categoryId) {
            return [];
        }
        const datasets = await this.allItems(categoryId);
        const sources = await Promise.all(datasets.map((dataset) => this.loadDataset(dataset, category)));
        return sources.filter((source): source is DataSourceDefinition => source !== undefined);
    }

    private async loadDataset(dataset: CollectionItem, category: PresetCategory): Promise<DataSourceDefinition | undefined> {
        const datasetId = getCollectionId(dataset);
        const metadata = getProperties(dataset);
        const datasetKey = metadata.get('edv:dataset') ?? dataset.name;
        if (!datasetId || !datasetKey || metadata.get('edv:type') !== 'dataset') {
            return undefined;
        }

        const items = await this.allItems(datasetId);
        const variantCollections = items.filter(isVariantCollection);
        const directLayers = items.filter(
            (item): item is Extract<CollectionItem, {type: 'layer'}> =>
                item.type === 'layer' && getProperties(item).get('edv:type') === 'preset',
        );
        const variants =
            variantCollections.length > 0
                ? (await Promise.all(variantCollections.map((item) => this.loadVariant(item, category)))).filter(
                      (variant): variant is DataSourceVariant => variant !== undefined,
                  )
                : directLayers.length > 0
                  ? [
                        {
                            key: 'default',
                            name: 'Default',
                            explicit: false,
                            presets: directLayers
                                .map((item) => this.createPreset(item, category, 'default'))
                                .filter((preset): preset is VisualizationPreset => preset !== undefined),
                        },
                    ]
                  : [];

        const timeStep = metadata.get('edv:timeStep');
        return {
            key: datasetKey,
            name: dataset.name ?? datasetKey,
            variants,
            defaultTime: parseFiniteNumber(metadata.get('edv:defaultTime')),
            defaultTimeStep: timeStep ? timeStepDictTotimeStepDuration(TimeStepFromJSON(JSON.parse(timeStep))) : undefined,
            citation: metadata.get('edv:citation') ?? '',
        };
    }

    private async loadVariant(item: CollectionItem, category: PresetCategory): Promise<DataSourceVariant | undefined> {
        const variantId = getCollectionId(item);
        if (!variantId) {
            return undefined;
        }
        const metadata = getProperties(item);
        const variantKey = metadata.get('edv:variant') ?? item.name;
        if (!variantKey) {
            return undefined;
        }
        const variantItems = await this.allItems(variantId);
        const presets = variantItems
            .filter(
                (candidate): candidate is Extract<CollectionItem, {type: 'layer'}> =>
                    candidate.type === 'layer' && getProperties(candidate).get('edv:type') === 'preset',
            )
            .map((candidate) => this.createPreset(candidate, category, variantKey))
            .filter((preset): preset is VisualizationPreset => preset !== undefined);
        return {
            key: variantKey,
            name: item.name ?? variantKey,
            crs: metadata.get('edv:crs'),
            explicit: true,
            presets,
        };
    }

    private createPreset(
        item: Extract<CollectionItem, {type: 'layer'}>,
        category: PresetCategory,
        variantKey: string,
    ): VisualizationPreset | undefined {
        if (!item.id.layerId || !item.id.providerId) {
            return undefined;
        }
        const metadata = getProperties(item);
        const key = metadata.get('edv:presetKey') ?? metadata.get('edv:preset') ?? item.name;
        if (!key) {
            return undefined;
        }
        return {
            key,
            displayName: metadata.get('edv:preset') ?? item.name ?? 'Layer',
            backgroundImage: metadata.get('edv:thumbnail') ?? 'assets/grey.jpg',
            connectorId: item.id.providerId,
            layerId: item.id.layerId,
            category,
            order: parseFiniteNumber(metadata.get('edv:order')) ?? 0,
            variantKey,
        };
    }

    private async findItem(collection: string, predicate: (item: CollectionItem) => boolean): Promise<CollectionItem | undefined> {
        const limit = 20;
        let offset = 0;
        while (true) {
            const page = await this.layerService.getLayerCollectionItems(LAYER_DB_PROVIDER_ID, collection, offset, limit);
            const item = page.items.find(predicate);
            if (item || page.items.length < limit) {
                return item;
            }
            offset += limit;
        }
    }

    private async allItems(collection: string): Promise<CollectionItem[]> {
        const result: CollectionItem[] = [];
        const limit = 20;
        let offset = 0;
        while (true) {
            const page = await this.layerService.getLayerCollectionItems(LAYER_DB_PROVIDER_ID, collection, offset, limit);
            result.push(...page.items);
            if (page.items.length < limit) {
                return result;
            }
            offset += limit;
        }
    }
}

function mergeAndSortDataSources(sources: DataSourceDefinition[]): DataSourceDefinition[] {
    const sourcesByKey = new Map<string, DataSourceDefinition>();
    for (const source of sources) {
        const existingSource = sourcesByKey.get(source.key);
        if (!existingSource) {
            sourcesByKey.set(source.key, {
                ...source,
                variants: source.variants.map((variant) => ({...variant, presets: [...variant.presets]})),
            });
            continue;
        }
        for (const variant of source.variants) {
            const existingVariant = existingSource.variants.find((candidate) => candidate.key === variant.key);
            if (existingVariant) {
                existingVariant.presets.push(...variant.presets);
                existingVariant.explicit ||= variant.explicit;
            } else {
                existingSource.variants.push({...variant, presets: [...variant.presets]});
            }
        }
    }

    const result = [...sourcesByKey.values()].filter((source) => source.variants.some((variant) => variant.presets.length > 0));
    for (const source of result) {
        for (const variant of source.variants) {
            const presetsByKey = new Map<string, VisualizationPreset>();
            for (const preset of variant.presets) {
                presetsByKey.set(preset.category + '/' + preset.key, preset);
            }
            variant.presets = [...presetsByKey.values()].sort(
                (a, b) => a.order - b.order || a.category.localeCompare(b.category) || a.key.localeCompare(b.key),
            );
        }
        source.variants = source.variants
            .filter((variant) => variant.presets.length > 0)
            .sort((a, b) => Number(b.explicit) - Number(a.explicit) || a.name.localeCompare(b.name) || a.key.localeCompare(b.key));
    }
    result.sort((a, b) => a.name.localeCompare(b.name) || a.key.localeCompare(b.key));
    return result;
}

function isVariantCollection(item: CollectionItem): boolean {
    if (item.type !== 'collection') {
        return false;
    }
    const properties = getProperties(item);
    return properties.get('edv:type') === 'variant' || properties.has('edv:variant');
}

function getProperties(item: CollectionItem): Map<string, string> {
    const metadata = new Map<string, string>();
    for (const property of (item.properties as unknown[] | undefined) ?? []) {
        if (Array.isArray(property) && property.length === 2) {
            metadata.set(String(property[0]), String(property[1]));
        }
    }
    return metadata;
}

const getCollectionId = (item: CollectionItem): string | undefined => (item.type === 'collection' ? item.id.collectionId : undefined);

function parseFiniteNumber(value: string | undefined): number | undefined {
    if (value === undefined) {
        return undefined;
    }
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
}

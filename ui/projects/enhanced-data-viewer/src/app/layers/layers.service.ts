import {computed, effect, inject, resource, ResourceRef, Service, signal, untracked} from '@angular/core';
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
    private readonly variantPresetCache = signal(new Map<string, VisualizationPreset[]>());
    private variantLoadGeneration = 0;
    private readonly variantLoadEpochs = new Map<string, number>();
    private readonly variantLoadingKeys = signal(new Set<string>());
    private readonly variantLoadTokens = new Map<string, number>();
    private nextVariantLoadToken = 0;
    private readonly variantLoadErrorKey = signal<string | undefined>(undefined);
    private readonly variantLoadError = signal<string | undefined>(undefined);

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
    readonly currentPresets = computed(() => {
        const source = this.selectedDataSource();
        const variant = this.selectedVariant();
        if (!source || !variant) {
            return [];
        }
        if (!variant.explicit) {
            return variant.presets;
        }
        return this.variantPresetCache().get(variantCacheKey(source.key, variant.key)) ?? [];
    });
    readonly variantLoading = computed(() => {
        const source = this.selectedDataSource();
        const variant = this.selectedVariant();
        return (
            source !== undefined &&
            variant !== undefined &&
            this.variantLoadingKeys().has(variantCacheKey(source.key, variant.key)) &&
            this.variantLoadErrorKey() !== variantCacheKey(source.key, variant.key)
        );
    });
    readonly variantError = computed(() => {
        const source = this.selectedDataSource();
        const variant = this.selectedVariant();
        return source !== undefined && variant !== undefined && this.variantLoadErrorKey() === variantCacheKey(source.key, variant.key)
            ? this.variantLoadError()
            : undefined;
    });
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
            if (this.catalogueLoading()) {
                this.invalidateVariantPresets();
                return;
            }
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
            if (variant?.explicit) {
                this.selectedPresetKey.set(wantedPresetKey);
                this.selectedPresetIndex.set(0);
                untracked(() => void this.ensureVariantPresets(selected, variant));
            } else {
                const preset = variant?.presets.find((candidate) => candidate.key === wantedPresetKey) ?? variant?.presets[0];
                this.selectedPresetKey.set(preset?.key);
                this.selectedPresetIndex.set(preset ? (variant?.presets.indexOf(preset) ?? 0) : 0);
            }
        });
    }

    retryCatalogue(): void {
        this.invalidateVariantPresets();
        this.catalogueResource.reload();
    }

    retryVariant(): void {
        const source = this.selectedDataSource();
        const variant = this.selectedVariant();
        if (!source || !variant?.explicit) {
            return;
        }
        this.removeCachedVariant(source.key, variant.key);
        untracked(() => void this.ensureVariantPresets(source, variant, true));
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
        if (!variant.explicit) {
            const currentPreset = variant.presets.find((preset) => preset.key === this.selectedPresetKey());
            const nextPreset = currentPreset ?? variant.presets[0];
            this.selectedPresetKey.set(nextPreset?.key);
            this.selectedPresetIndex.set(nextPreset ? variant.presets.indexOf(nextPreset) : 0);
        } else {
            const cached = this.variantPresetCache().get(variantCacheKey(this.selectedDataSource()?.key ?? '', variant.key));
            if (cached) {
                this.selectPresetFrom(cached);
            } else {
                this.selectedPresetIndex.set(0);
            }
        }
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
                ? variantCollections
                      .map((item) => this.loadVariant(item, category))
                      .filter((variant): variant is DataSourceVariant => variant !== undefined)
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

    private loadVariant(item: CollectionItem, category: PresetCategory): DataSourceVariant | undefined {
        const variantId = getCollectionId(item);
        if (!variantId) {
            return undefined;
        }
        const metadata = getProperties(item);
        const variantKey = metadata.get('edv:variant') ?? item.name;
        if (!variantKey) {
            return undefined;
        }
        return {
            key: variantKey,
            name: item.name ?? variantKey,
            crs: metadata.get('edv:crs'),
            explicit: true,
            presets: [],
            collectionRefs: [{collectionId: variantId, category}],
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

    private invalidateVariantPresets(): void {
        this.variantLoadGeneration += 1;
        this.variantLoadEpochs.clear();
        this.variantPresetCache.set(new Map());
        this.variantLoadingKeys.set(new Set());
        this.variantLoadTokens.clear();
        this.variantLoadErrorKey.set(undefined);
        this.variantLoadError.set(undefined);
    }

    private removeCachedVariant(sourceKey: string, variantKey: string): void {
        const cache = new Map(this.variantPresetCache());
        const key = variantCacheKey(sourceKey, variantKey);
        cache.delete(key);
        this.variantPresetCache.set(cache);
        this.variantLoadEpochs.set(key, (this.variantLoadEpochs.get(key) ?? 0) + 1);
        this.variantLoadingKeys.update((keys) => {
            const next = new Set(keys);
            next.delete(key);
            return next;
        });
        this.variantLoadTokens.delete(key);
        if (this.variantLoadErrorKey() === key) {
            this.variantLoadErrorKey.set(undefined);
        }
        this.variantLoadError.set(undefined);
    }

    private async ensureVariantPresets(source: DataSourceDefinition, variant: DataSourceVariant, force = false): Promise<void> {
        const key = variantCacheKey(source.key, variant.key);
        const cached = this.variantPresetCache().get(key);
        if (!force && cached) {
            if (this.selectedDataSource()?.key === source.key && this.selectedVariantKey() === variant.key) {
                this.selectPresetFrom(cached);
            }
            return;
        }
        if (this.variantLoadTokens.has(key)) {
            return;
        }
        const generation = this.variantLoadGeneration;
        const epoch = this.variantLoadEpochs.get(key) ?? 0;
        const token = ++this.nextVariantLoadToken;
        this.variantLoadTokens.set(key, token);
        this.variantLoadingKeys.update((keys) => new Set(keys).add(key));
        if (this.variantLoadErrorKey() === key) {
            this.variantLoadErrorKey.set(undefined);
            this.variantLoadError.set(undefined);
        }
        try {
            const itemsByCollection = await Promise.all(
                (variant.collectionRefs ?? []).map(async (reference) => ({
                    category: reference.category,
                    items: await this.allItems(reference.collectionId),
                })),
            );
            const presets = itemsByCollection
                .flatMap(({category, items}) =>
                    items
                        .filter(
                            (item): item is Extract<CollectionItem, {type: 'layer'}> =>
                                item.type === 'layer' && getProperties(item).get('edv:type') === 'preset',
                        )
                        .map((item) => this.createPreset(item, category, variant.key)),
                )
                .filter((preset): preset is VisualizationPreset => preset !== undefined);
            const byKey = new Map<string, VisualizationPreset>();
            for (const preset of presets) {
                byKey.set(preset.category + '/' + preset.key, preset);
            }
            const sorted = [...byKey.values()].sort(
                (a, b) => a.order - b.order || a.category.localeCompare(b.category) || a.key.localeCompare(b.key),
            );
            if (generation !== this.variantLoadGeneration || epoch !== (this.variantLoadEpochs.get(key) ?? 0)) {
                this.finishVariantLoad(key, token);
                return;
            }
            const cache = new Map(this.variantPresetCache());
            cache.set(key, sorted);
            this.variantPresetCache.set(cache);
            this.finishVariantLoad(key, token);
            if (this.selectedDataSource()?.key === source.key && this.selectedVariantKey() === variant.key) {
                if (this.variantLoadErrorKey() === key) {
                    this.variantLoadErrorKey.set(undefined);
                    this.variantLoadError.set(undefined);
                }
                this.selectPresetFrom(sorted);
            }
        } catch (error) {
            if (generation !== this.variantLoadGeneration || epoch !== (this.variantLoadEpochs.get(key) ?? 0)) {
                this.finishVariantLoad(key, token);
                return;
            }
            this.finishVariantLoad(key, token);
            if (this.selectedDataSource()?.key === source.key && this.selectedVariantKey() === variant.key) {
                this.variantLoadErrorKey.set(key);
                this.variantLoadError.set(error instanceof Error ? error.message : String(error));
            }
        }
    }

    private finishVariantLoad(key: string, token: number): void {
        if (this.variantLoadTokens.get(key) !== token) {
            return;
        }
        this.variantLoadTokens.delete(key);
        this.variantLoadingKeys.update((keys) => {
            const next = new Set(keys);
            next.delete(key);
            return next;
        });
    }

    private selectPresetFrom(presets: VisualizationPreset[]): void {
        const preset = presets.find((candidate) => candidate.key === this.selectedPresetKey()) ?? presets[0];
        this.selectedPresetKey.set(preset?.key);
        this.selectedPresetIndex.set(preset ? presets.indexOf(preset) : 0);
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
                variants: source.variants.map((variant) => ({
                    ...variant,
                    presets: [...variant.presets],
                    collectionRefs: variant.collectionRefs ? [...variant.collectionRefs] : undefined,
                })),
            });
            continue;
        }
        for (const variant of source.variants) {
            const existingVariant = existingSource.variants.find((candidate) => candidate.key === variant.key);
            if (existingVariant) {
                existingVariant.presets.push(...variant.presets);
                existingVariant.explicit ||= variant.explicit;
                existingVariant.collectionRefs = mergeCollectionRefs(existingVariant.collectionRefs, variant.collectionRefs);
            } else {
                existingSource.variants.push({
                    ...variant,
                    presets: [...variant.presets],
                    collectionRefs: variant.collectionRefs ? [...variant.collectionRefs] : undefined,
                });
            }
        }
    }

    const hasContent = (variant: DataSourceVariant): boolean => variant.presets.length > 0 || (variant.collectionRefs?.length ?? 0) > 0;
    const result = [...sourcesByKey.values()].filter((source) => source.variants.some(hasContent));
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
            .filter(hasContent)
            .sort((a, b) => Number(b.explicit) - Number(a.explicit) || a.name.localeCompare(b.name) || a.key.localeCompare(b.key));
    }
    result.sort((a, b) => a.name.localeCompare(b.name) || a.key.localeCompare(b.key));
    return result;
}

function mergeCollectionRefs(
    current: DataSourceVariant['collectionRefs'],
    additional: DataSourceVariant['collectionRefs'],
): DataSourceVariant['collectionRefs'] {
    const refs = [...(current ?? []), ...(additional ?? [])];
    const unique = new Map(refs.map((reference) => [reference.category + '/' + reference.collectionId, reference]));
    return [...unique.values()];
}

const variantCacheKey = (sourceKey: string, variantKey: string): string => sourceKey + '/' + variantKey;

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

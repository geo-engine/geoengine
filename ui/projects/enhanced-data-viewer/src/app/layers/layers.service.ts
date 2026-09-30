import {computed, effect, inject, resource, ResourceRef, Service, signal} from '@angular/core';
import {CollectionItem, TimeStepFromJSON} from '@geoengine/api-client';
import {LAYER_DB_PROVIDER_ID, LAYER_DB_ROOT_COLLECTION_ID, LayersService, timeStepDictTotimeStepDuration} from '@geoengine/common';
import {AppConfig} from '../app-config.service';
import {
    DataSourceDefinition,
    DataSourceLayer,
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
    readonly currentPresets = computed(() => this.selectedDataSource()?.presets ?? []);
    readonly presetGroups = computed(() => {
        const groups = new Map<PresetCategory, VisualizationPreset[]>();
        for (const preset of this.currentPresets()) {
            const presets = groups.get(preset.category) ?? [];
            presets.push(preset);
            groups.set(preset.category, presets);
        }
        return [...groups.entries()].map(([category, presets]) => ({category, label: PRESET_CATEGORY_LABELS[category], presets}));
    });
    readonly activePreset = computed(() => this.currentPresets()[this.selectedPresetIndex()] ?? this.currentPresets()[0]);
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
                this.selectedPresetIndex.set(0);
                return;
            }
            const current = this.selectedDataSource();
            const next = current ? sources.find((source) => source.key === current.key) : undefined;
            const selected = next ?? sources[0];
            this.selectedDataSource.set(selected);
            if (current?.key !== selected.key || this.selectedPresetIndex() >= selected.presets.length) {
                this.selectedPresetIndex.set(0);
            }
        });
    }

    retryCatalogue(): void {
        this.catalogueResource.reload();
    }

    /** Discover datasets in the EDV -> category -> dataset -> preset layer hierarchy. */
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

        // Each dataset is independent, so fetch its preset layers concurrently.
        const datasets = await this.allItems(categoryId);
        const sources = await Promise.all(datasets.map((dataset) => this.loadDataset(dataset, category)));
        return sources.filter((source): source is DataSourceDefinition => source !== undefined);
    }

    private async loadDataset(dataset: CollectionItem, category: PresetCategory): Promise<DataSourceDefinition | undefined> {
        const datasetId = getCollectionId(dataset);
        const metadata = getProperties(dataset);
        if (!datasetId || metadata.get('edv:type') !== 'dataset') {
            return undefined;
        }

        const items = await this.allItems(datasetId);
        const presets = items
            .filter((item) => item.type === 'layer' && getProperties(item).get('edv:type') === 'preset')
            .map((item) => this.createPreset(item, category));
        const timeStep = metadata.get('edv:timeStep');

        return {
            key: metadata.get('edv:dataset') ?? datasetId,
            name: dataset.name ?? metadata.get('edv:dataset') ?? datasetId,
            presets,
            defaultTime: parseFiniteNumber(metadata.get('edv:defaultTime')),
            defaultTimeStep: timeStep ? timeStepDictTotimeStepDuration(TimeStepFromJSON(JSON.parse(timeStep))) : undefined,
            citation: metadata.get('edv:citation') ?? '',
        };
    }

    private createPreset(item: CollectionItem, category: PresetCategory): VisualizationPreset {
        if (item.type !== 'layer' || !item.id.layerId) {
            throw new Error(`EDV preset ${item.name} has no layer id`);
        }
        const metadata = getProperties(item);

        return {
            displayName: metadata.get('edv:preset') ?? item.name ?? 'Layer',
            backgroundImage: metadata.get('edv:thumbnail') ?? 'assets/grey.jpg',
            connectorId: item.id.providerId,
            layerId: item.id.layerId,
            category,
            order: parseFiniteNumber(metadata.get('edv:order')) ?? 0,
        };
    }

    /** Find one item while stopping pagination as soon as it is found. */
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

    /** Fetch every page because the API's collection endpoint is paginated. */
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

/** Combine presets for datasets shared across categories, retaining the first dataset's metadata. */
function mergeAndSortDataSources(sources: DataSourceDefinition[]): DataSourceDefinition[] {
    const sourcesByKey = new Map<string, DataSourceDefinition>();
    for (const source of sources) {
        const existingSource = sourcesByKey.get(source.key);
        if (existingSource) {
            existingSource.presets.push(...source.presets);
        } else {
            sourcesByKey.set(source.key, source);
        }
    }

    const sourcesWithPresets = [...sourcesByKey.values()].filter((source) => source.presets.length > 0);
    for (const source of sourcesWithPresets) {
        source.presets.sort((a, b) => a.order - b.order || a.layerId.localeCompare(b.layerId));
    }
    sourcesWithPresets.sort((a, b) => a.presets[0].order - b.presets[0].order || a.key.localeCompare(b.key));
    return sourcesWithPresets;
}

/** Convert the API's property tuples into convenient metadata lookups. */
function getProperties(item: CollectionItem): Map<string, string> {
    const metadata = new Map<string, string>();
    for (const property of (item.properties as unknown[] | undefined) ?? []) {
        if (Array.isArray(property) && property.length === 2) {
            metadata.set(String(property[0]), String(property[1]));
        }
    }
    return metadata;
}

/** Return a collection ID while narrowing the CollectionItem union. */
const getCollectionId = (item: CollectionItem): string | undefined => (item.type === 'collection' ? item.id.collectionId : undefined);

/** Parse optional numeric metadata without allowing NaN or infinities. */
function parseFiniteNumber(value: string | undefined): number | undefined {
    if (value === undefined) {
        return undefined;
    }
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
}

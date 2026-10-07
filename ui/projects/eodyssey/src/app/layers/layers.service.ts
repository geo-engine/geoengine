import {computed, DestroyRef, effect, inject, resource, ResourceRef, Service, signal, untracked} from '@angular/core';
import {takeUntilDestroyed} from '@angular/core/rxjs-interop';
import {CollectionItem, TimeStepFromJSON} from '@geoengine/api-client';
import {MapService} from '@geoengine/core';
import {LAYER_DB_PROVIDER_ID, LAYER_DB_ROOT_COLLECTION_ID, LayersService, timeStepDictTotimeStepDuration} from '@geoengine/common';
import {toLonLat} from 'ol/proj';
import {unByKey} from 'ol/Observable';
import type {EventsKey} from 'ol/events';
import {coverageContains, parseCoverage} from './coverage';
import type {GeographicCenter} from './coverage';
import {DataSourceDefinition, DataSourceLayer, DataSourceVariant, VisualizationPreset} from './data-sources';

/**
 * Service for managing layers in the EOdyssey.
 */
@Service()
export class EdvLayersService {
    readonly layerService = inject(LayersService);
    private readonly mapService = inject(MapService);
    private readonly destroyRef = inject(DestroyRef);

    readonly selectedDataSource = signal<DataSourceDefinition | undefined>(undefined);
    readonly selectedVariantKey = signal<string | undefined>(undefined);
    readonly selectedPresetKey = signal<string | undefined>(undefined);
    readonly selectedPresetIndex = signal(0);
    readonly mapCenter = signal<GeographicCenter | undefined>(undefined);
    private pendingCenterSelection = true;
    private viewCenterListener?: EventsKey;
    /** Snapshot of the visualization on the map, independent of the pending catalogue selection. */
    private readonly appliedVisualization = signal<{source: DataSourceDefinition; preset: VisualizationPreset} | undefined>(undefined);
    readonly appliedDataSource = computed(() => this.appliedVisualization()?.source);
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
        params: () => LAYER_DB_ROOT_COLLECTION_ID,
        loader: ({params}) => this.loadCatalogue(params),
    });

    readonly dataSources = computed(() => (this.catalogueResource.hasValue() ? this.catalogueResource.value() : []));
    readonly catalogueLoading = computed(() => this.catalogueResource.isLoading());
    readonly currentVariants = computed(() => this.selectedDataSource()?.variants ?? []);
    readonly sortedVariants = computed(() =>
        [...this.currentVariants()].sort((a, b) => a.name.localeCompare(b.name) || a.key.localeCompare(b.key)),
    );
    readonly hasCoverageVariants = computed(() => this.currentVariants().some((variant) => variant.coverage !== undefined));
    readonly mapCenterVariantKey = computed(() => {
        const source = this.selectedDataSource();
        const center = this.mapCenter();
        return source && center ? this.findCoverageVariantKey(source, center, this.selectedVariantKey()) : undefined;
    });
    readonly mapCenterSelectionMessage = computed(() => {
        if (!this.hasCoverageVariants()) return undefined;
        if (!this.mapCenter()) return 'The map center is not available yet.';
        return this.mapCenterVariantKey() ? undefined : 'No available region covers the map center.';
    });
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
    readonly selectedPreset = computed(() => {
        const presets = this.currentPresets();
        return presets.find((preset) => preset.key === this.selectedPresetKey());
    });
    readonly canApplyPreset = computed(() => {
        const selected = this.selectedPreset();
        const applied = this.appliedVisualization()?.preset;
        return !!selected && (selected.connectorId !== applied?.connectorId || selected.layerId !== applied?.layerId);
    });
    readonly mapTileLayer = computed<DataSourceLayer | undefined>(() => {
        const preset = this.appliedVisualization()?.preset;
        if (!preset) {
            return undefined;
        }
        return {dataConnectorId: preset.connectorId, layerId: preset.layerId};
    });

    constructor() {
        this.mapService
            .getViewStream()
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((view) => {
                if (this.viewCenterListener) unByKey(this.viewCenterListener);
                this.viewCenterListener = view.on('change:center', () => this.updateMapCenter());
                this.updateMapCenter();
            });
        this.destroyRef.onDestroy(() => {
            if (this.viewCenterListener) unByKey(this.viewCenterListener);
        });
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
            if (sourceChanged) this.pendingCenterSelection = true;
            this.selectedDataSource.set(selected);

            const wantedVariantKey = sourceChanged ? undefined : this.selectedVariantKey();
            const centerVariantKey = sourceChanged ? this.getMapCenterVariantKey(selected) : undefined;
            const variant =
                selected.variants.find((candidate) => candidate.key === centerVariantKey) ??
                selected.variants.find((candidate) => candidate.key === wantedVariantKey) ??
                selected.variants[0];
            this.selectedVariantKey.set(variant?.key);
            if (sourceChanged && this.mapCenter()) this.pendingCenterSelection = false;

            this.selectedPresetKey.set(undefined);
            this.selectedPresetIndex.set(0);
            if (variant) untracked(() => void this.ensureVariantPresets(selected, variant));
        });
    }

    retryCatalogue(): void {
        this.invalidateVariantPresets();
        this.catalogueResource.reload();
    }

    retryVariant(): void {
        const source = this.selectedDataSource();
        const variant = this.selectedVariant();
        if (!source || !variant) {
            return;
        }
        this.removeCachedVariant(source.key, variant.key);
        untracked(() => void this.ensureVariantPresets(source, variant, true));
    }

    setSelectedDataSource(key: string): void {
        const dataSource = this.dataSources().find((source) => source.key === key);
        if (!dataSource || this.selectedDataSource()?.key === dataSource.key) {
            return;
        }
        this.selectedDataSource.set(dataSource);
        this.pendingCenterSelection = true;
        const centerVariantKey = this.getMapCenterVariantKey(dataSource);
        this.selectedVariantKey.set(centerVariantKey ?? dataSource.variants[0]?.key);
        if (this.mapCenter()) this.pendingCenterSelection = false;
        this.selectedPresetKey.set(undefined);
        this.selectedPresetIndex.set(0);
    }

    setSelectedVariant(key: string): void {
        const variant = this.currentVariants().find((candidate) => candidate.key === key);
        if (!variant) {
            return;
        }
        if (this.selectedVariantKey() !== variant.key) {
            this.selectedPresetKey.set(undefined);
        }
        this.pendingCenterSelection = false;
        this.selectedVariantKey.set(variant.key);
        this.selectedPresetIndex.set(0);
    }

    selectMapCenterVariant(): void {
        this.updateMapCenter();
        const key = this.mapCenterVariantKey();
        if (key) this.setSelectedVariant(key);
    }

    setSelectedPreset(key: string): void {
        const index = this.currentPresets().findIndex((preset) => preset.key === key);
        if (index < 0) {
            return;
        }
        this.selectedPresetKey.set(key);
        this.selectedPresetIndex.set(index);
    }

    applySelectedPreset(): void {
        const source = this.selectedDataSource();
        const preset = this.selectedPreset();
        if (source && preset && this.canApplyPreset()) {
            this.appliedVisualization.set({source, preset});
        }
    }

    /** Load the EDV -> data source -> region -> preset catalogue. */
    private async loadCatalogue(rootCollectionId: string): Promise<DataSourceDefinition[]> {
        const edvCollection = await this.findItem(rootCollectionId, (item) => item.type === 'collection' && item.name === 'EDV');
        const edvCollectionId = edvCollection && getCollectionId(edvCollection);
        if (!edvCollectionId) {
            throw new Error('EDV collection was not found under the layer database root');
        }
        const items = await this.allItems(edvCollectionId);
        const sources = await Promise.all(items.filter(isCollection).map((item) => this.loadDataset(item)));
        requireUniqueKeys(sources, 'data source');
        return sources
            .filter((source) => source.variants.length > 0)
            .sort((a, b) => a.name.localeCompare(b.name) || a.key.localeCompare(b.key));
    }

    private async loadDataset(dataset: Extract<CollectionItem, {type: 'collection'}>): Promise<DataSourceDefinition> {
        const metadata = getProperties(dataset);
        const key = requiredProperty(dataset, 'edv:dataset');
        const items = await this.allItems(dataset.id.collectionId);
        const variants = items.filter(isCollection).map((item) => this.loadVariant(item));
        requireUniqueKeys(variants, 'region in ' + dataset.name);
        variants.sort((a, b) => a.name.localeCompare(b.name) || a.key.localeCompare(b.key));
        const timeStep = metadata.get('edv:timeStep');
        return {
            key,
            name: dataset.name,
            variants,
            defaultTime: parseFiniteNumber(metadata.get('edv:defaultTime')),
            defaultTimeStep: timeStep ? timeStepDictTotimeStepDuration(TimeStepFromJSON(JSON.parse(timeStep))) : undefined,
            citation: metadata.get('edv:citation') ?? '',
        };
    }

    private loadVariant(item: Extract<CollectionItem, {type: 'collection'}>): DataSourceVariant {
        const crs = requiredProperty(item, 'edv:crs');
        return {
            key: crs,
            name: item.name,
            crs,
            coverage: parseCoverage(getProperties(item).get('edv:coverage')),
            collectionId: item.id.collectionId,
        };
    }

    private createPreset(item: Extract<CollectionItem, {type: 'layer'}>): VisualizationPreset {
        const metadata = getProperties(item);
        return {
            key: requiredProperty(item, 'edv:presetKey'),
            displayName: item.name,
            backgroundImage: metadata.get('edv:thumbnail') ?? 'assets/grey.jpg',
            connectorId: item.id.providerId,
            layerId: item.id.layerId,
            order: parseFiniteNumber(metadata.get('edv:order')) ?? 0,
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
            const items = await this.allItems(variant.collectionId);
            const presets = items
                .filter((item): item is Extract<CollectionItem, {type: 'layer'}> => item.type === 'layer')
                .map((item) => this.createPreset(item));
            requireUniqueKeys(presets, 'preset in ' + variant.name);
            presets.sort((a, b) => a.order - b.order || a.key.localeCompare(b.key));
            if (generation !== this.variantLoadGeneration || epoch !== (this.variantLoadEpochs.get(key) ?? 0)) {
                this.finishVariantLoad(key, token);
                return;
            }
            const cache = new Map(this.variantPresetCache());
            cache.set(key, presets);
            this.variantPresetCache.set(cache);
            this.finishVariantLoad(key, token);
            if (this.selectedDataSource()?.key === source.key && this.selectedVariantKey() === variant.key) {
                if (this.variantLoadErrorKey() === key) {
                    this.variantLoadErrorKey.set(undefined);
                    this.variantLoadError.set(undefined);
                }
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

    private updateMapCenter(): void {
        const view = this.mapService.getView();
        const center = view.getCenter();
        if (!center) {
            this.mapCenter.set(undefined);
            return;
        }
        const [longitude, latitude] = toLonLat(center, view.getProjection());
        if (!Number.isFinite(longitude) || !Number.isFinite(latitude)) {
            this.mapCenter.set(undefined);
            return;
        }
        const geographicCenter = {longitude, latitude};
        this.mapCenter.set(geographicCenter);
        const source = this.selectedDataSource();
        if (this.pendingCenterSelection && source) {
            const key = this.getMapCenterVariantKey(source);
            if (key && key !== this.selectedVariantKey()) this.setAutomaticVariant(key);
            this.pendingCenterSelection = false;
        }
    }

    private getMapCenterVariantKey(source: DataSourceDefinition | undefined): string | undefined {
        const center = this.mapCenter();
        if (!source || !center) return undefined;
        return this.findCoverageVariantKey(source, center);
    }

    private findCoverageVariantKey(source: DataSourceDefinition, center: GeographicCenter, preferredKey?: string): string | undefined {
        const matching = [...source.variants]
            .filter((variant) => variant.coverage && coverageContains(variant.coverage, center))
            .sort((a, b) => a.name.localeCompare(b.name) || a.key.localeCompare(b.key));
        return matching.find((variant) => variant.key === preferredKey)?.key ?? matching[0]?.key;
    }

    private setAutomaticVariant(key: string): void {
        if (this.selectedVariantKey() !== key) this.selectedPresetKey.set(undefined);
        this.selectedVariantKey.set(key);
        this.selectedPresetIndex.set(0);
    }
}

const isCollection = (item: CollectionItem): item is Extract<CollectionItem, {type: 'collection'}> => item.type === 'collection';

const requiredProperty = (item: CollectionItem, key: string): string => {
    const value = getProperties(item).get(key);
    if (!value) throw new Error('Missing ' + key + ' on ' + item.name);
    return value;
};

const requireUniqueKeys = (items: readonly {key: string}[], context: string): void => {
    const keys = new Set<string>();
    for (const item of items) {
        if (keys.has(item.key)) throw new Error('Duplicate ' + context + ': ' + item.key);
        keys.add(item.key);
    }
};

const variantCacheKey = (sourceKey: string, variantKey: string): string => sourceKey + '/' + variantKey;

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

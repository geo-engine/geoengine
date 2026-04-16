import {Injectable, inject} from '@angular/core';
import {firstValueFrom, ReplaySubject} from 'rxjs';
import {
    AutocompleteHandlerRequest,
    LayerCollection,
    LayersApi,
    ProviderCapabilities,
    SearchHandlerRequest,
    Layer as LayerDict,
    TypedResultDescriptor,
    ProviderLayerId,
    RasterSymbology as RasterSymbologyDict,
    UpdateLayerCollection,
    UpdateLayer,
    AddLayerCollection,
    AddLayer,
    LayerProviderListing,
    TypedDataProviderDefinition,
} from '@geoengine/api-client';

import {apiConfigurationWithAccessKey, UserService} from '../user/user.service';
import {UUID} from '../datasets/dataset.model';
import {WorkflowsService} from '../workflows/workflows.service';
import {LayerMetadata, RasterLayerMetadata, VectorLayerMetadata} from '../layers/layer-metadata.model';
import {RandomColorService} from '../util/services/random-color.service';
import {Layer, RasterLayer, VectorLayer} from '../layers/layer.model';
import {RasterSymbology, VectorSymbology, VectorSymbologyDict} from '../symbology/symbology.model';
import {createVectorSymbology} from '../util/symbologies';

@Injectable({
    providedIn: 'root',
})
export class LayersService {
    private sessionService = inject(UserService);
    private workflowsService = inject(WorkflowsService);
    private randomColorService = inject(RandomColorService);

    layersApi = new ReplaySubject<LayersApi>(1);

    constructor() {
        this.sessionService.getSessionStream().subscribe({
            next: (session) => this.layersApi.next(new LayersApi(apiConfigurationWithAccessKey(session.sessionToken))),
        });
    }

    async getLayerCollectionItems(provider: UUID, collection: string, offset = 0, limit = 20): Promise<LayerCollection> {
        const layersApi = await firstValueFrom(this.layersApi);

        return layersApi.listCollectionHandler({provider, collection, offset, limit});
    }

    async getRootLayerCollectionItems(offset = 0, limit = 20): Promise<LayerCollection> {
        const layersApi = await firstValueFrom(this.layersApi);

        return layersApi.listRootCollectionsHandler({offset, limit});
    }

    async getLayer(provider: UUID, layer: string): Promise<LayerDict> {
        const layersApi = await firstValueFrom(this.layersApi);

        return layersApi.layerHandler({provider, layer});
    }

    async registerAndGetLayerWorkflowId(providerId: UUID, layerId: string): Promise<UUID> {
        const layersApi = await firstValueFrom(this.layersApi);

        const workflow = await layersApi.layerToWorkflowIdHandler({provider: providerId, layer: layerId});

        return workflow.id;
    }

    async getWorkflowIdMetadata(workflowId: UUID): Promise<VectorLayerMetadata | RasterLayerMetadata> {
        const workflowMetadataDict = await this.getWorkflowIdMetadataDict(workflowId);
        return LayerMetadata.fromDict(workflowMetadataDict);
    }

    async getWorkflowIdMetadataDict(workflowId: UUID): Promise<TypedResultDescriptor> {
        return await this.workflowsService.getMetadata(workflowId);
    }

    /**
     * Fetches the capabilities of a layer provider.
     */
    async capabilities(providerId: UUID, options?: {abortController?: AbortController}): Promise<ProviderCapabilities> {
        const layersApi = await firstValueFrom(this.layersApi);

        return await layersApi.providerCapabilitiesHandler(
            {provider: providerId},
            {
                signal: options?.abortController?.signal,
            },
        );
    }

    /**
     * Searches a layer collection with autocomplete.
     *
     * @returns an array of matching layer (collection) names
     *
     * Returns an empty array…
     * - on success, when no results are found
     * - on error, e.g., when autocomplete is not supported by the backend
     * - on abort, e.g., when `options.abortController` is aborted
     */
    async autocompleteSearch(request: AutocompleteHandlerRequest, options?: {abortController?: AbortController}): Promise<Array<string>> {
        const layersApi = await firstValueFrom(this.layersApi);
        return await layersApi
            .autocompleteHandler(request, {
                signal: options?.abortController?.signal,
            })
            // on error or abort, just return an empty result
            .catch(() => []);
    }

    /**
     * Searches a layer collection.
     *
     * @returns a virtual layer collection of matching layers and layer collections
     */
    async search(request: SearchHandlerRequest, options?: {abortController?: AbortController}): Promise<LayerCollection> {
        const layersApi = await firstValueFrom(this.layersApi);
        return await layersApi.searchHandler(request, {
            signal: options?.abortController?.signal,
        });
    }

    async resolveLayer(layerId: ProviderLayerId): Promise<Layer> {
        const layer = await this.getLayer(layerId.providerId, layerId.layerId);

        const workflowId = await this.registerAndGetLayerWorkflowId(layerId.providerId, layerId.layerId);

        const metadata = await this.getWorkflowIdMetadata(workflowId);

        if (metadata instanceof VectorLayerMetadata) {
            return new VectorLayer({
                name: layer.name,
                workflowId,
                isVisible: true,
                isLegendVisible: false,
                symbology: layer.symbology
                    ? VectorSymbology.fromVectorSymbologyDict(layer.symbology as VectorSymbologyDict)
                    : createVectorSymbology(metadata.dataType.getCode(), this.randomColorService.getRandomColorRgba()),
            });
        } else if (metadata instanceof RasterLayerMetadata) {
            return new RasterLayer({
                name: layer.name,
                workflowId,
                isVisible: true,
                isLegendVisible: false,
                symbology: layer.symbology
                    ? RasterSymbology.fromRasterSymbologyDict(layer.symbology as RasterSymbologyDict)
                    : RasterSymbology.fromRasterSymbologyDict({
                          type: 'raster',
                          opacity: 1.0,
                          rasterColorizer: {
                              type: 'singleBand',
                              band: 0,
                              bandColorizer: {
                                  type: 'linearGradient',
                                  breakpoints: [
                                      {value: 1, color: [0, 0, 0, 255]},
                                      {value: 255, color: [255, 255, 255, 255]},
                                  ],
                                  overColor: [255, 255, 255, 127],
                                  underColor: [0, 0, 0, 127],
                                  noDataColor: [0, 0, 0, 0],
                              },
                          },
                      }),
            });
        } else {
            // TODO: implement plots, etc.
            throw new Error('Adding this workflow type is unimplemented, yet');
        }
    }

    async addLayerToCollection(layer: UUID, collection: UUID): Promise<void> {
        const layersApi = await firstValueFrom(this.layersApi);
        return await layersApi.addExistingLayerToCollection({
            collection,
            layer,
        });
    }

    async removeLayerFromCollection(layer: UUID, collection: UUID): Promise<void> {
        const layersApi = await firstValueFrom(this.layersApi);
        return await layersApi.removeLayerFromCollection({
            collection,
            layer,
        });
    }

    async addCollectionToCollection(collection: UUID, parent: UUID): Promise<void> {
        const layersApi = await firstValueFrom(this.layersApi);
        return await layersApi.addExistingCollectionToCollection({
            parent,
            collection,
        });
    }

    async removeCollectionFromCollection(collection: UUID, parent: UUID): Promise<void> {
        const layersApi = await firstValueFrom(this.layersApi);
        return await layersApi.removeCollectionFromCollection({
            parent,
            collection,
        });
    }

    async updateLayerCollection(collection: UUID, update: UpdateLayerCollection): Promise<void> {
        const layersApi = await firstValueFrom(this.layersApi);
        return await layersApi.updateCollection({
            collection,
            updateLayerCollection: update,
        });
    }

    async removeLayerCollection(collection: UUID): Promise<void> {
        const layersApi = await firstValueFrom(this.layersApi);
        return await layersApi.removeCollection({
            collection,
        });
    }

    async updateLayer(layer: UUID, update: UpdateLayer): Promise<void> {
        const layersApi = await firstValueFrom(this.layersApi);
        return await layersApi.updateLayer({
            layer,
            updateLayer: update,
        });
    }

    async removeLayer(layer: UUID): Promise<void> {
        const layersApi = await firstValueFrom(this.layersApi);
        return await layersApi.removeLayer({
            layer,
        });
    }

    async addCollection(parent: UUID, add: AddLayerCollection): Promise<UUID> {
        const layersApi = await firstValueFrom(this.layersApi);
        const response = await layersApi.addCollection({
            collection: parent,
            addLayerCollection: add,
        });

        return response.id;
    }

    async addLayer(parent: UUID, add: AddLayer): Promise<UUID> {
        const layersApi = await firstValueFrom(this.layersApi);
        const response = await layersApi.addLayer({
            collection: parent,
            addLayer: add,
        });

        return response.id;
    }

    async getProviders(offset = 0, limit = 20): Promise<LayerProviderListing[]> {
        const layersApi = await firstValueFrom(this.layersApi);

        return layersApi.listProviders({limit: limit, offset: offset});
    }

    async getProviderDefinition(provider: string): Promise<TypedDataProviderDefinition> {
        const layersApi = await firstValueFrom(this.layersApi);

        return layersApi.getProviderDefinition({provider});
    }

    async updateProviderDefinition(provider: string, typedDataProviderDefinition: TypedDataProviderDefinition): Promise<void> {
        const layersApi = await firstValueFrom(this.layersApi);

        await layersApi.updateProviderDefinition({provider, typedDataProviderDefinition});
    }

    async addProvider(typedDataProviderDefinition: TypedDataProviderDefinition): Promise<string> {
        const layersApi = await firstValueFrom(this.layersApi);

        return await layersApi.addProvider({typedDataProviderDefinition}).then((response) => response.id);
    }

    async deleteProvider(provider: string): Promise<void> {
        const layersApi = await firstValueFrom(this.layersApi);

        await layersApi.deleteProvider({provider});
    }
}

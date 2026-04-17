import {Injectable, inject} from '@angular/core';
import {
    DataPath,
    Dataset,
    DatasetDefinition,
    DatasetListing,
    DatasetsApi,
    MetaDataDefinition,
    MetaDataSuggestion,
    OrderBy,
    Provenance,
    SuggestMetaDataHandlerRequest,
    Symbology,
    UpdateDataset,
    Volume,
    VolumeFileLayersResponse,
} from '@geoengine/api-client';
import {ReplaySubject, firstValueFrom} from 'rxjs';
import {UserService, apiConfigurationWithAccessKey} from '../user/user.service';

@Injectable({
    providedIn: 'root',
})
export class DatasetsService {
    private sessionService = inject(UserService);

    datasetApi = new ReplaySubject<DatasetsApi>(1);

    constructor() {
        this.sessionService.getSessionStream().subscribe({
            next: (session) => this.datasetApi.next(new DatasetsApi(apiConfigurationWithAccessKey(session.sessionToken))),
        });
    }

    async getDataset(name: string): Promise<Dataset> {
        const datasetApi = await firstValueFrom(this.datasetApi);

        return datasetApi.getDatasetHandler({
            dataset: name,
        });
    }

    async updateDataset(datasetName: string, update: UpdateDataset): Promise<void> {
        const datasetApi = await firstValueFrom(this.datasetApi);

        return datasetApi.updateDatasetHandler({dataset: datasetName, updateDataset: update});
    }

    async getDatasets(offset = 0, limit = 20, filter?: string): Promise<DatasetListing[]> {
        const datasetApi = await firstValueFrom(this.datasetApi);

        return datasetApi.listDatasetsHandler({
            order: OrderBy.NameAsc,
            offset,
            limit,
            filter,
        });
    }

    async getLoadingInfo(datasetName: string): Promise<MetaDataDefinition> {
        const datasetApi = await firstValueFrom(this.datasetApi);

        return datasetApi.getLoadingInfoHandler({
            dataset: datasetName,
        });
    }

    async suggestMetaData(suggest: SuggestMetaDataHandlerRequest): Promise<MetaDataSuggestion> {
        const datasetApi = await firstValueFrom(this.datasetApi);

        return datasetApi.suggestMetaDataHandler(suggest);
    }

    async updateLoadingInfo(datasetName: string, metaDataDefinition: MetaDataDefinition): Promise<void> {
        const datasetApi = await firstValueFrom(this.datasetApi);

        return datasetApi.updateLoadingInfoHandler({
            dataset: datasetName,
            metaDataDefinition,
        });
    }

    async updateSymbology(datasetName: string, symbology: Symbology): Promise<void> {
        const datasetApi = await firstValueFrom(this.datasetApi);

        return datasetApi.updateDatasetSymbologyHandler({
            dataset: datasetName,
            symbology,
        });
    }

    async updateProvenance(datasetName: string, provenances: Array<Provenance>): Promise<void> {
        const datasetApi = await firstValueFrom(this.datasetApi);

        return datasetApi.updateDatasetProvenanceHandler({
            dataset: datasetName,
            provenances: {
                provenances,
            },
        });
    }

    async deleteDataset(datasetName: string): Promise<void> {
        const datasetApi = await firstValueFrom(this.datasetApi);

        return datasetApi.deleteDatasetHandler({
            dataset: datasetName,
        });
    }

    async createDataset(dataPath: DataPath, definition: DatasetDefinition): Promise<string> {
        const datasetApi = await firstValueFrom(this.datasetApi);

        return datasetApi
            .createDatasetHandler({
                createDataset: {
                    dataPath,
                    definition,
                },
            })
            .then((response) => response.datasetName);
    }

    async getVolumes(): Promise<Volume[]> {
        const datasetApi = await firstValueFrom(this.datasetApi);

        return datasetApi.listVolumesHandler();
    }

    async getVolumeFileLayers(volumeName: string, fileName: string): Promise<VolumeFileLayersResponse> {
        const uploadsApi = await firstValueFrom(this.datasetApi);

        return uploadsApi.listVolumeFileLayersHandler({volumeName, fileName});
    }
}

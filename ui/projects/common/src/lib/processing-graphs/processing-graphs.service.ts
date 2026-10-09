import {Injectable, inject} from '@angular/core';
import {ProcessingGraph, ProcessingGraphsApi, ProvenanceEntry, TypedResultDescriptor} from '@geoengine/api-client';
import {ReplaySubject, firstValueFrom} from 'rxjs';
import {UserService, apiConfigurationWithAccessKey} from '../user/user.service';
import {UUID} from '../datasets/dataset.model';

@Injectable({
    providedIn: 'root',
})
export class ProcessingGraphsService {
    private sessionService = inject(UserService);

    processingGraphsApi = new ReplaySubject<ProcessingGraphsApi>(1);

    constructor() {
        this.sessionService.getSessionStream().subscribe({
            next: (session) => this.processingGraphsApi.next(new ProcessingGraphsApi(apiConfigurationWithAccessKey(session.sessionToken))),
        });
    }

    async getProcessingGraph(id: UUID): Promise<ProcessingGraph> {
        const processingGraphsApi = await firstValueFrom(this.processingGraphsApi);

        return processingGraphsApi.loadProcessingGraphHandler({
            id,
        });
    }

    async getMetadata(id: UUID): Promise<TypedResultDescriptor> {
        const processingGraphsApi = await firstValueFrom(this.processingGraphsApi);

        return processingGraphsApi.getProcessingGraphMetadataHandler({
            id,
        });
    }

    async getProvenance(id: UUID): Promise<Array<ProvenanceEntry>> {
        const processingGraphsApi = await firstValueFrom(this.processingGraphsApi);

        return processingGraphsApi.getProcessingGraphProvenanceHandler({
            id,
        });
    }

    /**
     * Downloads a ZIP archive of the processing graph, its provenance and the output metadata.
     * Returns the archive together with the response headers, e.g., to derive the filename.
     */
    async getMetadataZip(id: UUID): Promise<{blob: Blob; headers: Headers}> {
        const processingGraphsApi = await firstValueFrom(this.processingGraphsApi);

        const response = await processingGraphsApi.getProcessingGraphAllMetadataZipHandlerRaw({
            id,
        });

        return {blob: await response.value(), headers: response.raw.headers};
    }

    async registerProcessingGraph(processingGraph: ProcessingGraph): Promise<UUID> {
        const processingGraphsApi = await firstValueFrom(this.processingGraphsApi);

        return processingGraphsApi
            .registerProcessingGraphHandler({
                processingGraph,
            })
            .then((response) => response.id);
    }
}

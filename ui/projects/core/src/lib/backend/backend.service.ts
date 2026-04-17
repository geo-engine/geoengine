import {Injectable, inject} from '@angular/core';
import {HttpClient, HttpEvent, HttpHeaders, HttpParams} from '@angular/common/http';
import {Observable, Subject} from 'rxjs';
import {CoreConfig} from '../config.service';
import {
    BBoxDict,
    CreateProjectResponseDict,
    PlotDict,
    ProjectFilterDict,
    ProjectListingDict,
    ProjectOrderByDict,
    ProjectPermissionDict,
    RegisterWorkflowResultDict,
    RegistrationDict,
    STRectangleDict,
    SrsString,
    TimeIntervalDict,
    TimeStepDict,
    UUID,
    PlotDataDict,
    UploadResponseDict,
    AutoCreateDatasetDict,
    DatasetNameResponseDict,
    SpatialReferenceSpecificationDict,
    DataSetProviderListingDict,
    ProvenanceEntryDict,
    DatasetOrderByDict,
    LayerCollectionDict,
    BackendInfoDict,
    WcsParamsDict,
    QuotaDict,
    WorkflowIdResponseDict,
    TaskStatusDict,
    TaskStatusType,
    UploadFilesResponseDict,
    UploadFileLayersResponseDict,
    RoleDescription,
    WfsParamsDict,
} from './backend.model';
import {
    ProjectLayer as ProjectLayerDict,
    Project as ProjectDict,
    Dataset as DatasetDict,
    Layer as LayerDict,
    Workflow as WorkflowDict,
    TypedResultDescriptor,
} from '@geoengine/api-client';
import {bboxDictToExtent, unixTimestampToIsoString} from '@geoengine/common';

@Injectable({
    providedIn: 'root',
})
export class BackendService {
    protected readonly http = inject(HttpClient);
    protected readonly config = inject(CoreConfig);

    get wmsBaseUrl(): string {
        return `${this.config.API_URL}/wms`;
    }

    get wcsBaseUrl(): string {
        return `${this.config.API_URL}/wcs`;
    }

    registerUser(request: {email: string; password: string; realName: string}): Observable<RegistrationDict> {
        return this.http.post<RegistrationDict>(this.config.API_URL + '/user', request);
    }

    createProject(
        request: {
            name: string;
            description: string;
            bounds: STRectangleDict;
            timeStep: TimeStepDict;
        },
        sessionId: UUID,
    ): Observable<CreateProjectResponseDict> {
        return this.http.post<CreateProjectResponseDict>(this.config.API_URL + '/project', request, {
            headers: BackendService.authorizationHeader(sessionId),
        });
    }

    updateProject(
        request: {
            id: UUID;
            name?: string;
            description?: string;
            layers?: Array<ProjectLayerDict | 'none' | 'delete'>;
            plots?: Array<PlotDict | 'none' | 'delete'>;
            bounds?: STRectangleDict;
            timeStep?: TimeStepDict;
        },
        sessionId: UUID,
    ): Observable<void> {
        return this.http.patch<void>(`${this.config.API_URL}/project/${request.id}`, request, {
            headers: BackendService.authorizationHeader(sessionId),
        });
    }

    deleteProject(projectId: UUID, sessionId: UUID): Observable<void> {
        return this.http.get<void>(`${this.config.API_URL}/project/${projectId}`, {
            headers: BackendService.authorizationHeader(sessionId),
        });
    }

    loadProject(projectId: UUID, sessionId: UUID, projectVersionId?: UUID): Observable<ProjectDict> {
        let requestUri = `${this.config.API_URL}/project/${projectId}`;
        if (projectVersionId) {
            requestUri += `/${projectVersionId}`;
        }

        return this.http.get<ProjectDict>(requestUri, {
            headers: BackendService.authorizationHeader(sessionId),
        });
    }

    listProjects(
        request: {
            permissions: Array<ProjectPermissionDict>;
            filter: ProjectFilterDict;
            order: ProjectOrderByDict;
            offset: number;
            limit: number;
        },
        sessionId: UUID,
    ): Observable<Array<ProjectListingDict>> {
        const params = new NullDiscardingHttpParams();
        params.setMapped('permissions', request.permissions, JSON.stringify);
        params.setMapped('filter', request.filter, (filter) => (filter === 'None' ? 'None' : JSON.stringify(filter)));
        params.set('order', request.order);
        params.setMapped('offset', request.offset, JSON.stringify);
        params.setMapped('limit', request.limit, JSON.stringify);

        return this.http.get<Array<ProjectListingDict>>(this.config.API_URL + '/projects', {
            params: params.httpParams,
            headers: BackendService.authorizationHeader(sessionId),
        });
    }

    registerWorkflow(workflow: WorkflowDict, sessionId: UUID): Observable<RegisterWorkflowResultDict> {
        return this.http.post<RegisterWorkflowResultDict>(this.config.API_URL + '/workflow', workflow, {
            headers: BackendService.authorizationHeader(sessionId),
        });
    }

    getWorkflow(workflowId: UUID, sessionId: UUID): Observable<WorkflowDict> {
        return this.http.get<WorkflowDict>(this.config.API_URL + `/workflow/${workflowId}`, {
            headers: BackendService.authorizationHeader(sessionId),
        });
    }

    getWorkflowMetadata(workflowId: UUID, sessionId: UUID): Observable<TypedResultDescriptor> {
        return this.http.get<TypedResultDescriptor>(this.config.API_URL + `/workflow/${workflowId}/metadata`, {
            headers: BackendService.authorizationHeader(sessionId),
        });
    }

    getBackendInfo(): Observable<BackendInfoDict> {
        return this.http.get<BackendInfoDict>(this.config.API_URL + '/info');
    }

    getBackendAvailable(): Observable<void> {
        return this.http.get<void>(this.config.API_URL + '/available');
    }

    getWorkflowProvenance(workflowId: UUID, sessionId: UUID): Observable<Array<ProvenanceEntryDict>> {
        return this.http.get<Array<ProvenanceEntryDict>>(this.config.API_URL + `/workflow/${workflowId}/provenance`, {
            headers: BackendService.authorizationHeader(sessionId),
        });
    }

    downloadWorkflowMetadata(workflowId: UUID, sessionId: UUID): Observable<HttpEvent<Blob>> {
        return this.http.get(this.config.API_URL + `/workflow/${workflowId}/allMetadata/zip`, {
            headers: BackendService.authorizationHeader(sessionId),
            responseType: 'blob',
            reportProgress: true,
            observe: 'events',
        });
    }

    downloadRasterLayer(workflowId: UUID, sessionId: UUID, wcsParams: WcsParamsDict): Observable<HttpEvent<Blob>> {
        const params = new NullDiscardingHttpParams();

        params.set('service', wcsParams.service);
        params.set('request', wcsParams.request);
        params.set('version', wcsParams.version);
        params.set('identifier', wcsParams.identifier);
        params.set('boundingbox', wcsParams.boundingbox);
        params.set('format', wcsParams.format);
        params.set('gridbasecrs', wcsParams.gridbasecrs);
        params.set('gridcs', wcsParams.gridcs);
        params.set('gridtype', wcsParams.gridtype);
        params.set('gridorigin', wcsParams.gridorigin);
        params.set('gridoffsets', wcsParams.gridoffsets);
        params.set('time', wcsParams.time);
        params.set('nodatavalue', wcsParams.nodatavalue);

        return this.http.get(this.config.API_URL + `/wcs/${workflowId}`, {
            headers: BackendService.authorizationHeader(sessionId),
            params: params.httpParams,
            responseType: 'blob',
            reportProgress: true,
            observe: 'events',
        });
    }

    setSessionProject(projectId: UUID, sessionId: UUID): Observable<void> {
        const response = new Subject<void>();
        this.http
            .post<void>(`${this.config.API_URL}/session/project/${projectId}`, null, {
                headers: BackendService.authorizationHeader(sessionId),
            })
            .subscribe(response);
        return response;
    }

    wfsGetFeature(request: WfsParamsDict, sessionId: UUID): Observable<Record<string, unknown>> {
        const params = new NullDiscardingHttpParams();

        params.set('service', 'WFS');
        params.set('version', '2.0.0');
        params.set('request', 'GetFeature');
        params.set('outputFormat', 'application/json');

        params.set('typeNames', `${request.workflowId}`);
        params.setMapped('bbox', request.bbox, (bbox) => bboxDictToExtent(bbox).join(','));
        params.setMapped('time', request.time, (time) => `${unixTimestampToIsoString(time.start)}/${unixTimestampToIsoString(time.end)}`);
        params.set('srsName', request.srsName);

        // these probably do not work yet
        params.set('namespaces', request.namespaces);
        params.setMapped('count', request.count, JSON.stringify);
        params.set('sortBy', request.sortBy);
        params.set('resultType', request.resultType);
        params.set('filter', request.filter);
        params.set('propertyName', request.propertyName);

        return this.http.get<Record<string, unknown>>(`${this.config.API_URL}/wfs/${request.workflowId}`, {
            headers: BackendService.authorizationHeader(sessionId),
            params: params.httpParams,
        });
    }

    getPlot(
        workflowId: UUID,
        request: {
            bbox: BBoxDict;
            crs: SrsString;
            time: TimeIntervalDict;
            spatialResolution: [number, number];
        },
        sessionId: UUID,
    ): Observable<PlotDataDict> {
        const params = new NullDiscardingHttpParams();

        params.setMapped('bbox', request.bbox, (bbox) => bboxDictToExtent(bbox).join(','));
        params.set('crs', request.crs);
        params.setMapped('time', request.time, (time) => `${unixTimestampToIsoString(time.start)}/${unixTimestampToIsoString(time.end)}`);
        params.setMapped('spatialResolution', request.spatialResolution, (resolution) => resolution.join(','));

        return this.http.get<PlotDataDict>(this.config.API_URL + `/plot/${workflowId}`, {
            headers: BackendService.authorizationHeader(sessionId),
            params: params.httpParams,
        });
    }

    getDataset(sessionId: UUID, datasetName: string): Observable<DatasetDict> {
        return this.http.get<DatasetDict>(this.config.API_URL + `/dataset/${datasetName}`, {
            headers: BackendService.authorizationHeader(sessionId),
        });
    }

    getDatasets(sessionId: UUID, offset = 0, limit = 20, order: DatasetOrderByDict = 'NameAsc'): Observable<Array<DatasetDict>> {
        const params = new NullDiscardingHttpParams();
        params.setMapped('offset', offset, (r) => r.toString());
        params.setMapped('limit', limit, (r) => r.toString());
        params.set('order', order);

        return this.http.get<Array<DatasetDict>>(this.config.API_URL + '/datasets', {
            params: params.httpParams,
            headers: BackendService.authorizationHeader(sessionId),
        });
    }

    upload(sessionId: UUID, form: FormData): Observable<HttpEvent<UploadResponseDict>> {
        return this.http.post<UploadResponseDict>(this.config.API_URL + '/upload', form, {
            headers: BackendService.authorizationHeader(sessionId),
            reportProgress: true,
            observe: 'events',
        });
    }

    getUploadFiles(sessionId: UUID, uploadId: UUID): Observable<UploadFilesResponseDict> {
        return this.http.get<UploadFilesResponseDict>(this.config.API_URL + `/uploads/${uploadId}/files`, {
            headers: BackendService.authorizationHeader(sessionId),
        });
    }

    getUploadFileLayers(sessionId: UUID, uploadId: UUID, fileName: string): Observable<UploadFileLayersResponseDict> {
        return this.http.get<UploadFileLayersResponseDict>(this.config.API_URL + `/uploads/${uploadId}/files/${fileName}/layers`, {
            headers: BackendService.authorizationHeader(sessionId),
        });
    }

    autoCreateDataset(sessionId: UUID, createDataset: AutoCreateDatasetDict): Observable<DatasetNameResponseDict> {
        return this.http.post<DatasetNameResponseDict>(this.config.API_URL + '/dataset/auto', createDataset, {
            headers: BackendService.authorizationHeader(sessionId),
        });
    }

    getSpatialReferenceSpecification(sessionId: UUID, srsString: SrsString): Observable<SpatialReferenceSpecificationDict> {
        return this.http.get<SpatialReferenceSpecificationDict>(this.config.API_URL + `/spatialReferenceSpecification/${srsString}`, {
            headers: BackendService.authorizationHeader(sessionId),
        });
    }

    getDatasetProviders(sessionId: UUID): Observable<Array<DataSetProviderListingDict>> {
        const params = new NullDiscardingHttpParams();
        params.set('order', 'NameAsc');
        params.set('offset', '0');
        params.set('limit', '20');

        return this.http.get<Array<DataSetProviderListingDict>>(this.config.API_URL + '/providers', {
            params: params.httpParams,
            headers: BackendService.authorizationHeader(sessionId),
        });
    }

    getLayerCollectionItems(sessionId: UUID, provider: UUID, collection: string, offset = 0, limit = 20): Observable<LayerCollectionDict> {
        const params = new NullDiscardingHttpParams();
        params.setMapped('offset', offset, (r) => r.toString());
        params.setMapped('limit', limit, (r) => r.toString());

        return this.http.get<LayerCollectionDict>(
            this.config.API_URL + `/layers/collections/${provider}/${encodeURIComponent(collection)}`,
            {
                params: params.httpParams,
                headers: BackendService.authorizationHeader(sessionId),
            },
        );
    }

    getRootLayerCollectionItems(sessionId: UUID, offset = 0, limit = 20): Observable<LayerCollectionDict> {
        const params = new NullDiscardingHttpParams();
        params.setMapped('offset', offset, (r) => r.toString());
        params.setMapped('limit', limit, (r) => r.toString());

        return this.http.get<LayerCollectionDict>(this.config.API_URL + '/layers/collections', {
            params: params.httpParams,
            headers: BackendService.authorizationHeader(sessionId),
        });
    }

    getLayerCollectionLayer(sessionId: UUID, provider: UUID, layer: string): Observable<LayerDict> {
        return this.http.get<LayerDict>(this.config.API_URL + `/layers/${provider}/${encodeURIComponent(layer)}`, {
            headers: BackendService.authorizationHeader(sessionId),
        });
    }

    public static authorizationHeader(sessionId: UUID): HttpHeaders {
        return new HttpHeaders().set('Authorization', `Bearer ${sessionId}`);
    }

    registerWorkflowForLayer(sessionId: UUID, provider: UUID, layer: string): Observable<WorkflowIdResponseDict> {
        return this.http.post<WorkflowIdResponseDict>(
            this.config.API_URL + `/layers/${provider}/${encodeURIComponent(layer)}/workflowId`,
            null,
            {
                headers: BackendService.authorizationHeader(sessionId),
            },
        );
    }

    getQuota(sessionId: UUID): Observable<QuotaDict> {
        return this.http.get<QuotaDict>(this.config.API_URL + '/quota', {
            headers: BackendService.authorizationHeader(sessionId),
        });
    }

    getTasksList(sessionId: UUID, filter: TaskStatusType | undefined, offset: number, limit: number): Observable<Array<TaskStatusDict>> {
        const params = new NullDiscardingHttpParams();
        params.set('filter', filter);
        params.setMapped('offset', offset, (r) => r.toString());
        params.setMapped('limit', limit, (r) => r.toString());

        return this.http.get<Array<TaskStatusDict>>(this.config.API_URL + '/tasks/list', {
            headers: BackendService.authorizationHeader(sessionId),
            params: params.httpParams,
        });
    }

    abortTask(sessionId: UUID, taskId: UUID, force = false): Observable<void> {
        const params = new NullDiscardingHttpParams();
        params.setMapped('force', force, (r) => r.toString());

        return this.http.delete<void>(this.config.API_URL + `/tasks/${taskId}`, {
            headers: BackendService.authorizationHeader(sessionId),
            params: params.httpParams,
        });
    }

    getRoleDescriptions(sessionId: UUID): Observable<Array<RoleDescription>> {
        return this.http.get<Array<RoleDescription>>(this.config.API_URL + '/user/roles/descriptions', {
            headers: BackendService.authorizationHeader(sessionId),
        });
    }
}

/**
 * A wrapper around `HttpParams` that automatically discards operations with empty values.
 */
class NullDiscardingHttpParams {
    httpParams: HttpParams = new HttpParams();

    set(param: string, value: string | undefined): void {
        if (value === undefined || value === null) {
            return;
        }

        this.httpParams = this.httpParams.set(param, value);
    }

    setMapped<V>(param: string, value: V | undefined, transform: (v: V) => string): void {
        if (value === undefined || value === null) {
            return;
        }

        this.httpParams = this.httpParams.set(param, transform(value));
    }
}

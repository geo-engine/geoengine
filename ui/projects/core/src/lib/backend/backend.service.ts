import {HttpClient, HttpEvent, HttpHeaders, HttpParams} from '@angular/common/http';
import {Injectable, inject} from '@angular/core';
import {Observable} from 'rxjs';
import {Dataset as DatasetDict, Layer as LayerDict} from '@geoengine/api-client';
import {bboxDictToExtent, unixTimestampToIsoString} from '@geoengine/common';
import {CoreConfig} from '../config.service';
import {
    SrsString,
    UUID,
    UploadResponseDict,
    AutoCreateDatasetDict,
    DatasetNameResponseDict,
    SpatialReferenceSpecificationDict,
    DataSetProviderListingDict,
    DatasetOrderByDict,
    LayerCollectionDict,
    BackendInfoDict,
    WcsParamsDict,
    QuotaDict,
    TaskStatusDict,
    TaskStatusType,
    UploadFilesResponseDict,
    UploadFileLayersResponseDict,
    RoleDescription,
    WfsParamsDict,
} from './backend.model';

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

    get ogcApiBaseUrl(): string {
        return `${this.config.API_URL}/ogc`;
    }

    getBackendInfo(): Observable<BackendInfoDict> {
        return this.http.get<BackendInfoDict>(this.config.API_URL + '/info');
    }

    getBackendAvailable(): Observable<void> {
        return this.http.get<void>(this.config.API_URL + '/available');
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

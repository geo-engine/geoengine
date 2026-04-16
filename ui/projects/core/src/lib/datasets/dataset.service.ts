import {Injectable, inject} from '@angular/core';
import {BackendService} from '../backend/backend.service';
import {Observable} from 'rxjs';
import {map, mergeMap} from 'rxjs/operators';
import {HttpEvent} from '@angular/common/http';
import {
    AutoCreateDatasetDict,
    DatasetNameResponseDict,
    UploadFileLayersResponseDict,
    UploadFilesResponseDict,
    UploadResponseDict,
    UUID,
} from '../backend/backend.model';
import {ProjectService} from '../project/project.service';
import {
    ClusteredPointSymbology,
    Dataset,
    Layer,
    LineSymbology,
    PointSymbology,
    PolygonSymbology,
    RandomColorService,
    RasterLayer,
    RasterSymbology,
    UserService,
    VectorDataTypes,
    VectorLayer,
    VectorResultDescriptor,
    VectorSymbology,
    colorToDict,
} from '@geoengine/common';

import {Workflow as WorkflowDict} from '@geoengine/api-client';

@Injectable({
    providedIn: 'root',
})
export class DatasetService {
    protected backend = inject(BackendService);
    protected userService = inject(UserService);
    protected projectService = inject(ProjectService);
    protected randomColorService = inject(RandomColorService);

    getDatasets(offset = 0, limit = 20): Observable<Array<Dataset>> {
        return this.userService.getSessionStream().pipe(
            mergeMap((session) => this.backend.getDatasets(session.sessionToken, offset, limit)),
            map((datasetDicts) => datasetDicts.map((dict) => Dataset.fromDict(dict))),
        );
    }

    getDataset(name: string): Observable<Dataset> {
        return this.userService.getSessionTokenForRequest().pipe(
            mergeMap((token) => this.backend.getDataset(token, name)),
            map((dict) => Dataset.fromDict(dict)),
        );
    }
    upload(form: FormData): Observable<HttpEvent<UploadResponseDict>> {
        return this.userService.getSessionTokenForRequest().pipe(mergeMap((token) => this.backend.upload(token, form)));
    }

    getUploadFiles(uploadId: UUID): Observable<UploadFilesResponseDict> {
        return this.userService.getSessionTokenForRequest().pipe(mergeMap((token) => this.backend.getUploadFiles(token, uploadId)));
    }

    getUploadFileLayers(uploadId: UUID, fileName: string): Observable<UploadFileLayersResponseDict> {
        return this.userService
            .getSessionTokenForRequest()
            .pipe(mergeMap((token) => this.backend.getUploadFileLayers(token, uploadId, fileName)));
    }

    autoCreateDataset(create: AutoCreateDatasetDict): Observable<DatasetNameResponseDict> {
        return this.userService.getSessionTokenForRequest().pipe(mergeMap((token) => this.backend.autoCreateDataset(token, create)));
    }

    addDatasetToMap(dataset: Dataset): Observable<void> {
        const workflow = dataset.createSourceWorkflow();
        return this.addDatasetToMapWithSourceWorkflow(dataset, workflow);
    }

    addDatasetToMapWithSourceWorkflow(dataset: Dataset, workflow: WorkflowDict): Observable<void> {
        return this.createLayerFromDatasetWithWorkflow(dataset, workflow).pipe(mergeMap((layer) => this.projectService.addLayer(layer)));
    }

    createLayerFromDataset(dataset: Dataset): Observable<Layer> {
        const workflow = dataset.createSourceWorkflow();
        return this.createLayerFromDatasetWithWorkflow(dataset, workflow);
    }

    createLayerFromDatasetWithWorkflow(dataset: Dataset, workflow: WorkflowDict): Observable<Layer> {
        return this.projectService.registerWorkflow(workflow).pipe(map((workflowId) => this.createLayer(workflowId, dataset)));
    }

    createLayer(workflowId: string, dataset: Dataset): Layer {
        if (dataset.resultDescriptor.getTypeString() === 'Raster') {
            const symbology = dataset.symbology as RasterSymbology;
            return new RasterLayer({
                workflowId,
                name: dataset.displayName,
                symbology: symbology
                    ? symbology
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
                isLegendVisible: false,
                isVisible: true,
            });
        } else {
            const resultDescriptor = dataset.resultDescriptor as VectorResultDescriptor;

            let symbology: VectorSymbology;

            switch (resultDescriptor.dataType) {
                case VectorDataTypes.MultiPoint:
                    symbology = (dataset.symbology as PointSymbology)
                        ? (dataset.symbology as PointSymbology)
                        : ClusteredPointSymbology.fromPointSymbologyDict({
                              type: 'point',
                              radius: {
                                  type: 'static',
                                  value: PointSymbology.DEFAULT_POINT_RADIUS,
                              },
                              stroke: {
                                  width: {
                                      type: 'static',
                                      value: 1,
                                  },
                                  color: {
                                      type: 'static',
                                      color: [0, 0, 0, 255],
                                  },
                              },
                              fillColor: {
                                  type: 'static',
                                  color: colorToDict(this.randomColorService.getRandomColorRgba()),
                              },
                          });
                    break;
                case VectorDataTypes.MultiLineString:
                    symbology = (dataset.symbology as LineSymbology)
                        ? (dataset.symbology as LineSymbology)
                        : LineSymbology.fromLineSymbologyDict({
                              type: 'line',
                              stroke: {
                                  width: {type: 'static', value: 1},
                                  color: {
                                      type: 'static',
                                      color: colorToDict(this.randomColorService.getRandomColorRgba()),
                                  },
                              },
                              autoSimplified: true,
                          });
                    break;
                case VectorDataTypes.MultiPolygon:
                    symbology = (dataset.symbology as PolygonSymbology)
                        ? (dataset.symbology as PolygonSymbology)
                        : PolygonSymbology.fromPolygonSymbologyDict({
                              type: 'polygon',
                              stroke: {
                                  width: {
                                      type: 'static',
                                      value: 1,
                                  },
                                  color: {
                                      type: 'static',
                                      color: [0, 0, 0, 255],
                                  },
                              },
                              fillColor: {
                                  type: 'static',
                                  color: colorToDict(this.randomColorService.getRandomColorRgba()),
                              },
                              autoSimplified: true,
                          });
                    break;
                default:
                    throw Error('unknown symbology type');
            }

            return new VectorLayer({
                workflowId,
                name: dataset.displayName,
                symbology,
                isLegendVisible: false,
                isVisible: true,
            });
        }
    }
}

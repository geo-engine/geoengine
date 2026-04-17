import {Component, ChangeDetectionStrategy, inject} from '@angular/core';
import {BackendService, BBoxDict, ProjectService, SourceOperatorDict, RasterResultDescriptorDict} from '@geoengine/core';
import {first, map, mergeMap, tap} from 'rxjs/operators';
import {DataSelectionService} from '../data-selection.service';
import {BehaviorSubject, combineLatest, Observable, of} from 'rxjs';
import {COUNTRY_METADATA, countryDatasetName} from './country-data.model';
import {
    ExpressionDict,
    HistogramDict,
    HistogramParams,
    PolygonSymbology,
    RasterDataTypes,
    RasterLayer,
    RasterStackerDict,
    ReprojectionDict,
    UserService,
    VectorLayer,
    CommonModule,
} from '@geoengine/common';
import {Workflow as WorkflowDict} from '@geoengine/api-client';
import {MatFormField, MatLabel} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {MatOption} from '@angular/material/autocomplete';
import {MatSelectSearchComponent} from 'ngx-mat-select-search';
import {FormsModule, ReactiveFormsModule} from '@angular/forms';
import {MatButton} from '@angular/material/button';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {AsyncPipe} from '@angular/common';

@Component({
    selector: 'geoengine-analysis',
    templateUrl: './analysis.component.html',
    styleUrls: ['./analysis.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        MatFormField,
        MatLabel,
        MatSelect,
        MatOption,
        MatSelectSearchComponent,
        CommonModule,
        FormsModule,
        ReactiveFormsModule,
        MatButton,
        MatProgressSpinner,
        AsyncPipe,
    ],
})
export class AnalysisComponent {
    private readonly projectService = inject(ProjectService);
    private readonly dataSelectionService = inject(DataSelectionService);
    private readonly backend = inject(BackendService);
    private readonly userService = inject(UserService);

    countries = new Array<string>();

    cannotComputePlot$: Observable<boolean>;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    plotData = new BehaviorSubject<any>(undefined);
    plotLoading = new BehaviorSubject(false);

    private selectedCountryName?: string = undefined;

    constructor() {
        this.cannotComputePlot$ = combineLatest([this.dataSelectionService.rasterLayer, this.dataSelectionService.polygonLayer]).pipe(
            map(([rasterLayer, polygonLayer]) => !rasterLayer || !polygonLayer),
        );

        for (const country of Object.keys(COUNTRY_METADATA)) {
            this.countries.push(country[0]);
        }
        this.countries.sort();
    }

    selectCountry(country: string): void {
        this.selectedCountryName = country;

        const workflow: WorkflowDict = {
            type: 'Vector',
            operator: {
                type: 'OgrSource',
                params: {
                    data: 'polygon_country_' + countryDatasetName(this.selectedCountryName),
                },
            },
        };

        this.projectService
            .registerWorkflow(workflow)
            .pipe(
                mergeMap((workflowId) =>
                    this.dataSelectionService.setPolygonLayer(
                        new VectorLayer({
                            workflowId,
                            name: country,
                            symbology: PolygonSymbology.fromPolygonSymbologyDict({
                                type: 'polygon',
                                stroke: {
                                    width: {
                                        type: 'static',
                                        value: 2,
                                    },
                                    color: {
                                        type: 'static',
                                        color: [54, 154, 203, 255],
                                    },
                                },
                                fillColor: {
                                    type: 'static',
                                    color: [54, 154, 203, 0],
                                },
                                autoSimplified: true,
                            }),
                            isLegendVisible: false,
                            isVisible: true,
                        }),
                    ),
                ),
            )
            .subscribe(() => {
                // success
            });
    }

    countryPredicate(filterString: string, element: string): boolean {
        return element.toLowerCase().includes(filterString);
    }

    computePlot(): void {
        if (!this.selectedCountryName) {
            return;
        }

        const countryMetadata = COUNTRY_METADATA.filter(([countryName]) => this.selectedCountryName === countryName);
        if (countryMetadata.length !== 1) {
            throw Error(`there is not metadata for country ${this.selectedCountryName}`);
        }
        const [, xmax, ymax, xmin, ymin] = countryMetadata[0];
        const countryBounds: BBoxDict = {
            lowerLeftCoordinate: {
                x: xmin,
                y: ymin,
            },
            upperRightCoordinate: {
                x: xmax,
                y: ymax,
            },
        };

        const countryRasterWorkflow: SourceOperatorDict = {
            type: 'GdalSource',
            params: {
                data: 'raster_country_' + countryDatasetName(this.selectedCountryName),
            },
        };

        combineLatest([
            this.dataSelectionService.rasterLayer.pipe(
                mergeMap<RasterLayer | undefined, Observable<WorkflowDict>>((layer) => {
                    if (!layer) {
                        return of(); // no next, just complete
                    }

                    return this.projectService.getWorkflow(layer.workflowId);
                }),
            ),
            this.dataSelectionService.rasterLayer.pipe(
                mergeMap<RasterLayer | undefined, Observable<RasterResultDescriptorDict>>((layer) => {
                    if (!layer) {
                        return of(); // no next, just complete
                    }

                    return this.projectService.getWorkflowMetaData(layer.workflowId) as Observable<RasterResultDescriptorDict>;
                }),
            ),
            this.dataSelectionService.dataRange,
        ])
            .pipe(
                first(),
                tap(() => {
                    this.plotLoading.next(true);
                    this.plotData.next(undefined);
                }),
                mergeMap(([rasterWorkflow, rasterResultDescriptor, dataRange]) =>
                    this.projectService.registerWorkflow({
                        type: 'Plot',
                        operator: {
                            type: 'Histogram',
                            params: {
                                // TODO: get params from selected data
                                buckets: {
                                    type: 'number',
                                    value: 20,
                                },
                                bounds: dataRange,
                            } as HistogramParams,
                            sources: {
                                source: {
                                    type: 'Expression',
                                    params: {
                                        expression: 'if B != 0 { A } else { NODATA }',
                                        // TODO: get data type from data
                                        outputType: RasterDataTypes.Float64.getCode(),
                                        outputBand: {
                                            name: 'expression',
                                            measurement: rasterResultDescriptor.bands[0].measurement,
                                        },
                                        mapNoData: false,
                                    },
                                    sources: {
                                        raster: {
                                            type: 'RasterStacker',
                                            params: {
                                                renameBands: {
                                                    type: 'default',
                                                },
                                            },
                                            sources: {
                                                rasters: [
                                                    {
                                                        type: 'Reprojection',
                                                        params: {
                                                            // country rasters are in 4326
                                                            targetSpatialReference: 'EPSG:4326',
                                                        },
                                                        sources: {
                                                            source: rasterWorkflow.operator,
                                                        },
                                                    } as ReprojectionDict,
                                                    countryRasterWorkflow,
                                                ],
                                            },
                                        } as RasterStackerDict,
                                    },
                                } as ExpressionDict,
                            },
                        } as HistogramDict,
                    }),
                ),
                mergeMap((workflowId) =>
                    combineLatest([
                        of(workflowId),
                        this.userService.getSessionTokenForRequest(),
                        this.projectService.getTimeOnce(),
                        this.projectService.getSpatialReferenceStream(),
                    ]),
                ),
                mergeMap(([workflowId, sessionToken, time, crs]) =>
                    this.backend.getPlot(
                        workflowId,
                        {
                            time: time.toDict(),
                            bbox: countryBounds,
                            crs: crs.srsString,
                            // TODO: set reasonable size
                            spatialResolution: [0.1, 0.1],
                        },
                        sessionToken,
                    ),
                ),
            )
            .subscribe({
                next: (plotData) => {
                    this.plotData.next(plotData.data);
                    this.plotLoading.next(false);
                },
                complete: () => {
                    // TODO: react on error?
                    this.plotLoading.next(false);
                },
            });
    }
}

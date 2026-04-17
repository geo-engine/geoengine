import {
    AfterViewInit,
    ChangeDetectionStrategy,
    ChangeDetectorRef,
    Component,
    ElementRef,
    inject,
    signal,
    viewChild,
    WritableSignal,
} from '@angular/core';
import {Breakpoints, BreakpointObserver} from '@angular/cdk/layout';
import {AsyncPipe} from '@angular/common';
import {MatGridListModule} from '@angular/material/grid-list';
import {MatMenuModule} from '@angular/material/menu';
import {MatIconModule} from '@angular/material/icon';
import {MatButtonModule} from '@angular/material/button';
import {MatCardModule} from '@angular/material/card';
import {firstValueFrom, lastValueFrom} from 'rxjs';
import {
    AutoCreateDatasetDict,
    BackendService,
    CoreModule,
    DatasetService,
    Extent,
    LayoutService,
    MapContainerComponent,
    ProjectService,
    UploadResponseDict,
    WGS_84,
} from '@geoengine/core';
import {
    ALL_COLORMAPS,
    Color,
    ColorMapSelectorComponent,
    extentToBboxDict,
    HistogramDict,
    LinearGradient,
    NotificationService,
    PaletteColorizer,
    PolygonSymbology,
    RasterLayer,
    RasterSymbology,
    SingleBandRasterColorizer,
    Time,
    TRANSPARENT,
    UserService,
    VectorLayer,
} from '@geoengine/common';
import {utc} from 'moment';
import {DataRange, DataSelectionService} from '../data-selection.service';
import {MatSelectChange} from '@angular/material/select';
import {Workflow} from '@geoengine/api-client';
import {createBox} from 'ol/interaction/Draw';
import OlFormatGeoJson from 'ol/format/GeoJSON';
import {HttpResponse} from '@angular/common/http';
import proj4 from 'proj4';
import {LegendComponent} from '../legend/legend.component';
import {toSignal} from '@angular/core/rxjs-interop';

interface Indicator {
    name: string;
    description: string;
    workflow: Workflow;
    symbology: RasterSymbology;
    dataRange: DataRange;
    measurement: 'continuous' | 'classification';
}

@Component({
    selector: 'geoengine-dashboard',
    templateUrl: './dashboard.component.html',
    styleUrl: './dashboard.component.scss',
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        CoreModule,
        AsyncPipe,
        MatGridListModule,
        MatMenuModule,
        MatIconModule,
        MatButtonModule,
        MatCardModule,
        LegendComponent,
        MapContainerComponent,
    ],
})
export class DashboardComponent implements AfterViewInit {
    readonly userService = inject(UserService);
    readonly dataSelectionService = inject(DataSelectionService);
    private readonly breakpointObserver = inject(BreakpointObserver);
    private readonly projectService = inject(ProjectService);
    private readonly notificationService = inject(NotificationService);
    private readonly datasetService = inject(DatasetService);
    private readonly changeDetectorRef = inject(ChangeDetectorRef);
    private readonly backend = inject(BackendService);

    indicators = INDICATORS;

    readonly isSelectingBox = signal(false);
    readonly selectedIndicator: WritableSignal<Indicator | undefined> = signal(undefined);
    readonly selectedBBox: WritableSignal<Extent | undefined> = signal(undefined);
    readonly isLandscape = signal(true);
    readonly plotWidthPx = signal(100);
    readonly plotHeightPx = signal(100);
    readonly layersReverse = toSignal(this.dataSelectionService.layers);
    /* eslint-disable @typescript-eslint/no-explicit-any */
    readonly plotData = signal<any>(undefined);
    readonly plotLoading = signal(false);

    readonly mapComponent = viewChild.required(MapContainerComponent);

    readonly analyzeCard = viewChild.required('analyzecard', {read: ElementRef});

    timeSteps: Time[] = [new Time(utc('2021-01-01')), new Time(utc('2022-01-01'))];

    // eslint-disable-next-line @typescript-eslint/no-misused-promises, @typescript-eslint/require-await
    async ngAfterViewInit(): Promise<void> {
        this.breakpointObserver.observe(Breakpoints.Web).subscribe((isLandscape) => {
            this.isLandscape.set(isLandscape.matches);
        });
    }

    async changeIndicator(event: MatSelectChange): Promise<void> {
        const indicator = event.value as Indicator;
        this.selectedIndicator.set(indicator);

        const workflowId = await firstValueFrom(this.projectService.registerWorkflow(indicator.workflow));

        const rasterLayer = new RasterLayer({
            name: 'EBV',
            workflowId,
            isVisible: true,
            isLegendVisible: false,
            symbology: indicator.symbology,
        });

        return await firstValueFrom(this.dataSelectionService.setRasterLayer(rasterLayer, this.timeSteps, indicator.dataRange));
    }

    selectBox(): void {
        this.isSelectingBox.set(true);
        this.notificationService.info('Select region on the map');

        // eslint-disable-next-line @typescript-eslint/no-misused-promises
        this.mapComponent().startDrawInteraction('Circle', true, createBox(), async (feature) => {
            const bbox = feature.getGeometry()?.getExtent();
            if (bbox) {
                this.selectedBBox.set([bbox[0], bbox[1], bbox[2], bbox[3]]);

                const olFeatureWriter = new OlFormatGeoJson();

                const geoJson = olFeatureWriter.writeFeatureObject(feature, {
                    featureProjection: WGS_84.spatialReference.srsString, // TODO
                    dataProjection: WGS_84.spatialReference.srsString,
                });

                const blob = new Blob([JSON.stringify(geoJson)], {type: 'application/json'});

                const form = new FormData();
                form.append('file', blob, 'draw.json');
                const uploadEvent = await lastValueFrom(this.datasetService.upload(form));
                const uploadDict = uploadEvent as unknown as HttpResponse<UploadResponseDict>;
                const uploadBody = uploadDict.body;
                if (!uploadBody) {
                    throw new Error('Upload failed');
                }

                const uploadId = uploadBody.id;

                const create: AutoCreateDatasetDict = {
                    upload: uploadId,
                    datasetName: await this.generateDatasetName(),
                    datasetDescription: '',
                    mainFile: 'draw.json',
                };

                const dataset = await firstValueFrom(this.datasetService.autoCreateDataset(create));

                const workflowId = await firstValueFrom(
                    this.projectService.registerWorkflow({
                        type: 'Vector',
                        operator: {
                            type: 'OgrSource',
                            params: {
                                data: dataset.datasetName,
                            },
                        },
                    }),
                );

                const observable = this.dataSelectionService.setPolygonLayer(
                    new VectorLayer({
                        workflowId,
                        name: 'drawn area',
                        symbology: PolygonSymbology.fromPolygonSymbologyDict({
                            type: 'polygon',
                            stroke: {
                                width: {
                                    type: 'static',
                                    value: 2,
                                },
                                color: {
                                    type: 'static',
                                    color: [128, 0, 0, 255],
                                },
                            },
                            fillColor: {
                                type: 'static',
                                color: [128, 0, 0, 128],
                            },
                            autoSimplified: true,
                        }),
                        isLegendVisible: false,
                        isVisible: true,
                    }),
                );

                await firstValueFrom(observable);
            }
            this.isSelectingBox.set(false);

            // this.setEditedExtent();
        });
    }

    private async generateDatasetName(): Promise<string> {
        const sessionId = await firstValueFrom(this.userService.getSessionTokenForRequest());
        const unixTime = Date.now();
        return `${sessionId}_${unixTime}`;
    }

    private computePlotSize(): void {
        const cardWidth = this.analyzeCard()?.nativeElement.clientWidth ?? 100;

        let plotWidth: number;

        if (this.isLandscape()) {
            plotWidth = cardWidth / 2 - 2 * LayoutService.remInPx;
        } else {
            plotWidth = cardWidth - 4 * LayoutService.remInPx;
            plotWidth = Math.min(plotWidth, window.innerWidth - 9 * LayoutService.remInPx);
        }

        const plotHeight = Math.min(plotWidth, window.innerHeight / 3);

        this.plotWidthPx.set(plotWidth);
        this.plotHeightPx.set(plotHeight);
    }

    async analyze(): Promise<void> {
        const indicator = this.selectedIndicator();
        const selectedBBox = this.selectedBBox();

        if (!indicator) {
            return;
        }

        if (!selectedBBox) {
            this.notificationService.error('Please select a region on the map');
            return;
        }

        this.plotData.set(undefined);

        this.computePlotSize();

        let workflow: Workflow;
        if (indicator.measurement === 'classification') {
            workflow = {
                type: 'Plot',
                operator: {
                    type: 'ClassHistogram',
                    params: {
                        columnName: null,
                    },
                    sources: {
                        source: indicator.workflow.operator,
                    },
                },
            };
        } else if (indicator.measurement === 'continuous') {
            workflow = {
                type: 'Plot',
                operator: {
                    type: 'Histogram',
                    params: {
                        attributeName: 'NDVI',
                        bounds: {
                            min: -1.0,
                            max: 1.0,
                        },
                        buckets: {
                            type: 'number',
                            value: 15,
                        },
                        interactive: false,
                    },
                    sources: {
                        source: indicator.workflow.operator,
                    },
                } as HistogramDict,
            };
        } else {
            this.notificationService.error('Invalid measurement for plotting');
            return;
        }

        this.plotLoading.set(true);

        const workflowId = await firstValueFrom(this.projectService.registerWorkflow(workflow));
        const sessionId = await firstValueFrom(this.userService.getSessionTokenForRequest());

        const time = await this.projectService.getTimeOnce();

        // reproject selectedBbox to EPSG:32632 using proj4

        // TODO: use proj string from spatial reference service
        const projString = '+proj=utm +zone=32 +datum=WGS84 +units=m +no_defs +type=crs';

        const ll = proj4('EPSG:4326', projString, [selectedBBox[0], selectedBBox[1]]);
        const ur = proj4('EPSG:4326', projString, [selectedBBox[2], selectedBBox[3]]);
        const extent = [ll[0], ll[1], ur[0], ur[1]];

        const plot = await firstValueFrom(
            this.backend.getPlot(
                workflowId,
                {
                    time: {
                        start: time.start.unix() * 1_000,
                        end: time.end.unix() * 1_000 + 1 /* add one millisecond to include the end time */,
                    },
                    bbox: extentToBboxDict([extent[0], extent[1], extent[2], extent[3]]),
                    // always use WGS 84 for computing the plot
                    crs: 'EPSG:32632',
                    spatialResolution: [10, 10],
                },
                sessionId,
            ),
        );

        this.plotData.set(plot.data);
        this.plotLoading.set(false);
    }

    async reset(): Promise<void> {
        this.selectedBBox.set(undefined);
        await firstValueFrom(this.dataSelectionService.clearPolygonLayer());
        this.changeDetectorRef.markForCheck();
    }
}

const INDICATORS: Array<Indicator> = [
    {
        name: 'Land type',
        description: 'Analyze the types of land within your selected area.',
        workflow: {
            type: 'Raster',
            operator: {
                type: 'GdalSource',
                params: {
                    data: 'rf_lucas_s2_3m_med_16in_sort_opset09_tile11',
                },
            },
        },
        symbology: new RasterSymbology(
            1.0,
            new SingleBandRasterColorizer(
                0,
                new PaletteColorizer(
                    new Map([
                        [0, Color.fromRgbaLike([0, 0, 0, 1])],
                        [1, Color.fromRgbaLike([0, 17, 255, 1])],
                        [2, Color.fromRgbaLike([0, 163, 255, 1])],
                        [3, Color.fromRgbaLike([64, 255, 182, 1])],
                        [4, Color.fromRgbaLike([182, 255, 64, 1])],
                        [5, Color.fromRgbaLike([255, 184, 0, 1])],
                        [6, Color.fromRgbaLike([255, 50, 0, 1])],
                        [7, Color.fromRgbaLike([128, 0, 0, 1])],
                    ]),
                    TRANSPARENT,
                    TRANSPARENT,
                ),
            ),
        ),
        dataRange: {min: 0, max: 7},
        measurement: 'classification',
    },
    {
        name: 'Vegetation',
        description: 'Assess the vegetation health and density in your chosen region.',
        workflow: {
            type: 'Raster',
            operator: {
                type: 'TemporalRasterAggregation',
                params: {
                    aggregation: {
                        type: 'mean',
                        ignoreNoData: true,
                        percentile: null,
                    },
                    window: {
                        granularity: 'years',
                        step: 1,
                    },
                    windowReference: null,
                    outputType: 'F32',
                },
                sources: {
                    raster: {
                        type: 'Expression',
                        params: {
                            expression: 'if (C == 3 || (C >= 7 && C <= 11)) { NODATA } else { (A - B) / (A + B) }',
                            outputType: 'F32',
                            outputBand: {
                                name: 'NDVI',
                                measurement: {
                                    type: 'continuous',
                                    measurement: 'NDVI',
                                    unit: 'NDVI',
                                },
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
                                            type: 'GdalSource',
                                            params: {
                                                data: 'sentinel2_10m_tile_11_band_B08_2022_2023_daily_raw_a',
                                            },
                                        },
                                        {
                                            type: 'GdalSource',
                                            params: {
                                                data: 'sentinel2_10m_tile_11_band_B04_2022_2023_daily_raw_a',
                                            },
                                        },
                                        {
                                            type: 'RasterTypeConversion',
                                            params: {
                                                outputDataType: 'U16',
                                            },
                                            sources: {
                                                raster: {
                                                    type: 'GdalSource',
                                                    params: {
                                                        data: 'sentinel2_20m_tile_11_band_SCL_2022_2023_daily_raw_a',
                                                    },
                                                },
                                            },
                                        },
                                    ],
                                },
                            },
                        },
                    },
                },
            },
        },
        symbology: new RasterSymbology(
            1.0,
            new SingleBandRasterColorizer(
                0,
                new LinearGradient(
                    ColorMapSelectorComponent.createLinearBreakpoints(ALL_COLORMAPS.VIRIDIS, 16, false, {min: -1, max: 1}),
                    // [
                    // (new ColorBreakpoint(-1, WHITE), new ColorBreakpoint(1, Color.fromRgbaLike([0, 255, 0, 1])))
                    // ],
                    TRANSPARENT,
                    TRANSPARENT,
                    TRANSPARENT,
                ),
            ),
        ),
        dataRange: {min: 0, max: 1},
        measurement: 'continuous',
    },
];

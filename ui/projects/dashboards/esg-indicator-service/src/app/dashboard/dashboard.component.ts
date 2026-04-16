import {
    AfterContentInit,
    AfterViewInit,
    ChangeDetectionStrategy,
    ChangeDetectorRef,
    Component,
    effect,
    ElementRef,
    inject,
    signal,
    untracked,
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
import {firstValueFrom} from 'rxjs';
import {BackendService, BBoxDict, CoreModule, DatasetService, MapContainerComponent, ProjectService, WfsParamsDict} from '@geoengine/core';
import {
    ColumnRangeFilterDict,
    extentToBboxDict,
    PolygonSymbology,
    RasterVectorJoinDict,
    SourceOperatorDict,
    Time,
    VectorLayer,
    VectorSymbology,
    UserService as CommonUserService,
    ColorMapSelectorComponent,
    ALL_COLORMAPS,
    ColorBreakpoint,
    RasterLayer,
    RasterSymbology,
    UserService,
    NotificationService,
} from '@geoengine/common';
import {utc} from 'moment';
import {DataSelectionService} from '../data-selection.service';
import {ComputationQuota, Workflow} from '@geoengine/api-client';
import {toSignal} from '@angular/core/rxjs-interop';
import {Router} from '@angular/router';
import proj4 from 'proj4';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {QuotaLogComponent} from '../quota-log/quota-log.component';

interface SelectedProperty {
    featureId: number;
    bbox: BBoxDict;
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
        QuotaLogComponent,
        MapContainerComponent,
    ],
})
export class DashboardComponent implements AfterViewInit, AfterContentInit {
    readonly userService = inject(UserService);
    readonly commonUserService = inject(CommonUserService);
    readonly dataSelectionService = inject(DataSelectionService);
    private readonly breakpointObserver = inject(BreakpointObserver);
    private readonly projectService = inject(ProjectService);
    private readonly notificationService = inject(NotificationService);
    private readonly datasetService = inject(DatasetService);
    private readonly changeDetectorRef = inject(ChangeDetectorRef);
    private readonly backend = inject(BackendService);
    private readonly router = inject(Router);

    maxScore = 5;

    readonly isSelectingBox = signal(false);
    readonly selectedFeature: WritableSignal<SelectedProperty | undefined> = signal(undefined);
    readonly isLandscape = signal(true);
    readonly plotWidthPx = signal(100);
    readonly plotHeightPx = signal(100);
    readonly layersReverse = toSignal(this.dataSelectionService.layers);
    readonly scoreLoading = signal(false);
    readonly score = signal<number | undefined>(undefined);

    readonly usageLoading = signal(false);
    readonly usage = signal<ComputationQuota | undefined>(undefined);

    readonly mapComponent = viewChild.required(MapContainerComponent);

    readonly analyzeCard = viewChild.required('analyzecard', {read: ElementRef});

    readonly usageCompontent = viewChild.required(QuotaLogComponent);

    timeSteps: Time[] = [new Time(utc('2021-05-01')), new Time(utc('2022-05-01'))];

    readonly scoreIndicator = viewChild<MatProgressSpinner>('scoreIndicator');
    readonly scoreColors = ColorMapSelectorComponent.createLinearBreakpoints(ALL_COLORMAPS['RdYlGn'], 16, false, {
        min: 0,
        max: this.maxScore,
    });

    constructor() {
        effect(() => {
            const [score, scoreIndicator] = [this.score(), this.scoreIndicator()];
            if (!score || !scoreIndicator) return;
            untracked(() => colorizeScoreIndicator(scoreIndicator, score, this.scoreColors));
        });
    }

    // eslint-disable-next-line @typescript-eslint/no-misused-promises, @typescript-eslint/require-await
    async ngAfterViewInit(): Promise<void> {
        this.breakpointObserver.observe(Breakpoints.Web).subscribe((isLandscape) => {
            this.isLandscape.set(isLandscape.matches);
        });
    }

    // eslint-disable-next-line @typescript-eslint/no-misused-promises, @typescript-eslint/require-await
    async ngAfterContentInit(): Promise<void> {
        this.loadData();

        // eslint-disable-next-line @typescript-eslint/no-misused-promises
        this.projectService.getSelectedFeatureStream().subscribe(async (featureSelection) => {
            const features = await this.dataSelectionService.getPolygonLayerFeatures();
            if (featureSelection.feature) {
                const actualFeature = features.find((f) => f.getId() === featureSelection.feature);
                const props = actualFeature?.getProperties();
                const id = props?.[PROPERTY_IDENTIFIER_COLUMN_NAME];
                const bbox = actualFeature?.getGeometry()?.getExtent();

                if (!id || !bbox) {
                    // TODO: show error message

                    this.selectedFeature.set(undefined);
                    return;
                }

                this.selectedFeature.set({
                    featureId: id,
                    bbox: {
                        lowerLeftCoordinate: {x: bbox[0], y: bbox[1]},
                        upperRightCoordinate: {x: bbox[2], y: bbox[3]},
                    },
                });
            } else {
                this.selectedFeature.set(undefined);
            }
        });
    }

    private async loadData(): Promise<void> {
        // wait for project to be loaded before redirecting
        await firstValueFrom(this.projectService.getProjectOnce());

        this.loadClassification();
        await this.loadProperties();
    }

    async loadClassification(): Promise<void> {
        const workflowId = await firstValueFrom(this.projectService.registerWorkflow(CLASSIFICATION_WORKFLOW));

        const rasterLayer = new RasterLayer({
            name: 'ESG Classification',
            workflowId,
            isVisible: true,
            isLegendVisible: false,
            symbology: CLASSIFICATION_SYMBOLOGY,
        });

        return await firstValueFrom(this.dataSelectionService.setRasterLayer(rasterLayer, this.timeSteps, CLASSIFICIATION_DATA_RANGE));
    }

    async loadProperties(): Promise<void> {
        const workflowId = await firstValueFrom(this.projectService.registerWorkflow(PROPERTIES_WORKFLOW));

        const polygonLayer = new VectorLayer({
            name: 'Bahn Properties',
            workflowId,
            isVisible: true,
            isLegendVisible: false,
            symbology: PROPERTIES_SYMBOLOGY,
        });

        return await firstValueFrom(this.dataSelectionService.setPolygonLayer(polygonLayer));
    }

    async analyze(): Promise<void> {
        const feature = this.selectedFeature();
        if (!feature) {
            return;
        }

        this.scoreLoading.set(true);
        this.score.set(undefined);

        const columnFilter: ColumnRangeFilterDict = {
            type: 'ColumnRangeFilter',
            params: {
                column: PROPERTY_IDENTIFIER_COLUMN_NAME,
                ranges: [[feature.featureId, feature.featureId]],
                keepNulls: false,
            },
            sources: {
                vector: PROPERTIES_SOURCE_OP,
            },
        };

        const rasterVectorJoin: RasterVectorJoinDict = {
            type: 'RasterVectorJoin',
            params: {
                names: {
                    type: 'names',
                    values: ['score'],
                },
                featureAggregation: 'mean',
                temporalAggregation: 'none',
                featureAggregationIgnoreNoData: true,
                temporalAggregationIgnoreNoData: true,
            },
            sources: {
                vector: columnFilter,
                rasters: [
                    {
                        type: 'GdalSource',
                        params: {
                            data: 'esg',
                        },
                    },
                ],
            },
        };

        const workflow: Workflow = {
            type: 'Vector',
            operator: rasterVectorJoin,
        };

        const workflowId = await firstValueFrom(this.projectService.registerWorkflow(workflow));

        const time = await this.projectService.getTimeOnce();

        // reproject bbox to EPSG:32632
        const ll = proj4('EPSG:4326', 'EPSG:32632', [feature.bbox.lowerLeftCoordinate.x, feature.bbox.lowerLeftCoordinate.y]);
        const ur = proj4('EPSG:4326', 'EPSG:32632', [feature.bbox.upperRightCoordinate.x, feature.bbox.upperRightCoordinate.y]);
        const extent: [number, number, number, number] = [ll[0], ll[1], ur[0], ur[1]];

        const wfsParams: WfsParamsDict = {
            workflowId,
            bbox: extentToBboxDict(extent),
            time: {
                start: time.start.unix() * 1_000,
                end: time.end.unix() * 1_000,
            },
            srsName: 'EPSG:32632',
        };

        const sessionId = await firstValueFrom(this.userService.getSessionTokenForRequest());

        const wfsResponse = await firstValueFrom(this.backend.wfsGetFeature(wfsParams, sessionId));

        const features = wfsResponse?.features;
        if (!Array.isArray(features) || features.length === 0) {
            // TODO: show error
            return;
        }

        const score = features[0].properties?.score;

        if (!score) {
            // TODO: show error
            return;
        }

        this.score.set(score);
        this.scoreLoading.set(false);

        await this.usageCompontent().refresh();
    }

    logout(): void {
        this.userService.logout();
        void this.router.navigate(['/signin']);
    }
}

const PROPERTIES_SOURCE_OP: SourceOperatorDict = {
    type: 'OgrSource',
    params: {data: 'bahn_properties'},
};

const PROPERTIES_WORKFLOW: Workflow = {
    type: 'Vector',
    operator: PROPERTIES_SOURCE_OP,
};

const PROPERTY_IDENTIFIER_COLUMN_NAME = 'identifier';

const PROPERTIES_SYMBOLOGY: VectorSymbology = PolygonSymbology.fromPolygonSymbologyDict({
    type: 'polygon',
    stroke: {
        width: {
            type: 'static',
            value: 4,
        },
        color: {
            type: 'static',
            color: [0, 0, 200, 255],
        },
    },
    fillColor: {
        type: 'static',
        color: [0, 0, 200, 128],
    },
    autoSimplified: true,
});

const CLASSIFICATION_SOURCE_OP: SourceOperatorDict = {
    type: 'GdalSource',
    params: {data: 'esg'},
};

const CLASSIFICATION_WORKFLOW: Workflow = {
    type: 'Raster',
    operator: CLASSIFICATION_SOURCE_OP,
};

const CLASSIFICIATION_DATA_RANGE = {
    min: 0,
    max: 5,
};

const CLASSIFICATION_SYMBOLOGY: RasterSymbology = RasterSymbology.fromRasterSymbologyDict({
    type: 'raster',
    opacity: 1,
    rasterColorizer: {
        type: 'singleBand',
        band: 0,
        bandColorizer: {
            type: 'linearGradient',
            breakpoints: [
                {
                    value: 0,
                    color: [142, 1, 82, 255],
                },
                {
                    value: 1,
                    color: [222, 119, 174, 255],
                },
                {
                    value: 2,
                    color: [253, 224, 239, 255],
                },
                {
                    value: 3,
                    color: [230, 245, 208, 255],
                },
                {
                    value: 4,
                    color: [127, 188, 65, 255],
                },
                {
                    value: 5,
                    color: [39, 100, 25, 255],
                },
            ],
            noDataColor: [0, 0, 0, 0],
            overColor: [0, 0, 0, 0],
            underColor: [0, 0, 0, 0],
        },
    },
});

/** Colorizes the score indicator based on the given score and breakpoints */
function colorizeScoreIndicator(element: MatProgressSpinner, score: number, breakpoints: Array<ColorBreakpoint>): void {
    const circle = element._elementRef.nativeElement.getElementsByTagName('circle')[0];
    if (!circle || breakpoints.length === 0) {
        return;
    }

    let color = breakpoints[0].color;
    for (const breakpoint of breakpoints) {
        if (score < breakpoint.value) {
            break;
        }
        color = breakpoint.color;
    }

    circle.style.stroke = color.rgbaCssString();
}

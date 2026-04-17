import {ChangeDetectionStrategy, ChangeDetectorRef, Component, ElementRef, OnDestroy, OnInit, inject, viewChild} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {
    ProjectService,
    BackendService,
    LayoutService,
    ProviderLayerIdDict,
    ProviderLayerCollectionIdDict,
    PlotDataDict,
    RasterResultDescriptorDict,
    MapService,
    SymbologyEditorComponent,
    WGS_84,
} from '@geoengine/core';
import {BehaviorSubject, combineLatest, firstValueFrom, from, Observable, of, Subscription} from 'rxjs';
import {AppConfig} from '../app-config.service';
import {filter, map, mergeMap} from 'rxjs/operators';
import {Country, CountryProviderService} from '../country-provider.service';
import {DataSelectionService, DataRange} from '../data-selection.service';
import {ActivatedRoute} from '@angular/router';
import {countryDatasetName} from '../country-selector/country-data.model';
import {
    ExpressionDict,
    LayersService,
    MeanRasterPixelValuesOverTimeDict,
    NotificationService,
    OperatorDict,
    PathChange,
    PathChangeSource,
    RasterDataType,
    RasterDataTypes,
    RasterLayer,
    RasterStackerDict,
    RasterSymbology,
    SourceOperatorDict,
    Time,
    UserService,
    extentToBboxDict,
    CommonModule,
    FxFlexDirective,
    FxLayoutDirective,
    FxLayoutAlignDirective,
} from '@geoengine/common';
import {LayerListing} from '@geoengine/api-client';
import {MatButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {MatSlideToggle} from '@angular/material/slide-toggle';
import {FormsModule} from '@angular/forms';
import {MatDivider} from '@angular/material/list';
import {CountrySelectorComponent} from '../country-selector/country-selector.component';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {AttributionsComponent} from '../attributions/attributions.component';
import {AsyncPipe} from '@angular/common';

interface Path {
    collectionId?: string;
    selectionIndexOrName: string | number;
}

@Component({
    selector: 'geoengine-ebv-ebv-selector',
    templateUrl: './ebv-selector.component.html',
    styleUrls: ['./ebv-selector.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        CommonModule,
        MatButton,
        FxFlexDirective,
        MatIcon,
        MatSlideToggle,
        FormsModule,
        MatDivider,
        FxLayoutDirective,
        FxLayoutAlignDirective,
        CountrySelectorComponent,
        MatProgressSpinner,
        AttributionsComponent,
        AsyncPipe,
    ],
})
export class EbvSelectorComponent implements OnInit, OnDestroy {
    private readonly userService = inject(UserService);
    private readonly config = inject<AppConfig>(AppConfig);
    private readonly changeDetectorRef = inject(ChangeDetectorRef);
    private readonly http = inject(HttpClient);
    private readonly projectService = inject(ProjectService);
    private readonly countryProviderService = inject(CountryProviderService);
    private readonly dataSelectionService = inject(DataSelectionService);
    private readonly route = inject(ActivatedRoute);
    private readonly mapService = inject(MapService);
    private readonly backend = inject(BackendService);
    private readonly layoutService = inject(LayoutService);
    private readonly layersService = inject(LayersService);
    private readonly notificationService = inject(NotificationService);

    readonly SUBGROUP_SEARCH_THRESHOLD = 5;

    readonly containerDiv = viewChild.required<ElementRef<HTMLDivElement>>('container');

    readonly isPlotButtonDisabled$: Observable<boolean>;

    rootCollectionId: ProviderLayerCollectionIdDict = {
        providerId: '77d0bf11-986e-43f5-b11d-898321f1854c',
        collectionId: 'classes',
    };

    preselectedPath: Array<string | number> = [];

    layerId?: ProviderLayerIdDict;
    layer?: RasterLayer;
    time?: Time;
    pinnedSymbology = true;

    lastRasterSymbology?: RasterSymbology;
    lastSelectedLayerId?: ProviderLayerIdDict;

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    readonly plotData = new BehaviorSubject<any>(undefined);
    readonly plotLoading = new BehaviorSubject(false);

    protected queryParamsSubscription?: Subscription;
    protected dataSubscription?: Subscription;
    protected rasterLayerSubscription?: Subscription;

    // keep track of selection to preselect same values when changing path inside same dataset
    protected previousPath: Path[] = [];
    protected previousLayer?: LayerListing;
    // TODO: previous selection as {id, name} because we need both

    constructor() {
        this.isPlotButtonDisabled$ = this.countryProviderService.getSelectedCountryStream().pipe(map((country) => !country));
        this.dataSelectionService.rasterLayer.subscribe((layer) => {
            if (layer) {
                this.lastRasterSymbology = layer.symbology;
            }
        });
    }

    ngOnInit(): void {
        this.queryParamsSubscription = this.handleQueryParams();
    }

    ngOnDestroy(): void {
        this.queryParamsSubscription?.unsubscribe();
        this.rasterLayerSubscription?.unsubscribe();
    }

    editSymbology(): void {
        this.layoutService.setSidenavContentComponent({
            component: SymbologyEditorComponent,
            keepParent: false,
            config: {
                layer: this.layer,
            },
        });
    }

    layerSelected(layer?: LayerListing): void {
        if (this.layerId) {
            this.lastSelectedLayerId = this.layerId;
        }

        if (layer) {
            this.previousLayer = layer;
        }

        this.layerId = layer?.id;
        this.showEbv(layer?.id);
    }

    // This method keeps a custom symbology as long as only entities are changed
    isOnlyEntityChangedFromLastId(): boolean {
        if (!this.lastSelectedLayerId || !this.layerId) {
            return false;
        }

        // Provider IDs are different (this should not happen)
        if (this.lastSelectedLayerId.providerId != this.layerId.providerId) {
            return false;
        }

        const lastLayerIdParts = this.lastSelectedLayerId.layerId.split('/');
        const newLayerIdParts = this.layerId.layerId.split('/');

        // EBV paths dont have the same length. This can only happen if one has scenarios and the other one does not.
        if (lastLayerIdParts.length != newLayerIdParts.length) {
            return false;
        }

        // The prefix of the EBV path (first three parts) should stay the same.
        if (
            lastLayerIdParts[0] !== newLayerIdParts[0] ||
            lastLayerIdParts[1] !== newLayerIdParts[1] ||
            lastLayerIdParts[2] !== newLayerIdParts[2]
        ) {
            return false;
        }

        // IF there is a scenario in both paths it is ok th be different
        // Only metrics must stay the same
        if (lastLayerIdParts.length == 7) {
            return lastLayerIdParts[5] === newLayerIdParts[5];
        }

        // IF there is no scenario in both paths thats also ok. Metric must not be different.
        if (lastLayerIdParts.length == 6) {
            return lastLayerIdParts[4] === newLayerIdParts[4];
        }

        // we should not reach this point. Better return false just in case.
        return false;
    }

    pathChange(change: PathChange): void {
        const collections = change.path;

        if (collections.length < 1) {
            // no selection => ignore
            return;
        }

        const inputPath: Path[] = collections.map((c) => {
            return {
                collectionId: c.id.collectionId,
                selectionIndexOrName: c.name,
            };
        });

        if (change.source === PathChangeSource.Preselection) {
            // preselection does not require further action, so we only remember the current path
            this.previousPath = inputPath;
            return;
        }

        if (
            this.previousPath.length > 3 &&
            collections.length > 3 &&
            this.previousPath[3].collectionId === collections[3].id.collectionId // [0] EBV Portal / [1] EBV Class / [2] EBV Name / [3] EBV Dataset
        ) {
            // new path inside same dataset, preselect same values as before
            const oldPathRemainder = this.previousPath.slice(inputPath.length);

            const path = inputPath.concat(oldPathRemainder);

            const outPath = path.map((collection) => collection.selectionIndexOrName).slice(1);

            if (this.previousLayer) {
                outPath.push(this.previousLayer.name);
            }

            this.preselectedPath = outPath;

            this.previousPath = path;

            this.changeDetectorRef.markForCheck();
            return;
        }

        // new dataset or collection selected, preselect defaults
        const path = [...inputPath];

        // pre-fill EBV Portal, EBV Class, EBV Name, EBV Dataset, Scenario, Metric, Entity => 7 items
        while (path.length < 7) {
            path.push({
                collectionId: undefined,
                selectionIndexOrName: 0,
            });
        }

        const outPath = path.map((collection) => collection.selectionIndexOrName).slice(1);

        this.preselectedPath = outPath;

        this.changeDetectorRef.markForCheck();

        this.previousPath = path;
    }

    showEbv(layerId?: ProviderLayerIdDict): void {
        if (!layerId) {
            return;
        }

        if (this.dataSubscription) {
            this.dataSubscription.unsubscribe();
        }

        this.dataSubscription = from(this.layersService.getLayer(layerId.providerId, layerId.layerId))
            .pipe(
                mergeMap((layer) => combineLatest([of(layer), this.projectService.registerWorkflow(layer.workflow)])),
                mergeMap(([layer, workflowId]) => {
                    if (!layer.symbology) {
                        throw new Error('Layer has no symbology');
                    }

                    if (!layer.metadata) {
                        throw new Error('Layer has no metadata');
                    }

                    if (!('timeSteps' in layer.metadata)) {
                        throw new Error('Layer has no timeSteps');
                    }

                    if (!('dataRange' in layer.metadata)) {
                        throw new Error('Layer has no dataRange');
                    }

                    const timeSteps: Array<Time> = JSON.parse(layer.metadata['timeSteps']).map((t: number) => new Time(t));
                    this.time = new Time(timeSteps[0].start, timeSteps[timeSteps.length - 1].end);

                    const range: [number, number] = JSON.parse(layer.metadata['dataRange']);
                    const dataRange: DataRange = {
                        min: range[0],
                        max: range[1],
                    };

                    const compatibleEntity = this.isOnlyEntityChangedFromLastId();
                    const keepSymbology = compatibleEntity && this.pinnedSymbology;
                    const symbology =
                        keepSymbology && this.lastRasterSymbology
                            ? this.lastRasterSymbology
                            : (RasterSymbology.fromDict(layer.symbology) as RasterSymbology);

                    const rasterLayer = new RasterLayer({
                        name: 'EBV',
                        workflowId,
                        isVisible: true,
                        isLegendVisible: false,
                        symbology,
                    });
                    this.layer = rasterLayer;

                    return combineLatest([
                        this.dataSelectionService.setRasterLayer(rasterLayer, timeSteps, dataRange),
                        this.dataSelectionService.clearPolygonLayer(),
                    ]);
                }),
            )
            .subscribe(() => this.changeDetectorRef.markForCheck());
    }

    plot(): void {
        from(this.computePlot()).subscribe({
            next: (plotData) => {
                this.plotData.next(plotData.data);
                this.plotLoading.next(false);
            },
            error: () => {
                // TODO: react on error?
                this.plotLoading.next(false);
            },
        });
    }

    createCountryOperator(country: Country, dataType: RasterDataType): OperatorDict | SourceOperatorDict {
        const operator = {
            type: 'GdalSource',
            params: {
                data: 'raster_country_' + countryDatasetName(country.name),
            },
        };

        if (dataType == RasterDataTypes.Byte) {
            return operator;
        }

        return {
            type: 'RasterTypeConversion',
            params: {
                outputDataType: dataType.getCode(),
            },
            sources: {
                raster: operator,
            },
        };
    }

    protected async computePlot(): Promise<PlotDataDict> {
        const rasterLayer = await firstValueFrom(this.dataSelectionService.rasterLayer);
        const selectedCountry = await firstValueFrom(this.countryProviderService.getSelectedCountryStream());
        const selectedTime = this.time;

        if (!rasterLayer || !selectedCountry || !selectedTime) {
            throw Error('No raster layer or country or time selected');
        }

        this.plotLoading.next(true);
        this.plotData.next(undefined);

        const rasterLayerMetadata = await firstValueFrom(this.projectService.getRasterLayerMetadata(rasterLayer));

        const sessionToken = await firstValueFrom(this.userService.getSessionTokenForRequest());

        const rasterWorkflow = await firstValueFrom(this.projectService.getWorkflow(rasterLayer.workflowId));

        // TODO: use native CRS from raster layer for plot -> determine resolution in this CRS
        const projectedRasterWorkflow = this.projectService.createProjectedOperator(
            rasterWorkflow.operator,
            rasterLayerMetadata,
            // always use WGS 84 for computing the plot
            WGS_84.spatialReference,
        );

        const projectedRasterWorkflowMetadata$ = this.projectService
            .registerWorkflow({
                type: 'Raster',
                operator: projectedRasterWorkflow,
            })
            .pipe(
                mergeMap(
                    (projectedRasterWorkflowId) =>
                        this.backend.getWorkflowMetadata(projectedRasterWorkflowId, sessionToken) as Observable<RasterResultDescriptorDict>,
                ),
            );

        const plotWorkflowId$ = this.projectService.registerWorkflow({
            type: 'Plot',
            operator: {
                type: 'MeanRasterPixelValuesOverTime',
                params: {
                    timePosition: 'start',
                    area: false,
                },
                sources: {
                    raster: {
                        type: 'Expression',
                        params: {
                            expression: 'if B IS NODATA { NODATA } else { A }',
                            outputType: RasterDataTypes.Float64.getCode(),
                            outputBand: {
                                name: 'expression',
                                measurement: {
                                    type: 'unitless',
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
                                        projectedRasterWorkflow,
                                        this.createCountryOperator(selectedCountry, rasterLayerMetadata.dataType),
                                    ],
                                },
                            } as RasterStackerDict,
                        },
                    } as ExpressionDict,
                },
            } as MeanRasterPixelValuesOverTimeDict,
        });

        const [projectedRasterWorkflowMetadata, plotWorkflowId] = await firstValueFrom(
            combineLatest([projectedRasterWorkflowMetadata$, plotWorkflowId$]),
        );

        let spatialResolution: [number, number] = [0.1, 0.1];
        if (
            projectedRasterWorkflowMetadata.resolution &&
            projectedRasterWorkflowMetadata.resolution.x > 0.1 &&
            projectedRasterWorkflowMetadata.resolution.y > 0.1
        ) {
            // TODO: communicate upper limit or think about long-running plot requests
            spatialResolution = [projectedRasterWorkflowMetadata.resolution.x, projectedRasterWorkflowMetadata.resolution.y];
        }

        return firstValueFrom(
            this.backend.getPlot(
                plotWorkflowId,
                {
                    time: {
                        start: selectedTime.start.unix() * 1_000,
                        end: selectedTime.end.unix() * 1_000 + 1 /* add one millisecond to include the end time */,
                    },
                    bbox: extentToBboxDict([selectedCountry.minx, selectedCountry.miny, selectedCountry.maxx, selectedCountry.maxy]),
                    // always use WGS 84 for computing the plot
                    crs: WGS_84.spatialReference.srsString,
                    spatialResolution,
                },
                sessionToken,
            ),
        );
    }

    private handleQueryParams(): Subscription {
        return this.route.queryParams
            .pipe(
                filter((params) => params.id),
                mergeMap((params) => this.http.get<EbvDatasetResponse>(`https://portal.geobon.org/api/v1/datasets/${params.id}`)),
            )
            .subscribe((response) => {
                if (response.code !== 200) {
                    this.notificationService.error('Could not load dataset');
                }

                const dataset = response.data[0];
                this.preselectedPath = [
                    dataset.ebv.ebv_class,
                    dataset.ebv.ebv_name,
                    dataset.title,
                    0 /*default scenario/metric*/,
                    0 /*default metric/entity*/,
                    0 /*default entity*/,
                ];
                this.changeDetectorRef.markForCheck();
            });
    }
}

interface EbvDatasetResponse {
    code: number;
    data: [
        {
            title: string;
            ebv: {
                // eslint-disable-next-line @typescript-eslint/naming-convention
                ebv_class: string;
                // eslint-disable-next-line @typescript-eslint/naming-convention
                ebv_name: string;
            };
        },
    ];
}

import {ChangeDetectionStrategy, Component, ElementRef, OnInit, computed, inject, signal, viewChild} from '@angular/core';
import {MatSidenavModule} from '@angular/material/sidenav';
import {ProjectService, MapService, MapContainerComponent, CoreModule, LoadingState} from '@geoengine/core';
import {AppConfig} from '../app-config.service';
import {Layer, NotificationService, RasterLayer, RasterSymbology, Time, UserService} from '@geoengine/common';
import {MatToolbar, MatToolbarModule} from '@angular/material/toolbar';
import {MatButtonModule} from '@angular/material/button';
import {MatIconModule} from '@angular/material/icon';
import {MatProgressBarModule} from '@angular/material/progress-bar';
import {MatTooltipModule} from '@angular/material/tooltip';
import {toSignal} from '@angular/core/rxjs-interop';
import {MatButtonToggleModule} from '@angular/material/button-toggle';
import {MatRadioModule} from '@angular/material/radio';
import {MatDatepickerInputEvent, MatDatepickerModule} from '@angular/material/datepicker';
import {combineLatest, of, firstValueFrom} from 'rxjs';
import {map, switchMap} from 'rxjs/operators';
import {Workflow} from '@geoengine/api-client';

const VISUALIZATION_PRESETS = [
    {
        preset: 'raw',
        label: 'Raw',
        imageSrc: 'assets/rgb.jpg',
        altText: 'Raw',
        enabled: true,
    },
    {
        preset: 'rgb',
        label: 'RGB',
        imageSrc: 'assets/rgb.jpg',
        altText: 'RGB',
        enabled: true,
    },
    {
        preset: 'ndvi',
        label: 'NDVI',
        imageSrc: 'assets/ndvi.jpg',
        altText: 'NDVI',
        enabled: true,
    },
    {
        preset: 'false-color',
        label: 'False Color',
        imageSrc: 'assets/false-color.jpg',
        altText: 'False Color',
        enabled: false,
    },
    {
        preset: 'swi',
        label: 'SWI',
        imageSrc: 'assets/swi.jpg',
        altText: 'SWI',
        enabled: false,
    },
] as const;

type DataSourceType = 'sentinel2L2a' | 'sentinel1';

type VisualizationPreset = (typeof VISUALIZATION_PRESETS)[number]['preset'];
type EnabledVisualizationPreset = Extract<(typeof VISUALIZATION_PRESETS)[number], {enabled: true}>['preset'];
const DEFAULT_VISUALIZATION_PRESET: EnabledVisualizationPreset = 'raw';
const DEFAULT_DATA_SOURCE: DataSourceType = 'sentinel2L2a';

@Component({
    selector: 'geoengine-main',
    templateUrl: './main.component.html',
    styleUrls: ['./main.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        CoreModule,
        MapContainerComponent,
        MatButtonModule,
        MatButtonToggleModule,
        MatDatepickerModule,
        MatIconModule,
        MatProgressBarModule,
        MatRadioModule,
        MatSidenavModule,
        MatToolbarModule,
        MatTooltipModule,
    ],
    host: {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        '(window:resize)': 'onResize()',
    },
})
export class MainComponent implements OnInit {
    readonly config = inject(AppConfig);
    readonly projectService = inject(ProjectService);
    readonly userService = inject(UserService);
    private readonly mapService = inject(MapService);
    private readonly notificationService = inject(NotificationService);

    readonly topToolbar = viewChild.required<MatToolbar, ElementRef<HTMLElement>>('topToolbar', {read: ElementRef});
    readonly mapComponent = viewChild.required(MapContainerComponent);

    readonly layersReverse = toSignal(this.projectService.getLayerStream().pipe(map((layers) => layers.slice().reverse())), {
        initialValue: [],
    });

    readonly isLoading = toSignal(
        this.projectService
            .getLayerStream()
            .pipe(
                switchMap((layers) =>
                    layers.length === 0
                        ? of(false)
                        : combineLatest(layers.map((l) => this.projectService.getLayerStatusStream(l))).pipe(
                              map((statuses) => statuses.some((s) => s === LoadingState.LOADING)),
                          ),
                ),
            ),
        {initialValue: false},
    );

    readonly totalHeight = signal(window.innerHeight);
    readonly topToolbarHeight = signal(64);

    readonly sessionToken = toSignal(this.userService.getSessionTokenStream());

    readonly middleContainerHeight = computed(() => this.totalHeight() - this.topToolbarHeight());

    readonly currentTime = toSignal(this.projectService.getTimeStream());
    readonly formattedTime = computed<string>(() => {
        const projectTime = this.currentTime();
        if (!projectTime) return '';
        return projectTime.start.format('DD.MM.YYYY');
    });
    readonly timeStepDuration = toSignal(this.projectService.getTimeStepDurationStream());
    readonly currentDate = computed<Date | undefined>(() => {
        const time = this.currentTime();
        if (!time) return undefined;
        return time.start.toDate();
    });

    readonly activeDataSource = signal<DataSourceType>(DEFAULT_DATA_SOURCE);

    readonly availablePresets = computed(() =>
        VISUALIZATION_PRESETS.filter((p) => PRESETS_BY_DATA_SOURCE[this.activeDataSource()].includes(p.preset)),
    );

    readonly activePreset = signal<VisualizationPreset>(DEFAULT_VISUALIZATION_PRESET);

    private readonly presetWorkflowIds = new Map<EnabledVisualizationPreset, string>();
    private activeLayer: RasterLayer | undefined;

    ngOnInit(): void {
        this.mapService.registerMapComponent(this.mapComponent());

        this.onToolbarResize();
        const topToolbarObserver = new ResizeObserver(() => this.onToolbarResize());
        topToolbarObserver.observe(this.topToolbar().nativeElement);

        void this.activatePreset(DEFAULT_VISUALIZATION_PRESET);
    }

    onResize(): void {
        this.totalHeight.set(window.innerHeight);
    }

    onToolbarResize(): void {
        this.topToolbarHeight.set(this.topToolbar().nativeElement.offsetHeight);
    }

    idFromLayer(index: number, layer: Layer): number {
        return layer.id;
    }

    isPresetEnabled(preset: VisualizationPreset): preset is EnabledVisualizationPreset {
        return preset in PRESET_DEFINITIONS[this.activeDataSource()];
    }

    async activatePreset(preset: VisualizationPreset): Promise<void> {
        if (!this.isPresetEnabled(preset)) {
            return;
        }

        if (this.activePreset() === preset && this.activeLayer) {
            return;
        }

        try {
            const workflowId = await this.getOrRegisterWorkflowId(preset);
            const layer = new RasterLayer({
                name: preset.toUpperCase(),
                workflowId,
                isVisible: true,
                isLegendVisible: false,
                symbology: PRESET_DEFINITIONS[this.activeDataSource()][preset]!.symbology,
            });

            await this.replaceActiveLayer(layer);
            this.activePreset.set(preset);
        } catch (error) {
            console.error('Failed to activate visualization preset', preset, error);
            this.notificationService.error('Could not load selected visualization preset');
        }
    }

    private async getOrRegisterWorkflowId(preset: EnabledVisualizationPreset): Promise<string> {
        const existingWorkflowId = this.presetWorkflowIds.get(preset);
        if (existingWorkflowId) {
            return existingWorkflowId;
        }

        const workflowId = await firstValueFrom(
            this.projectService.registerWorkflow(PRESET_DEFINITIONS[this.activeDataSource()][preset]!.workflow),
        );
        this.presetWorkflowIds.set(preset, workflowId);
        return workflowId;
    }

    private async replaceActiveLayer(nextLayer: RasterLayer): Promise<void> {
        await firstValueFrom(this.projectService.clearLayers());

        await firstValueFrom(this.projectService.addLayer(nextLayer));
        this.activeLayer = nextLayer;
    }

    async onDataSourceChange(dataSource: DataSourceType): Promise<void> {
        if (this.activeDataSource() === dataSource) return;
        this.activeDataSource.set(dataSource);
        this.presetWorkflowIds.clear();
        this.activeLayer = undefined;
        this.activePreset.set(DEFAULT_VISUALIZATION_PRESET);
        await this.activatePreset(DEFAULT_VISUALIZATION_PRESET);
    }

    async timeForward(): Promise<void> {
        const time = this.currentTime();
        const timeStepDuration = this.timeStepDuration();

        if (!time || !timeStepDuration) return;

        const updatedTime = time.add(timeStepDuration.durationAmount, timeStepDuration.durationUnit);
        await this.projectService.setTime(updatedTime);
    }

    async timeBackwards(): Promise<void> {
        const time = this.currentTime();
        const timeStepDuration = this.timeStepDuration();

        if (!time || !timeStepDuration) return;

        const updatedTime = time.subtract(timeStepDuration.durationAmount, timeStepDuration.durationUnit);
        await this.projectService.setTime(updatedTime);
    }

    async setDate(event: MatDatepickerInputEvent<Date>): Promise<void> {
        if (!event?.value) return;

        const utcDate = new Date(Date.UTC(event.value.getFullYear(), event.value.getMonth(), event.value.getDate()));
        const time = new Time(utcDate);
        await this.projectService.setTime(time);
    }
}

const STAC_PROVIDER_ID_SENTNEL_2 = 'b274275c-373d-4a3f-8b45-9b48e9614329';

const STAC_PROVIDER_ID_SENTINEL1 = '452a4ca2-306c-4ab2-a6e2-ffe42d5c49c8';

const RAW_SENTINEL2_WORKFLOW: Workflow = {
    type: 'Raster',
    operator: {
        type: 'MultiBandGdalSource',
        params: {
            data: `_:${STAC_PROVIDER_ID_SENTNEL_2}:\`dataset/epsg32632_u16_10\``,
        },
    },
};

const RAW_SENTINEL1_WORKFLOW: Workflow = {
    type: 'Raster',
    operator: {
        type: 'MultiBandGdalSource',
        params: {
            data: `_:${STAC_PROVIDER_ID_SENTINEL1}:\`dataset/epsg32632_f32_20\``,
        },
    },
};

const RGB_WORKFLOW: Workflow = {
    type: 'Raster',
    operator: {
        type: 'MultiBandGdalSource',
        params: {
            data: `_:${STAC_PROVIDER_ID_SENTNEL_2}:\`dataset/epsg32632_u8_10\``,
        },
    },
};

const NDVI_WORKFLOW: Workflow = {
    type: 'Raster',
    operator: {
        type: 'Expression',
        params: {
            expression: 'if (A == 3 || (A >= 7 && A <= 11)) { NODATA } else { (B - C) / (B + C) }',
            mapNoData: false,
            outputBand: {
                name: 'NDVI',
                measurement: {
                    type: 'continuous',
                    measurement: 'NDVI',
                    unit: 'NDVI',
                },
            },
            outputType: 'F32',
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
                            type: 'RasterTypeConversion',
                            params: {
                                outputDataType: 'U16',
                            },
                            sources: {
                                raster: {
                                    type: 'Interpolation',
                                    params: {
                                        interpolation: 'nearestNeighbor',
                                        outputResolution: {
                                            type: 'fraction',
                                            x: 2.0,
                                            y: 2.0,
                                        },
                                    },
                                    sources: {
                                        raster: {
                                            type: 'BandFilter',
                                            params: {
                                                bands: [1],
                                            },
                                            sources: {
                                                raster: {
                                                    type: 'MultiBandGdalSource',
                                                    params: {
                                                        data: `_:${STAC_PROVIDER_ID_SENTNEL_2}:\`dataset/epsg32632_u8_20\``,
                                                    },
                                                },
                                            },
                                        },
                                    },
                                },
                            },
                        },
                        {
                            type: 'BandFilter',
                            params: {
                                bands: [3, 4],
                            },
                            sources: {
                                raster: {
                                    type: 'MultiBandGdalSource',
                                    params: {
                                        data: `_:${STAC_PROVIDER_ID_SENTNEL_2}:\`dataset/epsg32632_u16_10\``,
                                    },
                                },
                            },
                        },
                    ],
                },
            },
        },
    },
};

const RGB_SYMBOLOGY = RasterSymbology.fromRasterSymbologyDict({
    type: 'raster',
    opacity: 1.0,
    rasterColorizer: {
        type: 'multiBand',
        redBand: 2,
        greenBand: 1,
        blueBand: 0,
        redMin: 0,
        redMax: 255,
        redScale: 1,
        greenMin: 0,
        greenMax: 255,
        greenScale: 1,
        blueMin: 0,
        blueMax: 255,
        blueScale: 1,
        noDataColor: [0, 0, 0, 0],
    },
});

const NDVI_SYMBOLOGY = RasterSymbology.fromRasterSymbologyDict({
    type: 'raster',
    opacity: 1.0,
    rasterColorizer: {
        type: 'singleBand',
        band: 0,
        bandColorizer: {
            type: 'linearGradient',
            breakpoints: [
                {
                    value: -0.1,
                    color: [0, 0, 0, 255],
                },
                {
                    value: 0.8,
                    color: [0, 255, 0, 255],
                },
            ],
            noDataColor: [0, 0, 0, 0],
            overColor: [246, 250, 254, 255],
            underColor: [247, 251, 255, 255],
        },
    },
});

const RAW_SENTINEL2_SYMBOLOGY = RasterSymbology.fromRasterSymbologyDict({
    type: 'raster',
    opacity: 1.0,
    rasterColorizer: {
        type: 'singleBand',
        band: 0,
        bandColorizer: {
            type: 'linearGradient',
            breakpoints: [
                {
                    value: 0,
                    color: [0, 0, 0, 255],
                },
                {
                    value: 2000,
                    color: [255, 255, 255, 255],
                },
            ],
            noDataColor: [0, 0, 0, 0],
            overColor: [255, 255, 255, 255],
            underColor: [0, 0, 0, 255],
        },
    },
});

const RAW_SENTINEL1_SYMBOLOGY = RasterSymbology.fromRasterSymbologyDict({
    type: 'raster',
    opacity: 1.0,
    rasterColorizer: {
        type: 'singleBand',
        band: 0,
        bandColorizer: {
            type: 'linearGradient',
            breakpoints: [
                {
                    value: 0,
                    color: [0, 0, 0, 255],
                },
                {
                    value: 0.3,
                    color: [255, 255, 255, 255],
                },
            ],
            noDataColor: [0, 0, 0, 0],
            overColor: [255, 255, 255, 255],
            underColor: [0, 0, 0, 255],
        },
    },
});

const PRESETS_BY_DATA_SOURCE: Record<DataSourceType, readonly VisualizationPreset[]> = {
    sentinel2L2a: ['raw', 'rgb', 'ndvi', 'false-color', 'swi'],
    sentinel1: ['raw'],
};

const PRESET_DEFINITIONS: Record<
    DataSourceType,
    Partial<Record<EnabledVisualizationPreset, {workflow: Workflow; symbology: RasterSymbology}>>
> = {
    sentinel2L2a: {
        raw: {
            workflow: RAW_SENTINEL2_WORKFLOW,
            symbology: RAW_SENTINEL2_SYMBOLOGY,
        },
        rgb: {
            workflow: RGB_WORKFLOW,
            symbology: RGB_SYMBOLOGY,
        },
        ndvi: {
            workflow: NDVI_WORKFLOW,
            symbology: NDVI_SYMBOLOGY,
        },
    },
    sentinel1: {
        raw: {
            workflow: RAW_SENTINEL1_WORKFLOW,
            symbology: RAW_SENTINEL1_SYMBOLOGY,
        },
    },
};

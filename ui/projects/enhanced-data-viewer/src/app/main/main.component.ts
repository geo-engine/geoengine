import {
    ChangeDetectionStrategy,
    Component,
    ElementRef,
    ResourceRef,
    afterNextRender,
    computed,
    inject,
    resource,
    signal,
    viewChild,
} from '@angular/core';
import {MatSidenavModule} from '@angular/material/sidenav';
import {ProjectService, MapService, MapContainerComponent, CoreModule} from '@geoengine/core';
import {AppConfig} from '../app-config.service';
import {Layer, LayersService, Time, TimeStepDuration, UserService} from '@geoengine/common';
import {MatToolbar, MatToolbarModule} from '@angular/material/toolbar';
import {MatButtonModule} from '@angular/material/button';
import {MatCheckboxModule} from '@angular/material/checkbox';
import {MatIconModule} from '@angular/material/icon';
import {MatTooltipModule} from '@angular/material/tooltip';
import {toSignal} from '@angular/core/rxjs-interop';
import {MatButtonToggleModule} from '@angular/material/button-toggle';
import {MatRadioModule} from '@angular/material/radio';
import {MatDatepickerInputEvent, MatDatepickerModule} from '@angular/material/datepicker';
import {CollectionItem} from '@geoengine/api-client';
import {ProviderLayerId} from '@geoengine/api-client/dist/models/ProviderLayerId';
import {A11yModule} from '@angular/cdk/a11y';

type PresetCategory = 'static' | 'harvested' | 'ad-hoc';

interface VisualizationPreset {
    displayName: string;
    backgroundImage: string;
    connectorId: string;
    collectionId: string;
    name: string;
    category: PresetCategory;
}

interface DataSourceDefinition {
    key: string;
    name: string;
    presets: VisualizationPreset[];
    defaultPresetIndex: number;
    defaultTime: number;
    defaultTimeStep: TimeStepDuration;
}

interface DataSourceLayer {
    dataConnectorId: string;
    layerId: string;
}

// Provider/connector IDs:
//   ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74 – Internal Layer DB (serves layers
//     registered via the API, including static, harvested, and provider layers)
//   b274275c-373d-4a3f-8b45-9b48e9614329 – Sentinel-1 Global Mosaics STAC Provider
//   c385386d-484e-5b40-9c56-0a6ca9f07243 – Sentinel-2 L2A STAC Provider
//   d496497e-595f-6c51-ad67-1b7dba0a1834 – Landsat C2 L1 OLI/TIRS STAC Provider
//   e5a7508f-6a60-7d62-be78-2c8ecb1b2945 – OpenGeoHub Landsat Mosaic STAC Provider
//   cbb21ee3-d15d-45c5-a175-66964adf4e85 – Personal Data Catalog (user datasets)

// Collection ID for the Internal Layer DB root: 05102bb3-a855-4a37-8a8a-30026a91fef1
// STAC providers use their collection name at the path "root" -> "dataTypes"/"projections" etc.

const LAYER_DB_PROVIDER_ID = 'ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74';
const LAYER_DB_ROOT_COLLECTION_ID = '05102bb3-a855-4a37-8a8a-30026a91fef1';

const STAC_PROVIDER_SENTINEL1 = 'b274275c-373d-4a3f-8b45-9b48e9614329';
const STAC_PROVIDER_SENTINEL2 = 'c385386d-484e-5b40-9c56-0a6ca9f07243';
const STAC_PROVIDER_LANDSAT = 'd496497e-595f-6c51-ad67-1b7dba0a1834';
const STAC_PROVIDER_OPENGEOLANDSAT = 'e5a7508f-6a60-7d62-be78-2c8ecb1b2945';

const PRESET_CATEGORY_LABELS: Record<PresetCategory, string> = {
    static: 'Static',
    harvested: 'Harvested',
    'ad-hoc': 'Ad-hoc (Data Provider)',
};

const DATA_SOURCES: DataSourceDefinition[] = [
    {
        key: 'sentinel1',
        name: 'Sentinel-1',
        defaultPresetIndex: 0,
        defaultTime: 1775001600000,
        defaultTimeStep: {durationAmount: 1, durationUnit: 'month'},
        presets: [
            // Static
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Sentinel-1 Static',
                category: 'static',
            },
            // Harvested
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Sentinel-1 VV Band (Harvested)',
                category: 'harvested',
            },
            {
                displayName: 'SAR False Color',
                backgroundImage: 'assets/false-color.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Sentinel-1 SAR False Color (Harvested)',
                category: 'harvested',
            },
            // Ad-hoc
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Sentinel-1 Global Mosaics Provider',
                category: 'ad-hoc',
            },
        ],
    },
    {
        key: 'sentinel2',
        name: 'Sentinel-2 L2A',
        defaultPresetIndex: 0,
        defaultTime: 1775001600000,
        defaultTimeStep: {durationAmount: 1, durationUnit: 'day'},
        presets: [
            // Static
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Sentinel-2 L2A Static',
                category: 'static',
            },
            // Harvested
            {
                displayName: 'True Color',
                backgroundImage: 'assets/rgb.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Sentinel-2 L2A True Color (Harvested)',
                category: 'harvested',
            },
            {
                displayName: 'NDVI',
                backgroundImage: 'assets/ndvi.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Sentinel-2 L2A NDVI (Harvested)',
                category: 'harvested',
            },
            // Ad-hoc
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Sentinel-2 L2A Provider',
                category: 'ad-hoc',
            },
        ],
    },
    {
        key: 'landsat',
        name: 'Landsat C2 L1 OLI/TIRS',
        defaultPresetIndex: 0,
        defaultTime: 1767225600000,
        defaultTimeStep: {durationAmount: 1, durationUnit: 'day'}, // 1-day step — STAC has consecutive days Jan 1-3 only
        presets: [
            // Static
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Landsat C2 L1 OLI/TIRS Static',
                category: 'static',
            },
            // Harvested
            {
                displayName: 'Red Band',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Landsat C2 L1 OLI/TIRS Red Band (Harvested)',
                category: 'harvested',
            },
            {
                displayName: 'True Color',
                backgroundImage: 'assets/rgb.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Landsat C2 L1 OLI/TIRS True Color (Harvested)',
                category: 'harvested',
            },
            {
                displayName: 'NDVI',
                backgroundImage: 'assets/ndvi.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Landsat C2 L1 OLI/TIRS NDVI (Harvested)',
                category: 'harvested',
            },
            // Ad-hoc
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Landsat C2 L1 OLI/TIRS Provider',
                category: 'ad-hoc',
            },
            {
                displayName: 'True Color',
                backgroundImage: 'assets/rgb.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Landsat C2 L1 OLI/TIRS Provider True Color',
                category: 'ad-hoc',
            },
        ],
    },
    {
        key: 'opengeohub-landsat',
        name: 'OpenGeoHub Landsat Mosaic',
        defaultPresetIndex: 0,
        defaultTime: 1730419200000,
        defaultTimeStep: {durationAmount: 2, durationUnit: 'months'},
        presets: [
            // Static
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'OpenGeoHub Landsat Bimonthly Mosaic Static',
                category: 'static',
            },
            // Harvested
            {
                displayName: 'Red Band',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'OpenGeoHub Landsat Bimonthly Mosaic Red Band (Harvested)',
                category: 'harvested',
            },
            {
                displayName: 'True Color',
                backgroundImage: 'assets/rgb.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'OpenGeoHub Landsat Bimonthly Mosaic True Color (Harvested)',
                category: 'harvested',
            },
            {
                displayName: 'NDVI',
                backgroundImage: 'assets/ndvi.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'OpenGeoHub Landsat Bimonthly Mosaic NDVI (Harvested)',
                category: 'harvested',
            },
            // Ad-hoc
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'OpenGeoHub Landsat Bimonthly Mosaic Provider',
                category: 'ad-hoc',
            },
            {
                displayName: 'True Color',
                backgroundImage: 'assets/rgb.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'OpenGeoHub Landsat Bimonthly Mosaic Provider True Color',
                category: 'ad-hoc',
            },
        ],
    },
];

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
        MatCheckboxModule,
        MatDatepickerModule,
        MatIconModule,
        MatRadioModule,
        MatSidenavModule,
        MatToolbarModule,
        MatTooltipModule,
        A11yModule,
    ],
    host: {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        '(window:resize)': 'onResize()',
    },
})
export class MainComponent {
    readonly config = inject(AppConfig);
    readonly projectService = inject(ProjectService);
    readonly userService = inject(UserService);
    private readonly layerService = inject(LayersService);
    private readonly mapService = inject(MapService);

    readonly topToolbar = viewChild.required<MatToolbar, ElementRef<HTMLElement>>('topToolbar', {read: ElementRef});
    readonly mapComponent = viewChild.required(MapContainerComponent);

    readonly layersReverse = signal<Array<Layer>>([]);

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
    readonly spatialReference = toSignal(this.projectService.getSpatialReferenceStream());

    readonly dataSources = DATA_SOURCES;

    readonly selectedDataSource = signal<string>(DATA_SOURCES[0].key);
    readonly selectedPresetIndex = signal<number>(0);
    readonly autoSelectTime = signal<boolean>(true);

    readonly currentPresets = computed(() => {
        const key = this.selectedDataSource();
        const ds = DATA_SOURCES.find((d) => d.key === key);
        return ds?.presets ?? [];
    });

    readonly presetGroups = computed(() => {
        const presets = this.currentPresets();
        const groups = new Map<PresetCategory, VisualizationPreset[]>();
        for (const preset of presets) {
            const group = groups.get(preset.category) ?? [];
            group.push(preset);
            groups.set(preset.category, group);
        }
        return Array.from(groups.entries()).map(([category, items]) => ({
            category,
            label: PRESET_CATEGORY_LABELS[category],
            presets: items,
        }));
    });

    readonly activePreset = computed(() => {
        const presets = this.currentPresets();
        const index = this.selectedPresetIndex();
        return presets[index] ?? presets[0];
    });

    private readonly presetRequestParams = computed(() => {
        const preset = this.activePreset();
        if (!preset) return undefined;
        return {connectorId: preset.connectorId, collectionId: preset.collectionId, name: preset.name};
    });

    readonly mapTileLayerResource: ResourceRef<DataSourceLayer | undefined> = resource({
        params: () => this.presetRequestParams(),
        loader: async ({params}) => {
            if (!params) return undefined;

            const limit = 20;
            let offset = 0;
            let layer: CollectionItem | undefined;

            while (!layer) {
                const items = await this.layerService.getLayerCollectionItems(params.connectorId, params.collectionId, offset, limit);

                if (items.items.length === 0) break;

                layer = items.items.find((item) => item.name === params.name);

                if (items.items.length < limit) break;

                offset += limit;
            }

            if (!layer) return undefined;

            const id = layer.id as ProviderLayerId;

            return {
                dataConnectorId: id.providerId,
                layerId: id.layerId,
            } as DataSourceLayer;
        },
    });

    readonly mapTileLayer = computed(() => this.mapTileLayerResource.value());
    readonly tileLoading = signal(false);
    readonly isLoading = computed(() => this.mapTileLayerResource.isLoading() || this.tileLoading());
    readonly testIsVisible = signal(true);

    constructor() {
        afterNextRender({
            read: () => {
                this.mapService.registerMapComponent(this.mapComponent());

                this.setInitialTime();

                this.onToolbarResize();
                const topToolbarObserver = new ResizeObserver(() => this.onToolbarResize());
                topToolbarObserver.observe(this.topToolbar().nativeElement);
            },
        });
    }

    private async setInitialTime(): Promise<void> {
        if (!this.autoSelectTime()) return;

        const ds = DATA_SOURCES.find((d) => d.key === this.selectedDataSource());
        if (!ds?.defaultTime) return;

        const utcDate = new Date(ds.defaultTime);
        const time = new Time(utcDate);
        await this.projectService.setTime(time);

        if (ds.defaultTimeStep) {
            this.projectService.setTimeStepDuration(ds.defaultTimeStep);
        }
    }

    onResize(): void {
        this.totalHeight.set(window.innerHeight);
    }

    onToolbarResize(): void {
        this.topToolbarHeight.set(this.topToolbar().nativeElement.offsetHeight);
    }

    async setSelectedDataSource(value: string): Promise<void> {
        const ds = DATA_SOURCES.find((d) => d.key === value);
        if (!ds) return;

        if (this.autoSelectTime() && ds.defaultTime) {
            const utcDate = new Date(ds.defaultTime);
            const time = new Time(utcDate);
            await this.projectService.setTime(time);
        }

        if (ds.defaultTimeStep) {
            this.projectService.setTimeStepDuration(ds.defaultTimeStep);
        }

        this.selectedDataSource.set(value);
        this.selectedPresetIndex.set(ds.defaultPresetIndex ?? 0);
    }

    selectPreset(index: number): void {
        this.selectedPresetIndex.set(index);
    }

    onTileLoading(loading: boolean): void {
        this.tileLoading.set(loading);
    }

    idFromLayer(index: number, layer: Layer): number {
        return layer.id;
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

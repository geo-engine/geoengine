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
import {Layer, LayersService, Time, UserService} from '@geoengine/common';
import {MatToolbar, MatToolbarModule} from '@angular/material/toolbar';
import {MatButtonModule} from '@angular/material/button';
import {MatIconModule} from '@angular/material/icon';
import {MatTooltipModule} from '@angular/material/tooltip';
import {toSignal} from '@angular/core/rxjs-interop';
import {MatButtonToggleModule} from '@angular/material/button-toggle';
import {MatRadioModule} from '@angular/material/radio';
import {MatDatepickerInputEvent, MatDatepickerModule} from '@angular/material/datepicker';
import {ProviderLayerId} from '@geoengine/api-client/dist/models/ProviderLayerId';
import {A11yModule} from '@angular/cdk/a11y';

interface VisualizationPreset {
    displayName: string;
    backgroundImage: string;
    connectorId: string;
    collectionId: string;
    name: string;
}

interface DataSourceDefinition {
    key: string;
    groupName: string;
    name: string;
    presets: VisualizationPreset[];
    defaultPresetIndex: number;
}

interface DataSourceLayer {
    dataConnectorId: string;
    layerId: string;
}

const DATA_SOURCES: DataSourceDefinition[] = [
    // ── Static ──────────────────────────────────────────
    {
        key: 'sentinel1-static',
        groupName: 'static',
        name: 'Sentinel-1 Static',
        defaultPresetIndex: 0,
        presets: [
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: 'ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74',
                collectionId: '05102bb3-a855-4a37-8a8a-30026a91fef1',
                name: 'Sentinel-1 Static',
            },
        ],
    },
    {
        key: 'sentinel2-static',
        groupName: 'static',
        name: 'Sentinel-2 L2A Static',
        defaultPresetIndex: 0,
        presets: [
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: 'ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74',
                collectionId: '05102bb3-a855-4a37-8a8a-30026a91fef1',
                name: 'Sentinel-2 L2A Static',
            },
        ],
    },

    // ── STAC Harvested ──────────────────────────────────
    {
        key: 'sentinel1-harvested',
        groupName: 'STAC harvested',
        name: 'Sentinel-1 Harvested',
        defaultPresetIndex: 0,
        presets: [
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: 'ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74',
                collectionId: '05102bb3-a855-4a37-8a8a-30026a91fef1',
                name: 'Sentinel-1 VV Band (Harvested)',
            },
            {
                displayName: 'SAR False Color',
                backgroundImage: 'assets/false-color.jpg',
                connectorId: 'ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74',
                collectionId: '05102bb3-a855-4a37-8a8a-30026a91fef1',
                name: 'Sentinel-1 SAR False Color (Harvested)',
            },
        ],
    },
    {
        key: 'sentinel2-harvested',
        groupName: 'STAC harvested',
        name: 'Sentinel-2 L2A Harvested',
        defaultPresetIndex: 0,
        presets: [
            {
                displayName: 'True Color',
                backgroundImage: 'assets/rgb.jpg',
                connectorId: 'ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74',
                collectionId: '05102bb3-a855-4a37-8a8a-30026a91fef1',
                name: 'Sentinel-2 L2A True Color (Harvested)',
            },
            {
                displayName: 'NDVI',
                backgroundImage: 'assets/ndvi.jpg',
                connectorId: 'ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74',
                collectionId: '05102bb3-a855-4a37-8a8a-30026a91fef1',
                name: 'Sentinel-2 L2A NDVI (Harvested)',
            },
        ],
    },

    // ── STAC Ad-hoc (Data Provider) ─────────────────────
    {
        key: 'sentinel2-provider',
        groupName: 'STAC ad-hoc (data provider)',
        name: 'Sentinel-2 L2A Provider',
        defaultPresetIndex: 0,
        presets: [
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: 'ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74',
                collectionId: '05102bb3-a855-4a37-8a8a-30026a91fef1',
                name: 'Sentinel-2 L2A Provider',
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

    readonly selectedDataSource = signal<string>(DATA_SOURCES[0].key);
    readonly selectedPresetIndex = signal<number>(0);

    readonly dataSourceGroups = computed(() => {
        const groups = new Map<string, DataSourceDefinition[]>();
        for (const ds of DATA_SOURCES) {
            const group = groups.get(ds.groupName) ?? [];
            group.push(ds);
            groups.set(ds.groupName, group);
        }
        return Array.from(groups.entries()).map(([groupName, items]) => ({groupName, items}));
    });

    readonly currentPresets = computed(() => {
        const key = this.selectedDataSource();
        const ds = DATA_SOURCES.find((d) => d.key === key);
        return ds?.presets ?? [];
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

            const items = await this.layerService.getLayerCollectionItems(params.connectorId, params.collectionId);

            const layer = items.items.find((item) => item.name === params.name);

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

                this.onToolbarResize();
                const topToolbarObserver = new ResizeObserver(() => this.onToolbarResize());
                topToolbarObserver.observe(this.topToolbar().nativeElement);
            },
        });
    }

    onResize(): void {
        this.totalHeight.set(window.innerHeight);
    }

    onToolbarResize(): void {
        this.topToolbarHeight.set(this.topToolbar().nativeElement.offsetHeight);
    }

    setSelectedDataSource(value: string): void {
        this.selectedDataSource.set(value);
        const ds = DATA_SOURCES.find((d) => d.key === value);
        this.selectedPresetIndex.set(ds?.defaultPresetIndex ?? 0);
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

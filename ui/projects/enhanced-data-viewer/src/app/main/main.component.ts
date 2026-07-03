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

interface DataSourceDefinition {
    key: string;
    groupName: string;
    connectorId: string;
    collectionId: string;
    name: string;
}

interface DataSourceLayer {
    dataConnectorId: string;
    layerId: string;
}

const DATA_SOURCES: DataSourceDefinition[] = [
    {
        key: 'sentinel1-static',
        groupName: 'static',
        connectorId: 'ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74',
        collectionId: '05102bb3-a855-4a37-8a8a-30026a91fef1',
        name: 'Sentinel-1 Global Mosaics',
    },
    {
        key: 'sentinel2-static',
        groupName: 'static',
        connectorId: 'ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74',
        collectionId: '05102bb3-a855-4a37-8a8a-30026a91fef1',
        name: 'Sentinel-2 L2A',
    },
    {
        key: 'sentinel2-stac',
        groupName: 'stac',
        connectorId: 'ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74',
        collectionId: '05102bb3-a855-4a37-8a8a-30026a91fef1',
        name: 'Sentinel-2 L2A STAC',
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

    readonly dataSourceGroups = computed(() => {
        const groups = new Map<string, DataSourceDefinition[]>();
        for (const ds of DATA_SOURCES) {
            const group = groups.get(ds.groupName) ?? [];
            group.push(ds);
            groups.set(ds.groupName, group);
        }
        return Array.from(groups.entries()).map(([groupName, items]) => ({groupName, items}));
    });

    readonly dataSourceResources: Record<string, ResourceRef<DataSourceLayer | undefined>>;

    readonly mapTileLayer = computed(() => {
        const key = this.selectedDataSource();
        const res = this.dataSourceResources[key];
        return res?.value();
    });
    readonly testIsVisible = signal(true);

    constructor() {
        this.dataSourceResources = Object.fromEntries(
            DATA_SOURCES.map((ds) => [
                ds.key,
                resource({
                    params: () => ({}),
                    loader: async () => {
                        const items = await this.layerService.getLayerCollectionItems(ds.connectorId, ds.collectionId);

                        const layer = items.items.find((item) => item.name === ds.name);

                        if (!layer) return undefined;

                        const id = layer.id as ProviderLayerId;

                        return {
                            dataConnectorId: id.providerId,
                            layerId: id.layerId,
                        };
                    },
                }),
            ]),
        );

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

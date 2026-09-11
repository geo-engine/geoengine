import {ChangeDetectionStrategy, Component, ResourceRef, afterNextRender, computed, inject, input, resource, signal} from '@angular/core';
import {CoreModule, ProjectService} from '@geoengine/core';
import {A11yModule} from '@angular/cdk/a11y';
import {MatDatepickerModule, MatDatepickerInputEvent} from '@angular/material/datepicker';
import {MatCheckboxModule} from '@angular/material/checkbox';
import {LayersService, Time, TimeStepDuration} from '@geoengine/common';
import {toSignal} from '@angular/core/rxjs-interop';
import {CollectionItem} from '@geoengine/api-client';
import {ProviderLayerId} from '@geoengine/api-client/dist/models/ProviderLayerId';

type PresetCategory = 'static' | 'harvested' | 'adHoc';

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

const PRESET_CATEGORY_LABELS: Record<PresetCategory, string> = {
    static: 'Static',
    harvested: 'Harvested',
    adHoc: 'Ad-hoc (Data Provider)',
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
                category: 'adHoc',
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
                displayName: 'True Color Image (TCI)',
                backgroundImage: 'assets/rgb.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Sentinel-2 L2A True Color Image (Harvested)',
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
                category: 'adHoc',
            },
        ],
    },
    {
        key: 'landsat',
        name: 'Landsat C2 L1 OLI/TIRS',
        defaultPresetIndex: 0,
        defaultTime: 1767916800000,
        defaultTimeStep: {durationAmount: 1, durationUnit: 'day'},
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
                category: 'adHoc',
            },
            {
                displayName: 'True Color',
                backgroundImage: 'assets/rgb.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Landsat C2 L1 OLI/TIRS Provider True Color',
                category: 'adHoc',
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
                category: 'adHoc',
            },
            {
                displayName: 'True Color',
                backgroundImage: 'assets/rgb.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'OpenGeoHub Landsat Bimonthly Mosaic Provider True Color',
                category: 'adHoc',
            },
        ],
    },
];

@Component({
    selector: 'geoengine-layers',
    changeDetection: ChangeDetectionStrategy.OnPush,
    template: `
        <div>
            <h4>Data Source</h4>
            <mat-radio-group class="data-sources" [value]="selectedDataSource()" (change)="setSelectedDataSource($event.value)">
                @for (ds of dataSources; track ds.key) {
                    <mat-radio-button [value]="ds.key">{{ ds.name }}</mat-radio-button>
                }
            </mat-radio-group>
        </div>
        <mat-divider></mat-divider>

        <div class="time-selection">
            <h4>Time Selection</h4>
            <mat-checkbox [checked]="autoSelectTime()" (change)="autoSelectTime.set($event.checked)">Auto select time</mat-checkbox>
            <div>
                <button
                    mat-icon-button
                    (click)="timeBackwards()"
                    matTooltip="Backwards {{ timeStepDuration()?.durationAmount }} {{ timeStepDuration()?.durationUnit }}"
                >
                    <mat-icon>navigate_before</mat-icon>
                </button>
                <input matInput [matDatepicker]="picker" size="0" [value]="currentDate()" (dateChange)="setDate($event)" />
                <button matButton (click)="picker.open()" class="calendar-open">{{ formattedTime() }}</button>
                <mat-datepicker #picker></mat-datepicker>
                <button
                    mat-icon-button
                    (click)="timeForward()"
                    matTooltip="Forward {{ timeStepDuration()?.durationAmount }} {{ timeStepDuration()?.durationUnit }}"
                >
                    <mat-icon>navigate_next</mat-icon>
                </button>
            </div>
        </div>
        <mat-divider></mat-divider>

        <div>
            <h4>Visualization Presets</h4>
            <mat-nav-list class="visualization-presets">
                @for (group of presetGroups(); track group.category) {
                    @if (debug()) {
                        <span class="preset-group-label">{{ group.label }}</span>
                    }
                    @for (preset of group.presets; track $index) {
                        <mat-list-item
                            [activated]="currentPresets().indexOf(preset) === selectedPresetIndex()"
                            [class.preset-active]="currentPresets().indexOf(preset) === selectedPresetIndex()"
                            (click)="selectPreset(currentPresets().indexOf(preset))"
                        >
                            <img matListItemTitle [src]="preset.backgroundImage" [alt]="preset.displayName" class="preset-icon" />
                            <span matListItemTitle>{{ preset.displayName }}</span>
                        </mat-list-item>
                    }
                }
            </mat-nav-list>
        </div>
        <mat-divider></mat-divider>
    `,
    styles: [
        `
            .data-sources {
                display: flex;
                flex-direction: column;
                gap: 0.25rem;

                .data-source-group-label {
                    font-size: 0.75rem;
                    font-weight: 600;
                    text-transform: uppercase;
                    letter-spacing: 0.05em;
                    color: var(--geoengine-primary-color, #2f6dff);
                    margin-top: 0.5rem;
                    margin-bottom: 0.125rem;

                    &:first-child {
                        margin-top: 0;
                    }
                }

                mat-radio-button {
                    display: inline-block;
                }
            }

            .time-selection {
                div {
                    display: flex;
                    flex-direction: row;
                    align-items: center;
                    gap: 0.5rem;
                }
                input,
                mat-datepicker {
                    visibility: hidden;
                    height: 0px;
                    width: 0px;
                    padding: 0;
                    margin: 0;
                    border: none;
                }
                .calendar-open {
                    flex: 1;
                }
            }

            .visualization-presets {
                display: flex;
                flex-direction: row;
                flex-wrap: wrap;
                gap: 0.5rem;

                .preset-group-label {
                    width: 100%;
                    font-size: 0.75rem;
                    font-weight: 600;
                    text-transform: uppercase;
                    letter-spacing: 0.05em;
                    color: var(--geoengine-primary-color, #2f6dff);
                    margin-top: 0.5rem;

                    &:first-child {
                        margin-top: 0;
                    }
                }

                width: 100%;
                border: none;

                mat-list-item {
                    width: calc(50% - 0.25rem);
                    text-align: center;
                    padding: 0;
                    cursor: pointer;
                    border: 2px solid transparent;
                    border-radius: 6px;
                    transition:
                        border-color 120ms ease,
                        box-shadow 120ms ease;
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    gap: 0;
                    overflow: hidden;

                    img {
                        width: 100%;
                        display: block;
                        border-radius: 4px;
                    }

                    span {
                        color: white;
                        text-shadow: 0 0 5px rgba(0, 0, 0, 0.7);
                        transform: translateY(-1rem);
                        margin-bottom: -1rem;
                        font-size: 0.85rem;
                        line-height: 1.2;
                    }

                    &.preset-active {
                        border-color: var(--geoengine-primary-color, #2f6dff);
                        box-shadow: 0 0 0 2px rgb(47 109 255 / 25%);

                        span {
                            font-weight: 600;
                        }
                    }
                }
            }

            geoengine-small-time-interaction ::ng-deep {
                /* TODO: fix this in the component itself */

                button:not(:first-child):not(:last-child) {
                    font-size: 0.65rem;
                }

                button:first-child {
                    width: 1rem;
                    height: 1rem;
                    margin-left: -1rem;
                    margin-right: 1rem;
                }

                button:last-child {
                    width: 1rem;
                    height: 1rem;
                }
            }
        `,
    ],
    imports: [A11yModule, CoreModule, MatDatepickerModule, MatCheckboxModule],
})
export class LayersComponent {
    readonly projectService = inject(ProjectService);
    private readonly layerService = inject(LayersService);

    readonly debug = input(false);

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
    readonly dataSources = DATA_SOURCES;

    readonly selectedDataSource = signal<string>(DATA_SOURCES[0].key);
    readonly selectedPresetIndex = signal<number>(0);
    readonly autoSelectTime = signal<boolean>(true);

    readonly currentPresets = computed(() => {
        const key = this.selectedDataSource();
        const ds = DATA_SOURCES.find((d) => d.key === key);
        const presets = ds?.presets ?? [];

        // The static and ad-hoc presets are not production-ready yet; they are hidden
        // unless the app is opened with the `debug` query parameter.
        if (this.debug()) return presets;
        return presets.filter((preset) => preset.category === 'harvested');
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
            };
        },
    });

    readonly mapTileLayer = computed(() => this.mapTileLayerResource.value());

    constructor() {
        afterNextRender(() => {
            void this.setInitialTime();
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

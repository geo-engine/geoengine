import {ChangeDetectionStrategy, Component, ResourceRef, afterNextRender, computed, inject, input, resource, signal} from '@angular/core';
import {CoreModule, ProjectService} from '@geoengine/core';
import {A11yModule} from '@angular/cdk/a11y';
import {MatDatepickerModule, MatDatepickerInputEvent} from '@angular/material/datepicker';
import {MatCheckboxModule} from '@angular/material/checkbox';
import {MatListModule} from '@angular/material/list';
import {LayersService, Time} from '@geoengine/common';
import {toSignal} from '@angular/core/rxjs-interop';
import {CollectionItem} from '@geoengine/api-client';
import {ProviderLayerId} from '@geoengine/api-client/dist/models/ProviderLayerId';
import {DATA_SOURCES, DataSourceLayer, PRESET_CATEGORY_LABELS, PresetCategory, VisualizationPreset} from './data-sources';

@Component({
    selector: 'geoengine-layers',
    changeDetection: ChangeDetectionStrategy.OnPush,
    template: `
        <div>
            <h2>Data Source</h2>
            <mat-selection-list [multiple]="false" class="data-sources" (selectionChange)="onDataSourceSelectionChange($event.options)">
                @for (ds of dataSources; track ds.key) {
                    <mat-list-option [value]="ds.key" [selected]="selectedDataSource() === ds.key" [matTooltip]="ds.name">
                        <span matListItemTitle>{{ ds.name }}</span>
                    </mat-list-option>
                }
            </mat-selection-list>
        </div>
        <mat-divider></mat-divider>

        <div class="time-selection">
            <h2>Time Selection</h2>
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
            <h2>Visualization Presets</h2>
            <mat-nav-list class="visualization-presets">
                @for (group of presetGroups(); track group.category) {
                    @if (debug()) {
                        <span class="preset-group-label">{{ group.label }}</span>
                    }
                    @for (preset of group.presets; track $index) {
                        <mat-list-item
                            [activated]="$index === selectedPresetIndex()"
                            [class.preset-active]="$index === selectedPresetIndex()"
                            (click)="selectPreset($index)"
                            [matTooltip]="preset.displayName"
                            [style.backgroundImage]="'url(' + preset.backgroundImage + ')'"
                        >
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
            $text1: 1rem;
            $text2: 0.85rem;
            $text3: 0.75rem;

            :host {
                display: block;
                padding: 1rem 0.25rem 1rem;
            }

            h2 {
                margin: 0 0 0.5rem;
                font-size: $text1;
                font-weight: 600;
                color: var(--mat-sys-on-surface);
            }

            mat-divider {
                margin: 1rem 0;
            }

            .data-sources {
                padding: 0;
                margin: -0.25rem;

                mat-list-option {
                    border-radius: 0.5rem;
                    padding: 0 0.25rem;

                    --mat-list-list-item-label-text-size: #{$text2};

                    span {
                        display: -webkit-box !important;
                        -webkit-line-clamp: 2;
                        -webkit-box-orient: vertical;
                        overflow: hidden;
                        white-space: normal !important;
                    }

                    ::ng-deep {
                        .mdc-list-item__end {
                            margin: 0;
                            padding: 0;
                        }
                        .mdc-radio {
                            padding-right: 0;
                        }
                    }
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
                width: 100%;
                border: none;
                margin-top: 0.5rem;

                .preset-group-label {
                    width: 100%;
                    font-size: $text3;
                    font-weight: 600;
                    text-transform: uppercase;
                    letter-spacing: 0.05em;
                    color: var(--geoengine-primary-color, #2f6dff);
                    margin-top: 0.5rem;

                    &:first-child {
                        margin-top: 0;
                    }
                }

                mat-list-item {
                    width: calc(50% - 0.25rem);
                    text-align: center;
                    padding: 0;
                    cursor: pointer;
                    border: 3px solid transparent;
                    border-radius: 0.5rem;
                    transition:
                        border-color 120ms ease,
                        box-shadow 120ms ease,
                        transform 120ms ease;
                    overflow: hidden;

                    height: auto;
                    aspect-ratio: 2 / 1;
                    background-size: cover;
                    background-position: center;

                    ::ng-deep .mdc-list-item__content {
                        align-self: flex-end;
                    }

                    [matListItemTitle] {
                        display: block;
                        width: 100%;
                        color: white;
                        text-shadow: 0 0 0.5rem rgba(0, 0, 0, 0.7);
                        font-size: $text2;
                        overflow: hidden;
                        text-overflow: ellipsis;
                    }

                    &.preset-active {
                        border-color: var(--geoengine-primary-color);

                        [matListItemTitle] {
                            font-weight: 600;
                        }
                    }
                }
            }

            .time-selection {
                --mat-button-text-label-text-size: #{$text2};
            }
        `,
    ],
    imports: [A11yModule, CoreModule, MatDatepickerModule, MatCheckboxModule, MatListModule],
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

    onDataSourceSelectionChange(options: readonly {value: string}[]): void {
        const selected = options[0]?.value;
        if (!selected) return;
        void this.setSelectedDataSource(selected);
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

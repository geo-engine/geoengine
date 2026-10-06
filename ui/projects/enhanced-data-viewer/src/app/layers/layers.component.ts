import {ChangeDetectionStrategy, Component, computed, effect, inject, resource, signal} from '@angular/core';
import {CoreModule, ProjectService, RasterLegendViewComponent} from '@geoengine/core';
import {A11yModule} from '@angular/cdk/a11y';
import {EdvLayersService} from './layers.service';
import {MatCheckboxModule} from '@angular/material/checkbox';
import {MatListModule} from '@angular/material/list';
import {MatProgressSpinnerModule} from '@angular/material/progress-spinner';
import {LayersService, RasterColorizer, RasterLayer, RasterLayerMetadata, RasterSymbology, Time} from '@geoengine/common';
import {toSignal} from '@angular/core/rxjs-interop';
import type {DataSourceDefinition} from './data-sources';
import {MatDatepickerInputEvent, MatDatepickerModule} from '@angular/material/datepicker';

@Component({
    selector: 'geoengine-layers',
    changeDetection: ChangeDetectionStrategy.OnPush,
    template: `
        @if (catalogueError(); as error) {
            <p class="catalogue-message catalogue-error">{{ error }}</p>
            <button matButton type="button" (click)="retryCatalogue()">Retry</button>
        } @else if (!catalogueLoading() && dataSources().length === 0) {
            <p class="catalogue-message">No data sources are available for this configuration.</p>
        }

        <div>
            <h2>Data Source</h2>
            @if (catalogueLoading()) {
                <div class="catalogue-loading" role="status">
                    <mat-spinner diameter="24" aria-label="Loading data sources"></mat-spinner>
                    <span>Loading data sources…</span>
                </div>
            }
            <mat-selection-list
                [multiple]="false"
                class="data-sources"
                [attr.aria-busy]="catalogueLoading()"
                (selectionChange)="onDataSourceSelectionChange($event.options)"
            >
                @for (dataSource of dataSources(); track dataSource.key) {
                    <mat-list-option
                        [value]="dataSource.key"
                        [selected]="selectedDataSource()?.key === dataSource.key"
                        [matTooltip]="dataSource.name"
                    >
                        <span matListItemTitle>{{ dataSource.name }}</span>
                    </mat-list-option>
                }
            </mat-selection-list>
        </div>
        <mat-divider></mat-divider>

        @if (currentVariants().length > 1) {
            <div>
                <h2>Region</h2>
                <mat-selection-list [multiple]="false" class="variants" (selectionChange)="onVariantSelectionChange($event.options)">
                    @for (variant of currentVariants(); track variant.key) {
                        <mat-list-option
                            [value]="variant.key"
                            [selected]="selectedVariant().key === variant.key"
                            [matTooltip]="variant.crs ?? variant.name"
                        >
                            <span matListItemTitle>{{ variant.name }}</span>
                        </mat-list-option>
                    }
                </mat-selection-list>
            </div>
            <mat-divider></mat-divider>
        }

        <div class="time-selection">
            <h2>Time Selection</h2>
            <mat-checkbox [checked]="autoSelectTime()" (change)="autoSelectTime.set($event.checked)">Auto select time</mat-checkbox>
            <div>
                <button
                    matIconButton
                    (click)="timeBackwards()"
                    matTooltip="Backwards {{ timeStepDuration()?.durationAmount }} {{ timeStepDuration()?.durationUnit }}"
                >
                    <mat-icon>navigate_before</mat-icon>
                </button>
                <input matInput [matDatepicker]="picker" size="0" [value]="currentDate()" (dateChange)="setDate($event)" />
                <button matButton (click)="picker.open()" class="calendar-open">{{ formattedTime() }}</button>
                <mat-datepicker #picker></mat-datepicker>
                <button
                    matIconButton
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
            @if (catalogueLoading() || variantLoading()) {
                <div class="catalogue-loading" role="status">
                    <mat-spinner diameter="24" aria-label="Loading visualization presets"></mat-spinner>
                    <span>Loading visualization presets…</span>
                </div>
            }
            @if (variantError(); as error) {
                <p class="catalogue-message catalogue-error">{{ error }}</p>
                <button matButton type="button" (click)="retryVariant()">Retry</button>
            }
            <mat-nav-list class="visualization-presets" [attr.aria-busy]="catalogueLoading() || variantLoading()">
                @for (group of presetGroups(); track group.category) {
                    @if (debug()) {
                        <span class="preset-group-label">{{ group.label }}</span>
                    }
                    @for (preset of group.presets; track preset.key) {
                        <mat-list-item
                            [activated]="preset === selectedPreset()"
                            [class.preset-active]="preset === selectedPreset()"
                            (click)="selectPreset(preset)"
                            [matTooltip]="preset.displayName"
                            [style.backgroundImage]="'url(' + preset.backgroundImage + ')'"
                        >
                            <span matListItemTitle>{{ preset.displayName }}</span>
                        </mat-list-item>
                    }
                }
            </mat-nav-list>
            <div class="apply-preset-action">
                <button mat-flat-button color="primary" type="button" [disabled]="!canApplyPreset()" (click)="applySelectedPreset()">
                    Apply visualization
                </button>
            </div>
        </div>
        <mat-divider></mat-divider>

        @if (isLegendVisible()) {
            <div class="legend">
                <h2>Legend</h2>
                @if (legendLayer.isLoading()) {
                    <mat-progress-spinner mode="indeterminate" diameter="32"></mat-progress-spinner>
                } @else if (legendLayer.status() === 'error') {
                    <span class="legend-error">Failed to load legend</span>
                } @else if (legend(); as legend) {
                    <span class="legend-layer-name" [matTooltip]="legend.layer.name">{{ legend.layer.name }}</span>
                    <geoengine-raster-legend-view [layer]="legend.layer" [metadata]="legend.metadata"></geoengine-raster-legend-view>
                }
            </div>
        }
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

            .catalogue-loading {
                display: flex;
                align-items: center;
                gap: 0.75rem;
                padding: 0.75rem 0;
                font-size: $text2;
                color: var(--mat-sys-on-surface-variant);
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

            .variants {
                padding: 0;
                margin: -0.25rem;

                mat-list-option {
                    border-radius: 0.5rem;
                    padding: 0 0.25rem;
                    --mat-list-list-item-label-text-size: #{$text2};
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

                button[matIconButton],
                a[matIconButton] {
                    display: inline-flex; /* Icons are vertically centered differently otherwise. */
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
                    background-origin: border-box;

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

            .apply-preset-action {
                display: flex;
                justify-content: center;
                margin-top: 1rem;
            }

            .time-selection {
                --mat-button-text-label-text-size: #{$text2};
            }

            .legend {
                .legend-layer-name {
                    display: block;
                    margin-bottom: 0.5rem;
                    font-size: $text2;
                    color: var(--mat-sys-on-surface-variant);
                    white-space: nowrap;
                    overflow: hidden;
                    text-overflow: ellipsis;
                }

                .legend-error {
                    font-size: $text2;
                    color: var(--mat-sys-error);
                }

                mat-progress-spinner {
                    margin: 0 auto;
                }
            }
        `,
    ],
    imports: [
        A11yModule,
        CoreModule,
        MatDatepickerModule,
        MatCheckboxModule,
        MatListModule,
        MatProgressSpinnerModule,
        RasterLegendViewComponent,
    ],
})
export class LayersComponent {
    readonly projectService = inject(ProjectService);
    readonly edvLayersService = inject(EdvLayersService);
    private readonly layerService = inject(LayersService);

    readonly debug = this.edvLayersService.debug;

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
    readonly dataSources = this.edvLayersService.dataSources;
    readonly catalogueLoading = this.edvLayersService.catalogueLoading;
    readonly catalogueError = this.edvLayersService.catalogueError;
    readonly variantLoading = this.edvLayersService.variantLoading;
    readonly variantError = this.edvLayersService.variantError;

    readonly autoSelectTime = signal<boolean>(true);

    readonly selectedDataSource = this.edvLayersService.selectedDataSource;
    readonly currentVariants = this.edvLayersService.currentVariants;
    readonly selectedVariant = this.edvLayersService.selectedVariant;
    readonly currentPresets = this.edvLayersService.currentPresets;
    readonly presetGroups = this.edvLayersService.presetGroups;
    readonly selectedPresetIndex = this.edvLayersService.selectedPresetIndex;
    readonly selectedPreset = this.edvLayersService.selectedPreset;
    readonly canApplyPreset = this.edvLayersService.canApplyPreset;
    readonly mapTileLayer = this.edvLayersService.mapTileLayer;

    readonly legendLayer = resource({
        params: () => ({layerId: this.edvLayersService.mapTileLayer()}),
        loader: async ({params: {layerId}}): Promise<{layer: RasterLayer; metadata: RasterLayerMetadata} | undefined> => {
            if (!layerId) return undefined;

            const layer = await this.layerService.getLayer(layerId.dataConnectorId, layerId.layerId);

            const processingGraphId = await this.layerService.registerAndGetLayerWorkflowId(layerId.dataConnectorId, layerId.layerId);

            if (layer.symbology?.type !== 'raster') return undefined;

            const metadata = await this.layerService.getWorkflowIdMetadata(processingGraphId);

            if (!(metadata instanceof RasterLayerMetadata)) return undefined;

            const rasterSymbology = layer.symbology;

            const rasterLayer = new RasterLayer({
                name: layer.name,
                workflowId: processingGraphId,
                isVisible: true,
                isLegendVisible: true,
                symbology: new RasterSymbology(rasterSymbology.opacity, RasterColorizer.fromDict(rasterSymbology.rasterColorizer)),
            });

            return {layer: rasterLayer, metadata};
        },
    });

    /** `value()` throws when the resource is in error state, so guard it with `hasValue()`. */
    readonly legend = computed(() => (this.legendLayer.hasValue() ? this.legendLayer.value() : undefined));

    /** Only show the legend if there is a map tile layer and the legend is either loading, in error state, or has a value. */
    readonly isLegendVisible = computed(
        () => !!this.mapTileLayer() && (this.legendLayer.isLoading() || this.legendLayer.status() === 'error' || !!this.legend()),
    );

    constructor() {
        effect(() => {
            const source = this.selectedDataSource();
            if (source) void this.applyDataSourceTime(source);
        });
    }

    readonly retryCatalogue = (): void => this.edvLayersService.retryCatalogue();
    readonly retryVariant = (): void => this.edvLayersService.retryVariant();

    onDataSourceSelectionChange(options: readonly {value: string}[]): void {
        const selected = options[0]?.value;
        if (!selected) return;
        void this.setSelectedDataSource(selected);
    }

    setSelectedDataSource(key: string): void {
        const dataSource = this.dataSources().find((d) => d.key === key);
        if (!dataSource) return;
        this.edvLayersService.setSelectedDataSource(dataSource.key);
    }

    onVariantSelectionChange(options: readonly {value: string}[]): void {
        const selected = options[0]?.value;
        if (selected) this.edvLayersService.setSelectedVariant(selected);
    }

    setSelectedVariant(key: string): void {
        this.edvLayersService.setSelectedVariant(key);
    }

    private async applyDataSourceTime(dataSource: DataSourceDefinition): Promise<void> {
        if (this.autoSelectTime() && dataSource.defaultTime) await this.projectService.setTime(new Time(new Date(dataSource.defaultTime)));
        if (dataSource.defaultTimeStep) this.projectService.setTimeStepDuration(dataSource.defaultTimeStep);
    }

    selectPreset(preset: DataSourceDefinition['variants'][number]['presets'][number]): void {
        this.edvLayersService.setSelectedPreset(preset.key);
    }

    applySelectedPreset(): void {
        this.edvLayersService.applySelectedPreset();
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

import {ChangeDetectionStrategy, Component, computed, inject, resource, signal} from '@angular/core';
import {CoreModule, ProjectService, RasterLegendViewComponent} from '@geoengine/core';
import {A11yModule} from '@angular/cdk/a11y';
import {EdvLayersService} from './layers.service';
import {MatCheckboxModule} from '@angular/material/checkbox';
import {MatListModule} from '@angular/material/list';
import {MatProgressSpinnerModule} from '@angular/material/progress-spinner';
import {LayersService, RasterColorizer, RasterLayer, RasterLayerMetadata, RasterSymbology, Time} from '@geoengine/common';
import {toSignal} from '@angular/core/rxjs-interop';
import type {DataSourceDefinition, VisualizationPreset} from './data-sources';
import {MatDatepickerInputEvent, MatDatepickerModule} from '@angular/material/datepicker';
import {MatFormFieldModule} from '@angular/material/form-field';
import {MatSelectModule} from '@angular/material/select';

@Component({
    selector: 'geoengine-layers',
    changeDetection: ChangeDetectionStrategy.OnPush,
    template: `
        @if (catalogueError(); as error) {
            <div class="catalogue-notice" role="alert">
                <p class="catalogue-message catalogue-error">{{ error }}</p>
                <button mat-stroked-button type="button" (click)="retryCatalogue()">Retry</button>
            </div>
        } @else if (!catalogueLoading() && dataSources().length === 0) {
            <p class="catalogue-message">No data sources are available for this configuration.</p>
        }

        <section class="panel-section" aria-labelledby="data-source-heading">
            <h2 id="data-source-heading">Data Source</h2>
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
        </section>
        <mat-divider></mat-divider>

        @if (currentVariants().length > 1) {
            <section class="panel-section" aria-labelledby="region-heading">
                <h2 id="region-heading">Region</h2>
                <mat-form-field appearance="outline" subscriptSizing="dynamic" class="region-select">
                    <mat-label>Available region</mat-label>
                    <mat-select panelWidth="320px" [value]="selectedVariant().key" (selectionChange)="setSelectedVariant($event.value)">
                        @for (variant of sortedVariants(); track variant.key) {
                            <mat-option [value]="variant.key">{{ variant.name }} ({{ variant.crs }})</mat-option>
                        }
                    </mat-select>
                </mat-form-field>
                @if (hasCoverageVariants()) {
                    <button
                        mat-stroked-button
                        type="button"
                        class="map-center-action"
                        aria-label="Select region at map center"
                        aria-describedby="map-center-hint"
                        [disabled]="!mapCenterVariantKey()"
                        (click)="selectMapCenterVariant()"
                    >
                        <mat-icon>my_location</mat-icon>
                        Select at map center
                    </button>
                    <p id="map-center-hint" class="catalogue-message">Choose the region covering the center of the map.</p>
                    @if (mapCenterSelectionMessage(); as message) {
                        <p class="catalogue-message" role="status">{{ message }}</p>
                    }
                } @else {
                    <p class="catalogue-message" role="status">Map-center selection is unavailable for this source.</p>
                }
            </section>
            <mat-divider></mat-divider>
        }

        <section class="panel-section time-selection" aria-labelledby="time-heading">
            <h2 id="time-heading">Time Selection</h2>
            <mat-checkbox [checked]="autoSelectTime()" (change)="autoSelectTime.set($event.checked)">Auto select time</mat-checkbox>
            <div class="time-controls">
                <button
                    matIconButton
                    type="button"
                    aria-label="Previous time step"
                    (click)="timeBackwards()"
                    matTooltip="Backwards {{ timeStepDuration()?.durationAmount }} {{ timeStepDuration()?.durationUnit }}"
                >
                    <mat-icon>navigate_before</mat-icon>
                </button>
                <input
                    matInput
                    [matDatepicker]="picker"
                    tabindex="-1"
                    aria-label="Selected date"
                    [value]="currentDate()"
                    (dateChange)="setDate($event)"
                />
                <button mat-stroked-button type="button" (click)="picker.open()" class="calendar-open" aria-label="Choose date">
                    <mat-icon>event</mat-icon>
                    {{ formattedTime() }}
                </button>
                <mat-datepicker #picker></mat-datepicker>
                <button
                    matIconButton
                    type="button"
                    aria-label="Next time step"
                    (click)="timeForward()"
                    matTooltip="Forward {{ timeStepDuration()?.durationAmount }} {{ timeStepDuration()?.durationUnit }}"
                >
                    <mat-icon>navigate_next</mat-icon>
                </button>
            </div>
        </section>
        <mat-divider></mat-divider>

        <section class="panel-section" aria-labelledby="presets-heading">
            <h2 id="presets-heading">Visualization Presets</h2>
            @if (catalogueLoading() || variantLoading()) {
                <div class="catalogue-loading" role="status">
                    <mat-spinner diameter="24" aria-label="Loading visualization presets"></mat-spinner>
                    <span>Loading visualization presets…</span>
                </div>
            }
            @if (variantError(); as error) {
                <div role="alert">
                    <p class="catalogue-message catalogue-error">{{ error }}</p>
                    <button mat-stroked-button type="button" class="retry-variant" (click)="retryVariant()">Retry</button>
                </div>
            }
            <mat-nav-list class="visualization-presets" [attr.aria-busy]="catalogueLoading() || variantLoading()">
                @for (preset of currentPresets(); track preset.key) {
                    <mat-list-item
                        [activated]="preset === selectedPreset()"
                        [class.preset-active]="preset === selectedPreset()"
                        [attr.aria-current]="preset === selectedPreset() ? 'true' : null"
                        (click)="selectPreset(preset)"
                        [matTooltip]="preset.displayName"
                        [style.backgroundImage]="'linear-gradient(transparent, rgba(0, 0, 0, 0.65)), url(' + preset.backgroundImage + ')'"
                    >
                        <span matListItemTitle>{{ preset.displayName }}</span>
                        @if (preset === selectedPreset()) {
                            <span matListItemMeta class="preset-selected-indicator" aria-hidden="true">
                                <mat-icon>check</mat-icon>
                            </span>
                        }
                    </mat-list-item>
                }
            </mat-nav-list>
            <div class="apply-preset-action">
                <button mat-flat-button color="primary" type="button" [disabled]="!canApplyPreset()" (click)="applySelectedPreset()">
                    Apply visualization
                </button>
            </div>
        </section>

        @if (isLegendVisible()) {
            <mat-divider></mat-divider>
            <section class="panel-section legend" aria-labelledby="legend-heading">
                <h2 id="legend-heading">Legend</h2>
                @if (legendLayer.isLoading()) {
                    <mat-progress-spinner mode="indeterminate" diameter="32"></mat-progress-spinner>
                } @else if (legendLayer.status() === 'error') {
                    <span class="legend-error">Failed to load legend</span>
                } @else if (legend(); as legend) {
                    <span class="legend-layer-name" [matTooltip]="legend.layer.name">{{ legend.layer.name }}</span>
                    <geoengine-raster-legend-view [layer]="legend.layer" [metadata]="legend.metadata"></geoengine-raster-legend-view>
                }
            </section>
        }
    `,
    styles: [
        `
            $text1: 1rem;
            $text2: 0.85rem;

            :host {
                display: block;
                min-width: 0;
                padding: 1rem 0.5rem;
                color: var(--geoengine-foreground-text-color);

                --mat-button-outlined-label-text-tracking: normal;
                --mat-button-filled-label-text-tracking: normal;
            }

            .panel-section {
                display: flex;
                flex-direction: column;
                gap: 0.75rem;
                min-width: 0;
            }

            h2 {
                margin: 0;
                font-size: $text1;
                line-height: 1.5;
                font-weight: 600;
            }

            mat-divider {
                margin: 1.25rem 0;
            }

            .catalogue-notice {
                margin-bottom: 1rem;

                button {
                    margin-top: 0.5rem;
                }
            }

            .retry-variant {
                margin-top: 0.5rem;
            }

            .catalogue-message {
                margin: 0;
                font-size: $text2;
                line-height: 1.5;
                color: var(--geoengine-foreground-secondary-text-color);
            }

            .catalogue-error {
                color: var(--geoengine-warn-color);
            }

            .catalogue-loading {
                display: flex;
                align-items: center;
                gap: 0.75rem;
                padding: 0.25rem 0;
                font-size: $text2;
                color: var(--geoengine-foreground-secondary-text-color);
            }

            .data-sources {
                padding: 0;
                margin: 0;

                mat-list-option {
                    border-radius: 0.5rem;
                    padding: 0 0.5rem;

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

            .region-select {
                display: block;
                width: 100%;

                --mat-form-field-container-text-size: #{$text2};
                --mat-select-trigger-text-size: #{$text2};
            }

            .map-center-action {
                width: 100%;
                height: 2.75rem;
                padding: 0 0.75rem;
                border-radius: 0.5rem;
                font-size: $text2;
                line-height: 1.4;
                white-space: nowrap;

                &:not(:disabled) {
                    color: var(--geoengine-primary-color);
                    border-color: var(--geoengine-primary-color);
                    background-color: color-mix(in srgb, var(--geoengine-primary-color) 6%, var(--geoengine-card-background-color));
                }
            }

            .time-selection {
                mat-checkbox {
                    --mat-checkbox-label-text-size: #{$text2};
                }

                .time-controls {
                    display: flex;
                    align-items: center;
                    gap: 0.25rem;
                }

                input {
                    position: absolute;
                    visibility: hidden;
                    height: 0;
                    width: 0;
                    padding: 0;
                    margin: 0;
                    border: none;
                }

                .calendar-open {
                    flex: 1;
                    min-width: 0;
                    padding: 0 0.5rem;
                    border-radius: 0.5rem;
                    font-size: $text2;
                }

                button[matIconButton] {
                    display: inline-flex; /* Icons are vertically centered differently otherwise. */
                    flex-shrink: 0;
                    --mat-icon-button-state-layer-size: 2.5rem;
                }
            }

            .visualization-presets {
                display: grid;
                grid-template-columns: repeat(2, minmax(0, 1fr));
                gap: 0.75rem;
                padding: 0;
                margin: 0;

                mat-list-item {
                    position: relative;
                    box-sizing: border-box;
                    width: 100%;
                    min-width: 0;
                    text-align: center;
                    padding: 0;
                    cursor: pointer;
                    border: 2px solid transparent;
                    border-radius: 0.5rem;
                    transition:
                        border-color 120ms ease,
                        box-shadow 120ms ease;
                    overflow: hidden;

                    height: auto;
                    aspect-ratio: 1.6 / 1;
                    background-size: cover;
                    background-position: center;
                    background-origin: border-box;

                    ::ng-deep .mdc-list-item__content {
                        align-self: flex-end;
                    }

                    [matListItemTitle] {
                        display: -webkit-box;
                        -webkit-line-clamp: 2;
                        -webkit-box-orient: vertical;
                        width: 100%;
                        color: white;
                        text-shadow: 0 0 0.5rem rgba(0, 0, 0, 0.7);
                        font-size: $text2;
                        line-height: 1.35;
                        white-space: normal;
                        overflow: hidden;
                    }

                    &.preset-active {
                        border-color: white;
                        box-shadow: 0 0 0 3px var(--geoengine-primary-color);

                        [matListItemTitle] {
                            font-weight: 600;
                        }
                    }
                }
            }

            .preset-selected-indicator {
                position: absolute;
                top: 0.25rem;
                right: 0.25rem;
                z-index: 1;
                display: grid;
                place-items: center;
                box-sizing: border-box;
                width: 1.5rem;
                height: 1.5rem;
                margin: 0;
                border: 2px solid white;
                border-radius: 50%;
                background-color: var(--geoengine-primary-color);
                color: white;
                pointer-events: none;

                mat-icon {
                    width: 1rem;
                    height: 1rem;
                    font-size: 1rem;
                    line-height: 1;
                    color: white;
                }
            }

            .apply-preset-action {
                button {
                    width: 100%;
                    min-height: 2.75rem;
                    border-radius: 0.5rem;
                }
            }

            .legend {
                .legend-layer-name {
                    display: block;
                    line-height: 1.5;
                    font-size: $text2;
                    color: var(--geoengine-foreground-secondary-text-color);
                    white-space: nowrap;
                    overflow: hidden;
                    text-overflow: ellipsis;
                }

                .legend-error {
                    font-size: $text2;
                    color: var(--geoengine-warn-color);
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
        MatFormFieldModule,
        MatCheckboxModule,
        MatListModule,
        MatSelectModule,
        MatProgressSpinnerModule,
        RasterLegendViewComponent,
    ],
})
export class LayersComponent {
    readonly projectService = inject(ProjectService);
    readonly edvLayersService = inject(EdvLayersService);
    private readonly layerService = inject(LayersService);

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
    readonly sortedVariants = this.edvLayersService.sortedVariants;
    readonly hasCoverageVariants = this.edvLayersService.hasCoverageVariants;
    readonly mapCenterVariantKey = this.edvLayersService.mapCenterVariantKey;
    readonly mapCenterSelectionMessage = this.edvLayersService.mapCenterSelectionMessage;
    readonly selectedVariant = this.edvLayersService.selectedVariant;
    readonly currentPresets = this.edvLayersService.currentPresets;
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

    selectMapCenterVariant(): void {
        this.edvLayersService.selectMapCenterVariant();
    }

    private async applyDataSourceTime(dataSource: DataSourceDefinition): Promise<void> {
        if (this.autoSelectTime() && dataSource.defaultTime) await this.projectService.setTime(new Time(new Date(dataSource.defaultTime)));
        if (dataSource.defaultTimeStep) this.projectService.setTimeStepDuration(dataSource.defaultTimeStep);
    }

    selectPreset(preset: VisualizationPreset): void {
        this.edvLayersService.setSelectedPreset(preset.key);
    }

    applySelectedPreset(): void {
        if (!this.canApplyPreset()) return;

        const source = this.selectedDataSource();
        const sourceChanged = source !== this.edvLayersService.appliedDataSource();
        this.edvLayersService.applySelectedPreset();
        if (source && sourceChanged) void this.applyDataSourceTime(source);
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

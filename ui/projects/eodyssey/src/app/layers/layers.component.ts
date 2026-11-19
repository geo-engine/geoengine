import {A11yModule} from '@angular/cdk/a11y';
import {ChangeDetectionStrategy, Component, DestroyRef, computed, effect, inject, resource, untracked} from '@angular/core';
import {MatCalendarView, MatDatepicker, MatDatepickerInputEvent, MatDatepickerModule} from '@angular/material/datepicker';
import {MatFormFieldModule} from '@angular/material/form-field';
import {MatListModule} from '@angular/material/list';
import {MatProgressSpinnerModule} from '@angular/material/progress-spinner';
import {MatSelectModule} from '@angular/material/select';
import {MatSlideToggleModule} from '@angular/material/slide-toggle';
import {LayersService, RasterColorizer, RasterLayer, RasterLayerMetadata, RasterSymbology, Time} from '@geoengine/common';
import {CoreModule, FootprintCalendarHeaderComponent, FootprintsService, ProjectService, RasterLegendViewComponent} from '@geoengine/core';
import {DataSourceNavigationComponent} from './data-source-navigation.component';
import type {DataSourceDefinition} from './data-sources';
import {EOdysseyLayerService} from './layers.service';

/** The dates the strip below the date shows. */
const STRIP_DATES = 31;

function formatDate(timeMs: number): string {
    return new Time(timeMs).start.format('DD.MM.YYYY');
}

@Component({
    selector: 'geoengine-layers',
    changeDetection: ChangeDetectionStrategy.OnPush,
    template: `
        @if (eodysseyLayerService.catalogueError(); as error) {
            <div class="catalogue-notice" role="alert">
                <p class="catalogue-message catalogue-error">{{ error }}</p>
                <button mat-stroked-button type="button" (click)="eodysseyLayerService.retryCatalogue()">Retry</button>
            </div>
        } @else if (!eodysseyLayerService.catalogueLoading() && eodysseyLayerService.categories().length === 0) {
            <div class="catalogue-notice" role="status">
                <p class="catalogue-message">No data sources are available for this configuration.</p>
                <button mat-stroked-button type="button" (click)="eodysseyLayerService.retryCatalogue()">Retry</button>
            </div>
        }

        <section class="panel-section" aria-labelledby="data-source-heading">
            <h2 id="data-source-heading">Data Source</h2>
            @if (eodysseyLayerService.catalogueLoading()) {
                <div class="catalogue-loading" role="status">
                    <mat-spinner diameter="24" aria-label="Loading data sources"></mat-spinner>
                    <span>Loading data sources…</span>
                </div>
            }
            <geoengine-data-source-navigation></geoengine-data-source-navigation>
        </section>
        <mat-divider></mat-divider>

        @if (eodysseyLayerService.currentVariants().length > 1) {
            <section class="panel-section" aria-labelledby="region-heading">
                <h2 id="region-heading">Region</h2>
                <mat-form-field appearance="outline" subscriptSizing="dynamic" class="region-select">
                    <mat-label>Available region</mat-label>
                    <mat-select
                        panelWidth="320px"
                        [value]="eodysseyLayerService.selectedVariant().key"
                        (selectionChange)="eodysseyLayerService.setSelectedVariant($event.value)"
                    >
                        @for (variant of eodysseyLayerService.sortedVariants(); track variant.key) {
                            <mat-option [value]="variant.key">{{ variant.name }} ({{ variant.crs }})</mat-option>
                        }
                    </mat-select>
                </mat-form-field>
                @if (eodysseyLayerService.hasCoverageVariants()) {
                    <button
                        mat-stroked-button
                        type="button"
                        class="map-center-action"
                        aria-label="Select region at map center"
                        aria-describedby="map-center-hint"
                        [disabled]="!eodysseyLayerService.mapCenterVariantKey()"
                        (click)="eodysseyLayerService.selectMapCenterVariant()"
                    >
                        <mat-icon>my_location</mat-icon>
                        Select at map center
                    </button>
                    <p id="map-center-hint" class="catalogue-message">Choose the region covering the center of the map.</p>
                    @if (eodysseyLayerService.mapCenterSelectionMessage(); as message) {
                        <p class="catalogue-message" role="status">{{ message }}</p>
                    }
                } @else {
                    <p class="catalogue-message" role="status">Map-center selection is unavailable for this source.</p>
                }
            </section>
            <mat-divider></mat-divider>
        }

        <section class="panel-section time-selection" aria-labelledby="time-heading">
            <h2 id="time-heading">Date</h2>
            <div class="time-controls">
                <input
                    matInput
                    [matDatepicker]="picker"
                    tabindex="-1"
                    aria-label="Selected date"
                    [value]="currentDate()"
                    [matDatepickerFilter]="footprintsService.dateFilter"
                    (dateChange)="setDate($event)"
                />
                <button mat-stroked-button type="button" (click)="picker.open()" class="calendar-open" aria-label="Choose date">
                    <mat-icon>event</mat-icon>
                    {{ formattedTime() }}
                </button>
                <mat-datepicker
                    #picker
                    [startView]="footprintsService.calendarStartView()"
                    (monthSelected)="selectCoarseUnit($event, 'year', picker)"
                    (yearSelected)="selectCoarseUnit($event, 'multi-year', picker)"
                    [dateClass]="footprintsService.dateClass"
                    [calendarHeaderComponent]="footprintCalendarHeader"
                ></mat-datepicker>
            </div>
            @if (strip(); as ticks) {
                @if (ticks.length) {
                    <div class="date-strip" role="group" aria-label="Dates around the selected date">
                        @for (tick of ticks; track tick.start) {
                            <!-- not in the tab order: the buttons below and the calendar reach every date -->
                            <button
                                type="button"
                                tabindex="-1"
                                class="tick"
                                [class.tick-data]="tick.status === 'data'"
                                [class.tick-unknown]="tick.status === 'unknown'"
                                [class.tick-selected]="tick.isSelected"
                                [attr.aria-label]="tick.label"
                                [attr.aria-current]="tick.isSelected ? 'date' : null"
                                [matTooltip]="tick.label"
                                (click)="selectTime(tick.start)"
                            ></button>
                        }
                    </div>
                    <p class="catalogue-message strip-legend">
                        <span>{{ ticks[0].date }}</span>
                        <span><span class="tick tick-data" aria-hidden="true"></span> scenes in or near this view</span>
                        <span>{{ ticks[ticks.length - 1].date }}</span>
                    </p>
                }
            }
            <div class="time-jumps">
                <button mat-stroked-button type="button" class="time-backwards" (click)="timeBackwards()">
                    <mat-icon>navigate_before</mat-icon>
                    {{ backwards()?.label }}
                </button>
                <button mat-stroked-button type="button" class="time-forward" (click)="timeForward()">
                    {{ forward()?.label }}
                    <mat-icon iconPositionEnd>navigate_next</mat-icon>
                </button>
            </div>
            @if (footprintsService.tooLarge()) {
                <p class="catalogue-message" role="status">Zoom in to see the dates with data.</p>
            }
        </section>
        <mat-divider></mat-divider>

        <section class="panel-section" aria-labelledby="availability-heading">
            <div class="section-heading">
                <h2 id="availability-heading">Data availability</h2>
                <mat-slide-toggle
                    labelPosition="before"
                    [checked]="footprintsService.overlayVisible()"
                    (change)="footprintsService.overlayVisible.set($event.checked)"
                >
                    <span class="coverage-label">
                        <span class="stripe-swatch" aria-hidden="true"></span>
                        Coverage on map
                    </span>
                </mat-slide-toggle>
            </div>
            @if (status(); as status) {
                <p class="coverage-status" [class.coverage-stale]="footprintsService.overlayLoading()" role="status">
                    <span class="status-dot" [class]="'status-' + status.kind" aria-hidden="true"></span>
                    {{ status.text }}
                </p>
            }
        </section>
        <mat-divider></mat-divider>

        <section class="panel-section" aria-labelledby="presets-heading">
            <div class="section-heading">
                <h2 id="presets-heading">Visualization</h2>
                <mat-slide-toggle
                    labelPosition="before"
                    class="data-on-map"
                    [checked]="eodysseyLayerService.dataOnMap()"
                    (change)="setDataOnMap($event.checked)"
                >
                    Data on map
                </mat-slide-toggle>
            </div>
            @if (eodysseyLayerService.catalogueLoading() || eodysseyLayerService.variantLoading()) {
                <div class="catalogue-loading" role="status">
                    <mat-spinner diameter="24" aria-label="Loading visualization presets"></mat-spinner>
                    <span>Loading visualization presets…</span>
                </div>
            }
            @if (eodysseyLayerService.variantError(); as error) {
                <div role="alert">
                    <p class="catalogue-message catalogue-error">{{ error }}</p>
                    <button mat-stroked-button type="button" class="retry-variant" (click)="eodysseyLayerService.retryVariant()">
                        Retry
                    </button>
                </div>
            }
            <mat-nav-list
                class="visualization-presets"
                [attr.aria-busy]="eodysseyLayerService.catalogueLoading() || eodysseyLayerService.variantLoading()"
            >
                @for (preset of eodysseyLayerService.currentPresets(); track preset.key) {
                    <mat-list-item
                        [activated]="preset === eodysseyLayerService.selectedPreset()"
                        [class.preset-active]="preset === eodysseyLayerService.selectedPreset()"
                        [attr.aria-current]="preset === eodysseyLayerService.selectedPreset() ? 'true' : null"
                        (click)="eodysseyLayerService.setSelectedPreset(preset.key)"
                        [matTooltip]="preset.displayName"
                        [style.backgroundImage]="'linear-gradient(transparent, rgba(0, 0, 0, 0.65)), url(' + preset.backgroundImage + ')'"
                    >
                        <span matListItemTitle>{{ preset.displayName }}</span>
                        @if (preset === eodysseyLayerService.selectedPreset()) {
                            <span matListItemMeta class="preset-selected-indicator" aria-hidden="true">
                                <mat-icon>check</mat-icon>
                            </span>
                        }
                    </mat-list-item>
                }
            </mat-nav-list>
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

            // a wrapped label continues next to the swatch, not below it
            .coverage-label {
                display: flex;
                align-items: center;
                gap: 0.4rem;
            }

            .stripe-swatch {
                flex: none;
                width: 0.9rem;
                height: 0.9rem;
                border: 1px solid var(--geoengine-accent-color);
                background: repeating-linear-gradient(
                    45deg,
                    var(--geoengine-accent-color) 0 2px,
                    var(--geoengine-faded-accent-color) 2px 5px
                );
            }

            .section-heading {
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 0.5rem;

                mat-slide-toggle {
                    --mat-slide-toggle-label-text-size: #{$text2};
                }
            }

            .coverage-status {
                display: flex;
                align-items: center;
                gap: 0.5rem;
                margin: 0;
                font-size: $text2;
                line-height: 1.5;

                // the answer of the view before, while the one of the moved view loads
                &.coverage-stale {
                    opacity: 0.5;
                }
            }

            .status-dot {
                flex: none;
                box-sizing: border-box;
                width: 0.8rem;
                height: 0.8rem;
                border: 2px solid var(--geoengine-foreground-secondary-text-color);
                border-radius: 50%;

                &.status-full {
                    border-color: var(--geoengine-primary-color);
                    background: var(--geoengine-primary-color);
                }

                &.status-part {
                    border-color: var(--geoengine-primary-color);
                    background: linear-gradient(90deg, var(--geoengine-primary-color) 50%, transparent 50%);
                }

                &.status-unknown {
                    border-style: dashed;
                }
            }

            .date-strip {
                display: flex;
                gap: 1px;
                height: 1.75rem;
            }

            // the bar of a date: tall with scenes, short without, faint where the layer cannot tell
            .tick {
                flex: 1;
                min-width: 0;
                display: flex;
                align-items: flex-end;
                padding: 0;
                border: none;
                border-radius: 2px;
                background: none;
                cursor: pointer;

                &::after {
                    content: '';
                    width: 100%;
                    height: 25%;
                    border-radius: 1px;
                    background: var(--geoengine-foreground-secondary-text-color);
                    opacity: 0.5;
                }

                &.tick-data::after {
                    height: 100%;
                    background: var(--geoengine-primary-color);
                    opacity: 1;
                }

                &.tick-unknown::after {
                    opacity: 0.2;
                }

                &.tick-selected {
                    outline: 2px solid var(--geoengine-foreground-text-color);
                    outline-offset: 1px;
                }
            }

            .strip-legend {
                display: flex;
                justify-content: space-between;
                gap: 0.5rem;

                .tick {
                    display: inline-flex;
                    width: 0.3rem;
                    height: 0.8rem;
                    vertical-align: middle;
                    cursor: default;
                }
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
                .time-controls {
                    position: relative;
                    display: flex;
                }

                .time-jumps {
                    display: flex;
                    gap: 0.5rem;

                    button {
                        flex: 1;
                        min-width: 0;
                        border-radius: 0.5rem;
                        font-size: $text2;
                    }
                }

                // the date picker opens below its input, so the hidden input sits under the time controls
                input {
                    position: absolute;
                    top: 100%;
                    left: 50%;
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
                    height: 2.75rem;
                    padding: 0 0.5rem;
                    border-radius: 0.5rem;
                    font-size: $text1;
                    font-weight: 600;
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
        MatListModule,
        MatSelectModule,
        MatSlideToggleModule,
        MatProgressSpinnerModule,
        RasterLegendViewComponent,
        DataSourceNavigationComponent,
    ],
})
export class LayersComponent {
    readonly projectService = inject(ProjectService);
    readonly eodysseyLayerService = inject(EOdysseyLayerService);
    readonly footprintsService = inject(FootprintsService);
    readonly footprintCalendarHeader = FootprintCalendarHeaderComponent;
    private readonly layerService = inject(LayersService);

    /** The time the date field shows: the one the footprints are for, which is the default time of a selected data source until it is applied. */
    readonly currentTime = this.footprintsService.time;
    readonly formattedTime = computed<string>(() => {
        const time = this.currentTime();
        return time ? formatDate(time.start.valueOf()) : '';
    });
    /** The calendar opens on the cell size of the time step: days, months or years. */
    readonly currentDate = computed<Date | undefined>(() => {
        const time = this.currentTime();
        if (!time) return undefined;
        return time.start.toDate();
    });

    /** How much of the view the selected date covers, or why that is not known. */
    readonly status = computed<{kind: 'full' | 'part' | 'none' | 'unknown'; text: string} | undefined>(() => {
        const footprints = this.footprintsService;
        if (footprints.overlayBlocked()) return {kind: 'unknown', text: 'Zoom in to load this layer.'};
        if (footprints.overlayTooLarge()) return {kind: 'unknown', text: 'Zoom in to see where the scenes of this date are.'};
        if (footprints.hasError()) return {kind: 'unknown', text: 'Could not load the data availability.'};
        switch (footprints.viewCoverage()) {
            case 'full':
                return {kind: 'full', text: 'Scenes cover this view'};
            case 'part':
                return {kind: 'part', text: 'Scenes cover part of this view'};
            case 'none':
                return {kind: 'none', text: 'No scenes in this view on this date'};
            case 'unknown':
                return {kind: 'unknown', text: 'Not known for this data source'};
            case undefined:
                return footprints.overlayLoading() ? {kind: 'unknown', text: 'Checking this view…'} : undefined;
        }
    });

    /** The loaded dates around the selected one as bars, so the next date with scenes is seen without the calendar. */
    readonly strip = computed(() => {
        const now = this.currentTime()?.start.valueOf() ?? NaN;
        return this.footprintsService.stepsAround(STRIP_DATES).map((step) => {
            const date = formatDate(step.time.start);
            const scenes = {data: 'scenes', empty: 'no scenes', unknown: 'not known'}[step.status];
            return {
                start: step.time.start,
                date,
                label: `${date}: ${scenes}`,
                status: step.status,
                isSelected: step.time.start <= now && now < step.time.end,
            };
        });
    });

    /** Where the buttons below the date go, which their labels say. */
    readonly backwards = computed(() => this.jump(-1));
    readonly forward = computed(() => this.jump(1));

    readonly legendLayer = resource({
        params: () => ({layerId: this.eodysseyLayerService.mapTileLayer()}),
        loader: async ({params: {layerId}}): Promise<{layer: RasterLayer; metadata: RasterLayerMetadata} | undefined> => {
            if (!layerId) return undefined;

            const layer = await this.layerService.getLayer(layerId.dataConnectorId, layerId.layerId);

            const processingGraphId = await this.layerService.registerAndGetLayerProcessingGraphId(
                layerId.dataConnectorId,
                layerId.layerId,
            );

            if (layer.symbology?.type !== 'raster') return undefined;

            const metadata = await this.layerService.getProcessingGraphIdMetadata(processingGraphId);

            if (!(metadata instanceof RasterLayerMetadata)) return undefined;

            const rasterSymbology = layer.symbology;

            const rasterLayer = new RasterLayer({
                name: layer.name,
                processingGraphId,
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
        () =>
            !!this.eodysseyLayerService.mapTileLayer() &&
            (this.legendLayer.isLoading() || this.legendLayer.status() === 'error' || !!this.legend()),
    );

    constructor() {
        // the panel shows the dates and the coverage of the selected one all the time
        this.footprintsService.datesNeeded.set(true);
        inject(DestroyRef).onDestroy(() => this.footprintsService.datesNeeded.set(false));
        // while the data is on the map, the map follows the selection, so there is one active data source only
        effect(() => {
            if (this.eodysseyLayerService.dataOnMap() && this.eodysseyLayerService.canApplyPreset()) {
                untracked(() => this.applySelectedPreset());
            }
        });
    }

    setDataOnMap(isOn: boolean): void {
        this.eodysseyLayerService.dataOnMap.set(isOn);
        if (!isOn) this.eodysseyLayerService.removeVisualization();
    }

    /** The time step of a data source is applied with it. */
    private applyDataSourceTimeStep(dataSource: DataSourceDefinition): void {
        if (dataSource.defaultTimeStep) this.projectService.setTimeStepDuration(dataSource.defaultTimeStep);
    }

    applySelectedPreset(): void {
        if (!this.eodysseyLayerService.canApplyPreset()) return;

        const source = this.eodysseyLayerService.selectedDataSource();
        const sourceChanged = source !== this.eodysseyLayerService.dataSourceOfTime();
        this.eodysseyLayerService.applySelectedPreset();
        if (source && sourceChanged) this.applyDataSourceTimeStep(source);
    }

    async timeForward(): Promise<void> {
        await this.stepTime(1);
    }

    async timeBackwards(): Promise<void> {
        await this.stepTime(-1);
    }

    /**
     * The nearest date with scenes. Without one in the loaded dates, the first date beyond them, whose dates load then.
     * One time step where the data source cannot tell its dates.
     */
    private jump(direction: 1 | -1): {time: Time; label: string} | undefined {
        const time = this.currentTime();
        const timeStep = this.footprintsService.timeStep();
        if (!time || !timeStep) return undefined;

        const withData = this.footprintsService.dateWithData(direction);
        if (withData) return {time: withData, label: formatDate(withData.start.valueOf())};
        const beyond = this.footprintsService.dateBeyondLoaded(direction);
        if (beyond) return {time: beyond, label: 'Look further'};

        const amount = `${timeStep.durationAmount} ${timeStep.durationUnit}`;
        return direction === 1
            ? {time: time.add(timeStep.durationAmount, timeStep.durationUnit), label: `${amount} on`}
            : {time: time.subtract(timeStep.durationAmount, timeStep.durationUnit), label: `${amount} back`};
    }

    private async stepTime(direction: 1 | -1): Promise<void> {
        const jump = this.jump(direction);
        if (jump) await this.projectService.setTime(jump.time);
    }

    async selectTime(start: number): Promise<void> {
        await this.projectService.setTime(new Time(start));
    }

    async setDate(event: MatDatepickerInputEvent<Date>): Promise<void> {
        if (!event?.value) return;
        await this.setCalendarDate(event.value);
    }

    /** A month (or year) is the time step, so picking one selects it. Else the calendar goes on to the days. */
    async selectCoarseUnit(date: Date, view: MatCalendarView, picker: MatDatepicker<Date>): Promise<void> {
        if (this.footprintsService.calendarStartView() !== view) return;
        picker.close();
        await this.setCalendarDate(date);
    }

    private async setCalendarDate(date: Date): Promise<void> {
        const utcDate = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
        await this.projectService.setTime(new Time(utcDate));
    }
}

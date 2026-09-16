import {afterNextRender, ChangeDetectionStrategy, Component, computed, inject, signal} from '@angular/core';
import {CoreModule, ProjectService} from '@geoengine/core';
import {A11yModule} from '@angular/cdk/a11y';
import {EdvLayersService} from './layers.service';
import {MatCheckboxModule} from '@angular/material/checkbox';
import {MatListModule} from '@angular/material/list';
import {Time} from '@geoengine/common';
import {toSignal} from '@angular/core/rxjs-interop';
import {DATA_SOURCES} from './data-sources';
import {MatDatepickerInputEvent, MatDatepickerModule} from '@angular/material/datepicker';

@Component({
    selector: 'geoengine-layers',
    changeDetection: ChangeDetectionStrategy.OnPush,
    template: `
        <div>
            <h2>Data Source</h2>
            <mat-selection-list [multiple]="false" class="data-sources" (selectionChange)="onDataSourceSelectionChange($event.options)">
                @for (dataSource of dataSources; track dataSource.key) {
                    <mat-list-option
                        [value]="dataSource.key"
                        [selected]="selectedDataSource().key === dataSource.key"
                        [matTooltip]="dataSource.name"
                    >
                        <span matListItemTitle>{{ dataSource.name }}</span>
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

            .time-selection {
                --mat-button-text-label-text-size: #{$text2};
            }
        `,
    ],
    imports: [A11yModule, CoreModule, MatDatepickerModule, MatCheckboxModule, MatListModule],
})
export class LayersComponent {
    readonly projectService = inject(ProjectService);
    readonly edvLayersService = inject(EdvLayersService);

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
    readonly dataSources = DATA_SOURCES;

    readonly autoSelectTime = signal<boolean>(true);

    readonly selectedDataSource = this.edvLayersService.selectedDataSource;
    readonly selectedDataSourceKey = computed(() => this.selectedDataSource()?.key ?? '');
    readonly currentPresets = this.edvLayersService.currentPresets;
    readonly presetGroups = this.edvLayersService.presetGroups;
    readonly selectedPresetIndex = this.edvLayersService.selectedPresetIndex;
    readonly mapTileLayer = this.edvLayersService.mapTileLayer;

    constructor() {
        afterNextRender(() => {
            void this.setInitialTime();
        });
    }

    private async setInitialTime(): Promise<void> {
        if (!this.autoSelectTime()) return;

        const dataSource = this.selectedDataSource();
        if (!dataSource?.defaultTime) return;

        const utcDate = new Date(dataSource.defaultTime);
        const time = new Time(utcDate);
        await this.projectService.setTime(time);

        if (dataSource.defaultTimeStep) {
            this.projectService.setTimeStepDuration(dataSource.defaultTimeStep);
        }
    }

    onDataSourceSelectionChange(options: readonly {value: string}[]): void {
        const selected = options[0]?.value;
        if (!selected) return;
        void this.setSelectedDataSource(selected);
    }

    async setSelectedDataSource(key: string): Promise<void> {
        const dataSource = DATA_SOURCES.find((d) => d.key === key);
        if (!dataSource) return;

        if (this.autoSelectTime() && dataSource.defaultTime) {
            const utcDate = new Date(dataSource.defaultTime);
            const time = new Time(utcDate);
            await this.projectService.setTime(time);
        }

        if (dataSource.defaultTimeStep) {
            this.projectService.setTimeStepDuration(dataSource.defaultTimeStep);
        }

        this.selectedDataSource.set(dataSource);
        this.selectedPresetIndex.set(dataSource.defaultPresetIndex ?? 0);
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

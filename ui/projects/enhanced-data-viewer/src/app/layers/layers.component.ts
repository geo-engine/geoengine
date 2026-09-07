import {ChangeDetectionStrategy, Component, computed, inject} from '@angular/core';
import {CoreModule, ProjectService} from '@geoengine/core';
import {A11yModule} from '@angular/cdk/a11y';
import {MatDatepickerModule, MatDatepickerInputEvent} from '@angular/material/datepicker';
import {AppConfig} from '../app-config.service';
import {Time, UserService} from '@geoengine/common';
import {toSignal} from '@angular/core/rxjs-interop';

@Component({
    selector: 'geoengine-layers',
    changeDetection: ChangeDetectionStrategy.OnPush,
    template: `
        <div>
            <h4>Data Source</h4>
            <mat-radio-group class="data-sources" value="sentinel-2-l2a">
                <mat-radio-button disabled>Sentinel-1</mat-radio-button>
                <mat-radio-button value="sentinel-2-l2a">Sentinel-2 L2A</mat-radio-button>
                <mat-radio-button disabled>Sentinel-3 L2</mat-radio-button>
                <mat-radio-button disabled>Landsat 8</mat-radio-button>
            </mat-radio-group>
        </div>
        <mat-divider></mat-divider>

        <div class="time-selection">
            <h4>Time Selection</h4>
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
                <mat-list-item [activated]="true">
                    <img matListItemTitle src="assets/rgb.jpg" alt="RGB" class="preset-icon" />
                    <span matListItemTitle>RGB</span>
                </mat-list-item>
                <mat-list-item [activated]="false">
                    <img matListItemTitle src="assets/ndvi.jpg" alt="NDVI" class="preset-icon" />
                    <span matListItemTitle>NDVI</span>
                </mat-list-item>
                <mat-list-item [activated]="false">
                    <img matListItemTitle src="assets/false-color.jpg" alt="False Color" class="preset-icon" />
                    <span matListItemTitle>False Color</span>
                </mat-list-item>
                <mat-list-item [activated]="false">
                    <img matListItemTitle src="assets/swi.jpg" alt="SWI" class="preset-icon" />
                    <span matListItemTitle>SWI</span>
                </mat-list-item>
            </mat-nav-list>
        </div>
        <mat-divider></mat-divider>
    `,
    styles: [
        `
            .data-sources {
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
                gap: 1rem;

                width: 100%;
                border: none;

                mat-list-item {
                    width: 46%;
                    text-align: center;
                    padding: 0;

                    img {
                        width: 100%;
                        height: 100%;
                        object-fit: cover;
                        border-radius: 4px;
                    }

                    span {
                        color: white;
                        text-shadow: 0 0 5px rgba(0, 0, 0, 0.7);
                        transform: translateY(-1rem);
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
    imports: [A11yModule, CoreModule, MatDatepickerModule],
})
export class LayersComponent {
    readonly config = inject(AppConfig);
    readonly projectService = inject(ProjectService);
    readonly userService = inject(UserService);

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

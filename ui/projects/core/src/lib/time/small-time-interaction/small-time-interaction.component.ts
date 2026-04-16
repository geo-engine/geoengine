import {Component, ChangeDetectionStrategy, Signal, computed, inject} from '@angular/core';
import {ProjectService} from '../../project/project.service';
import {LayoutService} from '../../layout.service';
import {TimeConfigComponent} from '../time-config/time-config.component';
import {Time, TimeStepDuration} from '@geoengine/common';
import {toSignal} from '@angular/core/rxjs-interop';
import {MatIconButton, MatButton} from '@angular/material/button';
import {MatTooltip} from '@angular/material/tooltip';
import {MatIcon} from '@angular/material/icon';
import {SafeHtmlPipe} from '../../util/pipes/safe-html.pipe';

@Component({
    selector: 'geoengine-small-time-interaction',
    templateUrl: './small-time-interaction.component.html',
    styleUrls: ['./small-time-interaction.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [MatIconButton, MatTooltip, MatIcon, MatButton, SafeHtmlPipe],
})
export class SmallTimeInteractionComponent {
    private readonly projectService = inject(ProjectService);
    private readonly layoutService = inject(LayoutService);

    private readonly time: Signal<Time | undefined> = toSignal(this.projectService.getTimeStream());

    readonly timeRepresentation: Signal<string> = computed(() => {
        const time = this.time();
        if (time === undefined) {
            return '';
        }
        return time.toString();
    });
    readonly hasTimeRepresentation: Signal<boolean> = computed(() => !!this.timeRepresentation());

    readonly timeStepDuration: Signal<TimeStepDuration> = toSignal(this.projectService.getTimeStepDurationStream(), {
        initialValue: {durationAmount: 1, durationUnit: 'months'}, // TODO: get from DEFAULTS?
    });

    async timeForward(): Promise<void> {
        const time = await this.projectService.getTimeOnce();

        const updatedTime = time.add(this.timeStepDuration().durationAmount, this.timeStepDuration().durationUnit);
        this.projectService.setTime(updatedTime);
    }

    async timeBackwards(): Promise<void> {
        const time = await this.projectService.getTimeOnce();

        const updatedTime = time.subtract(this.timeStepDuration().durationAmount, this.timeStepDuration().durationUnit);
        this.projectService.setTime(updatedTime);
    }

    openTimeConfig(): void {
        this.layoutService.setSidenavContentComponent({component: TimeConfigComponent});
    }
}

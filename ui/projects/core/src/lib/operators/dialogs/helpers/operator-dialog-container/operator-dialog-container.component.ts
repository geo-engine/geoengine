import {ChangeDetectionStrategy, Component, input} from '@angular/core';
import {MatCardModule} from '@angular/material/card';
import {MatProgressBarModule} from '@angular/material/progress-bar';

@Component({
    selector: 'geoengine-operator-dialog-container',
    templateUrl: './operator-dialog-container.component.html',
    styleUrls: ['./operator-dialog-container.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [MatCardModule, MatProgressBarModule],
})
export class OperatorDialogContainerComponent {
    readonly loading = input<boolean>(false);
}

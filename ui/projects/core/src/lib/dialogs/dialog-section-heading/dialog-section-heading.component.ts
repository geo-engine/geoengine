import {Component, ChangeDetectionStrategy, input} from '@angular/core';

@Component({
    selector: 'geoengine-dialog-section-heading',
    templateUrl: './dialog-section-heading.component.html',
    styleUrls: ['./dialog-section-heading.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
})
export class DialogSectionHeadingComponent {
    readonly title = input<string>();
    readonly subtitle = input<string>();
}

import {ChangeDetectionStrategy, Component, output} from '@angular/core';
import {MatIconButton} from '@angular/material/button';
import {MatTooltip} from '@angular/material/tooltip';
import {MatIcon} from '@angular/material/icon';

@Component({
    selector: 'geoengine-zoom-handles',
    templateUrl: './zoom-handles.component.html',
    styleUrls: ['./zoom-handles.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [MatIconButton, MatTooltip, MatIcon],
})
export class ZoomHandlesComponent {
    readonly zoomIn = output<void>();

    readonly zoomOut = output<void>();
}

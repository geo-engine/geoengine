import {Component, ChangeDetectionStrategy, HostBinding} from '@angular/core';
import {MatGridList, MatGridTile} from '@angular/material/grid-list';

@Component({
    selector: 'geoengine-attributions',
    templateUrl: './attributions.component.html',
    styleUrls: ['./attributions.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [MatGridList, MatGridTile],
})
export class AttributionsComponent {
    @HostBinding('className') componentClass = 'mat-typography';
}

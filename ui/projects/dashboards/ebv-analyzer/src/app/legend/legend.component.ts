import {Component, ChangeDetectionStrategy, input} from '@angular/core';
import {RasterLayer} from '@geoengine/common';
import {RasterLegendComponent} from '@geoengine/core';

@Component({
    selector: 'geoengine-legend',
    templateUrl: './legend.component.html',
    styleUrls: ['./legend.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [RasterLegendComponent],
})
export class LegendComponent {
    readonly layer = input<RasterLayer>();
}

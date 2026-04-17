import {Component, ChangeDetectionStrategy, input, signal} from '@angular/core';
import {RasterLayer} from '@geoengine/common';
import {CoreModule, RasterLegendComponent} from '@geoengine/core';

@Component({
    selector: 'geoengine-legend',
    templateUrl: './legend.component.html',
    styleUrls: ['./legend.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [CoreModule, RasterLegendComponent],
})
export class LegendComponent {
    readonly layer = input<RasterLayer>();

    readonly visible = signal(true);
}

import {Component, ChangeDetectionStrategy, input} from '@angular/core';
import {RasterLayer, FxLayoutDirective, FxLayoutAlignDirective, FxLayoutGapDirective} from '@geoengine/common';
import {RasterLegendComponent} from '@geoengine/core';

@Component({
    selector: 'geoengine-legend',
    templateUrl: './legend.component.html',
    styleUrls: ['./legend.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [FxLayoutDirective, FxLayoutAlignDirective, FxLayoutGapDirective, RasterLegendComponent],
})
export class LegendComponent {
    readonly layer = input<RasterLayer>();
}

import {Component, ChangeDetectionStrategy, OnChanges, SimpleChanges, ChangeDetectorRef, inject, input} from '@angular/core';
import {RasterLayer, VectorLayer, FxLayoutDirective, FxLayoutAlignDirective, FxLayoutGapDirective} from '@geoengine/common';
import {RasterLegendComponent, CoreModule} from '@geoengine/core';

@Component({
    selector: 'geoengine-legend',
    templateUrl: './legend.component.html',
    styleUrls: ['./legend.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [FxLayoutDirective, FxLayoutAlignDirective, FxLayoutGapDirective, RasterLegendComponent, CoreModule],
})
export class LegendComponent implements OnChanges {
    readonly changeDetectorRef = inject(ChangeDetectorRef);

    readonly layer = input<VectorLayer | RasterLayer>();

    ngOnChanges(_changes: SimpleChanges): void {
        this.changeDetectorRef.markForCheck();
    }

    get asRasterLayer(): RasterLayer | undefined {
        const layer = this.layer();
        return layer instanceof RasterLayer ? layer : undefined;
    }

    get asVectorLayer(): VectorLayer | undefined {
        const layer = this.layer();
        return layer instanceof VectorLayer ? layer : undefined;
    }
}

import {Component, ChangeDetectionStrategy, Input, OnChanges, SimpleChanges, ChangeDetectorRef, inject} from '@angular/core';
import {RasterLayer} from '@geoengine/common';
import {RasterLegendComponent} from '@geoengine/core';

@Component({
    selector: 'geoengine-legend',
    templateUrl: './legend.component.html',
    styleUrls: ['./legend.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [RasterLegendComponent],
})
export class LegendComponent implements OnChanges {
    readonly changeDetectorRef = inject(ChangeDetectorRef);

    @Input() layer?: RasterLayer = undefined;

    ngOnChanges(_changes: SimpleChanges): void {
        this.changeDetectorRef.markForCheck();
    }
}

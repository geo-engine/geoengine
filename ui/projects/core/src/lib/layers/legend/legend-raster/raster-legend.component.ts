import {ChangeDetectionStrategy, Component, effect, inject, input, signal, untracked} from '@angular/core';
import {firstValueFrom} from 'rxjs';
import {RasterLayer, RasterLayerMetadata} from '@geoengine/common';
import {ProjectService} from '../../../project/project.service';
import {RasterLegendViewComponent} from './raster-legend-view.component';

/**
 * The raster legend component.
 * It retrieves the layer metadata from the `ProjectService` and displays it using the `RasterLegendViewComponent`.
 */
@Component({
    selector: 'geoengine-raster-legend',
    template: `<geoengine-raster-legend-view
        [layer]="layer()"
        [metadata]="metadata()"
        [orderValuesDescending]="orderValuesDescending()"
        [showBandNames]="showBandNames()"
    />`,
    styles: [':host { display: block; }'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [RasterLegendViewComponent],
})
export class RasterLegendComponent {
    private readonly projectService = inject(ProjectService);

    readonly layer = input.required<RasterLayer>();
    readonly orderValuesDescending = input<boolean>(false);
    readonly showBandNames = input<boolean>(true);

    readonly metadata = signal<RasterLayerMetadata | undefined>(undefined);

    constructor() {
        effect(() => {
            const layer = this.layer();
            untracked(() => {
                void firstValueFrom(this.projectService.getRasterLayerMetadata(layer)).then((metadata) => {
                    if (untracked(this.layer) !== layer) {
                        return; // layer changed in the meantime
                    }
                    this.metadata.set(metadata);
                });
            });
        });
    }
}

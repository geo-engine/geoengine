import {ChangeDetectionStrategy, Component, input} from '@angular/core';
import {LayerMetadata, RasterLayerMetadata, VectorLayerMetadata} from '../../layers/layer-metadata.model';
import {VectorDataTypes} from '../../operators/datatype.model';
import {Colorizer} from '../../colors/colorizer.model';
import {SingleBandRasterColorizer} from '../../symbology/symbology.model';

import {MatIconModule} from '@angular/material/icon';
import {FxLayoutDirective} from '../../util/directives/flexbox-legacy.directive';
import {PolygonIconComponent} from '../../layer-icons/polygon-icon/polygon-icon.component';
import {LineIconComponent} from '../../layer-icons/line-icon/line-icon.component';
import {PointIconComponent} from '../../layer-icons/point-icon/point-icon.component';
import {RasterIconComponent} from '../../layer-icons/raster-icon/raster-icon.component';

@Component({
    selector: 'geoengine-layer-collection-layer-details',
    templateUrl: './layer-collection-layer-details.component.html',
    styleUrls: ['./layer-collection-layer-details.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [MatIconModule, FxLayoutDirective, PolygonIconComponent, LineIconComponent, PointIconComponent, RasterIconComponent],
})
export class LayerCollectionLayerDetailsComponent {
    readonly description = input<string | undefined>();
    readonly layerMetadata = input<LayerMetadata | undefined>(undefined);

    readonly VectorDataTypes = VectorDataTypes;

    readonly rasterColorizer = new SingleBandRasterColorizer(
        0,
        Colorizer.fromDict({
            type: 'linearGradient',
            breakpoints: [
                {value: 0, color: [122, 122, 122, 255]},
                {value: 1, color: [255, 255, 255, 255]},
            ],
            overColor: [255, 255, 255, 127],
            underColor: [122, 122, 122, 127],
            noDataColor: [0, 0, 0, 0],
        }),
    );

    get rasterLayerMetadata(): RasterLayerMetadata | undefined {
        const layerMetadata = this.layerMetadata();
        if (layerMetadata?.layerType === 'raster') {
            return layerMetadata as RasterLayerMetadata;
        }
        return undefined;
    }

    get vectorLayerMetadata(): VectorLayerMetadata | undefined {
        const layerMetadata = this.layerMetadata();
        if (layerMetadata?.layerType === 'vector') {
            return layerMetadata as VectorLayerMetadata;
        }
        return undefined;
    }

    get minTimeString(): string | undefined {
        const layerMetadata = this.layerMetadata();

        if (!layerMetadata) {
            return undefined;
        }

        if (!layerMetadata.time) {
            return 'undefined';
        }

        return layerMetadata.time.startStringOrNegInf();
    }

    get maxTimeString(): string | undefined {
        const layerMetadata = this.layerMetadata();

        if (!layerMetadata) {
            return undefined;
        }

        if (!layerMetadata.time) {
            return 'undefined';
        }

        return layerMetadata.time.endStringOrPosInf();
    }

    get timeString(): string {
        const layerMetadata = this.layerMetadata();

        if (!layerMetadata) {
            return 'undefined';
        }

        if (!layerMetadata.time) {
            return 'undefined';
        }

        const min = layerMetadata.time.startStringOrNegInf() || 'undefined';
        const max = layerMetadata.time.endStringOrPosInf() || 'undefined';

        return '[ ' + min + ' ,  ' + max + ' )';
    }

    get bboxLowerLeftString(): string | undefined {
        const layerMetadata = this.layerMetadata();
        if (layerMetadata?.bbox) {
            return `Min: ${layerMetadata.bbox.xmin} : ${layerMetadata.bbox.ymin}`;
        }
        return undefined;
    }

    get bboxUpperRightString(): string | undefined {
        const layerMetadata = this.layerMetadata();
        if (layerMetadata?.bbox) {
            return `Max: ${layerMetadata.bbox.xmax} : ${layerMetadata.bbox.ymax}`;
        }
        return undefined;
    }
}

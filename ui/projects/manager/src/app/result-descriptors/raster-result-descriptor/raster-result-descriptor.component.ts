import {Component, input} from '@angular/core';
import {BoundingBox2D, GeoTransform, GridBoundingBox2D} from '@geoengine/common';
import {RasterBandDescriptor, TypedRasterResultDescriptor, RegularTimeDimension} from '@geoengine/api-client';
import {FormsModule} from '@angular/forms';
import {MatFormField, MatLabel, MatInput} from '@angular/material/input';
import {
    MatTable,
    MatColumnDef,
    MatHeaderCellDef,
    MatHeaderCell,
    MatCellDef,
    MatCell,
    MatHeaderRowDef,
    MatHeaderRow,
    MatRowDef,
    MatRow,
} from '@angular/material/table';

@Component({
    selector: 'geoengine-manager-raster-result-descriptor',
    templateUrl: './raster-result-descriptor.component.html',
    styleUrl: './raster-result-descriptor.component.scss',
    imports: [
        FormsModule,
        MatFormField,
        MatLabel,
        MatInput,
        MatTable,
        MatColumnDef,
        MatHeaderCellDef,
        MatHeaderCell,
        MatCellDef,
        MatCell,
        MatHeaderRowDef,
        MatHeaderRow,
        MatRowDef,
        MatRow,
    ],
})
export class RasterResultDescriptorComponent {
    readonly resultDescriptor = input.required<TypedRasterResultDescriptor>();

    displayedColumns: string[] = ['index', 'name', 'measurement'];

    convertUnixToIso(timestamp: number): string {
        const date = new Date(timestamp);
        return date.toISOString();
    }

    get bandDataSource(): RasterBandDescriptor[] {
        return this.resultDescriptor().bands;
    }

    get boundingBox(): BoundingBox2D {
        const gt = GeoTransform.fromDict(this.resultDescriptor().spatialGrid.spatialGrid.geoTransform);
        const pxBounds = GridBoundingBox2D.fromDict(this.resultDescriptor().spatialGrid.spatialGrid.gridBounds);
        return gt.gridBoundsToSpatialBounds(pxBounds);
    }

    get spatialResolution(): SpatialResolution {
        return {
            x: this.resultDescriptor().spatialGrid.spatialGrid.geoTransform.xPixelSize,
            y: this.resultDescriptor().spatialGrid.spatialGrid.geoTransform.yPixelSize,
        };
    }

    get regularTimeDimension(): RegularTimeDimension | undefined {
        const rd = this.resultDescriptor().time.dimension;
        return rd?.type === 'regular' ? rd : undefined;
    }
}

interface SpatialResolution {
    x: number;
    y: number;
}

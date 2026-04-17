import {SpatialGridDefinition as SpatialGridDefinitionDict} from '@geoengine/api-client';
import {ToDict} from '../time/time.model';
import {GeoTransform} from './geo-transform.model';
import {GridBoundingBox2D} from './grid-bounding-box.model';
import {BoundingBox2D} from '../spatial-bounds/bounding-box';
import {Coordinate2D} from '../spatial-features/coordinate.model';

export class SpatialGridDefinition implements ToDict<SpatialGridDefinitionDict> {
    readonly geoTransform: GeoTransform;
    readonly gridBounds: GridBoundingBox2D;

    constructor(geoTransform: GeoTransform, gridBounds: GridBoundingBox2D) {
        this.geoTransform = geoTransform;
        this.gridBounds = gridBounds;
    }

    public bbox(): BoundingBox2D {
        return this.geoTransform.gridBoundsToSpatialBounds(this.gridBounds);
    }

    public get pixelSizeX(): number {
        return this.geoTransform.pixelSizeX;
    }

    public get pixelSizeY(): number {
        return this.geoTransform.pixelSizeY;
    }

    public get originCoordinate(): Coordinate2D {
        return this.geoTransform.originCoordinate;
    }

    public get numberOfPixelsX(): number {
        return this.gridBounds.numberOfPixelsX;
    }

    public get numberOfPixelsY(): number {
        return this.gridBounds.numberOfPixelsY;
    }

    toDict(): SpatialGridDefinitionDict {
        return {
            geoTransform: this.geoTransform.toDict(),
            gridBounds: this.gridBounds.toDict(),
        };
    }

    public static fromDict(dict: SpatialGridDefinitionDict): SpatialGridDefinition {
        return new SpatialGridDefinition(GeoTransform.fromDict(dict.geoTransform), GridBoundingBox2D.fromDict(dict.gridBounds));
    }
}

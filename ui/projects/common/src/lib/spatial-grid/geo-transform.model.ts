import {GeoTransform as GeoTransformDict} from '@geoengine/api-client';
import {ToDict} from '../time/time.model';
import {Coordinate2D} from '../spatial-features/coordinate.model';
import {GridIdx2D} from './grid-idx.model';
import {GridBoundingBox2D} from './grid-bounding-box.model';
import {BoundingBox2D} from '../spatial-bounds/bounding-box';

export class GeoTransform implements ToDict<GeoTransformDict> {
    readonly originCoordinate: Coordinate2D;
    readonly pixelSizeX: number;
    readonly pixelSizeY: number;

    constructor(originCoordinate: Coordinate2D, pixelSizeX: number, pixelSizeY: number) {
        this.originCoordinate = originCoordinate;
        this.pixelSizeX = pixelSizeX;
        this.pixelSizeY = pixelSizeY;
    }

    public pixelToCoordinateUlEdge(gridIdx: GridIdx2D): Coordinate2D {
        const ulx = this.originCoordinate.x + this.pixelSizeX * gridIdx.xIdx;
        const uly = this.originCoordinate.y + this.pixelSizeY * gridIdx.yIdx;

        return new Coordinate2D([ulx, uly]);
    }

    public pixelToCoordinateCenter(gridIdx: GridIdx2D): Coordinate2D {
        const ul = this.pixelToCoordinateUlEdge(gridIdx);
        return new Coordinate2D([ul.x + 0.5 * this.pixelSizeX, ul.y + 0.5 * this.pixelSizeY]);
    }

    public coordinateToPixelUlEdge(coord: Coordinate2D): GridIdx2D {
        const ulx = (coord.x - this.originCoordinate.x) / this.pixelSizeX;
        const uly = (coord.y - this.originCoordinate.y) / this.pixelSizeY;

        return new GridIdx2D(ulx, uly);
    }

    public gridBoundsToSpatialBounds(gridBounds: GridBoundingBox2D): BoundingBox2D {
        const ul = this.pixelToCoordinateUlEdge(gridBounds.topLeftIdx);
        const lrIdxPlusOne = new GridIdx2D(gridBounds.bottomRightIdx.xIdx + 1, gridBounds.bottomRightIdx.yIdx + 1);
        const lr = this.pixelToCoordinateUlEdge(lrIdxPlusOne);
        return new BoundingBox2D([ul.x, lr.y, lr.x, ul.y]);
    }

    public spatialToGridBounds(spatialBounds: BoundingBox2D): GridBoundingBox2D {
        const ul = this.coordinateToPixelUlEdge(spatialBounds.upperLeftCoordinate);
        const lr = this.coordinateToPixelUlEdge(spatialBounds.lowerRightCoordinate);
        const lrPlusOne = new GridIdx2D(lr.xIdx + 1, lr.yIdx + 1);
        return new GridBoundingBox2D(ul, lrPlusOne);
    }

    toDict(): GeoTransformDict {
        return {
            originCoordinate: this.originCoordinate.toDict(),
            xPixelSize: this.pixelSizeX,
            yPixelSize: this.pixelSizeY,
        };
    }

    public static fromDict(dict: GeoTransformDict): GeoTransform {
        return new GeoTransform(Coordinate2D.fromDict(dict.originCoordinate), dict.xPixelSize, dict.yPixelSize);
    }
}

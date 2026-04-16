import {GridBoundingBox2D as GridBoundingBox2DDict} from '@geoengine/api-client';
import {ToDict} from '../time/time.model';
import {GridIdx2D} from './grid-idx.model';

export class GridBoundingBox2D implements ToDict<GridBoundingBox2DDict> {
    topLeftIdx: GridIdx2D;
    bottomRightIdx: GridIdx2D;

    constructor(upperLeftIdx: GridIdx2D, lowerRightIdx: GridIdx2D) {
        this.bottomRightIdx = lowerRightIdx;
        this.topLeftIdx = upperLeftIdx;
    }

    public contains(idx: GridIdx2D): boolean {
        const conX = this.topLeftIdx.xIdx <= idx.xIdx && idx.xIdx <= this.bottomRightIdx.xIdx;
        const conY = this.topLeftIdx.yIdx <= idx.yIdx && idx.yIdx <= this.bottomRightIdx.yIdx;
        return conX && conY;
    }

    public get numberOfPixelsX(): number {
        return this.bottomRightIdx.xIdx - this.topLeftIdx.xIdx + 1;
    }

    public get numberOfPixelsY(): number {
        return this.bottomRightIdx.yIdx - this.topLeftIdx.yIdx + 1;
    }

    toDict(): GridBoundingBox2DDict {
        return {
            bottomRightIdx: this.bottomRightIdx.toDict(),
            topLeftIdx: this.topLeftIdx.toDict(),
        };
    }

    public static fromDict(dict: GridBoundingBox2DDict): GridBoundingBox2D {
        return new GridBoundingBox2D(GridIdx2D.fromDict(dict.topLeftIdx), GridIdx2D.fromDict(dict.bottomRightIdx));
    }
}

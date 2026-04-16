import {GridIdx2D as GridIdx2DDict} from '@geoengine/api-client';
import {ToDict} from '../time/time.model';

export class GridIdx2D implements ToDict<GridIdx2DDict> {
    readonly xIdx: number;
    readonly yIdx: number;

    constructor(xIdx: number, yIdx: number) {
        if (!Number.isInteger(xIdx)) {
            throw new Error('xIdx is not an int');
        }
        if (!Number.isInteger(yIdx)) {
            throw new Error('yIdx is not an int');
        }
        this.xIdx = xIdx;
        this.yIdx = yIdx;
    }

    toDict(): GridIdx2DDict {
        return {
            xIdx: this.xIdx,
            yIdx: this.yIdx,
        };
    }

    public static fromDict(dict: GridIdx2DDict): GridIdx2D {
        return new GridIdx2D(dict.xIdx, dict.yIdx);
    }
}

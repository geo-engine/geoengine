import {Extent} from 'ol/extent';
import {Coordinate2D} from '../spatial-features/coordinate.model';
import {ToDict} from '../time/time.model';
import {BoundingBox2D as BBoxDict, SpatialPartition2D as SpatialPartitionDict} from '@geoengine/api-client';

export class BoundingBox2D implements ToDict<BBoxDict> {
    private readonly inner: [number, number, number, number];

    /**
     * Returns an new boundingbox.
     *
     * @param param0 The bounds of the new BoundingBox as `[xmin, ymin, xmax, ymax]`.
     */
    constructor([xmin, ymin, xmax, ymax]: [number, number, number, number]) {
        if (xmin > xmax || ymin > ymax) {
            throw new Error('Invalid bounding box');
        }
        this.inner = [xmin, ymin, xmax, ymax];
    }

    public get xmin(): number {
        return this.inner[0];
    }

    public get ymin(): number {
        return this.inner[1];
    }

    public get xmax(): number {
        return this.inner[2];
    }

    public get ymax(): number {
        return this.inner[3];
    }

    public get upperLeftCoordinate(): Coordinate2D {
        return new Coordinate2D([this.xmin, this.ymax]);
    }

    public get lowerRightCoordinate(): Coordinate2D {
        return new Coordinate2D([this.xmax, this.ymin]);
    }

    public get upperRightCoordinate(): Coordinate2D {
        return new Coordinate2D([this.xmax, this.ymax]);
    }

    public get lowerLeftCoordinate(): Coordinate2D {
        return new Coordinate2D([this.xmin, this.ymin]);
    }

    /**
     * Creates a new BoundingBox from the given numbers.
     *
     * @param xmin - The minimum x value.
     * @param ymin - The minimum y value.
     * @param xmax - The maximum x value.
     * @param ymax - The maximum y value.
     * @returns - A new BoundingBox.
     */
    public static fromNumbers(xmin: number, ymin: number, xmax: number, ymax: number): BoundingBox2D {
        return new BoundingBox2D([xmin, ymin, xmax, ymax]);
    }

    /**
     * Creates a new BoundingBox from the given coordinates.
     *
     * @param upperLeft - The upper left coordinate.
     * @param lowerRight - The lower right coordinate.
     * @returns - A new BoundingBox.
     */
    public static fromCoordinates(lowerLeft: Coordinate2D, upperRight: Coordinate2D): BoundingBox2D {
        return BoundingBox2D.fromNumbers(lowerLeft.x, lowerLeft.y, upperRight.x, upperRight.y);
    }

    /**
     * Creates a new BoundingBox from a `BBoxDict`.
     *
     * @param dict - The input dict.
     * @returns - A new BoundingBox.
     */
    public static fromDict(dict: BBoxDict): BoundingBox2D {
        const ll = Coordinate2D.fromDict(dict.lowerLeftCoordinate);
        const ur = Coordinate2D.fromDict(dict.upperRightCoordinate);
        return BoundingBox2D.fromCoordinates(ll, ur);
    }

    /**
     * Creates a new BoundingBox from a `SpatialPartitionDict`.
     *
     * @param dict - The input dict.
     * @returns - A new BoundingBox.
     */
    public static fromSpatialPartitionDict(dict: SpatialPartitionDict): BoundingBox2D {
        const ul = Coordinate2D.fromDict(dict.upperLeftCoordinate);
        const lr = Coordinate2D.fromDict(dict.lowerRightCoordinate);
        return BoundingBox2D.fromNumbers(ul.x, lr.y, lr.x, ul.y);
    }

    /**
     * Creates a new BoundingBox from an OpenLayers extent.
     *
     * @param extent - The input extent.
     * @returns - A new BoundingBox.
     */
    public static fromOlExtent(extent: Extent): BoundingBox2D {
        if (extent.length !== 4) {
            throw new Error('Invalid extent');
        }
        return new BoundingBox2D(extent as [number, number, number, number]);
    }

    /**
     * Transforms a bounding box to a OpenLayers extent.
     *
     * @returns - The OpenLayers extent.
     */
    public toOlExtent(): Extent {
        return this.inner;
    }

    /**
     * Transforms a bounding box to a `BBoxDict`.
     *
     * @returns - The `BBoxDict`.
     */
    public toDict(): BBoxDict {
        return {
            lowerLeftCoordinate: this.lowerLeftCoordinate.toDict(),
            upperRightCoordinate: this.upperRightCoordinate.toDict(),
        };
    }

    /**
     * Returns the `BoundingBox2D` covering self and other.
     *
     * @param other
     * @returns - A new union `BoundingBox2D`.
     */
    public union(other: BoundingBox2D): BoundingBox2D {
        return BoundingBox2D.fromNumbers(
            Math.min(this.xmin, other.xmin),
            Math.min(this.ymin, other.ymin),
            Math.max(this.xmax, other.xmax),
            Math.max(this.ymax, other.ymax),
        );
    }

    /**
     * Folds an iterable of BoundingBox2D into the BoundingBox2D containing them all.
     *
     * @param iter
     * @returns - A new `BoundingBox2D` covering all input boxes.
     */
    public static unionFold(iter: BoundingBox2D[]): BoundingBox2D | undefined {
        let totalBounds: BoundingBox2D | undefined = undefined;
        for (const b of iter) {
            if (totalBounds) {
                totalBounds = b.union(totalBounds);
            } else {
                totalBounds = b;
            }
        }
        return totalBounds;
    }
}

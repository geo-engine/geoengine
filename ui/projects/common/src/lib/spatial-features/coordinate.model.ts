import {ToDict} from '../time/time.model';
import {Coordinate2D as CoordinateDict} from '@geoengine/api-client';

/**
 * A 2D coordinate.
 *
 * The coordinate is immutable.
 *(x, y) is the same as (longitude, latitude).
 */
export class Coordinate2D implements ToDict<CoordinateDict> {
    private readonly inner: [number, number];

    /**
     * Creates a new Coordinate2D.
     *
     * @param param0 The x and y coordinate.
     */
    constructor([x, y]: [number, number]) {
        this.inner = [x, y];
    }

    public get x(): number {
        return this.inner[0];
    }

    public get y(): number {
        return this.inner[1];
    }

    /**
     * Creates a new Coordinate2D from a dictionary.
     *
     * @param dict The dictionary.
     * @returns The Coordinate2D.
     */
    public static fromDict(dict: CoordinateDict): Coordinate2D {
        return new Coordinate2D([dict.x, dict.y]);
    }

    /**
     * Transforms the coordinate to a dictionary.
     *
     * @returns The dictionary representation of the coordinate.
     */
    public toDict(): CoordinateDict {
        return {x: this.x, y: this.y};
    }

    /**
     * Returns a string representation of the coordinate.
     */
    public toString(): string {
        return `(${this.x}, ${this.y})`;
    }

    /**
     * Returns the coordinate as an OpenLayers coordinate.
     *
     * @returns The coordinate as an OpenLayers coordinate.
     */
    public toOlCoordinate(): [number, number] {
        return this.inner;
    }

    /**
     * Creates a new Coordinate2D from an OpenLayers coordinate.
     *
     * @param coord - The OpenLayers coordinate.
     * @returns The Coordinate2D.
     */
    public static fromOlCoordinate(coord: [number, number]): Coordinate2D {
        return new Coordinate2D(coord);
    }
}

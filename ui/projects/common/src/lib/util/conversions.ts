import {utc} from 'moment';
import {Feature as OlFeature} from 'ol';
import OlFormatGeoJson from 'ol/format/GeoJSON';
import OlGeometry from 'ol/geom/Geometry';
import {Extent as OlExtent} from 'ol/extent';
import {Observable, ReplaySubject} from 'rxjs';
import {BoundingBox2D as BBoxDict, ResponseError} from '@geoengine/api-client';
import {Time} from '../time/time.model';

/**
 * Converts an `OlExtent` to an extent as a tuple of four numbers.
 * Throws an error if something went wrong.
 */
export const olExtentToTuple = (extent: OlExtent): [number, number, number, number] => {
    if (extent.length !== 4) {
        throw Error('OlExtent must be of size 4');
    }
    return [extent[0], extent[1], extent[2], extent[3]];
};

export const extentToBboxDict = ([minx, miny, maxx, maxy]: [number, number, number, number]): BBoxDict => ({
    lowerLeftCoordinate: {
        x: minx,
        y: miny,
    },
    upperRightCoordinate: {
        x: maxx,
        y: maxy,
    },
});

export const bboxDictToExtent = (extent: BBoxDict): [number, number, number, number] => {
    const minx = extent.lowerLeftCoordinate.x;
    const miny = extent.lowerLeftCoordinate.y;
    const maxx = extent.upperRightCoordinate.x;
    const maxy = extent.upperRightCoordinate.y;

    return [minx, miny, maxx, maxy];
};

/**
 * Convert a unix timestamp in ms to an ISO timestamp string.
 */
export const unixTimestampToIsoString = (unixTimestamp: number): string => utc(unixTimestamp).toISOString();

export function hashCode(str: string): number {
    // java String#hashCode
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        hash = str.charCodeAt(i) + ((hash << 5) - hash); // eslint-disable-line no-bitwise
    }
    return hash;
}

// TODO: use a faster hash function
export function featureToHash(feature: OlFeature<OlGeometry>): number {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const sortObjKeys = (obj: Record<string, any>): any => {
        Object.keys(obj)
            .sort()
            .reduce(
                (x, key) => {
                    x[key] = obj[key];
                    return x;
                },
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                {} as Record<string, any>,
            );
    };

    const jsonObj = new OlFormatGeoJson().writeFeatureObject(feature);
    if (jsonObj['properties']) {
        jsonObj['properties'] = sortObjKeys(jsonObj['properties']);
    }
    const json = JSON.stringify(jsonObj);

    return hashCode(json);
}

/**
 * Subscribes to the observable and consumes it completely.
 * Returns a new observable to listen to the values.
 */
export function subscribeAndProvide<T>(observable: Observable<T>): Observable<T> {
    const subject = new ReplaySubject<T>();

    observable.subscribe({
        next: (value) => subject.next(value),
        error: (error) => subject.error(error),
        complete: () => subject.complete(),
    });

    return subject.asObservable();
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export async function errorToText(error: any, defaultMessage: string): Promise<string> {
    if (!(error instanceof ResponseError)) {
        return defaultMessage;
    }
    const e = error;
    const errorJson = await e.response.json().catch(() => ({}));
    const errorMessage = errorJson.message ?? defaultMessage;

    return errorMessage;
}

// we use this non-breaking hyphen to avoid line breaks in the time format
export const NON_BREAKING_HYPHEN = '‑';

/**
 * Checks if the `timeSteps` contain information about months or days.
 * For instance, for the steps 2019-01-01, 2019-02-01, 2019-03-01, the format would be 'YYYY-MM'.
 */
export function estimateTimeFormat(timeSteps: Array<Time>): string {
    let useMonth = false;
    let useDay = false;
    for (const timeStep of timeSteps) {
        // in range [1, 31]
        if (timeStep.start.date() > 1) {
            useDay = useMonth = true;
            break;
        }
        // in range [0, 11]
        if (timeStep.start.month() > 0) {
            useMonth = true;
            // we cannot break, it could still be that we need to use days
        }
    }

    if (useDay) {
        return `YYYY${NON_BREAKING_HYPHEN}MM${NON_BREAKING_HYPHEN}DD`;
    } else if (useMonth) {
        return `YYYY${NON_BREAKING_HYPHEN}MM`;
    } else {
        return 'YYYY'; // default: yearly
    }
}

/**
 * Used as filter argument for T | undefined
 */
// eslint-disable-next-line prefer-arrow/prefer-arrow-functions
export function isDefined<T>(arg: T | null | undefined): arg is T {
    return arg !== null && arg !== undefined;
}

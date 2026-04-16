import {SpatialReference} from '@geoengine/common';
import {WGS_84} from '../spatial-references/spatial-reference.service';

/**
 * @returns the target spatial reference for a common projection of the given inputs
 */
export function getProjectionTarget(inputRefs: Array<SpatialReference>): SpatialReference {
    if (inputRefs.length === 0) {
        return WGS_84.spatialReference;
    }

    if (inputRefs.findIndex((s) => s.srsString === WGS_84.spatialReference.srsString) > 0) {
        return WGS_84.spatialReference;
    }

    return inputRefs[0];
}

/**
 * @returns the bbox as a request parameter for OGC services
 */
export function bboxAsOgcString(minx: number, maxx: number, miny: number, maxy: number, epsgString: string): string {
    // TODO: properly handle axis order

    if (epsgString === 'EPSG:4326') {
        return `${miny},${minx},${maxy},${maxx}`;
    }

    return `${minx},${miny},${maxx},${maxy}`;
}

/**
 * @returns the grid origin as a request parameter for OGC services
 */
export function gridOriginAsOgcString(minx: number, maxy: number, epsgString: string): string {
    // TODO: properly handle axis order

    if (epsgString === 'EPSG:4326') {
        return `${maxy},${minx}`;
    }

    return `${minx},${maxy}`;
}

/**
 * @returns the grid offsets as a request parameter for OGC services
 */
export function gridOffsetsAsOgcString(resolutionX: number, resolutionY: number, epsgString: string): string {
    // TODO: properly handle axis order

    if (epsgString === 'EPSG:4326') {
        return `${resolutionY},${resolutionX}`;
    }

    return `${resolutionX},${resolutionY}`;
}

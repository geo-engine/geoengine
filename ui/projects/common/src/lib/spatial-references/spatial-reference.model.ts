import {SpatialReferenceSpecification as SpatialReferenceSpecificationDict} from '@geoengine/api-client';

export type SrsString = string;

export class SpatialReferenceSpecification {
    readonly name: string;
    readonly spatialReference: SpatialReference;
    readonly projString: string;
    readonly extent: [number, number, number, number];
    readonly axisLabels?: [string, string];

    constructor(dict: SpatialReferenceSpecificationDict) {
        this.name = dict.name;
        this.spatialReference = SpatialReference.fromSrsString(dict.spatialReference);
        this.projString = dict.projString;
        this.extent = [
            dict.extent.lowerLeftCoordinate.x,
            dict.extent.lowerLeftCoordinate.y,
            dict.extent.upperRightCoordinate.x,
            dict.extent.upperRightCoordinate.y,
        ];
        this.axisLabels = dict.axisLabels ? [dict.axisLabels[0], dict.axisLabels[1]] : ['x', 'y'];
    }

    static fromDict(dict: SpatialReferenceSpecificationDict): SpatialReferenceSpecification {
        return new SpatialReferenceSpecification(dict);
    }
}

export class SpatialReference {
    readonly srsString: SrsString;

    constructor(srsString: SrsString) {
        this.srsString = srsString;
    }

    static fromSrsString(srsString: SrsString): SpatialReference {
        return new SpatialReference(srsString);
    }

    equals(other: SpatialReference): boolean {
        return this.srsString === other.srsString;
    }

    wcsUrn(): string | undefined {
        if (this.srsString.startsWith('EPSG:')) {
            return `urn:ogc:def:crs:EPSG::${this.srsString.substring(5)}`;
        }

        return undefined;
    }
}

export class NamedSpatialReference {
    readonly name: string;
    readonly spatialReference: SpatialReference;

    constructor(name: string, srsString: SrsString) {
        this.name = name;
        this.spatialReference = SpatialReference.fromSrsString(srsString);
    }

    equals(other: NamedSpatialReference): boolean {
        return this.name === other.name && this.spatialReference.equals(other.spatialReference);
    }
}

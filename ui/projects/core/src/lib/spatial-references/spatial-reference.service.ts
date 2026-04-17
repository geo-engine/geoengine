import {Injectable, inject} from '@angular/core';
import {merge, Observable, of} from 'rxjs';
import {map, mergeMap} from 'rxjs/operators';
import {BBoxDict, SpatialReferenceSpecificationDict, SrsString, UUID} from '../backend/backend.model';
import {BackendService} from '../backend/backend.service';
import {get as olGetProjection, addProjection as olAddProjection} from 'ol/proj';
import {register as olProj4Register} from 'ol/proj/proj4';
import OlProjection from 'ol/proj/Projection';
import {Units} from 'ol/proj/Units';
import proj4 from 'proj4';
import {CoreConfig} from '../config.service';
import {transformExtent} from 'ol/proj';
import {getIntersection} from 'ol/extent';
import {
    bboxDictToExtent,
    extentToBboxDict,
    NamedSpatialReference,
    SpatialReference,
    SpatialReferenceSpecification,
    UserService,
} from '@geoengine/common';

export const WEB_MERCATOR = new NamedSpatialReference('WGS 84 / Pseudomercator', 'EPSG:3857');
export const WGS_84 = new NamedSpatialReference('WGS 84', 'EPSG:4326');

/**
 * Service for managing spatial references and projections
 */
@Injectable({
    providedIn: 'root',
})
export class SpatialReferenceService {
    protected backend = inject(BackendService);
    protected userService = inject(UserService);
    protected readonly config = inject(CoreConfig);

    private specs = new Map<string, SpatialReferenceSpecification>();

    constructor() {
        this.registerDefaults();
    }

    /**
     * get the list of known spatial references
     */
    getSpatialReferences(): Array<NamedSpatialReference> {
        const srefs = [];

        for (const spec of this.specs.values()) {
            srefs.push(new NamedSpatialReference(spec.name, spec.spatialReference.srsString));
        }

        srefs.sort((a, b) => a.name.localeCompare(b.name));
        return srefs;
    }

    /**
     * lookup specification by srs string authority:code
     */
    getSpatialReferenceSpecification(srsString: SrsString): Observable<SpatialReferenceSpecification> {
        const spec = this.specs.get(srsString.toUpperCase());

        if (spec) {
            return of(spec);
        }

        return this.getAndRegisterSpec(srsString);
    }

    getOlProjection(spatialReference: SpatialReference): OlProjection {
        const projection = olGetProjection(spatialReference.srsString);

        if (projection) {
            return projection;
        } else {
            throw new Error(`Projection ${spatialReference.srsString} not found`);
        }
    }

    reprojectExtent(
        extent: [number, number, number, number],
        from: SpatialReference,
        to: SpatialReference,
    ): [number, number, number, number] {
        return transformExtent(extent, from.srsString, to.srsString) as [number, number, number, number];
    }

    clipBoundsIfAvailable(extent: [number, number, number, number], projection: SpatialReference): [number, number, number, number] {
        try {
            const olProjection = this.getOlProjection(projection);
            return getIntersection(extent, olProjection.getExtent()) as [number, number, number, number];
        } catch (_e) {
            // on error, just return the extent
            return extent;
        }
    }

    reprojectBbox(bbox: BBoxDict, from: SpatialReference, to: SpatialReference): BBoxDict {
        return extentToBboxDict(this.reprojectExtent(bboxDictToExtent(bbox), from, to));
    }

    private registerDefaults(): void {
        this.specs.set(
            'EPSG:4326',
            new SpatialReferenceSpecification({
                name: 'WGS84',
                spatialReference: 'EPSG:4326',
                projString: '+proj=longlat +datum=WGS84 +no_defs +type=crs',
                extent: {
                    lowerLeftCoordinate: {
                        x: -180,
                        y: -90,
                    },
                    upperRightCoordinate: {
                        x: 180,
                        y: 90,
                    },
                },
                axisLabels: ['longitude', 'latitude'],
            }),
        );

        this.specs.set(
            'EPSG:3857',
            new SpatialReferenceSpecification({
                name: 'WGS84 Webmercator',
                spatialReference: 'EPSG:3857',
                projString:
                    '+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +nadgrids=@null +wktext +no_defs +type=crs',
                extent: {
                    lowerLeftCoordinate: {
                        x: -20037508.34,
                        y: -20037508.34,
                    },
                    upperRightCoordinate: {
                        x: 20037508.34,
                        y: 20037508.34,
                    },
                },
            }),
        );

        merge(
            this.config.SPATIAL_REFERENCES.filter((sref) => !this.specs.has(sref.SRS_STRING)).map((sref) =>
                this.getAndRegisterSpec(sref.SRS_STRING),
            ),
        ).subscribe((specs) => {
            specs.forEach((spec) => {
                this.specs.set(spec.spatialReference.srsString, spec);
            });
        });
    }

    private getAndRegisterSpec(srsString: SrsString): Observable<SpatialReferenceSpecification> {
        return this.userService.getSessionTokenForRequest().pipe(
            mergeMap((token: UUID) => this.backend.getSpatialReferenceSpecification(token, srsString)),
            map((dict: SpatialReferenceSpecificationDict) => {
                const spec = SpatialReferenceSpecification.fromDict(dict);

                proj4.defs(spec.spatialReference.srsString, spec.projString);
                const def = proj4.defs(spec.spatialReference.srsString);
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                olProj4Register(proj4 as any /* TODO: remove any */);

                olAddProjection(
                    new OlProjection({
                        code: spec.spatialReference.srsString,
                        extent: spec.extent,
                        units: def?.units as Units,
                    }),
                );

                return spec;
            }),
        );
    }
}

import {Time} from '../time/time.model';
import {LayerType} from './layer.model';
import {SpatialReference} from '../spatial-references/spatial-reference.model';
import {GeoJSON as OlFormatGeoJSON} from 'ol/format';
import OlFeature from 'ol/Feature';
import {ProjectionLike as OlProjectionLike} from 'ol/proj';
import {featureToHash} from '../util/conversions';
import {FeatureLike} from 'ol/Feature';
import OlRenderFeature, {toFeature} from 'ol/render/Feature';
import {UUID} from '../datasets/dataset.model';

export abstract class LayerData {
    readonly type: LayerType;
    readonly time: Time;
    readonly spatialReference: SpatialReference;

    protected constructor(type: LayerType, time: Time, spatialReference: SpatialReference) {
        this.type = type;
        this.spatialReference = spatialReference;
        this.time = time;
    }
}

export class RasterData extends LayerData {
    readonly workflowId: UUID;

    constructor(time: Time, spatialReference: SpatialReference, workflowId: UUID) {
        if (time.end.isAfter(time.start)) {
            time = new Time(time.start);
        }
        super('raster', time, spatialReference);
        this.workflowId = workflowId;
    }
}

export class VectorData extends LayerData {
    readonly data: Array<OlFeature>;
    readonly extent: [number, number, number, number];

    constructor(time: Time, projection: SpatialReference, data: Array<OlFeature>, extent: [number, number, number, number]) {
        super('vector', time, projection);
        this.data = data;
        this.extent = extent;
        this.fakeIds(); // FIXME: use real IDs ...
    }

    static olParse(
        time: Time,
        projection: SpatialReference,
        extent: [number, number, number, number],
        // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-redundant-type-constituents
        source: Document | Node | any | string, // any is also used by ol
        optOptions?: {dataProjection: OlProjectionLike; featureProjection: OlProjectionLike},
    ): VectorData {
        const featureLikes = new OlFormatGeoJSON().readFeatures(source, optOptions);
        const features = featureLikes.map((f: FeatureLike) => (f instanceof OlRenderFeature ? toFeature(f) : f));
        return new VectorData(time, projection, features, extent);
    }

    fakeIds(): void {
        for (const feature of this.data) {
            if (feature.getId() === undefined) {
                feature.setId(featureToHash(feature));
            }
        }
    }
}

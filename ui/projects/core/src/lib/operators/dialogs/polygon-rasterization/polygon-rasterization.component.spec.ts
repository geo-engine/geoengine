import moment from 'moment';
import {describe, expect, it} from 'vitest';
import {VectorOperator} from '@geoengine/api-client';
import {
    Coordinate2D,
    GeoTransform,
    GridBoundingBox2D,
    GridIdx2D,
    RasterDataTypes,
    RasterLayerMetadata,
    SpatialGridDefinition,
    SpatialGridDescriptor,
    SpatialReference,
} from '@geoengine/common';
import {
    PolygonRasterizationFormValues,
    gridOfRaster,
    polygonRasterizationParams,
    reprojectedVectorOperator,
} from './polygon-rasterization.component';

const defaults: PolygonRasterizationFormValues = {
    resolutionX: 0.1,
    resolutionY: 0.2,
    originX: 0,
    originY: 0.05,
    valueMode: 'constant',
    constant: 1,
    column: '',
    aggregation: 'max',
    ignoreNoData: true,
    outputType: undefined,
    timeMode: 'featureTimes',
    granularity: 'months',
    step: 1,
    referenceChecked: false,
    reference: moment.utc(0),
};

describe('polygonRasterizationParams', () => {
    it('creates the parameters of a constant value', () => {
        expect(polygonRasterizationParams(defaults)).toEqual({
            spatialResolution: {x: 0.1, y: 0.2},
            originCoordinate: {x: 0, y: 0.05},
            value: {type: 'constant', value: 1},
            outputType: undefined,
            time: {type: 'featureTimes'},
        });
    });

    it('creates the parameters of counts and attributes', () => {
        expect(polygonRasterizationParams({...defaults, valueMode: 'count'}).value).toEqual({type: 'count'});

        expect(
            polygonRasterizationParams({
                ...defaults,
                valueMode: 'attribute',
                column: 'class',
                aggregation: 'medianEstimate',
                ignoreNoData: false,
            }).value,
        ).toEqual({type: 'attribute', column: 'class', aggregation: 'medianEstimate', ignoreNoData: false});
    });

    it('creates the parameters of the output type', () => {
        expect(polygonRasterizationParams({...defaults, outputType: RasterDataTypes.Float32}).outputType).toEqual('F32');
    });

    it('creates the parameters of the time', () => {
        expect(polygonRasterizationParams({...defaults, timeMode: 'ignore'}).time).toEqual({type: 'ignore'});

        expect(polygonRasterizationParams({...defaults, timeMode: 'regular', granularity: 'years', step: 2}).time).toEqual({
            type: 'regular',
            step: {granularity: 'years', step: 2},
            reference: undefined,
        });

        expect(
            polygonRasterizationParams({
                ...defaults,
                timeMode: 'regular',
                referenceChecked: true,
                reference: moment.utc('2020-01-01'),
            }).time,
        ).toEqual({
            type: 'regular',
            step: {granularity: 'months', step: 1},
            reference: Date.UTC(2020, 0, 1),
        });
    });
});

describe('gridOfRaster', () => {
    it('copies the grid and data type of a raster', () => {
        const metadata = new RasterLayerMetadata(
            RasterDataTypes.Byte,
            SpatialReference.fromSrsString('EPSG:4326'),
            [],
            new SpatialGridDescriptor(
                new SpatialGridDefinition(
                    new GeoTransform(new Coordinate2D([-180, 90]), 0.1, -0.2),
                    new GridBoundingBox2D(new GridIdx2D(0, 0), new GridIdx2D(899, 3599)),
                ),
                'source',
            ),
        );

        expect(gridOfRaster(metadata)).toEqual({
            resolutionX: 0.1,
            resolutionY: 0.2,
            originX: -180,
            originY: 90,
            outputType: RasterDataTypes.Byte,
        });
    });
});

describe('reprojectedVectorOperator', () => {
    const operator: VectorOperator = {type: 'OgrSource', params: {data: 'germany'}};
    const wgs84 = SpatialReference.fromSrsString('EPSG:4326');
    const utm = SpatialReference.fromSrsString('EPSG:32632');

    it('keeps the operator without a different target', () => {
        expect(reprojectedVectorOperator(operator, wgs84, undefined)).toBe(operator);
        expect(reprojectedVectorOperator(operator, wgs84, wgs84)).toBe(operator);
    });

    it('reprojects the operator to a different target', () => {
        expect(reprojectedVectorOperator(operator, wgs84, utm)).toEqual({
            type: 'Reprojection',
            params: {targetSpatialReference: 'EPSG:32632'},
            sources: {source: operator},
        });
    });
});

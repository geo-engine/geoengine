import {describe, expect, it} from 'vitest';
import {RasterOperator} from '@geoengine/api-client';
import {Coordinate2D, GeoTransform, GridBoundingBox2D, GridIdx2D, SpatialGridDefinition} from '@geoengine/common';
import {Regrid, regriddedOperator} from './raster-stacker.component';

const operator: RasterOperator = {
    type: 'GdalSource',
    params: {data: 'ndvi'},
};

function grid(originX: number, originY: number, pixelSizeX: number, pixelSizeY: number): SpatialGridDefinition {
    return new SpatialGridDefinition(
        new GeoTransform(new Coordinate2D([originX, originY]), pixelSizeX, pixelSizeY),
        new GridBoundingBox2D(new GridIdx2D(0, 0), new GridIdx2D(99, 99)),
    );
}

function regrid(originX: number, originY: number, resolutionX: number, resolutionY: number): Regrid {
    return {origin: {x: originX, y: originY}, resolution: {x: resolutionX, y: resolutionY}};
}

describe('regriddedOperator', () => {
    it('keeps a layer that is already on the target grid', () => {
        const result = regriddedOperator(operator, 'EPSG:4326', 'EPSG:4326', grid(0, 0, 0.1, -0.1), regrid(0, 0, 0.1, 0.1));

        expect(result).toBe(operator);
    });

    it('reprojects a layer in another spatial reference', () => {
        const result = regriddedOperator(operator, 'EPSG:4326', 'EPSG:3857', grid(0, 0, 100, -100), regrid(0, 0, 100, 100));

        expect(result).toEqual({
            type: 'Warp',
            params: {targetSpatialReference: 'EPSG:3857'},
            sources: {raster: operator},
        });
    });

    it('resamples a coarser layer to a finer target', () => {
        const result = regriddedOperator(operator, 'EPSG:4326', 'EPSG:4326', grid(0, 0, 0.2, -0.2), regrid(0, 0, 0.1, 0.1));

        expect(result).toEqual({
            type: 'Warp',
            params: {outputResolution: {x: 0.1, y: 0.1}},
            sources: {raster: operator},
        });
    });

    it('resamples a finer layer to a coarser target', () => {
        const result = regriddedOperator(operator, 'EPSG:4326', 'EPSG:4326', grid(0, 0, 0.1, -0.1), regrid(0, 0, 0.2, 0.2));

        expect(result).toEqual({
            type: 'Warp',
            params: {outputResolution: {x: 0.2, y: 0.2}},
            sources: {raster: operator},
        });
    });

    it('resamples a layer that is coarser in one axis and finer in the other', () => {
        const result = regriddedOperator(operator, 'EPSG:4326', 'EPSG:4326', grid(0, 0, 0.2, -0.05), regrid(0, 0, 0.1, 0.1));

        expect(result).toEqual({
            type: 'Warp',
            params: {outputResolution: {x: 0.1, y: 0.1}},
            sources: {raster: operator},
        });
    });

    it('moves the origin of a layer with the target resolution', () => {
        const result = regriddedOperator(operator, 'EPSG:4326', 'EPSG:4326', grid(0, 0, 0.1, -0.1), regrid(0.05, 0.05, 0.1, 0.1));

        expect(result).toEqual({
            type: 'Warp',
            params: {outputOriginReference: {x: 0.05, y: 0.05}},
            sources: {raster: operator},
        });
    });

    it('reprojects, resamples and moves the origin in one Warp', () => {
        const result = regriddedOperator(operator, 'EPSG:4326', 'EPSG:3857', grid(0, 0, 90, -110), regrid(10, 20, 100, 100));

        expect(result).toEqual({
            type: 'Warp',
            params: {
                targetSpatialReference: 'EPSG:3857',
                outputResolution: {x: 100, y: 100},
                outputOriginReference: {x: 10, y: 20},
            },
            sources: {raster: operator},
        });
    });
});

import {Color, colorToDict} from '../colors/color';
import {ClusteredPointSymbology, LineSymbology, PointSymbology, PolygonSymbology, VectorSymbology} from '../symbology/symbology.model';

/**
 * @returns a vector symbology for the given data type and using the given color as fill color (points, polygons) or stroke color (lines)
 */
export function createVectorSymbology(dataType: 'Data' | 'MultiPoint' | 'MultiLineString' | 'MultiPolygon', color: Color): VectorSymbology {
    switch (dataType) {
        case 'Data':
            // TODO: cope with that
            throw Error('we cannot add data layers here, yet');
        case 'MultiPoint':
            return ClusteredPointSymbology.fromPointSymbologyDict({
                type: 'point',
                radius: {type: 'static', value: PointSymbology.DEFAULT_POINT_RADIUS},
                stroke: {
                    width: {type: 'static', value: 1},
                    color: {type: 'static', color: [0, 0, 0, 255]},
                },
                fillColor: {type: 'static', color: colorToDict(color)},
            });
        case 'MultiLineString':
            return LineSymbology.fromLineSymbologyDict({
                type: 'line',
                stroke: {
                    width: {type: 'static', value: 1},
                    color: {type: 'static', color: colorToDict(color)},
                },
                autoSimplified: true,
            });
        case 'MultiPolygon':
            return PolygonSymbology.fromPolygonSymbologyDict({
                type: 'polygon',
                stroke: {
                    width: {type: 'static', value: 1},
                    color: {type: 'static', color: [0, 0, 0, 255]},
                },
                fillColor: {type: 'static', color: colorToDict(color)},
                autoSimplified: true,
            });
    }
}

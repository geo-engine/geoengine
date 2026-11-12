import {describe, expect, it} from 'vitest';
import {RasterSymbology} from '@geoengine/common';
import {tileVersion} from './map-layer.component';

describe('tileVersion', () => {
    const symbology = (opacity: number): RasterSymbology =>
        RasterSymbology.fromRasterSymbologyDict({
            type: 'raster',
            opacity,
            rasterColorizer: {
                type: 'singleBand',
                band: 0,
                bandColorizer: {
                    type: 'linearGradient',
                    breakpoints: [
                        {value: 0, color: [0, 0, 0, 255]},
                        {value: 1, color: [255, 255, 255, 255]},
                    ],
                    overColor: [255, 255, 255, 255],
                    underColor: [0, 0, 0, 255],
                    noDataColor: [0, 0, 0, 0],
                },
            },
        });

    const graph = 'ffffffff-ffff-4fff-afff-ffffffffffff';
    const otherGraph = 'eeeeeeee-eeee-4eee-aeee-eeeeeeeeeeee';

    it('stays the same for the same layer', () => {
        expect(tileVersion(graph, symbology(1))).toBe(tileVersion(graph, symbology(1)));
    });

    it('changes with the symbology', () => {
        expect(tileVersion(graph, symbology(0.5))).not.toBe(tileVersion(graph, symbology(1)));
    });

    it('changes with the processing graph', () => {
        expect(tileVersion(otherGraph, symbology(1))).not.toBe(tileVersion(graph, symbology(1)));
    });
});

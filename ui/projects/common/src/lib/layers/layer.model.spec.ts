import {describe, expect, it} from 'vitest';
import {RasterLayer, UNSAVED_LAYER_ID} from './layer.model';
import {RasterSymbology} from '../symbology/symbology.model';

describe('Layer', () => {
    const symbology = RasterSymbology.fromRasterSymbologyDict({
        type: 'raster',
        opacity: 1,
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

    const newLayer = (): RasterLayer =>
        new RasterLayer({
            name: 'NDVI',
            processingGraphId: 'ffffffff-ffff-4fff-afff-ffffffffffff',
            isVisible: true,
            isLegendVisible: false,
            symbology,
            source: {providerId: 'ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74', layerId: 'ndvi'},
        });

    it('sends a new layer without id, so that the server assigns one', () => {
        const layer = newLayer();

        expect(layer.id).toBe(UNSAVED_LAYER_ID);
        expect(layer.isSaved).toBe(false);
        expect(layer.toDict().id).toBeUndefined();
        expect(layer.toDict().source).toEqual({providerId: 'ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74', layerId: 'ndvi'});
    });

    it('keeps the id of a stored layer when it changes', () => {
        const layer = newLayer().updateFields({id: 'a5f4b2b4-8b6c-4c1e-9f0e-2d7d6f2f0f11'});
        const renamed = layer.updateFields({name: 'renamed'});

        expect(renamed.isSaved).toBe(true);
        expect(renamed.toDict().id).toBe('a5f4b2b4-8b6c-4c1e-9f0e-2d7d6f2f0f11');
        expect(renamed.toDict().name).toBe('renamed');
        expect(renamed.source).toEqual(layer.source);
    });

    it('round-trips a stored layer', () => {
        const layer = newLayer().updateFields({id: 'a5f4b2b4-8b6c-4c1e-9f0e-2d7d6f2f0f11'});

        const dict = layer.toProjectLayerDict();

        expect(RasterLayer.fromDict(dict).toProjectLayerDict()).toEqual(dict);
    });
});

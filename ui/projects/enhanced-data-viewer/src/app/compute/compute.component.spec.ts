import {describe, expect, it, vi} from 'vitest';
import {signal} from '@angular/core';
import OlFeature from 'ol/Feature';
import OlGeomPolygon from 'ol/geom/Polygon';
import OlLayerVector from 'ol/layer/Vector';
import OlSourceVector from 'ol/source/Vector';
import {hasDrawnBoxSelection} from './histogram-box-selection';

describe('hasDrawnBoxSelection', () => {
    it('returns true when the overlay contains a polygon drawn as a box', () => {
        const geometry = new OlGeomPolygon([
            [
                [0, 0],
                [1, 0],
                [1, 1],
                [0, 1],
                [0, 0],
            ],
        ]);

        const feature = new OlFeature({geometry});
        const source = new OlSourceVector({features: [feature]});
        const overlayLayer = new OlLayerVector({source});

        expect(hasDrawnBoxSelection({overlayLayer: () => overlayLayer})).toBe(true);
    });

    it('returns false when there is no box overlay', () => {
        expect(hasDrawnBoxSelection(undefined)).toBe(false);
        expect(hasDrawnBoxSelection({overlayLayer: () => undefined})).toBe(false);
    });

    it('reacts when the overlay signal changes from empty to drawn', () => {
        const overlayLayer = signal<OlLayerVector<OlSourceVector<OlFeature>> | undefined>(undefined);
        const map = {overlayLayer: vi.fn(() => overlayLayer())};

        const geometry = new OlGeomPolygon([
            [
                [0, 0],
                [1, 0],
                [1, 1],
                [0, 1],
                [0, 0],
            ],
        ]);

        expect(hasDrawnBoxSelection(map)).toBe(false);

        const feature = new OlFeature({geometry});
        const source = new OlSourceVector({features: [feature]});
        overlayLayer.set(new OlLayerVector({source}));

        expect(hasDrawnBoxSelection(map)).toBe(true);
    });
});

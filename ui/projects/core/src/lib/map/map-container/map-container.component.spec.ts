import {describe, expect, it, beforeAll} from 'vitest';
import OlMap from 'ol/Map';
import {defaults as defaultInteractions, DoubleClickZoom} from 'ol/interaction';

import {setMapDoubleClickZoom} from './map-utils';

beforeAll(() => {
    globalThis.ResizeObserver = class ResizeObserver {
        observe(): void {
            /* empty */
        }
        unobserve(): void {
            /* empty */
        }
        disconnect(): void {
            /* empty */
        }
    };
});

describe('setMapDoubleClickZoom', () => {
    it('toggles the OpenLayers double-click zoom interaction', () => {
        const map = new OlMap({
            interactions: defaultInteractions({doubleClickZoom: true}),
        });

        const interaction = map
            .getInteractions()
            .getArray()
            .find((candidate) => candidate instanceof DoubleClickZoom);

        expect(interaction).toBeDefined();
        expect(interaction?.getActive()).toBe(true);

        setMapDoubleClickZoom(map, false);
        expect(interaction?.getActive()).toBe(false);

        setMapDoubleClickZoom(map, true);
        expect(interaction?.getActive()).toBe(true);
    });
});

import {describe, expect, it} from 'vitest';
import {axisLabelLimit} from './vega-viewer.component';

describe('axisLabelLimit', () => {
    it('scales with the chart height', () => {
        expect(axisLabelLimit(140)).toBe(70);
    });

    it('is capped at the Vega-Lite default', () => {
        expect(axisLabelLimit(1000)).toBe(200);
    });
});

import {clamp} from './math';

describe('Math Utils', () => {
    it('clamps', () => {
        expect(clamp(5, 0, 10)).toBe(5);
        expect(clamp(0.5, 0, 1)).toBe(0.5);
        expect(clamp(1, 2, 4)).toBe(2);
        expect(clamp(5, 1, 4)).toBe(4);
    });
});

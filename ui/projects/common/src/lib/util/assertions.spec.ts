import {assertNever, isNullOrUndefined} from './assertions';

describe('assertions', () => {
    it('should handle never assertions', () => {
        expect(() => assertNever(undefined as never)).toThrow();
    });

    it('should handle null or undefined assertions', () => {
        expect(isNullOrUndefined(null)).toBe(true);
        expect(isNullOrUndefined(undefined)).toBe(true);
        expect(isNullOrUndefined(0)).toBe(false);
        expect(isNullOrUndefined('')).toBe(false);
        expect(isNullOrUndefined(false)).toBe(false);
    });
});

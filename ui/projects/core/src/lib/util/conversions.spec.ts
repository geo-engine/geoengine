import {olExtentToTuple} from '@geoengine/common';

describe('Conversion Utils', () => {
    it('converts OlExtents', () => {
        expect(olExtentToTuple([1, 2, 3, 4])).toEqual([1, 2, 3, 4]);
        expect(() => olExtentToTuple([1, 2, 3])).toThrowError('OlExtent must be of size 4');
    });
});

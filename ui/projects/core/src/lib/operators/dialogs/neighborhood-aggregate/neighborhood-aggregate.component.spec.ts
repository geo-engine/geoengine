import {increaseMatrix, decreaseMatrix, rotateMatrixClockwise} from './neighborhood-aggregate.component';

describe('RasterKernel', () => {
    it('increases a matrix', () => {
        expect(increaseMatrix([[1]])).toEqual([
            [0, 0, 0],
            [0, 1, 0],
            [0, 0, 0],
        ]);

        expect(
            increaseMatrix([
                [1, 2, 3],
                [4, 5, 6],
                [7, 8, 9],
            ]),
        ).toEqual([
            [0, 0, 0, 0, 0],
            [0, 1, 2, 3, 0],
            [0, 4, 5, 6, 0],
            [0, 7, 8, 9, 0],
            [0, 0, 0, 0, 0],
        ]);
    });

    it('decreases a matrix', () => {
        expect(
            decreaseMatrix([
                [0, 0, 0],
                [0, 1, 0],
                [0, 0, 0],
            ]),
        ).toEqual([[1]]);

        expect(decreaseMatrix([[1]])).toEqual([[1]]);

        expect(
            decreaseMatrix([
                [9, 9, 9, 9, 9],
                [9, 1, 2, 3, 9],
                [9, 3, 4, 5, 9],
                [9, 5, 6, 7, 9],
                [9, 9, 9, 9, 9],
            ]),
        ).toEqual([
            [1, 2, 3],
            [3, 4, 5],
            [5, 6, 7],
        ]);
    });

    it('rotates a matrix', () => {
        expect(
            rotateMatrixClockwise([
                [1, 2, 3],
                [4, 5, 6],
                [7, 8, 9],
            ]),
        ).toEqual([
            [7, 4, 1],
            [8, 5, 2],
            [9, 6, 3],
        ]);

        expect(rotateMatrixClockwise([[1]])).toEqual([[1]]);
    });
});

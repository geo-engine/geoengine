import {RgbaTuple, convertFractionsToRgbas} from '../color';

/**
 * The rainbow color map.
 */
const COLORMAP_RAINBOW_DATA: Array<[number, number, number]> = [
    [0.0, 0.0, 0.5],
    [0.0, 0.0, 0.803],
    [0.0, 0.03333, 1.0],
    [0.0, 0.3, 1.0],
    [0.0, 0.5666, 1.0],
    [0.0, 0.8333, 1.0],
    [0.1613, 1.0, 0.8064],
    [0.3763, 1.0, 0.5914],
    [0.5914, 1.0, 0.376],
    [0.8064, 1.0, 0.1613],
    [1.0, 0.9012, 0.0],
    [1.0, 0.6543, 0.0],
    [1.0, 0.4074, 0.0],
    [1.0, 0.1605, 0.0],
    [0.803, 0.0, 0.0],
    [0.5, 0.0, 0.0],
];

const COLORMAP_BLACKWHITE_DATA: Array<[number, number, number]> = [
    [0, 0, 0],
    [1, 1, 1],
];

export const GENERIC_COLORMAPS: Record<string, Array<RgbaTuple>> = {
    BLACKWHITE: convertFractionsToRgbas(COLORMAP_BLACKWHITE_DATA),
    RAINBOW: convertFractionsToRgbas(COLORMAP_RAINBOW_DATA),
};

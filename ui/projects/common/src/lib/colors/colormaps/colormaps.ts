import {GENERIC_COLORMAPS} from './generic-colormaps';
import {MORELAND_COLORMAPS} from './moreland-colormaps';
import {MPL_CB_COLORMAPS} from './mpl-cb-colormaps/mpl-cb-colormaps';
import {MPL_COLORMAPS} from './mpl-colormaps';
import {SCIENTIFIC_COLORMAPS} from './scientific-colormaps/scientific-colormaps';

export const ALL_COLORMAPS = {...MPL_COLORMAPS, ...GENERIC_COLORMAPS, ...MORELAND_COLORMAPS, ...SCIENTIFIC_COLORMAPS, ...MPL_CB_COLORMAPS};

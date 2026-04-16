import {hashCode} from './conversions';

/**
 * Create a Data URI image from a string
 */
export const createIconDataUrl = (iconName: string): string => {
    // TODO: replace with proper icons
    // from `http://stackoverflow.com/questions/3426404/
    // create-a-hexadecimal-colour-based-on-a-string-with-javascript`
    const intToRGB = (i: number): string => {
        const c = (i & 0x00ffffff).toString(16).toUpperCase(); // eslint-disable-line no-bitwise

        return '00000'.substring(0, 6 - c.length) + c;
    };

    const color = '#' + intToRGB(hashCode(iconName));
    const size = 64;

    const canvas = document.createElement('canvas');
    canvas.width = size;
    canvas.height = size;

    const context = canvas.getContext('2d');
    if (context) {
        context.fillStyle = color;
        context.fillRect(0, 0, 64, 64);
    }

    return canvas.toDataURL('image/png');
};

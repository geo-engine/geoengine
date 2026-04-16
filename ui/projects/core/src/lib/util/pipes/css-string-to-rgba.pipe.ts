import {Pipe, PipeTransform} from '@angular/core';
import {TRANSPARENT} from '@geoengine/common';

@Pipe({name: 'cssStringToRgbaPipe'})
export class CssStringToRgbaPipe implements PipeTransform {
    // TODO: change to colorutils!
    transform(rgbaCssString: string): [number, number, number, number] {
        if (rgbaCssString === undefined || rgbaCssString === '') {
            // transparent fallback
            return TRANSPARENT.rgbaTuple();
        }

        const rgba = /^rgba?\(\s*(\d{1,3})\s*,\s*(\d{1,3})\s*,\s*(\d{1,3})(?:\s*,\s*(0?\.?\d+|1(?:\.0+)?))?\s*\)$/i.exec(rgbaCssString);

        if (rgba) {
            return [parseInt(rgba[1], 10), parseInt(rgba[2], 10), parseInt(rgba[3], 10), rgba[4] === undefined ? 1 : parseFloat(rgba[4])];
        }

        const threeDigitMatch = /^#([0-9a-f]{3})$/i.exec(rgbaCssString);
        if (threeDigitMatch) {
            const threeDigit = threeDigitMatch[1];
            // in three-character format, each value is multiplied by 0x11 to give an
            // even scale from 0x00 to 0xff
            return [
                parseInt(threeDigit.charAt(0), 16) * 0x11,
                parseInt(threeDigit.charAt(1), 16) * 0x11,
                parseInt(threeDigit.charAt(2), 16) * 0x11,
                1,
            ];
        }

        const sixDigitMatch = /^#([0-9a-f]{6})$/i.exec(rgbaCssString);
        if (sixDigitMatch) {
            const sixDigit = sixDigitMatch[1];
            return [parseInt(sixDigit.substr(0, 2), 16), parseInt(sixDigit.substr(2, 2), 16), parseInt(sixDigit.substr(4, 2), 16), 1];
        }

        // transparent fallback
        return TRANSPARENT.rgbaTuple();
    }
}

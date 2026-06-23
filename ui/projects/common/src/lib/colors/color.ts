export interface RgbaStruct {
    r: number;
    g: number;
    b: number;
    a: number;
}

interface RgbStruct {
    r: number;
    g: number;
    b: number;
}

export type RgbaTuple = [number, number, number, number];
export type RgbTuple = [number, number, number];
export type RgbaLike = RgbaTuple | RgbTuple | RgbaStruct | RgbStruct | Color | IRgba | string;

export type RgbaColorDict = Array<number>;

export const rgbaColorFromDict = (dict: RgbaColorDict): RgbaTuple => [dict[0], dict[1], dict[2], dict[3] / 255.0];

export function colorToDict(color: Color): RgbaColorDict {
    const rgbaColor = color.rgbaTuple();
    return [rgbaColor[0], rgbaColor[1], rgbaColor[2], Math.round(rgbaColor[3] * 255.0)];
}

/**
 * Converts RGB colors in fractions to RGB values represented as ints in 0..256, alpha is set to 1
 */
export const convertFractionsToRgbas = (colors: Array<[number, number, number]>): Array<RgbaTuple> =>
    colors.map(([r, g, b]) => {
        r = Math.round(r * 255);
        g = Math.round(g * 255);
        b = Math.round(b * 255);
        return [r, g, b, 1];
    });

/**
 *  An interface for types representing a Rgba color
 */
export interface IRgba {
    rgbaTuple(): RgbaTuple;

    rgbaStruct(): RgbaStruct;

    rgbaCssString(): string;
}

/**
 * Color class representing colors
 */
export class Color implements IRgba, RgbaStruct {
    r: number;
    g: number;
    b: number;
    a: number;

    constructor(config: RgbaStruct) {
        this.r = config.r;
        this.g = config.g;
        this.b = config.b;
        this.a = config.a;
    }

    rgbaStruct(): RgbaStruct {
        return this;
    }

    rgbaTuple(): RgbaTuple {
        return [this.r, this.g, this.b, this.a];
    }

    rgbTuple(): RgbTuple {
        return [this.r, this.g, this.b];
    }

    rgbaCssString(): string {
        return Color.rgbaToCssString(this);
    }

    clone(): Color {
        return new Color({
            r: this.r,
            g: this.g,
            b: this.b,
            a: this.a,
        });
    }

    equals(other: RgbaLike): boolean {
        const o = Color.fromRgbaLike(other);
        return this.r === o.r && this.g === o.g && this.b === o.b && this.a === o.a;
    }

    static rgbaToCssString(rgba: RgbaLike): string {
        const temp = Color.fromRgbaLike(rgba, false).rgbaStruct();
        return `rgba(${temp.r.toString()}, ${temp.g.toString()}, ${temp.b.toString()}, ${temp.a.toString()})`;
    }

    static rgbaTupleToStruct(rgbaTuple: RgbaTuple | RgbTuple): RgbaStruct {
        if (rgbaTuple.length < 3 || rgbaTuple.length > 4) {
            throw new Error('Invalid RGB(A) tuple size!');
        }

        const alpha: number = rgbaTuple.length === 4 ? rgbaTuple[3] : 1.0;

        return {r: rgbaTuple[0], g: rgbaTuple[1], b: rgbaTuple[2], a: alpha};
    }

    /**
     * Creates a Color instance from RgbaLike Types.
     *
     * @param rgba: the rgba like input
     * @param clone: clone if already an instance of Color. Defaults to TRUE!
     */
    static fromRgbaLike(rgba: RgbaLike, clone = true): Color {
        if (!rgba) {
            // return some default on empty deserialization
            return BLACK;
        }

        if (rgba instanceof Color) {
            if (clone) {
                return rgba.clone();
            }
            return rgba;
        }

        if (rgba instanceof Array) {
            return new Color(Color.rgbaTupleToStruct(rgba));
        }

        if (typeof rgba === 'string') {
            rgba = stringToRgbaStruct(rgba);
        }

        if (typeof rgba === 'object' && 'rgbaStruct' in (rgba as IRgba)) {
            return new Color((rgba as IRgba).rgbaStruct());
        }

        if ((rgba as RgbaStruct).a || (rgba as RgbaStruct).a === 0) {
            return new Color(rgba as RgbaStruct);
        }

        if ((rgba as RgbStruct).r) {
            const rgb = rgba as RgbStruct;
            return new Color({
                r: rgb.r,
                g: rgb.g,
                b: rgb.b,
                a: 1,
            });
        }

        throw new Error('invalid RgbaLike ' + JSON.stringify(rgba));
    }

    /**
     * Interpolates between two colors
     *
     * @param a: first Color -> 0
     * @param b: second Color -> 1
     * @param fraction: value between 0 and 1
     */
    static interpolate(a: RgbaLike, b: RgbaLike, fraction: number): Color {
        const ra = Color.fromRgbaLike(a, false);
        const rb = Color.fromRgbaLike(b, false);
        if (fraction === 0) {
            return ra;
        }
        if (fraction === 1) {
            return rb;
        }
        if (ra.equals(rb)) {
            return ra;
        }
        const clr = {
            r: ra.r * (1 - fraction) + rb.r * fraction,
            g: ra.g * (1 - fraction) + rb.g * fraction,
            b: ra.b * (1 - fraction) + rb.b * fraction,
            a: ra.a * (1 - fraction) + rb.a * fraction,
        };
        return Color.fromRgbaLike(clr, false);
    }

    static colorDifference(a: RgbaLike, b: RgbaLike): number {
        const ra = Color.fromRgbaLike(a, false).rgbTuple();
        const rb = Color.fromRgbaLike(b, false).rgbTuple();

        return ra.map((baseColor, i) => Math.pow(baseColor - rb[i], 2)).reduce((acc, value) => acc + value);
    }
}

export const BLACK = Color.fromRgbaLike([0, 0, 0, 1]);
export const WHITE = Color.fromRgbaLike([255, 255, 255, 1]);
export const TRANSPARENT = Color.fromRgbaLike([0, 0, 0, 0]);

/**
 * Should a string also be RgbaLike?
 */
export const stringToRgbaStruct = (rgbaCssString: string): RgbaStruct => {
    if (rgbaCssString === undefined || rgbaCssString === '') {
        throw new Error('cant parse empty string into a color');
    }

    const rgba = /^rgba?\(\s*(\d{1,3})\s*,\s*(\d{1,3})\s*,\s*(\d{1,3})(?:\s*,\s*(0?\.?\d+|1(?:\.0+)?))?\s*\)$/i.exec(rgbaCssString);

    if (rgba) {
        return {
            r: parseInt(rgba[1], 10),
            g: parseInt(rgba[2], 10),
            b: parseInt(rgba[3], 10),
            a: rgba[4] === undefined ? 1 : parseFloat(rgba[4]),
        };
    }

    const threeDigitMatch = /^#([0-9a-f]{3})$/i.exec(rgbaCssString);
    if (threeDigitMatch) {
        const threeDigit = threeDigitMatch[1];

        // in three-character format, each value is multiplied by 0x11 to give an
        // even scale from 0x00 to 0xff
        return {
            r: parseInt(threeDigit.charAt(0), 16) * 0x11,
            g: parseInt(threeDigit.charAt(1), 16) * 0x11,
            b: parseInt(threeDigit.charAt(2), 16) * 0x11,
            a: 1,
        };
    }

    const sixDigitMatch = /^#([0-9a-f]{6})$/i.exec(rgbaCssString);
    if (sixDigitMatch) {
        const sixDigit = sixDigitMatch[1];
        return {
            r: parseInt(sixDigit.substr(0, 2), 16),
            g: parseInt(sixDigit.substr(2, 2), 16),
            b: parseInt(sixDigit.substr(4, 2), 16),
            a: 1,
        };
    }

    throw new Error('cant parse string into a color' + rgbaCssString);
};

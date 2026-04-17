import {Breakpoint as BreakpointDict} from '@geoengine/api-client';
import {Color, RgbaLike, colorToDict, rgbaColorFromDict} from './color';

export type BreakPointValue = number;

/**
 * A ColorBreakpoint is a tuple consisting of value and RGBA.
 */
export class ColorBreakpoint {
    value: BreakPointValue;
    color: Color;

    constructor(value: BreakPointValue, color: Color) {
        this.color = color;
        this.value = value;
    }

    static fromDict(dict: BreakpointDict): ColorBreakpoint {
        return new ColorBreakpoint(dict.value, Color.fromRgbaLike(rgbaColorFromDict(dict.color)));
    }

    /**
     * Clones the ColorBreakpoint
     */
    clone(): ColorBreakpoint {
        return new ColorBreakpoint(this.value, this.color);
    }

    /**
     * Clones the ColorBreakpoint and replaces the color.
     */
    cloneWithColor(color: RgbaLike): ColorBreakpoint {
        const cln = this.clone();
        cln.setColor(color);
        return cln;
    }

    /**
     * Clones the ColorBreakpoint and replaces the value.
     */
    cloneWithValue(value: BreakPointValue): ColorBreakpoint {
        const cln = this.clone();
        cln.setValue(value);
        return cln;
    }

    /**
     * Transforms the ColorBreakpoint int a BreakpointDict.
     */
    toDict(): BreakpointDict {
        return {
            value: this.value,
            color: colorToDict(this.color),
        };
    }

    /**
     * Sets the color to the provided value.
     */
    setColor(color: RgbaLike): void {
        this.color = Color.fromRgbaLike(color);
    }

    /**
     * Sets the value of the ColorBreakpoint to the provided value.
     */
    setValue(value: BreakPointValue): void {
        this.value = value;
    }

    /**
     * Returns true if the value is a number.
     */
    valueIsNumber(): boolean {
        return typeof this.value === 'number';
    }

    /**
     * Tests a ColorBreakpoint for equality with another one.
     */
    equals(other: ColorBreakpoint): boolean {
        return other && this.color.equals(other.color) && this.value === other.value;
    }
}

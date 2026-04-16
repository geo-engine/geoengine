import {Color, RgbaColorDict, colorToDict, rgbaColorFromDict} from './color';
import {ColorBreakpoint} from './color-breakpoint.model';
import {
    Colorizer as ColorizerDict,
    LinearGradient as LinearGradientDict,
    LogarithmicGradient as LogarithmicGradientDict,
    PaletteColorizer as PaletteColorizerDict,
} from '@geoengine/api-client';

export type ColorizerType = 'linearGradient' | 'logarithmicGradient' | 'palette';

export abstract class Colorizer {
    abstract readonly noDataColor: Color;

    static fromDict(dict: ColorizerDict): Colorizer {
        if (dict.type === LinearGradient.TYPE_NAME) {
            return LinearGradient.fromLinearGradientDict(dict);
        } else if (dict.type === LogarithmicGradient.TYPE_NAME) {
            return LogarithmicGradient.fromLogarithmicGradientDict(dict);
        } else if (dict.type === PaletteColorizer.TYPE_NAME) {
            return PaletteColorizer.fromPaletteDict(dict);
        }

        throw new Error('Unimplemented or invalid colorizer');
    }

    abstract getColor(value: number | undefined): Color;

    abstract getBreakpoints(): Array<ColorBreakpoint>;

    abstract equals(other: Colorizer): boolean;

    abstract clone(): Colorizer;

    abstract toDict(): ColorizerDict;

    abstract isGradient(): boolean;

    abstract isDiscrete(): boolean;

    isContinuous(): boolean {
        return !this.isDiscrete();
    }

    abstract getColorAtIndex(index: number): Color;

    abstract getNumberOfColors(): number;
}

export class LinearGradient extends Colorizer {
    static readonly TYPE_NAME = 'linearGradient';

    readonly breakpoints: Array<ColorBreakpoint>;
    readonly noDataColor: Color;
    readonly overColor: Color;
    readonly underColor: Color;

    constructor(breakpoints: Array<ColorBreakpoint>, noDataColor: Color, overColor: Color, underColor: Color) {
        super();

        this.overColor = overColor;
        this.underColor = underColor;

        this.noDataColor = noDataColor;

        this.breakpoints = breakpoints;
    }

    static fromLinearGradientDict(dict: LinearGradientDict): LinearGradient {
        return new LinearGradient(
            dict.breakpoints.map((b) => ColorBreakpoint.fromDict(b)),
            Color.fromRgbaLike(rgbaColorFromDict(dict.noDataColor)),
            Color.fromRgbaLike(rgbaColorFromDict(dict.overColor)),
            Color.fromRgbaLike(rgbaColorFromDict(dict.underColor)),
        );
    }

    getColor(value: number | undefined): Color {
        if (value === undefined || value === null) {
            return this.noDataColor;
        }

        if (!this.breakpoints.length) {
            return this.underColor; // TODO: maybe return an error?
        }

        if (value < this.breakpoints[0].value) {
            return this.underColor;
        }

        if (value > this.breakpoints[this.breakpoints.length - 1].value) {
            return this.overColor;
        }

        const reversedBreakpoints = [...this.breakpoints].reverse();
        const index = this.breakpoints.length - 1 - reversedBreakpoints.findIndex((b) => value >= b.value);
        const nextIndex = Math.min(index + 1, this.breakpoints.length - 1);

        const brk = this.breakpoints[index];
        const nextBrk = this.breakpoints[nextIndex];

        const diff = value - brk.value;
        const fracDiff = diff / (nextBrk.value - brk.value);

        return Color.interpolate(brk.color, nextBrk.color, fracDiff);
    }

    getBreakpoints(): Array<ColorBreakpoint> {
        return this.breakpoints;
    }

    equals(other: Colorizer): boolean {
        if (other instanceof LinearGradient) {
            if (this.breakpoints.length !== other.breakpoints.length) {
                return false;
            }

            for (let i = 0; i < this.breakpoints.length; i++) {
                if (this.breakpoints[i].equals(other.breakpoints[i])) {
                    return false;
                }
            }

            return (
                this.overColor.equals(other.overColor) &&
                this.underColor.equals(other.underColor) &&
                this.noDataColor.equals(other.noDataColor)
            );
        }

        return false;
    }

    clone(): Colorizer {
        return new LinearGradient(
            this.breakpoints.map((b) => b.clone()),
            this.noDataColor.clone(),
            this.overColor.clone(),
            this.underColor.clone(),
        );
    }

    cloneWith(updates: {
        readonly breakpoints?: Array<ColorBreakpoint>;
        readonly noDataColor?: Color;
        readonly overColor?: Color;
        readonly underColor?: Color;
    }): LinearGradient {
        return new LinearGradient(
            updates.breakpoints ?? this.breakpoints.map((b) => b.clone()),
            updates.noDataColor ?? this.noDataColor.clone(),
            updates.overColor ?? this.overColor.clone(),
            updates.underColor ?? this.underColor.clone(),
        );
    }

    toDict(): ColorizerDict {
        return {
            type: LinearGradient.TYPE_NAME,
            breakpoints: this.breakpoints.map((b) => b.toDict()),
            noDataColor: colorToDict(this.noDataColor),
            overColor: colorToDict(this.overColor),
            underColor: colorToDict(this.underColor),
        };
    }

    isGradient(): boolean {
        return true;
    }

    isDiscrete(): boolean {
        return false;
    }

    getColorAtIndex(index: number): Color {
        return this.breakpoints[index].color;
    }

    getNumberOfColors(): number {
        return this.breakpoints.length;
    }
}

export class LogarithmicGradient extends Colorizer {
    static readonly TYPE_NAME = 'logarithmicGradient';

    readonly breakpoints: Array<ColorBreakpoint>;
    readonly noDataColor: Color;
    readonly overColor: Color;
    readonly underColor: Color;

    constructor(breakpoints: Array<ColorBreakpoint>, noDataColor: Color, overColor: Color, underColor: Color) {
        super();

        this.overColor = overColor;

        this.underColor = underColor;

        this.noDataColor = noDataColor;
        this.breakpoints = breakpoints;
    }

    static fromLogarithmicGradientDict(dict: LogarithmicGradientDict): LogarithmicGradient {
        return new LogarithmicGradient(
            dict.breakpoints.map((b) => ColorBreakpoint.fromDict(b)),
            Color.fromRgbaLike(rgbaColorFromDict(dict.noDataColor)),
            Color.fromRgbaLike(rgbaColorFromDict(dict.overColor)),
            Color.fromRgbaLike(rgbaColorFromDict(dict.underColor)),
        );
    }

    getColor(value: number | undefined): Color {
        if (value === undefined || value === null) {
            return this.noDataColor;
        }

        if (!this.breakpoints.length) {
            return this.underColor; // TODO: maybe return an error?
        }

        if (value < this.breakpoints[0].value) {
            return this.underColor;
        }

        if (value > this.breakpoints[this.breakpoints.length - 1].value) {
            return this.overColor;
        }

        const index = this.breakpoints.findIndex((b) => value >= b.value);

        const brk = this.breakpoints[index];
        const nextBrk = this.breakpoints[Math.min(index + 1, this.breakpoints.length - 1)];

        const diff = value - brk.value;
        const fracDiff = diff / (nextBrk.value - brk.value);
        return Color.interpolate(brk.color, nextBrk.color, fracDiff);
    }

    getBreakpoints(): Array<ColorBreakpoint> {
        return this.breakpoints;
    }

    equals(other: Colorizer): boolean {
        if (!(other instanceof LogarithmicGradient)) {
            return false;
        }

        if (this.breakpoints.length !== other.breakpoints.length) {
            return false;
        }

        for (let i = 0; i < this.breakpoints.length; i++) {
            if (this.breakpoints[i].equals(other.breakpoints[i])) {
                return false;
            }
        }

        return (
            this.overColor.equals(other.overColor) && this.underColor.equals(other.underColor) && this.noDataColor.equals(other.noDataColor)
        );
    }

    clone(): Colorizer {
        return new LogarithmicGradient(
            this.breakpoints.map((b) => b.clone()),
            this.noDataColor.clone(),
            this.overColor.clone(),
            this.underColor.clone(),
        );
    }

    cloneWith(updates: {
        readonly breakpoints?: Array<ColorBreakpoint>;
        readonly noDataColor?: Color;
        readonly overColor?: Color;
        readonly underColor?: Color;
    }): LogarithmicGradient {
        return new LogarithmicGradient(
            updates.breakpoints ?? this.breakpoints.map((b) => b.clone()),
            updates.noDataColor ?? this.noDataColor.clone(),
            updates.overColor ?? this.overColor.clone(),
            updates.underColor ?? this.underColor.clone(),
        );
    }

    toDict(): ColorizerDict {
        return {
            type: LogarithmicGradient.TYPE_NAME,
            breakpoints: this.breakpoints.map((b) => b.toDict()),
            noDataColor: colorToDict(this.noDataColor),
            overColor: colorToDict(this.overColor),
            underColor: colorToDict(this.underColor),
        };
    }

    isGradient(): boolean {
        return true;
    }

    isDiscrete(): boolean {
        return false;
    }

    getColorAtIndex(index: number): Color {
        return this.breakpoints[index].color;
    }

    getNumberOfColors(): number {
        return this.breakpoints.length;
    }
}

export class PaletteColorizer extends Colorizer {
    static readonly TYPE_NAME = 'palette';

    readonly colors: Map<number, Color>;
    readonly noDataColor: Color;
    readonly defaultColor: Color;

    constructor(colors: Map<number, Color>, noDataColor: Color, defaultColor: Color) {
        super();
        this.colors = colors;
        this.noDataColor = noDataColor;
        this.defaultColor = defaultColor;
    }

    static fromPaletteDict(dict: PaletteColorizerDict): PaletteColorizer {
        const colors = new Map<number, Color>();
        for (const i of Object.keys(dict.colors)) {
            colors.set(parseInt(i, 10), Color.fromRgbaLike(rgbaColorFromDict(dict.colors[i])));
        }
        return new PaletteColorizer(
            colors,
            Color.fromRgbaLike(rgbaColorFromDict(dict.noDataColor)),
            Color.fromRgbaLike(rgbaColorFromDict(dict.defaultColor)),
        );
    }

    getColor(value: number): Color {
        const color = this.colors.get(value);
        if (color === undefined) {
            return this.defaultColor;
        }
        return color;
    }

    getBreakpoints(): Array<ColorBreakpoint> {
        const breakpoints: Array<ColorBreakpoint> = [];
        this.colors.forEach((value, key) => {
            breakpoints.push(new ColorBreakpoint(key, value));
        });
        return breakpoints;
    }

    equals(other: Colorizer): boolean {
        if (other instanceof PaletteColorizer) {
            if (this.colors.size !== other.colors.size) {
                return false;
            }

            for (const [i, color] of this.colors.entries()) {
                if (color !== other.colors.get(i)) {
                    return false;
                }
            }

            return this.noDataColor.equals(other.noDataColor) && this.defaultColor.equals(other.defaultColor);
        }
        return false;
    }

    clone(): Colorizer {
        const colors = new Map();
        for (const i of this.colors.keys()) {
            colors.set(i, this.colors.get(i));
        }

        return new PaletteColorizer(colors, this.noDataColor.clone(), this.defaultColor.clone());
    }

    cloneWith(updates: {
        readonly colors?: Map<number, Color>;
        readonly noDataColor?: Color;
        readonly defaultColor?: Color;
    }): PaletteColorizer {
        let colors;
        if (updates.colors) {
            colors = updates.colors;
        } else {
            colors = new Map();
            for (const [i, color] of this.colors.entries()) {
                colors.set(i, color);
            }
        }

        return new PaletteColorizer(
            colors,
            updates.noDataColor ?? this.noDataColor.clone(),
            updates.defaultColor ?? this.defaultColor.clone(),
        );
    }

    toDict(): ColorizerDict {
        const colors: Record<string, RgbaColorDict> = {};

        for (const [i, color] of this.colors.entries()) {
            colors[i] = colorToDict(color);
        }

        return {
            type: PaletteColorizer.TYPE_NAME,
            colors,
            noDataColor: colorToDict(this.noDataColor),
            defaultColor: colorToDict(this.defaultColor),
        };
    }

    isGradient(): boolean {
        return false;
    }

    isDiscrete(): boolean {
        return true;
    }

    getColorAtIndex(index: number): Color {
        const key = Array.from(this.colors.keys())[index];
        const color = this.colors.get(key);

        if (!color) {
            throw Error(`index ${index} does not exist on Map`);
        }

        return color;
    }

    getNumberOfColors(): number {
        return this.colors.size;
    }
}

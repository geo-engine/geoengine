import {Color, RgbaTuple, WHITE, BLACK, rgbaColorFromDict, colorToDict, TRANSPARENT} from '../colors/color';
import {Feature as OlFeature} from 'ol';
import {Circle as OlStyleCircle, Fill as OlStyleFill, Stroke as OlStyleStroke, Style as OlStyle, Text as OlStyleText} from 'ol/style';
import {StyleFunction as OlStyleFunction} from 'ol/style/Style';
import {Colorizer, LinearGradient, LogarithmicGradient, PaletteColorizer} from '../colors/colorizer.model';
import {FeatureLike} from 'ol/Feature';
import OlGeometry from 'ol/geom/Geometry';
import {ColorBreakpoint} from '../colors/color-breakpoint.model';
import {
    Symbology as SymbologyDict,
    PointSymbology as PointSymbologyDict,
    LineSymbology as LineSymbologyDict,
    PolygonSymbology as PolygonSymbologyDict,
    NumberParam as NumberParamDict,
    ColorParam as ColorParamDict,
    DerivedColor as DerivedColorDict,
    StrokeParam as StrokeParamDict,
    TextSymbology as TextSymbologyDict,
    DerivedNumber as DerivedNumberDict,
    RasterSymbology as RasterSymbologyDict,
    RasterColorizer as RasterColorizerDict,
    MultiBandRasterColorizer as MultiBandRasterColorizerDict,
} from '@geoengine/api-client';
import {PointIconStyle} from '../layer-icons/point-icon/point-icon.component';
import {LineIconStyle} from '../layer-icons/line-icon/line-icon.component';
import {PolygonIconStyle} from '../layer-icons/polygon-icon/polygon-icon.component';
import {Time} from '../time/time.model';
import {BoundingBox2D} from '../spatial-bounds/bounding-box';
import {SpatialReference} from '../spatial-references/spatial-reference.model';

/**
 * List of the symbology types used in geoengine
 */
export enum SymbologyType {
    RASTER,
    POINT,
    LINE,
    POLYGON,
}

// List of constants used by layer symbology.
export const DEFAULT_VECTOR_STROKE_COLOR: Color = Color.fromRgbaLike([0, 0, 0, 1]);
export const DEFAULT_VECTOR_FILL_COLOR: Color = Color.fromRgbaLike([255, 0, 0, 1]);
export const DEFAULT_VECTOR_HIGHLIGHT_STROKE_COLOR: Color = Color.fromRgbaLike([255, 255, 255, 1]);
export const DEFAULT_VECTOR_HIGHLIGHT_FILL_COLOR: Color = Color.fromRgbaLike([0, 153, 255, 1]);
export const DEFAULT_VECTOR_HIGHLIGHT_TEXT_COLOR: Color = Color.fromRgbaLike([255, 255, 255, 1]);
export const DEFAULT_POINT_RADIUS = 8;
export const DEFAULT_POINT_CLUSTER_RADIUS_ATTRIBUTE = '___radius';
export const DEFAULT_POINT_CLUSTER_TEXT_ATTRIBUTE = '___numberOfPoints';
export const MIN_ALLOWED_POINT_RADIUS = 1;
export const MAX_ALLOWED_POINT_RADIUS = 100;
export const MAX_ALLOWED_TEXT_LENGTH = 25;

// export type StrokeDashStyle = Array<number>;

const STYLE_CACHE: Record<string, OlStyle> = {};

export type VectorSymbologyDict = PointSymbologyDict | LineSymbologyDict | PolygonSymbologyDict;

export interface SymbologyWorkflow<SymbologyType> {
    symbology: SymbologyType;
    workflowId: string;
}

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface IconStyle {}

export abstract class Symbology {
    static fromDict(dict: SymbologyDict): Symbology {
        if (dict.type === 'raster') {
            return RasterSymbology.fromRasterSymbologyDict(dict);
        } else if (dict.type === 'point') {
            return PointSymbology.fromPointSymbologyDict(dict);
        } else if (dict.type === 'line') {
            return LineSymbology.fromLineSymbologyDict(dict);
        } else if (dict.type === 'polygon') {
            return PolygonSymbology.fromPolygonSymbologyDict(dict);
        }

        throw new Error('Invalid Symbology type.');
    }

    abstract toDict(): SymbologyDict;

    abstract clone(): Symbology;

    abstract equals(other: Symbology): boolean;

    abstract getSymbologyType(): SymbologyType;

    abstract getIconStyle(): IconStyle;

    get iconStyle(): IconStyle {
        return this.getIconStyle();
    }

    get symbologyType(): SymbologyType {
        return this.getSymbologyType();
    }
}

export abstract class VectorSymbology extends Symbology {
    static fromVectorSymbologyDict(dict: VectorSymbologyDict): VectorSymbology {
        if (dict.type === 'point') {
            if (dict.radius.type === 'derived' && dict.radius.attribute === ClusteredPointSymbology.RADIUS_COLUMN) {
                return ClusteredPointSymbology.fromPointSymbologyDict(dict);
            } else {
                return PointSymbology.fromPointSymbologyDict(dict);
            }
        } else if (dict.type === 'line') {
            return LineSymbology.fromLineSymbologyDict(dict);
        } else if (dict.type === 'polygon') {
            return PolygonSymbology.fromPolygonSymbologyDict(dict);
        }
        throw new Error('Invalid Vector Symbology type.');
    }

    createStyleFunction(): OlStyleFunction {
        return (feature: FeatureLike, _resolution: number): OlStyle => {
            const styler = this.createStyler(feature as unknown as OlFeature<OlGeometry>);

            const key = styler.cacheKey();
            if (key in STYLE_CACHE) {
                return STYLE_CACHE[key];
            } else {
                const style = styler.createStyle();
                STYLE_CACHE[key] = style;
                return style;
            }
        };
    }

    createStyle(feature: OlFeature<OlGeometry>): OlStyle {
        return this.createStyler(feature).createStyle();
    }

    createHighlightStyle(feature: OlFeature<OlGeometry>): OlStyle {
        return this.createStyler(feature).createHighlightStyle();
    }

    abstract override clone(): VectorSymbology;

    protected abstract createStyler(feature: OlFeature<OlGeometry>): Styler;
}

export abstract class Styler {
    abstract createStyle(): OlStyle;
    abstract createHighlightStyle(): OlStyle;
    abstract cacheKey(): string;

    static invertColor(color: RgbaTuple): RgbaTuple {
        return [255 - color[0], 255 - color[1], 255 - color[2], color[3]];
    }

    static colorToKey(color: RgbaTuple): string {
        return `${color[0]}${color[1]}${color[2]}${color[3]}`;
    }
}

export class PointStyler extends Styler {
    radius: number;
    fillColor: RgbaTuple;
    stroke: StrokeStyler;
    text?: TextStyler;

    constructor(radius: number, fillColor: RgbaTuple, stroke: StrokeStyler, text?: TextStyler) {
        super();
        this.radius = radius;
        this.fillColor = fillColor;
        this.stroke = stroke;
        this.text = text;
    }

    createStyle(): OlStyle {
        const imageStyle = new OlStyleCircle({
            radius: this.radius,
            fill: new OlStyleFill({color: this.fillColor}),
            stroke: this.stroke.createStyle() as unknown as OlStyleStroke,
        });

        return new OlStyle({
            image: imageStyle,
            text: this.text ? (this.text.createStyle() as unknown as OlStyleText) : undefined,
        });
    }

    createHighlightStyle(): OlStyle {
        const imageStyle = new OlStyleCircle({
            radius: this.radius,
            fill: new OlStyleFill({color: Styler.invertColor(this.fillColor)}),
            stroke: this.stroke.createHighlightStyle() as unknown as OlStyleStroke,
        });

        return new OlStyle({
            image: imageStyle,
            text: this.text ? (this.text.createHighlightStyle() as unknown as OlStyleText) : undefined,
        });
    }

    cacheKey(): string {
        return `${this.radius}${Styler.colorToKey(this.fillColor)}${this.stroke.cacheKey()}${this.text ? this.text.cacheKey() : ''}`;
    }
}

export class LineStyler extends Styler {
    stroke: StrokeStyler;
    text?: TextStyler;

    constructor(stroke: StrokeStyler, text?: TextStyler) {
        super();
        this.stroke = stroke;
        this.text = text;
    }

    createStyle(): OlStyle {
        return new OlStyle({
            stroke: this.stroke.createStyle() as unknown as OlStyleStroke,
            text: this.text ? (this.text.createStyle() as unknown as OlStyleText) : undefined,
        });
    }

    createHighlightStyle(): OlStyle {
        return new OlStyle({
            stroke: this.stroke.createHighlightStyle() as unknown as OlStyleStroke,
            text: this.text ? (this.text.createHighlightStyle() as unknown as OlStyleText) : undefined,
        });
    }

    cacheKey(): string {
        return `${this.stroke.cacheKey()}${this.text ? this.text.cacheKey() : ''}`;
    }
}

export class PolygonStyler extends Styler {
    fillColor: RgbaTuple;
    stroke: StrokeStyler;
    text?: TextStyler;

    constructor(fillColor: RgbaTuple, stroke: StrokeStyler, text?: TextStyler) {
        super();
        this.fillColor = fillColor;
        this.stroke = stroke;
        this.text = text;
    }

    createStyle(): OlStyle {
        return new OlStyle({
            fill: new OlStyleFill({color: this.fillColor}),
            stroke: this.stroke.createStyle() as unknown as OlStyleStroke,
            text: this.text ? (this.text.createStyle() as unknown as OlStyleText) : undefined,
        });
    }

    createHighlightStyle(): OlStyle {
        return new OlStyle({
            fill: new OlStyleFill({color: Styler.invertColor(this.fillColor)}),
            stroke: this.stroke.createHighlightStyle() as unknown as OlStyleStroke,
            text: this.text ? (this.text.createHighlightStyle() as unknown as OlStyleText) : undefined,
        });
    }

    cacheKey(): string {
        return `${Styler.colorToKey(this.fillColor)}${this.stroke.cacheKey()}${this.text ? this.text.cacheKey() : ''}`;
    }
}

export class StrokeStyler extends Styler {
    width: number;
    color: RgbaTuple;

    constructor(width: number, color: RgbaTuple) {
        super();
        this.width = width;
        this.color = color;
    }

    createStyle(): OlStyle {
        return new OlStyleStroke({
            color: this.color,
            width: this.width,
        }) as unknown as OlStyle;
    }

    createHighlightStyle(): OlStyle {
        return new OlStyleStroke({
            color: Styler.invertColor(this.color),
            width: this.width,
        }) as unknown as OlStyle;
    }

    cacheKey(): string {
        return `${this.width}${Styler.colorToKey(this.color)}`;
    }
}

export class TextStyler extends Styler {
    text: string;
    fillColor: RgbaTuple;
    stroke: StrokeStyler;

    constructor(text: string, fillColor: RgbaTuple, stroke: StrokeStyler) {
        super();
        this.text = text;
        this.fillColor = fillColor;
        this.stroke = stroke;
    }

    createStyle(): OlStyle {
        return new OlStyleText({
            text: this.text.slice(0, MAX_ALLOWED_TEXT_LENGTH),
            fill: new OlStyleFill({
                color: this.fillColor,
            }),
            stroke: this.stroke.createStyle() as unknown as OlStyleStroke,
        }) as unknown as OlStyle;
    }

    createHighlightStyle(): OlStyle {
        return new OlStyleText({
            text: this.text.slice(0, MAX_ALLOWED_TEXT_LENGTH),
            fill: new OlStyleFill({
                color: Styler.invertColor(this.fillColor),
            }),
            stroke: this.stroke.createHighlightStyle() as unknown as OlStyleStroke,
        }) as unknown as OlStyle;
    }

    cacheKey(): string {
        return `${this.text}${Styler.colorToKey(this.fillColor)}${this.stroke.cacheKey()}`;
    }
}

export class PointSymbology extends VectorSymbology {
    static readonly DEFAULT_POINT_RADIUS = 10;

    // TODO: visiblity
    radius: NumberParam;
    fillColor: ColorParam;
    stroke: Stroke;

    text?: TextSymbology;

    constructor(radius: NumberParam, fillColor: ColorParam, stroke: Stroke, text?: TextSymbology) {
        super();
        this.radius = radius;
        this.fillColor = fillColor;
        this.stroke = stroke;
        this.text = text;
    }

    static fromPointSymbologyDict(dict: PointSymbologyDict): PointSymbology {
        return new PointSymbology(
            NumberParam.fromDict(dict.radius),
            ColorParam.fromDict(dict.fillColor),
            Stroke.fromDict(dict.stroke),
            dict.text ? TextSymbology.fromDict(dict.text) : undefined,
        );
    }

    createStyler(feature: OlFeature<OlGeometry>): PointStyler {
        return new PointStyler(
            this.radius.getNumber(feature),
            this.fillColor.getColor(feature).rgbaTuple(),
            this.stroke.createStyler(feature),
            this.text ? (this.text.createStyler(feature) as unknown as TextStyler) : undefined,
        );
    }

    equals(other: PointSymbology): boolean {
        return (
            other instanceof PointSymbology &&
            this.radius.equals(other.radius) &&
            this.fillColor.equals(other.fillColor) &&
            this.stroke.equals(other.stroke) &&
            textSymbologyEquality(this.text, other.text)
        );
    }

    clone(): PointSymbology {
        return new PointSymbology(
            this.radius.clone(),
            this.fillColor.clone(),
            this.stroke.clone(),
            this.text ? this.text.clone() : undefined,
        );
    }

    toDict(): SymbologyDict {
        return {
            type: 'point',
            radius: this.radius.toDict(),
            fillColor: this.fillColor.toDict(),
            stroke: this.stroke.toDict(),
            text: this.text ? this.text.toDict() : undefined,
        };
    }

    getSymbologyType(): SymbologyType {
        return SymbologyType.POINT;
    }

    getIconStyle(): PointIconStyle {
        return {
            strokeWidth: this.stroke.width.getDefault(),
            // strokeDashStyle: StrokeDashStyle;
            strokeRGBA: this.stroke.color.getDefault(),
            fillRGBA: this.fillColor.getDefault(),
        };
    }
}

export class ClusteredPointSymbology extends VectorSymbology {
    static readonly RADIUS_COLUMN = '___radius';
    static readonly COUNT_COLUMN = '___count';
    static readonly DELTA_PX = 1;

    // TODO: visiblity

    readonly fillColor: ColorParam;
    readonly stroke: Stroke;

    readonly radius = new DerivedNumber(ClusteredPointSymbology.RADIUS_COLUMN, 1, PointSymbology.DEFAULT_POINT_RADIUS);
    readonly text = new TextSymbology(
        ClusteredPointSymbology.COUNT_COLUMN,
        new StaticColor(WHITE),
        new Stroke(new StaticNumber(1), new StaticColor(BLACK)),
    );

    constructor(fillColor: ColorParam, stroke: Stroke) {
        super();
        this.fillColor = fillColor;
        this.stroke = stroke;
    }

    static fromPointSymbologyDict(dict: PointSymbologyDict): ClusteredPointSymbology {
        // we ignore anything that is stored for radius and text
        return new ClusteredPointSymbology(ColorParam.fromDict(dict.fillColor), Stroke.fromDict(dict.stroke));
    }

    createStyler(feature: OlFeature<OlGeometry>): PointStyler {
        return new PointStyler(
            this.radius.getNumber(feature),
            this.fillColor.getColor(feature).rgbaTuple(),
            this.stroke.createStyler(feature),
            this.text.createStyler(feature) as unknown as TextStyler,
        );
    }

    equals(other: ClusteredPointSymbology): boolean {
        return other instanceof ClusteredPointSymbology && this.fillColor.equals(other.fillColor) && this.stroke.equals(other.stroke);
    }

    clone(): ClusteredPointSymbology {
        return new ClusteredPointSymbology(this.fillColor.clone(), this.stroke.clone());
    }

    toDict(): SymbologyDict {
        return {
            type: 'point',
            radius: this.radius.toDict(),
            fillColor: this.fillColor.toDict(),
            stroke: this.stroke.toDict(),
            text: this.text ? this.text.toDict() : undefined,
        };
    }

    getSymbologyType(): SymbologyType {
        return SymbologyType.POINT;
    }

    getIconStyle(): PointIconStyle {
        return {
            strokeWidth: this.stroke.width.getDefault(),
            // strokeDashStyle: StrokeDashStyle;
            strokeRGBA: this.stroke.color.getDefault(),
            fillRGBA: this.fillColor.getDefault(),
        };
    }
}

export class LineSymbology extends VectorSymbology {
    stroke: Stroke;
    text?: TextSymbology;
    autoSimplified: boolean;

    constructor(stroke: Stroke, text?: TextSymbology, autoSimplified = true) {
        super();
        this.stroke = stroke;
        this.text = text;
        this.autoSimplified = autoSimplified;
    }

    static fromLineSymbologyDict(dict: LineSymbologyDict): LineSymbology {
        return new LineSymbology(Stroke.fromDict(dict.stroke), dict.text ? TextSymbology.fromDict(dict.text) : undefined);
    }

    createStyler(feature: OlFeature<OlGeometry>): Styler {
        return new LineStyler(
            this.stroke.createStyler(feature),
            this.text ? (this.text.createStyler(feature) as unknown as TextStyler) : undefined,
        );
    }

    equals(other: LineSymbology): boolean {
        return other instanceof LineSymbology && this.stroke.equals(other.stroke) && textSymbologyEquality(this.text, other.text);
    }

    clone(): LineSymbology {
        return new LineSymbology(this.stroke.clone(), this.text ? this.text.clone() : undefined);
    }

    toDict(): SymbologyDict {
        return {
            type: 'line',
            stroke: this.stroke.toDict(),
            text: this.text ? this.text.toDict() : undefined,
            autoSimplified: this.autoSimplified,
        };
    }

    getSymbologyType(): SymbologyType {
        return SymbologyType.LINE;
    }

    getIconStyle(): LineIconStyle {
        return {
            strokeWidth: this.stroke.width.getDefault(),
            // strokeDashStyle: StrokeDashStyle;
            strokeRGBA: this.stroke.color.getDefault(),
        };
    }
}

export class PolygonSymbology extends VectorSymbology {
    fillColor: ColorParam;
    stroke: Stroke;

    text?: TextSymbology;

    autoSimplified: boolean;

    constructor(fillColor: ColorParam, stroke: Stroke, text?: TextSymbology, autoSimplified = true) {
        super();
        this.fillColor = fillColor;
        this.stroke = stroke;
        this.text = text;

        this.autoSimplified = autoSimplified;
    }

    static fromPolygonSymbologyDict(dict: PolygonSymbologyDict): PolygonSymbology {
        return new PolygonSymbology(
            ColorParam.fromDict(dict.fillColor),
            Stroke.fromDict(dict.stroke),
            dict.text ? TextSymbology.fromDict(dict.text) : undefined,
        );
    }

    createStyler(feature: OlFeature<OlGeometry>): Styler {
        return new PolygonStyler(
            this.fillColor.getColor(feature).rgbaTuple(),
            this.stroke.createStyler(feature),
            this.text ? (this.text.createStyler(feature) as unknown as TextStyler) : undefined,
        );
    }

    equals(other: PolygonSymbology): boolean {
        return (
            other instanceof PolygonSymbology &&
            this.fillColor.equals(other.fillColor) &&
            this.stroke.equals(other.stroke) &&
            textSymbologyEquality(this.text, other.text)
        );
    }

    clone(): PolygonSymbology {
        return new PolygonSymbology(this.fillColor.clone(), this.stroke.clone(), this.text ? this.text.clone() : undefined);
    }

    toDict(): SymbologyDict {
        return {
            type: 'polygon',
            fillColor: this.fillColor.toDict(),
            stroke: this.stroke.toDict(),
            text: this.text ? this.text.toDict() : undefined,
            autoSimplified: this.autoSimplified,
        };
    }

    getSymbologyType(): SymbologyType {
        return SymbologyType.POLYGON;
    }

    getIconStyle(): PolygonIconStyle {
        return {
            strokeWidth: this.stroke.width.getDefault(),
            // strokeDashStyle: StrokeDashStyle;
            strokeRGBA: this.stroke.color.getDefault(),
            fillRGBA: this.fillColor.getDefault(),
        };
    }
}

export class RasterSymbology extends Symbology {
    readonly opacity: number;
    readonly rasterColorizer: RasterColorizer;

    constructor(opacity: number, colorizer: RasterColorizer) {
        super();
        this.opacity = opacity;
        this.rasterColorizer = colorizer;
    }

    static fromRasterSymbologyDict(dict: RasterSymbologyDict): RasterSymbology {
        return new RasterSymbology(dict.opacity, RasterColorizer.fromDict(dict.rasterColorizer));
    }

    equals(other: RasterSymbology): boolean {
        return (
            other instanceof RasterSymbology && this.opacity === other.opacity && this.rasterColorizer.equals(other.rasterColorizer) //&&
        );
    }

    clone(): RasterSymbology {
        return new RasterSymbology(this.opacity, this.rasterColorizer.clone());
    }

    cloneWith(updates: {readonly opacity?: number; readonly colorizer?: RasterColorizer}): RasterSymbology {
        return new RasterSymbology(updates.opacity ?? this.opacity, updates.colorizer ?? this.rasterColorizer.clone());
    }

    toDict(): SymbologyDict {
        return {
            type: 'raster',
            opacity: this.opacity,
            rasterColorizer: this.rasterColorizer.toDict(),
        };
    }

    getSymbologyType(): SymbologyType {
        return SymbologyType.RASTER;
    }

    getIconStyle(): IconStyle {
        throw new Error('Raster has custom icon renderer.');
    }

    getBreakpoints(): Array<ColorBreakpoint> {
        return this.rasterColorizer.getBreakpoints();
    }
}

export abstract class RasterColorizer {
    static fromDict(dict: RasterColorizerDict): RasterColorizer {
        if (dict.type === 'singleBand') {
            return new SingleBandRasterColorizer(dict.band, Colorizer.fromDict(dict.bandColorizer));
        }
        if (dict.type === 'multiBand') {
            return new MultiBandRasterColorizer(
                dict.redBand,
                dict.greenBand,
                dict.blueBand,
                dict.redMin,
                dict.redMax,
                dict.redScale ?? 1,
                dict.greenMin,
                dict.greenMax,
                dict.greenScale ?? 1,
                dict.blueMin,
                dict.blueMax,
                dict.blueScale ?? 1,
                dict.noDataColor ? Color.fromRgbaLike(rgbaColorFromDict(dict.noDataColor)) : TRANSPARENT,
            );
        }
        throw new Error('unable to deserialize `RasterColorizer`');
    }

    abstract equals(other: RasterColorizer): boolean;

    abstract clone(): RasterColorizer;

    abstract toDict(): RasterColorizerDict;

    abstract getBreakpoints(): Array<ColorBreakpoint>;

    abstract isDiscrete(): boolean;

    isContinuous(): boolean {
        return !this.isDiscrete();
    }

    abstract getNumberOfColors(): number;

    abstract isGradient(): boolean;

    abstract getColorAtIndex(index: number): Color;
}

export class SingleBandRasterColorizer extends RasterColorizer {
    readonly band: number;
    readonly bandColorizer: Colorizer;

    constructor(band: number, colorizer: Colorizer) {
        super();
        this.band = band;
        this.bandColorizer = colorizer;
    }

    override equals(other: RasterColorizer): boolean {
        if (other instanceof SingleBandRasterColorizer) {
            return this.band === other.band && this.bandColorizer.equals(other.bandColorizer);
        }
        return false;
    }
    override clone(): RasterColorizer {
        return new SingleBandRasterColorizer(this.band, this.bandColorizer.clone());
    }
    override toDict(): RasterColorizerDict {
        return {
            type: 'singleBand',
            band: this.band,
            bandColorizer: this.bandColorizer.toDict(),
        };
    }

    getBreakpoints(): Array<ColorBreakpoint> {
        return this.bandColorizer.getBreakpoints();
    }

    override isDiscrete(): boolean {
        return this.bandColorizer.isDiscrete();
    }

    getNumberOfColors(): number {
        return this.bandColorizer.getNumberOfColors();
    }

    override isGradient(): boolean {
        return this.bandColorizer.isGradient();
    }

    override getColorAtIndex(index: number): Color {
        return this.bandColorizer.getColorAtIndex(index);
    }

    replaceBand(band: number): SingleBandRasterColorizer {
        return new SingleBandRasterColorizer(band, this.bandColorizer.clone());
    }
}

export class MultiBandRasterColorizer extends RasterColorizer {
    readonly redBand: number;
    readonly greenBand: number;
    readonly blueBand: number;
    readonly redMin: number;
    readonly redMax: number;
    readonly redScale: number;
    readonly greenMin: number;
    readonly greenMax: number;
    readonly greenScale: number;
    readonly blueMin: number;
    readonly blueMax: number;
    readonly blueScale: number;
    readonly noDataColor: Color;

    constructor(
        redBand: number,
        greenBand: number,
        blueBand: number,
        redMin: number,
        redMax: number,
        redScale: number,
        greenMin: number,
        greenMax: number,
        greenScale: number,
        blueMin: number,
        blueMax: number,
        blueScale: number,
        noDataColor: Color,
    ) {
        super();
        this.redBand = redBand;
        this.greenBand = greenBand;
        this.blueBand = blueBand;
        this.redMin = redMin;
        this.redMax = redMax;
        this.redScale = redScale;
        this.greenMin = greenMin;
        this.greenMax = greenMax;
        this.greenScale = greenScale;
        this.blueMin = blueMin;
        this.blueMax = blueMax;
        this.blueScale = blueScale;
        this.noDataColor = noDataColor;
    }

    override equals(other: RasterColorizer): boolean {
        if (!(other instanceof MultiBandRasterColorizer)) {
            return false;
        }

        return (
            this.redBand === other.redBand &&
            this.greenBand === other.greenBand &&
            this.blueBand === other.blueBand &&
            this.redMin === other.redMin &&
            this.redMax === other.redMax &&
            this.redScale === other.redScale &&
            this.greenMin === other.greenMin &&
            this.greenMax === other.greenMax &&
            this.greenScale === other.greenScale &&
            this.blueMin === other.blueMin &&
            this.blueMax === other.blueMax &&
            this.blueScale === other.blueScale &&
            this.noDataColor.equals(other.noDataColor)
        );
    }
    override clone(): MultiBandRasterColorizer {
        return new MultiBandRasterColorizer(
            this.redBand,
            this.greenBand,
            this.blueBand,
            this.redMin,
            this.redMax,
            this.redScale,
            this.greenMin,
            this.greenMax,
            this.greenScale,
            this.blueMin,
            this.blueMax,
            this.blueScale,
            this.noDataColor,
        );
    }

    withBands(redBand: number, greenBand: number, blueBand: number): MultiBandRasterColorizer {
        return new MultiBandRasterColorizer(
            redBand,
            greenBand,
            blueBand,
            this.redMin,
            this.redMax,
            this.redScale,
            this.greenMin,
            this.greenMax,
            this.greenScale,
            this.blueMin,
            this.blueMax,
            this.blueScale,
            this.noDataColor,
        );
    }

    withParams(
        redMin: number,
        redMax: number,
        redScale: number,
        greenMin: number,
        greenMax: number,
        greenScale: number,
        blueMin: number,
        blueMax: number,
        blueScale: number,
        noDataColor: Color,
    ): MultiBandRasterColorizer {
        return new MultiBandRasterColorizer(
            this.redBand,
            this.greenBand,
            this.blueBand,
            redMin,
            redMax,
            redScale,
            greenMin,
            greenMax,
            greenScale,
            blueMin,
            blueMax,
            blueScale,
            noDataColor,
        );
    }

    override toDict(): MultiBandRasterColorizerDict {
        return {
            type: 'multiBand',
            redBand: this.redBand,
            greenBand: this.greenBand,
            blueBand: this.blueBand,
            redMin: this.redMin,
            redMax: this.redMax,
            redScale: this.redScale,
            greenMin: this.greenMin,
            greenMax: this.greenMax,
            greenScale: this.greenScale,
            blueMin: this.blueMin,
            blueMax: this.blueMax,
            blueScale: this.blueScale,
            noDataColor: colorToDict(this.noDataColor),
        };
    }

    override getBreakpoints(): Array<ColorBreakpoint> {
        return [];
    }

    override isDiscrete(): boolean {
        return false;
    }

    override getNumberOfColors(): number {
        return 0;
    }

    override isGradient(): boolean {
        return false;
    }

    override getColorAtIndex(_index: number): Color {
        return TRANSPARENT;
    }
}

export abstract class ColorParam {
    static fromDict(dict: ColorParamDict): ColorParam {
        if (dict.type === 'static') {
            return new StaticColor(Color.fromRgbaLike(rgbaColorFromDict(dict.color)));
        } else if (dict.type === 'derived') {
            return DerivedColor.fromDerivedColorDict(dict);
        } else {
            throw new Error('unable to deserialize `NumberParam`');
        }
    }

    abstract equals(other: ColorParam): boolean;

    abstract clone(): ColorParam;

    abstract toDict(): ColorParamDict;

    abstract getColor(feature: OlFeature<OlGeometry>): Color;

    abstract getDefault(): Color;
}

export abstract class NumberParam {
    static fromDict(dict: NumberParamDict): NumberParam {
        if (dict.type === 'static') {
            return new StaticNumber(dict.value);
        } else if (dict.type === 'derived') {
            return DerivedNumber.fromDerivedNumberDict(dict);
        } else {
            throw new Error('unable to deserialize `NumberParam`');
        }
    }

    abstract equals(other: NumberParam): boolean;

    abstract clone(): NumberParam;

    abstract toDict(): NumberParamDict;

    abstract getNumber(feature: OlFeature<OlGeometry>): number;

    abstract getDefault(): number;
}

export class StaticColor extends ColorParam {
    color: Color;

    constructor(color: Color) {
        super();
        this.color = color;
    }

    getColor(_feature: OlFeature<OlGeometry>): Color {
        return this.color;
    }

    equals(other: ColorParam): boolean {
        if (other instanceof StaticColor) {
            return this.color.equals(other.color);
        }
        return false;
    }

    clone(): ColorParam {
        return new StaticColor(this.color.clone());
    }

    toDict(): ColorParamDict {
        return {
            type: 'static',
            color: colorToDict(this.color),
        };
    }

    getDefault(): Color {
        return this.color;
    }
}

export class StaticNumber extends NumberParam {
    num: number;

    constructor(num: number) {
        super();
        this.num = num;
    }

    getNumber(_feature: OlFeature<OlGeometry>): number {
        return this.num;
    }

    equals(other: NumberParam): boolean {
        if (other instanceof StaticNumber) {
            return this.num === other.num;
        }
        return false;
    }

    clone(): NumberParam {
        return new StaticNumber(this.num);
    }

    toDict(): NumberParamDict {
        return {
            type: 'static',
            value: this.num,
        };
    }

    getDefault(): number {
        return this.num;
    }
}

export class DerivedColor implements ColorParam {
    attribute: string;
    colorizer: Colorizer;

    constructor(attribute: string, colorizer: Colorizer) {
        this.attribute = attribute;
        this.colorizer = colorizer;
    }

    static fromDerivedColorDict(dict: DerivedColorDict): DerivedColor {
        return new DerivedColor(dict.attribute, Colorizer.fromDict(dict.colorizer));
    }

    getColor(feature: OlFeature<OlGeometry>): Color {
        return this.colorizer.getColor(feature.get(this.attribute));
    }

    equals(other: ColorParam): boolean {
        if (other instanceof DerivedColor) {
            return this.attribute === other.attribute && this.colorizer.equals(other.colorizer);
        }
        return false;
    }

    clone(): ColorParam {
        return new DerivedColor(this.attribute, this.colorizer.clone());
    }

    toDict(): ColorParamDict {
        return {
            type: 'derived',
            attribute: this.attribute,
            colorizer: this.colorizer.toDict(),
        };
    }

    getDefault(): Color {
        if (this.colorizer instanceof LinearGradient || this.colorizer instanceof LogarithmicGradient) {
            // TODO: refactor over/under color
            return this.colorizer.underColor;
        } else if (this.colorizer instanceof PaletteColorizer) {
            return this.colorizer.defaultColor;
        } else {
            throw Error('unknown colorizer type');
        }
    }
}

export class DerivedNumber extends NumberParam {
    attribute: string;
    factor: number;
    defaultValue: number;

    constructor(attribute: string, factor: number, defaultValue: number) {
        super();
        this.attribute = attribute;
        this.factor = factor;
        this.defaultValue = defaultValue;
    }

    static fromDerivedNumberDict(dict: DerivedNumberDict): NumberParam {
        return new DerivedNumber(dict.attribute, dict.factor, dict.defaultValue);
    }

    getNumber(feature: OlFeature<OlGeometry>): number {
        const value = feature.get(this.attribute) * this.factor;
        // ensure to only have values >= 0
        return Math.max(value, 0);
    }

    equals(other: NumberParam): boolean {
        if (other instanceof DerivedNumber) {
            return this.attribute === other.attribute && this.factor === other.factor;
        }
        return false;
    }

    clone(): NumberParam {
        return new DerivedNumber(this.attribute, this.factor, this.defaultValue);
    }

    toDict(): NumberParamDict {
        return {
            type: 'derived',
            attribute: this.attribute,
            factor: this.factor,
            defaultValue: this.defaultValue,
        };
    }

    getDefault(): number {
        return this.defaultValue;
    }
}

export class Stroke {
    width: NumberParam;
    color: ColorParam;
    // TODO: dash

    constructor(width: NumberParam, color: ColorParam) {
        this.width = width;
        this.color = color;
    }

    static fromDict(dict: StrokeParamDict): Stroke {
        return new Stroke(NumberParam.fromDict(dict.width), ColorParam.fromDict(dict.color));
    }

    createStyle(feature: OlFeature<OlGeometry>): OlStyleStroke {
        return new OlStyleStroke({
            color: this.color.getColor(feature).rgbTuple(),
            width: this.width.getNumber(feature),
        });
    }

    equals(other: Stroke): boolean {
        return this.width.equals(other.width) && this.color.equals(other.color);
    }

    clone(): Stroke {
        return new Stroke(this.width.clone(), this.color.clone());
    }

    toDict(): StrokeParamDict {
        return {
            width: this.width.toDict(),
            color: this.color.toDict(),
        };
    }

    createStyler(feature: OlFeature<OlGeometry>): StrokeStyler {
        return new StrokeStyler(this.width.getNumber(feature), this.color.getColor(feature).rgbaTuple());
    }
}

export class TextSymbology {
    attribute: string;
    fillColor: ColorParam;
    stroke: Stroke;

    constructor(attribute: string, fillColor: ColorParam, stroke: Stroke) {
        this.attribute = attribute;
        this.fillColor = fillColor;
        this.stroke = stroke;
    }

    static fromDict(dict: TextSymbologyDict): TextSymbology {
        if (dict == null || dict === undefined) {
            throw Error('unable to deserialize `TextSymbology`');
        }

        return new TextSymbology(dict.attribute, ColorParam.fromDict(dict.fillColor), Stroke.fromDict(dict.stroke));
    }

    createStyler(feature: OlFeature<OlGeometry>): OlStyleText {
        const featureAttributeValue = feature.get(this.attribute);
        let featureAttributeString: string;
        if (featureAttributeValue === null || featureAttributeValue === undefined) {
            featureAttributeString = '';
        } else {
            featureAttributeString = featureAttributeValue.toString();
        }

        const textStyler = new TextStyler(
            featureAttributeString,
            this.fillColor.getColor(feature).rgbaTuple(),
            this.stroke.createStyler(feature),
        );
        return textStyler as unknown as OlStyleText;
    }

    equals(other: TextSymbology): boolean {
        return this.attribute === other.attribute && this.fillColor.equals(other.fillColor) && this.stroke.equals(other.stroke);
    }

    clone(): TextSymbology {
        return new TextSymbology(this.attribute, this.fillColor.clone(), this.stroke.clone());
    }

    toDict(): TextSymbologyDict {
        return {
            attribute: this.attribute,
            fillColor: this.fillColor.toDict(),
            stroke: this.stroke.toDict(),
        };
    }
}

function textSymbologyEquality(a?: TextSymbology, b?: TextSymbology): boolean {
    if (!a || !b) {
        return false;
    }

    return a.equals(b);
}

export interface SymbologyQueryParams {
    time: Time;
    bbox: BoundingBox2D;
    resolution: {x: number; y: number};
    spatialReference: SpatialReference;
}

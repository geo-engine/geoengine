import {Pipe, PipeTransform, inject} from '@angular/core';
import {DomSanitizer, SafeStyle} from '@angular/platform-browser';
import {Color, RgbaTuple} from '../../colors/color';
import {ColorBreakpoint} from '../../colors/color-breakpoint.model';
import {RasterColorizer} from '../../symbology/symbology.model';
import {Colorizer} from '../../colors/colorizer.model';

/**
 * Pipe to transform a colorizer into a CSS gradient.
 */
@Pipe({
    name: 'geoengineColorizerCssGradient',
})
export class ColorizerCssGradientPipe implements PipeTransform {
    protected sanitizer = inject(DomSanitizer);

    transform(colorizer: Colorizer, angle = 180): SafeStyle {
        const colors = colorizer.getBreakpoints().map((breakpoint) => breakpoint.color);
        const style = colorsToCssGradient(colors, angle);
        return this.sanitizer.bypassSecurityTrustStyle(style);
    }
}

/**
 * Pipe to transform a RasterColorizer into a CSS gradient.
 */
@Pipe({
    name: 'geoengineRasterColorizerCssGradient',
})
export class RasterColorizerCssGradientPipe implements PipeTransform {
    protected sanitizer = inject(DomSanitizer);

    transform(colorizer: RasterColorizer, angle = 180): SafeStyle {
        const colors = colorizer.getBreakpoints().map((breakpoint) => breakpoint.color);
        const style = colorsToCssGradient(colors, angle);
        return this.sanitizer.bypassSecurityTrustStyle(style);
    }
}

/**
 * Pipe to transform an `Array<ColorBreakpoint>`  into a CSS gradient.
 */
@Pipe({
    name: 'geoengineColorBreakpointsCssGradient',
})
export class ColorBreakpointsCssGradientPipe implements PipeTransform {
    protected sanitizer = inject(DomSanitizer);

    transform(breakpoints: Array<ColorBreakpoint>, angle = 180): SafeStyle {
        const colors = breakpoints.map((breakpoint) => breakpoint.color);
        const style = colorsToCssGradient(colors, angle);
        return this.sanitizer.bypassSecurityTrustStyle(style);
    }
}

/**
 * Pipe to transform an `Array<RgbaColor>`  into a CSS gradient.
 */
@Pipe({
    name: 'geoengineRgbaTuplesCssGradient',
})
export class RgbaArrayCssGradientPipe implements PipeTransform {
    protected sanitizer = inject(DomSanitizer);

    transform(rgbaColors: Array<RgbaTuple>, angle = 180): SafeStyle {
        const colors = rgbaColors.map((rgba) => Color.fromRgbaLike(rgba));
        const style = colorsToCssGradient(colors, angle);
        return this.sanitizer.bypassSecurityTrustStyle(style);
    }
}

/**
 * Convert an array of colors to a CSS gradient.
 */
function colorsToCssGradient(colors: Array<Color>, angle = 180): string {
    const numColors = colors.length;
    const elementSize = 100.0 / numColors;
    const halfElementSize = elementSize / 2.0;

    const validAngle = angle !== undefined ? angle % 360.0 : 180;

    const colorString = colors.reduce<string>(
        (acc, color, i) => acc + `, ${color.rgbaCssString()} ${i * elementSize + halfElementSize}%`,
        '',
    );

    const cssString = `linear-gradient(${validAngle}deg ${colorString})`;

    return cssString;
}

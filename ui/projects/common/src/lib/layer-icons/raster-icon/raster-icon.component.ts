import {ChangeDetectionStrategy, Component, computed, input} from '@angular/core';
import {Color, TRANSPARENT} from '../../colors/color';
import {Colorizer, LinearGradient} from '../../colors/colorizer.model';
import {ColorBreakpoint} from '../../colors/color-breakpoint.model';
import {RasterColorizer, SingleBandRasterColorizer} from '../../symbology/symbology.model';

/**
 * a simple interface to model a cell in the raster icon
 */
interface Cell {
    xStart: number;
    yStart: number;
    xSize: number;
    ySize: number;
    colorString: string;
}

/**
 * The raster icon component displays colored cells to visualize the raster style
 */
@Component({
    selector: 'geoengine-raster-icon',
    templateUrl: './raster-icon.component.svg',
    styleUrls: ['./raster-icon.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
})
export class RasterIconComponent {
    /** number of cells in x direction */
    readonly xCells = input.required<number>();
    /** number of cells in y direction */
    readonly yCells = input.required<number>();
    /** the raster style used to color the icon */
    readonly colorizer = input.required<RasterColorizer>();
    /** This is the number of pixels used for the icon */
    readonly cellSpace = input<number>(24);

    /**
     * the array of generated (colored and positioned) cells.
     */
    readonly cells = computed<Array<Cell>>(() => {
        const rasterColorizer = this.colorizer();
        const cellSpace = this.cellSpace();
        const xCells = this.xCells();
        const yCells = this.yCells();

        let colorizer: Colorizer;
        if (rasterColorizer instanceof SingleBandRasterColorizer) {
            colorizer = rasterColorizer.bandColorizer;
        } else {
            colorizer = RAINBOW_COLORIZER;
        }

        return RasterIconComponent.generateCells(colorizer, cellSpace, xCells, yCells);
    });

    /**
     * generates an array of cell descriptors which are used by the template
     */
    static generateCells(colorizer: Colorizer, cellSpace: number, xCells: number, yCells: number): Array<Cell> {
        const cells = new Array<Cell>(xCells * yCells);
        for (let y = 0; y < yCells; y++) {
            for (let x = 0; x < xCells; x++) {
                const idx = xCells * y + x;
                cells[idx] = {
                    xStart: (x * cellSpace) / xCells,
                    yStart: (y * cellSpace) / yCells,
                    xSize: cellSpace / xCells,
                    ySize: cellSpace / yCells,
                    colorString: Color.rgbaToCssString(RasterIconComponent.cellColor(colorizer, xCells, yCells, x, y)),
                };
            }
        }
        return cells;
    }

    private static cellColor(colorizer: Colorizer, xCells: number, yCells: number, x: number, y: number): Color {
        const validSymbology = colorizer && colorizer.getNumberOfColors() > 0;
        if (!validSymbology) {
            return Color.fromRgbaLike('#ff0000');
        }
        const numberOfCells = xCells * yCells;
        const numberOfColors = colorizer.getNumberOfColors();
        const isGradient = colorizer.isGradient();
        const scale = isGradient ? numberOfColors / (xCells + yCells - 1) : numberOfColors / numberOfCells;
        const idx = y * xCells + x;
        let colorIdx = 0;
        if (numberOfColors === 2) {
            colorIdx = y % 2 === 0 ? x % 2 : (x + 1) % 2;
        } else {
            const uidx = isGradient ? xCells - 1 - x + y : idx;
            colorIdx = Math.trunc(uidx * scale) % numberOfColors;
        }
        return colorizer.getColorAtIndex(colorIdx);
    }
}

export const RAINBOW_COLORIZER: Colorizer = new LinearGradient(
    [
        new ColorBreakpoint(0, Color.fromRgbaLike('#ff0000')),
        new ColorBreakpoint(0, Color.fromRgbaLike('#ffa500')),
        new ColorBreakpoint(0, Color.fromRgbaLike('#ffff00')),
        new ColorBreakpoint(0, Color.fromRgbaLike('#008000')),
        new ColorBreakpoint(0, Color.fromRgbaLike('#0000ff')),
        new ColorBreakpoint(0, Color.fromRgbaLike('#4b0082')),
        new ColorBreakpoint(0, Color.fromRgbaLike('#ee82ee')),
    ],
    TRANSPARENT,
    TRANSPARENT,
    TRANSPARENT,
);

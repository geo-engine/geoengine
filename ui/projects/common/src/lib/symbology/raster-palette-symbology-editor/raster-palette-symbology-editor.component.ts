import {Component, Input, ChangeDetectionStrategy, OnChanges, SimpleChanges, input, output, viewChild} from '@angular/core';
import {PaletteColorizer} from '../../colors/colorizer.model';
import {ColorAttributeInput, ColorAttributeInputComponent} from '../../colors/color-attribute-input/color-attribute-input.component';
import {Color} from '../../colors/color';
import {ColorMapSelectorComponent} from '../../colors/color-map-selector/color-map-selector.component';
import {ColorTableEditorComponent} from '../../colors/color-table-editor/color-table-editor.component';
import {ColorBreakpoint} from '../../colors/color-breakpoint.model';
import {Measurement} from '@geoengine/api-client';
import {MatCard, MatCardHeader, MatCardTitleGroup, MatCardTitle, MatCardSubtitle, MatCardContent} from '@angular/material/card';
import {MatIcon} from '@angular/material/icon';
import {FormsModule} from '@angular/forms';

/**
 * An editor for generating raster symbologies.
 */
@Component({
    selector: 'geoengine-raster-palette-symbology-editor',
    templateUrl: 'raster-palette-symbology-editor.component.html',
    styleUrls: ['raster-palette-symbology-editor.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        MatCard,
        MatCardHeader,
        MatCardTitleGroup,
        MatCardTitle,
        MatCardSubtitle,
        MatIcon,
        MatCardContent,
        ColorAttributeInputComponent,
        FormsModule,
        ColorTableEditorComponent,
    ],
})
export class RasterPaletteSymbologyEditorComponent implements OnChanges {
    readonly colorMapSelector = viewChild.required(ColorMapSelectorComponent);

    readonly colorPaletteEditor = viewChild.required(ColorTableEditorComponent);

    @Input() colorizer!: PaletteColorizer;
    readonly measurement = input<Measurement>();

    readonly colorizerChange = output<PaletteColorizer>();

    // The min value used for color table generation
    layerMinValue: number | undefined = undefined;
    // The max value used for color table generation
    layerMaxValue: number | undefined = undefined;

    colorTable: Array<ColorBreakpoint> = [];

    protected defaultColor?: ColorAttributeInput;
    protected noDataColor?: ColorAttributeInput;

    ngOnChanges(changes: SimpleChanges): void {
        if (changes.colorizer) {
            this.colorTable = this.colorizer.getBreakpoints();
            this.updateNodataAndDefaultColor();
            this.updateLayerMinMaxFromColorizer();
        }
    }

    updateColorTable(colorTable: Array<ColorBreakpoint>): void {
        const colors = new Map<number, Color>();
        for (const breakpoint of colorTable) {
            colors.set(breakpoint.value, breakpoint.color);
        }

        if (this.colorMapEquals(this.colorizer.colors, colors)) {
            return;
        }

        this.colorizer = this.colorizer.cloneWith({colors});
        this.colorizerChange.emit(this.colorizer);
    }

    /**
     * Set the max value to use for color table generation
     */
    updateLayerMinValue(min: number): void {
        if (this.layerMinValue !== min) {
            this.layerMinValue = min;
        }
    }

    /**
     * Set the max value to use for color table generation
     */
    updateLayerMaxValue(max: number): void {
        if (this.layerMaxValue !== max) {
            this.layerMaxValue = max;
        }
    }

    updateBounds(histogramSignal: {binStart: [number, number]}): void {
        if (histogramSignal?.binStart?.length !== 2) {
            return;
        }

        const [min, max] = histogramSignal.binStart;

        this.updateLayerMinValue(min);
        this.updateLayerMaxValue(max);
    }

    getDefaultColor(): ColorAttributeInput {
        if (!this.defaultColor) {
            throw new Error('uninitialized defaultColor');
        }

        return this.defaultColor;
    }

    updateDefaultColor(defaultColorInput: ColorAttributeInput): void {
        const defaultColor = defaultColorInput.value;

        this.colorizer = this.colorizer.cloneWith({defaultColor: defaultColor});

        this.colorizerChange.emit(this.colorizer);
    }

    getNoDataColor(): ColorAttributeInput {
        if (!this.noDataColor) {
            throw new Error('uninitialized noDataColor');
        }

        return this.noDataColor;
    }

    /**
     * Set the no data color
     */
    updateNoDataColor(noDataColorInput: ColorAttributeInput): void {
        const noDataColor = noDataColorInput.value;

        this.colorizer = this.colorizer.cloneWith({noDataColor});

        this.colorizerChange.emit(this.colorizer);
    }

    createColorMap(): Map<number, Color> {
        const colorMap = new Map<number, Color>();
        const colorizer: PaletteColorizer = this.colorizer;
        colorizer.getBreakpoints().forEach((bp, index) => {
            colorMap.set(bp.value, colorizer.getColorAtIndex(index));
        });
        return colorMap;
    }

    /**
     * Sets the layer min/max values from the colorizer.
     */
    updateLayerMinMaxFromColorizer(): void {
        const breakpoints = this.colorizer.getBreakpoints();
        this.updateLayerMinValue(breakpoints[0].value);
        this.updateLayerMaxValue(breakpoints[breakpoints.length - 1].value);
    }

    private updateNodataAndDefaultColor(): void {
        this.defaultColor = {
            key: 'Default Color',
            value: this.colorizer.defaultColor,
        };
        this.noDataColor = {
            key: 'No Data Color',
            value: this.colorizer.noDataColor,
        };
    }

    private colorMapEquals(a: Map<number, Color>, b: Map<number, Color>): boolean {
        if (a.size !== b.size) {
            return false;
        }

        for (const [key, aValue] of a) {
            const bValue = b.get(key);

            if (!bValue || !aValue.equals(bValue)) {
                return false;
            }
        }

        return true;
    }
}

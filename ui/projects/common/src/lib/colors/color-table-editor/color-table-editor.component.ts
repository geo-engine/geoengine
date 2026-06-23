import {CdkVirtualScrollViewport, CdkFixedSizeVirtualScroll, CdkVirtualForOf} from '@angular/cdk/scrolling';
import {
    ChangeDetectionStrategy,
    Component,
    ChangeDetectorRef,
    inject,
    input,
    output,
    viewChild,
    computed,
    linkedSignal,
    effect,
} from '@angular/core';
import {WHITE} from '../color';
import {
    ColorAttributeInput,
    ColorAttributeInputHinter,
    ColorAttributeInputComponent,
} from '../color-attribute-input/color-attribute-input.component';
import {ColorBreakpoint} from '../color-breakpoint.model';
import {Measurement} from '@geoengine/api-client';
import {ClassificationMeasurement} from '../../layers/measurement';
import {FormsModule} from '@angular/forms';
import {MatIconButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';

@Component({
    selector: 'geoengine-color-table-editor',
    templateUrl: './color-table-editor.component.html',
    styleUrls: ['./color-table-editor.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        CdkVirtualScrollViewport,
        CdkFixedSizeVirtualScroll,
        CdkVirtualForOf,
        ColorAttributeInputComponent,
        FormsModule,
        MatIconButton,
        MatIcon,
    ],
})
export class ColorTableEditorComponent {
    private ref = inject(ChangeDetectorRef);

    // Symbology to use for creating color tabs
    readonly colorTable = input.required<Array<ColorBreakpoint>>();

    readonly measurement = input<Measurement>();

    // Symbology altered through color tab inputs
    readonly colorTableChanged = output<Array<ColorBreakpoint>>();

    readonly virtualScrollViewport = viewChild.required(CdkVirtualScrollViewport);

    readonly colorAttributes = linkedSignal<Array<ColorBreakpoint>, Array<ColorAttributeInput>>({
        source: () => this.colorTable(),
        computation: (colorTable: Array<ColorBreakpoint>) =>
            colorTable.map((color: ColorBreakpoint) => {
                return {key: color.value.toString(), value: color.color};
            }),
        equal: (a, b) => {
            if (a.length !== b.length) {
                return false;
            }
            for (let i = 0; i < a.length; i++) {
                if (a[i].key !== b[i].key) {
                    return false;
                }
                if (!a[i].value.equals(b[i].value)) {
                    return false;
                }
            }
            return true;
        },
    });
    readonly colorHints = computed<ColorAttributeInputHinter | undefined>(() => {
        const measurement = this.measurement();

        if (measurement instanceof ClassificationMeasurement) {
            return measurement;
        }

        return undefined;
    });

    constructor() {
        effect(() => {
            const colorAttributes = this.colorAttributes();
            let hadError = false;

            const colorTable: Array<ColorBreakpoint> = colorAttributes.map((color: ColorAttributeInput) => {
                const value = Number(color.key);

                if (isNaN(value)) {
                    hadError = true;
                }

                return new ColorBreakpoint(value, color.value);
            });

            if (hadError) {
                return;
            }

            this.colorTableChanged.emit(colorTable);
        });
    }

    /**
     * Recreate the color map so that only values for which a ColorAttributeInput exists
     * are contained within the map. This is necessary because the $event that gets
     * passed to updateColor won't contain the previous rasterValue to delete in manually at
     * the time of updating.
     */
    updateColorAt(index: number, color: ColorAttributeInput): void {
        const colorAttributes = [...this.colorAttributes()]; // copy
        colorAttributes.splice(index, 1, color);

        this.colorAttributes.set(sorted(colorAttributes));
    }

    removeColorAt(index: number): void {
        const colorAttributes = [...this.colorAttributes()]; // copy
        colorAttributes.splice(index, 1);

        this.colorAttributes.set(colorAttributes);
    }

    appendColor(): void {
        const colorAttributes = this.colorAttributes();

        let newValue;
        if (colorAttributes.length) {
            // Determine a value so that the new tab will appear at the bottom of the list.
            newValue = parseFloat(colorAttributes[colorAttributes.length - 1].key) + 1;
        } else {
            newValue = 0;
        }

        this.colorAttributes.set(sorted([...colorAttributes, {key: newValue.toString(), value: WHITE}]));

        setTimeout(() => this.virtualScrollViewport().scrollTo({bottom: 0}), 0); // Delay of 0 to include new tab in scroll
    }

    isNoNumber(index: number): boolean {
        return isNaN(Number(this.colorAttributes()[index].key));
    }
}

/**
 * Sort allColors by raster layer values, so ColorAttributeInputs are displayed in the correct order.
 * Only called by parent when apply is pressed, so Inputs don't jump around while user is editing.
 */
const sorted = (colorAttributes: Array<ColorAttributeInput>): Array<ColorAttributeInput> =>
    colorAttributes.sort((a: ColorAttributeInput, b: ColorAttributeInput) => Math.sign(parseFloat(a.key) - parseFloat(b.key)));

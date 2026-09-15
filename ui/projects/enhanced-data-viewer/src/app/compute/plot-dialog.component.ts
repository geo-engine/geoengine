import {afterNextRender, ChangeDetectionStrategy, Component, DestroyRef, ElementRef, inject, signal, viewChild} from '@angular/core';
import {MAT_DIALOG_DATA, MatDialogContent} from '@angular/material/dialog';
import {VegaViewerComponent, VegaChartData} from '@geoengine/common';
import {DialogHeaderComponent, LayoutService} from '@geoengine/core';

@Component({
    selector: 'geoengine-plot-dialog',
    template: `
        <geoengine-dialog-header>Plot</geoengine-dialog-header>
        <mat-dialog-content>
            <geoengine-vega-viewer [chartData]="chartData" [width]="plotWidth()" [height]="plotHeight()"></geoengine-vega-viewer>
        </mat-dialog-content>
    `,
    styles: [
        `
            mat-dialog-content {
                max-height: unset;
                max-width: unset;
            }
        `,
    ],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [MatDialogContent, VegaViewerComponent, DialogHeaderComponent],
})
export class PlotDialogComponent {
    readonly chartData = inject<VegaChartData>(MAT_DIALOG_DATA);
    readonly plotWidth = signal<number | undefined>(undefined);
    readonly plotHeight = signal<number | undefined>(undefined);
    readonly destroyRef = inject(DestroyRef);
    readonly hostElement = inject(ElementRef).nativeElement as HTMLElement;
    readonly matDialogContent = viewChild.required<MatDialogContent, ElementRef<HTMLElement>>(MatDialogContent, {read: ElementRef});

    constructor() {
        afterNextRender({
            read: () => {
                const resizeObserver = new ResizeObserver(() => this.onResize());
                resizeObserver.observe(this.matDialogContent().nativeElement);
                this.destroyRef.onDestroy(() => resizeObserver.disconnect());
            },
        });
    }

    private onResize(): void {
        this.plotWidth.set(DIALOG_SIZE_PCT * window.innerWidth - DIALOG_PADDING);
        this.plotHeight.set(DIALOG_SIZE_PCT * window.innerHeight - DIALOG_PADDING - LayoutService.getToolbarHeightPx());
    }
}

const DIALOG_SIZE_PCT = 0.9;
const DIALOG_PADDING = 2 * LayoutService.remInPx;

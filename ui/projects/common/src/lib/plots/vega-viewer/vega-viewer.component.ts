import {Component, ChangeDetectionStrategy, ElementRef, inject, input, output, viewChild, signal, effect, untracked} from '@angular/core';
import vegaEmbed, {VisualizationSpec} from 'vega-embed';
import {View} from 'vega';
import {TopLevelSpec as VlSpec} from 'vega-lite';
import {Spec as VgSpec} from 'vega';
import {VegaChartData} from '../plot.model';
import {CommonConfig} from '../../config.service';

interface VegaHandle {
    view: View;
    spec: VlSpec | VgSpec;
    vgSpec: VgSpec;
    finalize: () => void;
}

@Component({
    selector: 'geoengine-vega-viewer',
    template: '<div #chart></div>',
    styleUrls: ['./vega-viewer.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
})
export class VegaViewerComponent {
    protected element = inject<ElementRef<HTMLElement>>(ElementRef);
    protected readonly config = inject(CommonConfig);

    readonly chartData = input<VegaChartData>();

    readonly width = input<number>();

    readonly height = input<number>();

    readonly interactionChange = output<Record<string, unknown>>();

    protected readonly chartContainer = viewChild.required<ElementRef<HTMLDivElement>>('chart');

    protected readonly vegaHandle = signal<VegaHandle | undefined>(undefined);

    constructor() {
        effect(() => {
            this.chartData();
            this.width();
            this.height();
            this.chartContainer();

            untracked(() => {
                this.clearContents();
                this.displayPlot();
            });
        });

        effect(() => {
            const vegaHandle = this.vegaHandle();
            const chartDataValue = this.chartData();

            if (!vegaHandle || !chartDataValue) {
                return;
            }

            const selectionName = chartDataValue.metadata?.selectionName;
            if (!selectionName) return;

            vegaHandle.view.addSignalListener(chartDataValue.metadata.selectionName, (_name, value) => {
                this.interactionChange.emit(value as /* fingers crossed */ Record<string, unknown>);
            });
        });
    }

    private displayPlot(): void {
        const chartData = this.chartData();
        if (!chartData) {
            return;
        }

        const div = this.chartContainer().nativeElement;

        const width = this.width() ?? div.clientWidth ?? this.element.nativeElement.offsetWidth;
        const height = this.height() ?? width / 2;

        const spec = JSON.parse(chartData.vegaString) as /* fingers crossed */ VisualizationSpec;

        vegaEmbed(div, spec, {
            actions: false,
            theme: this.config.PLOTS.THEME,
            renderer: 'svg',
            config: {
                autosize: {
                    type: 'fit',
                    contains: 'padding',
                },
            },
            // This is required, since width and height are ignored for vega-lite specs see https://github.com/vega/vega-embed Options -> Width
            patch: (s: VgSpec) => {
                s.width = width;
                s.height = height;
                return s;
            },
            width,
            height,
        })
            .then((result) => this.vegaHandle.set(result))
            .catch((_error) => {
                // TODO: react on error
            });
    }

    private clearContents(): void {
        const div = this.chartContainer().nativeElement;
        const vegaHandle = this.vegaHandle();

        if (vegaHandle) {
            vegaHandle.finalize();
            this.vegaHandle.set(undefined);
        }

        while (div.firstChild) {
            div.firstChild.remove();
        }
    }
}

import {BehaviorSubject, Observable, ReplaySubject} from 'rxjs';
import {map} from 'rxjs/operators';
import {Component, ChangeDetectionStrategy, ElementRef, AfterViewInit, inject, viewChild} from '@angular/core';
import * as dagreD3 from 'dagre-d3';
import * as d3 from 'd3';
import {MAT_DIALOG_DATA, MatDialogRef} from '@angular/material/dialog';
import {OperatorParams, OperatorSourcesDict} from '../../backend/backend.model';
import {LayoutService} from '../../layout.service';
import {ProjectService} from '../../project/project.service';

import {createIconDataUrl, Layer, FxLayoutDirective, FxFlexDirective} from '@geoengine/common';
import {LegacyTypedOperatorOperator} from '@geoengine/api-client';
import {DialogHeaderComponent} from '../../dialogs/dialog-header/dialog-header.component';
import {MatProgressSpinner} from '@angular/material/progress-spinner';
import {DialogSectionHeadingComponent} from '../../dialogs/dialog-section-heading/dialog-section-heading.component';
import {MatList, MatListItem, MatListSubheaderCssMatStyler, MatDivider} from '@angular/material/list';
import {MatLine} from '@angular/material/grid-list';
import {MatTooltip} from '@angular/material/tooltip';
import {AsyncPipe} from '@angular/common';

const GRAPH_STYLE = {
    general: {
        width: 200,
        headerHeight: 48,
        margin: 5,
    },
    operator: {
        height: 136,
        borderHeight: 1,
    },
    surrounding: {
        margin: 40,
        detailComponentWidth: 200,
    },
};

@Component({
    selector: 'geoengine-lineage-graph',
    templateUrl: './lineage-graph.component.html',
    styleUrls: ['./lineage-graph.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        DialogHeaderComponent,
        FxLayoutDirective,
        MatProgressSpinner,
        FxFlexDirective,
        DialogSectionHeadingComponent,
        MatList,
        MatListItem,
        MatLine,
        MatListSubheaderCssMatStyler,
        MatTooltip,
        MatDivider,
        AsyncPipe,
    ],
})
export class LineageGraphComponent implements AfterViewInit {
    private elementRef = inject(ElementRef);
    private projectService = inject(ProjectService);
    private layoutService = inject(LayoutService);
    private dialogRef = inject<MatDialogRef<LineageGraphComponent>>(MatDialogRef);
    private config = inject<{
        layer: Layer;
    }>(MAT_DIALOG_DATA);

    readonly svg = viewChild.required<ElementRef>('svg');
    readonly g = viewChild.required<ElementRef>('g');

    svgWidth$: Observable<number>;
    svgHeight$: Observable<number>;

    loaderDiameter$: Observable<number>;
    loaderLeft$: Observable<string>;

    loading$ = new BehaviorSubject(true);

    title = 'Layer Lineage';
    layer: Layer;

    selectedOperator$ = new ReplaySubject<LegacyTypedOperatorOperator>(1);
    selectedOperatorIcon$ = new ReplaySubject<string>(1);
    parameters$ = new ReplaySubject<Array<{key: string; value: string}>>(1);

    private maxWidth$ = new BehaviorSubject<number>(1);
    private maxHeight$ = new BehaviorSubject<number>(1);

    private svgRatio = 0.7;

    constructor() {
        this.svgWidth$ = this.maxWidth$.pipe(map((width) => Math.ceil(this.svgRatio * width)));
        this.svgHeight$ = this.maxHeight$;

        this.loaderDiameter$ = this.svgWidth$.pipe(map((width) => width / 6));
        this.loaderLeft$ = this.loaderDiameter$.pipe(map((diameter) => `calc(50% - ${diameter}px / 2)`));

        this.layer = this.config.layer;
        this.title = `Lineage for ${this.layer.name}`;
    }

    ngAfterViewInit(): void {
        setTimeout(() => {
            this.calculateDialogBounds();

            setTimeout(() => {
                this.drawGraph();
            });
        });
    }

    private calculateDialogBounds(): void {
        let dialogContainer;
        let parent = this.elementRef.nativeElement.parentElement;
        while (!dialogContainer) {
            dialogContainer = parent.querySelector('.cdk-overlay-pane');
            parent = parent.parentElement;
        }

        const width = parseInt(getComputedStyle(dialogContainer).maxWidth, 10) - 2 * LayoutService.remInPx;
        const maxHeight = window.innerHeight * 0.8;

        this.maxWidth$.next(width);
        this.maxHeight$.next(maxHeight);
    }

    private drawGraph(): void {
        this.projectService.getWorkflow(this.layer.workflowId).subscribe((workflow) => {
            const graph = new dagreD3.graphlib.Graph().setGraph({}).setDefaultEdgeLabel(() => ({label: ''}));

            LineageGraphComponent.addOperatorsToGraph(graph, workflow.operator);

            LineageGraphComponent.addLayerToGraph(graph, this.layer, 0);

            // create the renderer
            const render = new dagreD3.render();

            // Set up an SVG group so that we can translate the final graph.
            // console.log(this.graphContainer.nativeElement);
            const svg = d3.select(this.svg().nativeElement);
            const svgGroup = d3.select(this.g().nativeElement);

            // Run the renderer. This is what draws the final graph.

            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            render(svgGroup as any, graph as any);

            LineageGraphComponent.fixLabelPosition(svg);

            this.loading$.next(false);

            // do this asynchronously to start a new cycle of change detection
            setTimeout(() => {
                const sizes = this.setupWidthObservables(graph);
                this.addZoomSupport(svg, svgGroup, graph, sizes.width, sizes.height);
                this.addClickHandler(svg, graph);
            });
        });
    }

    private static addOperatorsToGraph(graph: dagreD3.graphlib.Graph, initialOperator: LegacyTypedOperatorOperator): void {
        let nextOperatorId = 0;

        const operatorQueue: Array<[number, LegacyTypedOperatorOperator]> = [[nextOperatorId++, initialOperator]];
        const edges: Array<[number, number, string]> = [];

        while (operatorQueue.length > 0) {
            const queueElement = operatorQueue.pop();

            if (!queueElement) {
                continue;
            }

            const [operatorId, operator] = queueElement;

            // add node to graph
            graph.setNode(`operator_${operatorId}`, {
                operator,
                type: 'operator',
                class: `operator operator_${operatorId}`,
                labelType: 'html',
                label: `
                <div class="header">
                    <img src="${createIconDataUrl(operator.type)}" class="icon" alt="${operator.type}">
                    </span>
                    ${operator.type}
                </div>
                <div class="parameters">
                    <table>
                        <tr>
                        ${this.parametersDisplayList(operator)
                            .map((kv) => `<td class="key">${kv.key}</td><td class="value">${kv.value}</td>`)
                            .join('</tr><tr>')}
                        </tr>
                    </table>
                </div>
                `,
                padding: 0,
                width: GRAPH_STYLE.general.width,
                height: GRAPH_STYLE.operator.height,
            });

            // add children
            const nonSourceOperator = operator;
            if (nonSourceOperator.sources) {
                const operatorSources = nonSourceOperator.sources as OperatorSourcesDict;
                for (const sourceKey of Object.keys(operatorSources)) {
                    const operatorSource = operatorSources[sourceKey] as
                        | LegacyTypedOperatorOperator
                        | Array<LegacyTypedOperatorOperator>
                        | undefined;

                    if (!operatorSource) {
                        continue;
                    }

                    let sources: Array<LegacyTypedOperatorOperator>;
                    if (operatorSource instanceof Array) {
                        sources = operatorSource;
                    } else {
                        sources = [operatorSource];
                    }

                    for (const source of sources) {
                        const childId = nextOperatorId++;
                        operatorQueue.push([childId, source]);
                        edges.push([childId, operatorId, sourceKey]);
                    }
                }
            }
        }

        // add edges to graph
        for (const [sourceId, targetId, sourceKey] of edges) {
            graph.setEdge(`operator_${sourceId}`, `operator_${targetId}`, {label: sourceKey});
        }

        // console.log(graph.edges(), graph);
    }

    private static addLayerToGraph(graph: dagreD3.graphlib.Graph, layer: Layer, workflowId: number): void {
        // add node
        graph.setNode(`layer_${layer.workflowId}`, {
            class: 'layer',
            type: 'layer',
            labelType: 'html',
            label: `<div class="header">${layer.name}</div>`,
            padding: 0,
            width: GRAPH_STYLE.general.width,
            height: GRAPH_STYLE.general.headerHeight,
        });

        // add edge
        graph.setEdge(`operator_${workflowId}`, `layer_${layer.workflowId}`, {
            class: 'layer-edge',
        });
    }

    private addZoomSupport(
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        svg: d3.Selection<SVGElement, any, any, any>,
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        svgGroup: d3.Selection<SVGElement, any, any, any>,
        graph: dagreD3.graphlib.Graph,
        svgWidth: number,
        svgHeight: number,
    ): void {
        // calculate available space after subtracting the margin
        const paddedWidth = svgWidth - GRAPH_STYLE.surrounding.margin;
        const paddedHeight = svgHeight - GRAPH_STYLE.surrounding.margin;

        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const grapWidth = (graph.graph() as any).width ?? 1; // TODO: remove any
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const grapHeight = (graph.graph() as any).height ?? 1; // TODO: remove any

        // calculate the initial zoom level that captures the whole graph
        const scale = Math.min(
            paddedWidth / grapWidth,
            paddedHeight / grapHeight,
            1, // do not scale more than 100% of size initially
        );

        const initialX = (svgWidth - scale * grapWidth) / 2;
        const initialY = (svgHeight - scale * grapHeight) / 2;

        // create zoom behavior
        const zoom = d3.zoom();

        // apply zoom to svg
        svgGroup
            .transition()
            .duration(500)
            // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/unbound-method
            .call(zoom.transform as any, d3.zoomIdentity.translate(initialX, initialY).scale(scale));

        // add zoom handler
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        zoom.on('zoom', (zoomEvent: d3.D3ZoomEvent<any, any>) => {
            const zoomTranslate = isNaN(zoomEvent.transform.x) ? [0, 0] : [zoomEvent.transform.x, zoomEvent.transform.y];
            const zoomScale = isNaN(zoomEvent.transform.k) ? 0 : zoomEvent.transform.k;
            // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
            svgGroup.attr('transform', `translate(${zoomTranslate})scale(${zoomScale})`);
        });
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        svg.call(zoom as any);
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    private addClickHandler(svg: d3.Selection<SVGElement, any, any, any>, graph: dagreD3.graphlib.Graph): void {
        svg.selectAll('.node').on('click', (_event, theNodeId) => {
            const nodeId = theNodeId as string; // conversion since the signature is of the wrong type

            const node = graph.node(nodeId);
            if (node.type === 'operator') {
                const operator: LegacyTypedOperatorOperator = node.operator;

                // update operator type
                this.selectedOperator$.next(operator);
                this.selectedOperatorIcon$.next(createIconDataUrl(operator.type));

                // update parameter view
                this.parameters$.next(LineageGraphComponent.parametersDisplayList(operator));

                // de-select all
                svg.selectAll('.operator').classed('highlight', false);
                // set highlight
                svg.select(`.${nodeId}`).classed('highlight', true);
            }
        });
    }

    private static parametersDisplayList(operator: LegacyTypedOperatorOperator): Array<{key: string; value: string}> {
        const list: Array<{key: string; value: string}> = [];

        const params = operator.params as OperatorParams | null;

        if (!params) {
            return list;
        }

        for (const key of Object.keys(params)) {
            let value = JSON.stringify(params[key], null, 2);

            if (value.startsWith('"') && value.endsWith('"')) {
                value = value.substr(1, value.length - 2);
            }

            list.push({key, value});
        }

        return list;
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    private static fixLabelPosition(svg: d3.Selection<SVGElement, any, any, any>): void {
        // HACK: move html label from center to top left
        svg.selectAll('.operator > .label > g > foreignObject')
            .attr('x', -GRAPH_STYLE.general.width / 2)
            .attr('y', -GRAPH_STYLE.operator.height / 2)
            .attr('width', GRAPH_STYLE.general.width)
            .attr('height', GRAPH_STYLE.operator.height);
        svg.selectAll('.layer > .label > g > foreignObject')
            .attr('x', -GRAPH_STYLE.general.width / 2)
            .attr('y', -GRAPH_STYLE.general.headerHeight / 2)
            .attr('width', GRAPH_STYLE.general.width)
            .attr('height', GRAPH_STYLE.general.headerHeight);
        svg.selectAll('.label > g').attr('transform', null);
    }

    private setupWidthObservables(graph: dagreD3.graphlib.Graph): {width: number; height: number} {
        const widthBound = (maxWidth: number, graphWidth: number): number =>
            Math.min(maxWidth - GRAPH_STYLE.surrounding.detailComponentWidth - GRAPH_STYLE.surrounding.margin, graphWidth);
        const heightBound = (maxWidth: number, _graphWidth: number): number =>
            // return Math.min(maxWidth, graphWidth + GRAPH_STYLE.surrounding.margin);
            // noinspection JSSuspiciousNameCombination
            maxWidth;
        // return the current width bounds

        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const grapWidth = (graph.graph() as any).width ?? 1;
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const grapHeight = (graph.graph() as any).height ?? 1;

        return {
            width: widthBound(this.maxWidth$.getValue(), grapWidth),
            height: heightBound(this.maxHeight$.getValue(), grapHeight),
        };
    }
}

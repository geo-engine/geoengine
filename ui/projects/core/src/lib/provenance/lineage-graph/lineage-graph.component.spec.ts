import * as dagreD3 from 'dagre-d3';
import {describe, expect, it} from 'vitest';
import {TypedOperator} from '@geoengine/api-client';
import {LineageGraphComponent} from './lineage-graph.component';

type AddOperatorsToGraph = (graph: dagreD3.graphlib.Graph, operator: TypedOperator['operator']) => void;

describe('LineageGraphComponent', () => {
    // a private static method, accessed through the class so that `this` stays bound
    const component = LineageGraphComponent as unknown as {addOperatorsToGraph: AddOperatorsToGraph};

    it('adds the nested source operators of a processing graph as nodes', () => {
        // only the root of a processing graph is wrapped in a `TypedOperator`, its sources are plain operators
        const processingGraph = {
            type: 'Raster',
            operator: {
                type: 'Expression',
                params: {expression: 'A + B', outputType: 'F32'},
                sources: {
                    a: {type: 'GdalSource', params: {data: 'ndvi'}},
                    b: {type: 'GdalSource', params: {data: 'elevation'}},
                },
            },
        } as unknown as TypedOperator;

        const graph = new dagreD3.graphlib.Graph().setGraph({}).setDefaultEdgeLabel(() => ({label: ''}));
        component.addOperatorsToGraph(graph, processingGraph.operator);

        const nodeTypes = graph.nodes().map((nodeId) => (graph.node(nodeId) as {operator: {type: string}}).operator.type);
        expect(nodeTypes.sort()).toEqual(['Expression', 'GdalSource', 'GdalSource']);
        expect(graph.edges()).toHaveLength(2);
    });
});

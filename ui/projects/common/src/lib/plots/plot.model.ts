import {VisualizationSpec} from 'vega-embed';
import {ProjectLayer as ProjectLayerDict, ProjectLayerUpdate as ProjectLayerUpdateDict, ProviderLayerId} from '@geoengine/api-client';
import {UUID} from '../datasets/dataset.model';
import {UNSAVED_LAYER_ID} from '../layers/layer.model';
import {ToDict} from '../time/time.model';

export type PlotType = 'JSON' | 'PNG';

/**
 * A plot of a project. It is a layer without symbology.
 */
export class Plot implements HasPlotId, ToDict<ProjectLayerUpdateDict> {
    /** The id of the plot in its project, or {@link UNSAVED_LAYER_ID} if it is not stored yet. */
    readonly id: string;
    readonly name: string;
    readonly description: string;
    readonly workflowId: UUID;
    readonly properties: Array<[string, string]>;
    readonly metadata: Record<string, string>;
    readonly source?: ProviderLayerId;

    constructor(config: {
        id?: string;
        name: string;
        description?: string;
        workflowId: string;
        properties?: Array<[string, string]>;
        metadata?: Record<string, string>;
        source?: ProviderLayerId;
    }) {
        this.id = config.id ?? UNSAVED_LAYER_ID;

        this.name = config.name;
        this.description = config.description ?? '';
        this.workflowId = config.workflowId;
        this.properties = config.properties ?? [];
        this.metadata = config.metadata ?? {};
        this.source = config.source;
    }

    static fromDict(dict: ProjectLayerDict): Plot {
        return new Plot({
            id: dict.id,
            name: dict.name,
            description: dict.description,
            workflowId: dict.processingGraph,
            properties: dict.properties.map(([key, value]) => [key, value]),
            metadata: dict.metadata,
            source: dict.source ?? undefined,
        });
    }

    /**
     * Whether the plot is stored in a project and has an id.
     */
    get isSaved(): boolean {
        return this.id !== UNSAVED_LAYER_ID;
    }

    updateFields(changes: {id?: string; name?: string; workflowId?: string}): Plot {
        return new Plot({
            id: changes.id ?? this.id,
            name: changes.name ?? this.name,
            description: this.description,
            workflowId: changes.workflowId ?? this.workflowId,
            properties: this.properties,
            metadata: this.metadata,
            source: this.source,
        });
    }

    equals(other: Plot): boolean {
        if (!(other instanceof Plot)) {
            return false;
        }

        return (
            this.id === other.id &&
            this.name === other.name &&
            this.description === other.description &&
            this.workflowId === other.workflowId
        );
    }

    /**
     * The plot as it is stored in a project. The plot must be saved.
     */
    toProjectLayerDict(): ProjectLayerDict {
        return {
            id: this.id,
            name: this.name,
            description: this.description,
            processingGraph: this.workflowId,
            properties: this.properties,
            metadata: this.metadata,
            source: this.source,
            visibility: {data: true, legend: false},
        };
    }

    /**
     * The plot for updating a project. A new plot has no id, so that the server assigns one.
     */
    toDict(): ProjectLayerUpdateDict {
        return {...this.toProjectLayerDict(), id: this.isSaved ? this.id : undefined};
    }
}

export interface HasPlotId {
    readonly id: string;
}

export interface VegaChartData {
    /**
     * The Vega-Lite spec of the plot
     */
    readonly vegaSpec: VisualizationSpec;
    readonly metadata?: {
        readonly selectionName?: string;
    };
}

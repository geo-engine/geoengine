import {ProjectLayer as ProjectLayerDict, ProjectLayerUpdate as ProjectLayerUpdateDict, ProviderLayerId} from '@geoengine/api-client';
import {UUID} from '../datasets/dataset.model';
import {RasterSymbology, Symbology, VectorSymbology} from '../symbology/symbology.model';
import {ToDict} from '../time/time.model';

export type LayerType = 'raster' | 'vector';

/**
 * The data connector that provides the layers and plots of projects, e.g. via OGC API Tiles.
 */
export const PROJECTS_PROVIDER_ID: UUID = 'a7fd61d6-d2d6-4684-a933-3a2129895237';

/**
 * The id of a layer that is not stored in a project yet.
 * The server assigns the id when the layer is added to a project.
 */
export const UNSAVED_LAYER_ID = '';

/**
 * The fields that layers and plots of a project share.
 */
export interface ProjectLayerConfig {
    /** The id of the layer in the project. Omit it for a new layer, the server assigns it. */
    id?: string;
    name: string;
    description?: string;
    processingGraphId: UUID;
    properties?: Array<[string, string]>;
    metadata?: Record<string, string>;
    /** The catalog layer this layer was copied from, if any. */
    source?: ProviderLayerId;
}

export abstract class Layer implements HasLayerId, HasLayerType, ToDict<ProjectLayerUpdateDict> {
    abstract readonly layerType: LayerType;

    /** The id of the layer in its project, or {@link UNSAVED_LAYER_ID} if it is not stored yet. */
    readonly id: string;

    readonly name: string;
    readonly description: string;
    readonly workflowId: UUID;

    readonly properties: Array<[string, string]>;
    readonly metadata: Record<string, string>;
    readonly source?: ProviderLayerId;

    readonly isVisible: boolean;
    readonly isLegendVisible: boolean;

    readonly symbology: Symbology;

    protected constructor(
        config: ProjectLayerConfig & {
            isVisible: boolean;
            isLegendVisible: boolean;
            symbology: Symbology;
        },
    ) {
        this.id = config.id ?? UNSAVED_LAYER_ID;

        this.name = config.name;
        this.description = config.description ?? '';
        this.workflowId = config.processingGraphId;
        this.properties = config.properties ?? [];
        this.metadata = config.metadata ?? {};
        this.source = config.source;
        this.isVisible = config.isVisible;
        this.isLegendVisible = config.isLegendVisible;
        this.symbology = config.symbology;
    }

    /**
     * Create the suitable layer type
     */
    static fromDict(dict: ProjectLayerDict): Layer {
        const symbologyType = dict.symbology?.type;
        if (symbologyType === 'raster') {
            return RasterLayer.fromDict(dict);
        }

        if (symbologyType === 'point' || symbologyType === 'line' || symbologyType === 'polygon') {
            return VectorLayer.fromDict(dict);
        }

        throw new Error(`Unknown layer type »${symbologyType ?? 'none'}«`);
    }

    /**
     * Whether the layer is stored in a project and has an id.
     */
    get isSaved(): boolean {
        return this.id !== UNSAVED_LAYER_ID;
    }

    // TODO: remove method, here?
    abstract updateFields(changes: {
        id?: string;
        name?: string;
        workflowId?: string;
        isVisible?: boolean;
        isLegendVisible?: boolean;
        symbology?: Symbology;
    }): Layer;

    abstract equals(other: Layer): boolean;

    /**
     * The layer for updating a project. A new layer has no id, so that the server assigns one.
     */
    toDict(): ProjectLayerUpdateDict {
        return {...this.toProjectLayerDict(), id: this.isSaved ? this.id : undefined};
    }

    /**
     * The layer as it is stored in a project. The layer must be saved.
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
            visibility: {
                data: this.isVisible,
                legend: this.isLegendVisible,
            },
            symbology: this.symbology.toDict(),
        };
    }

    get isVectorLayer(): boolean {
        return this.layerType === 'vector';
    }

    get isRasterLayer(): boolean {
        return this.layerType === 'raster';
    }

    /**
     * The fields of `dict` that all layers and plots share.
     */
    protected static configFromDict(dict: ProjectLayerDict): ProjectLayerConfig {
        return {
            id: dict.id,
            name: dict.name,
            description: dict.description,
            processingGraphId: dict.processingGraph,
            properties: dict.properties.map(([key, value]) => [key, value]),
            metadata: dict.metadata,
            source: dict.source ?? undefined,
        };
    }

    /**
     * The fields of this layer that all layers and plots share, for creating an updated copy.
     */
    protected configWithChanges(changes: {id?: string; name?: string; workflowId?: string}): ProjectLayerConfig {
        return {
            id: changes.id ?? this.id,
            name: changes.name ?? this.name,
            description: this.description,
            processingGraphId: changes.workflowId ?? this.workflowId,
            properties: this.properties,
            metadata: this.metadata,
            source: this.source,
        };
    }

    protected baseEquals(other: Layer): boolean {
        return (
            this.id === other.id &&
            this.name === other.name &&
            this.description === other.description &&
            this.workflowId === other.workflowId &&
            this.isVisible === other.isVisible &&
            this.isLegendVisible === other.isLegendVisible &&
            this.symbology === other.symbology
        );
    }
}

export class VectorLayer extends Layer {
    readonly layerType = 'vector';

    override readonly symbology: VectorSymbology;

    constructor(
        config: ProjectLayerConfig & {
            isVisible: boolean;
            isLegendVisible: boolean;
            symbology: VectorSymbology;
        },
    ) {
        super(config);
        this.symbology = config.symbology;
    }

    static override fromDict(dict: ProjectLayerDict): Layer {
        if (dict.symbology?.type === 'point' || dict.symbology?.type === 'line' || dict.symbology?.type === 'polygon') {
            return new VectorLayer({
                ...Layer.configFromDict(dict),
                isLegendVisible: dict.visibility.legend,
                isVisible: dict.visibility.data,
                symbology: VectorSymbology.fromVectorSymbologyDict(dict.symbology),
            });
        }
        throw new Error('missing `Vector` to deserialize');
    }

    updateFields(changes: {
        id?: string;
        name?: string;
        workflowId?: string;
        isVisible?: boolean;
        isLegendVisible?: boolean;
        symbology?: VectorSymbology;
    }): VectorLayer {
        return new VectorLayer({
            ...this.configWithChanges(changes),
            isVisible: changes.isVisible ?? this.isVisible,
            isLegendVisible: changes.isLegendVisible ?? this.isLegendVisible,
            symbology: changes.symbology ?? this.symbology,
        });
    }

    equals(other: Layer): boolean {
        return other instanceof VectorLayer && this.baseEquals(other);
    }
}

export class RasterLayer extends Layer {
    readonly layerType = 'raster';

    override readonly symbology: RasterSymbology;

    constructor(
        config: ProjectLayerConfig & {
            isVisible: boolean;
            isLegendVisible: boolean;
            symbology: RasterSymbology;
        },
    ) {
        super(config);
        this.symbology = config.symbology;
    }

    static override fromDict(dict: ProjectLayerDict): Layer {
        if (dict.symbology?.type === 'raster') {
            return new RasterLayer({
                ...Layer.configFromDict(dict),
                isLegendVisible: dict.visibility.legend,
                isVisible: dict.visibility.data,
                symbology: RasterSymbology.fromRasterSymbologyDict(dict.symbology),
            });
        }
        throw new Error('invalid symbology type');
    }

    updateFields(changes: {
        id?: string;
        name?: string;
        workflowId?: string;
        isVisible?: boolean;
        isLegendVisible?: boolean;
        symbology?: RasterSymbology;
    }): RasterLayer {
        return new RasterLayer({
            ...this.configWithChanges(changes),
            isVisible: changes.isVisible ?? this.isVisible,
            isLegendVisible: changes.isLegendVisible ?? this.isLegendVisible,
            symbology: changes.symbology ?? this.symbology,
        });
    }

    equals(other: Layer): boolean {
        return other instanceof RasterLayer && this.baseEquals(other);
    }
}

export interface HasLayerId {
    readonly id: string;
}

export interface HasLayerType {
    readonly layerType: LayerType;
}

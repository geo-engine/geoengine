import type {TimeStepDuration} from '@geoengine/common';
import type {GeographicCoverage} from './coverage';

export interface VisualizationPreset {
    key: string;
    displayName: string;
    backgroundImage: string;
    connectorId: string;
    layerId: string;
    order: number;
}
export interface DataSourceVariant {
    key: string;
    name: string;
    crs: string;
    coverage?: GeographicCoverage;
    /** The region collection containing this variant's preset layers. */
    collectionId: string;
}
export interface DataSourceCategory {
    /** Layer collection id; categories do not carry data-source metadata. */
    key: string;
    name: string;
    sources: DataSourceDefinition[];
}
export interface DataSourceDefinition {
    key: string;
    name: string;
    variants: DataSourceVariant[];
    defaultTimeStep?: TimeStepDuration;
    citation: string;
    /** Maximum map viewport dimensions in EPSG:3857 projection units (metres). */
    maxMapExtent?: MapExtent;
}
export interface DataSourceLayer {
    dataConnectorId: string;
    layerId: string;
}

export interface MapExtent {
    width: number;
    height: number;
}

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
export interface DataSourceDefinition {
    key: string;
    name: string;
    variants: DataSourceVariant[];
    defaultTime?: number;
    defaultTimeStep?: TimeStepDuration;
    citation: string;
}
export interface DataSourceLayer {
    dataConnectorId: string;
    layerId: string;
}

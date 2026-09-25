import type {TimeStepDuration} from '@geoengine/common';

export const PRESET_CATEGORIES = ['static', 'harvested', 'adHoc'] as const;
export type PresetCategory = (typeof PRESET_CATEGORIES)[number];
export interface VisualizationPreset {
    key: string;
    displayName: string;
    backgroundImage: string;
    connectorId: string;
    layerId: string;
    category: PresetCategory;
    order: number;
    variantKey: string;
}
export interface DataSourceVariant {
    key: string;
    name: string;
    crs?: string;
    explicit: boolean;
    presets: VisualizationPreset[];
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
export const PRESET_CATEGORY_LABELS: Record<PresetCategory, string> = {
    static: 'Static',
    harvested: 'Harvested',
    adHoc: 'Ad-hoc (Data Provider)',
};

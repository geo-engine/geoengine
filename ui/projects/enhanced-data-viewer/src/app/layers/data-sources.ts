import type {TimeStepDuration} from '@geoengine/common';

export const PRESET_CATEGORIES = ['static', 'harvested', 'adHoc'] as const;
export type PresetCategory = (typeof PRESET_CATEGORIES)[number];
export interface VisualizationPreset {
    displayName: string;
    backgroundImage: string;
    connectorId: string;
    layerId: string;
    category: PresetCategory;
    order: number;
}
export interface DataSourceDefinition {
    key: string;
    name: string;
    presets: VisualizationPreset[];
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

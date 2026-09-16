export type PresetCategory = 'static' | 'harvested' | 'adHoc';
import type {TimeStepDuration} from '@geoengine/common';

export interface VisualizationPreset {
    displayName: string;
    backgroundImage: string;
    connectorId: string;
    collectionId: string;
    name: string;
    category: PresetCategory;
}

export interface DataSourceDefinition {
    key: string;
    name: string;
    presets: VisualizationPreset[];
    defaultPresetIndex: number;
    defaultTime: number;
    defaultTimeStep: TimeStepDuration;
    citation: string; // TODO: get from provenance API
}

export interface DataSourceLayer {
    dataConnectorId: string;
    layerId: string;
}

// Provider/connector IDs:
//   ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74 – Internal Layer DB (serves layers
//     registered via the API, including static, harvested, and provider layers)
//   b274275c-373d-4a3f-8b45-9b48e9614329 – Sentinel-1 Global Mosaics STAC Provider
//   c385386d-484e-5b40-9c56-0a6ca9f07243 – Sentinel-2 L2A STAC Provider
//   d496497e-595f-6c51-ad67-1b7dba0a1834 – Landsat C2 L1 OLI/TIRS STAC Provider
//   e5a7508f-6a60-7d62-be78-2c8ecb1b2945 – OpenGeoHub Landsat Mosaic STAC Provider
//   cbb21ee3-d15d-45c5-a175-66964adf4e85 – Personal Data Catalog (user datasets)

// Collection ID for the Internal Layer DB root: 05102bb3-a855-4a37-8a8a-30026a91fef1
// STAC providers use their collection name at the path "root" -> "dataTypes"/"projections" etc.

const LAYER_DB_PROVIDER_ID = 'ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74';
const LAYER_DB_ROOT_COLLECTION_ID = '05102bb3-a855-4a37-8a8a-30026a91fef1';

export const PRESET_CATEGORY_LABELS: Record<PresetCategory, string> = {
    static: 'Static',
    harvested: 'Harvested',
    adHoc: 'Ad-hoc (Data Provider)',
};

export const DATA_SOURCES: DataSourceDefinition[] = [
    {
        key: 'sentinel1',
        name: 'Sentinel-1',
        defaultPresetIndex: 0,
        defaultTime: 1775001600000,
        defaultTimeStep: {durationAmount: 1, durationUnit: 'month'},
        citation: `Copernicus Sentinel data [Year]`,
        presets: [
            // Static
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Sentinel-1 Static',
                category: 'static',
            },
            // Harvested
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Sentinel-1 VV Band (Harvested)',
                category: 'harvested',
            },
            {
                displayName: 'SAR False Color',
                backgroundImage: 'assets/false-color.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Sentinel-1 SAR False Color (Harvested)',
                category: 'harvested',
            },
            // Ad-hoc
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Sentinel-1 Global Mosaics Provider',
                category: 'adHoc',
            },
        ],
    },
    {
        key: 'sentinel2',
        name: 'Sentinel-2 L2A',
        defaultPresetIndex: 0,
        defaultTime: 1775001600000,
        defaultTimeStep: {durationAmount: 1, durationUnit: 'day'},
        citation: `Copernicus Sentinel data [Year]`,
        presets: [
            // Static
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Sentinel-2 L2A Static',
                category: 'static',
            },
            // Harvested
            {
                displayName: 'True Color',
                backgroundImage: 'assets/rgb.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Sentinel-2 L2A True Color (Harvested)',
                category: 'harvested',
            },
            {
                displayName: 'True Color Image (TCI)',
                backgroundImage: 'assets/rgb.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Sentinel-2 L2A True Color Image (Harvested)',
                category: 'harvested',
            },
            {
                displayName: 'NDVI',
                backgroundImage: 'assets/ndvi.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Sentinel-2 L2A NDVI (Harvested)',
                category: 'harvested',
            },
            // Ad-hoc
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Sentinel-2 L2A Provider',
                category: 'adHoc',
            },
        ],
    },
    {
        key: 'landsat',
        name: 'Landsat C2 L1 OLI/TIRS',
        defaultPresetIndex: 0,
        defaultTime: 1767916800000,
        defaultTimeStep: {durationAmount: 1, durationUnit: 'day'},
        citation: `Landsat imagery courtesy of the U.S. Geological Survey`,
        presets: [
            // Static
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Landsat C2 L1 OLI/TIRS Static',
                category: 'static',
            },
            // Harvested
            {
                displayName: 'Red Band',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Landsat C2 L1 OLI/TIRS Red Band (Harvested)',
                category: 'harvested',
            },
            {
                displayName: 'True Color',
                backgroundImage: 'assets/rgb.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Landsat C2 L1 OLI/TIRS True Color (Harvested)',
                category: 'harvested',
            },
            {
                displayName: 'NDVI',
                backgroundImage: 'assets/ndvi.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Landsat C2 L1 OLI/TIRS NDVI (Harvested)',
                category: 'harvested',
            },
            // Ad-hoc
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Landsat C2 L1 OLI/TIRS Provider',
                category: 'adHoc',
            },
            {
                displayName: 'True Color',
                backgroundImage: 'assets/rgb.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Landsat C2 L1 OLI/TIRS Provider True Color',
                category: 'adHoc',
            },
        ],
    },
    {
        key: 'opengeohub-landsat',
        name: 'OpenGeoHub Landsat Mosaic',
        defaultPresetIndex: 0,
        defaultTime: 1730419200000,
        defaultTimeStep: {durationAmount: 2, durationUnit: 'months'},
        citation: `Landsat imagery courtesy of the U.S. Geological Survey`,
        presets: [
            // Static
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'OpenGeoHub Landsat Bimonthly Mosaic Static',
                category: 'static',
            },
            // Harvested
            {
                displayName: 'Red Band',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'OpenGeoHub Landsat Bimonthly Mosaic Red Band (Harvested)',
                category: 'harvested',
            },
            {
                displayName: 'True Color',
                backgroundImage: 'assets/rgb.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'OpenGeoHub Landsat Bimonthly Mosaic True Color (Harvested)',
                category: 'harvested',
            },
            {
                displayName: 'NDVI',
                backgroundImage: 'assets/ndvi.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'OpenGeoHub Landsat Bimonthly Mosaic NDVI (Harvested)',
                category: 'harvested',
            },
            // Ad-hoc
            {
                displayName: 'Default',
                backgroundImage: 'assets/grey.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'OpenGeoHub Landsat Bimonthly Mosaic Provider',
                category: 'adHoc',
            },
            {
                displayName: 'True Color',
                backgroundImage: 'assets/rgb.jpg',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'OpenGeoHub Landsat Bimonthly Mosaic Provider True Color',
                category: 'adHoc',
            },
        ],
    },
    {
        key: 'clms-burnt-area',
        name: 'CLMS Burnt Area',
        defaultPresetIndex: 0,
        defaultTime: 1782913271000, // 2026-07-01
        defaultTimeStep: {durationAmount: 1, durationUnit: 'days'},
        citation:
            "European Union's Copernicus Land Monitoring Service information; https://doi.org/10.2909/bfd77180-7d7c-4c1c-b193-1489f735d5f1",
        presets: [
            // Burnt Fraction
            {
                displayName: 'Burnt Fraction',
                backgroundImage: 'assets/fire_frac.png',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Burnt Area Burnt Fraction',
                category: 'harvested',
            }, // Burn Probability
            {
                displayName: 'Burn Probability',
                backgroundImage: 'assets/fire_prob.png',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Burnt Area Burn Probability',
                category: 'harvested',
            }, // Day of Burn
            {
                displayName: 'Day of Burn',
                backgroundImage: 'assets/fire_day.png',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Burnt Area Day of Burn',
                category: 'harvested',
            }, // Large Fire Probability
            {
                displayName: 'Large Fire Probability',
                backgroundImage: 'assets/fire_prob.png',
                connectorId: LAYER_DB_PROVIDER_ID,
                collectionId: LAYER_DB_ROOT_COLLECTION_ID,
                name: 'Burnt Area Large Fire Probability',
                category: 'harvested',
            },
        ],
    },
];

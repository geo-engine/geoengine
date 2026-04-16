import {Injectable} from '@angular/core';
import {CommonConfigStructure, CommonConfig, Delays as CommonDelays, mergeDeepOverrideLists} from '@geoengine/common';

interface Delays extends CommonDelays {
    readonly LOADING: {
        readonly MIN: number;
    };
    readonly TOOLTIP: number;
    readonly DEBOUNCE: number;
    readonly STORAGE_DEBOUNCE: number;
    readonly GUEST_LOGIN_HINT: number;
}

export interface ConfigDefaults {
    readonly PROJECT: {
        readonly NAME: string;
        readonly TIME:
            | string
            | {
                  start: string;
                  end?: string;
              };
        readonly TIMESTEP: '15 minutes' | '1 hour' | '1 day' | '1 month' | '6 months' | '1 year';
        readonly PROJECTION: 'EPSG:3857' | 'EPSG:4326';
    };
    /**
     * The default extent to focus on in EPSG:4326.
     */
    readonly FOCUS_EXTENT: [number, number, number, number];
}

export interface ConfigMap {
    readonly BASEMAPS: Basemaps;
    readonly DEFAULT_BASEMAP: keyof Basemaps;
    readonly REFRESH_LAYERS_ON_CHANGE: boolean;
    readonly VALID_CRS: Array<string>;
}

export type Basemaps = Record<string, Basemap>;

export interface Basemap {
    readonly TYPE: Wms['TYPE'] | VectorTiles['TYPE'];
    readonly URL: string;
    readonly ATTRIBUTION: string;
}

export interface Wms extends Basemap {
    readonly TYPE: 'WMS';
    readonly LAYER: string | LayerPerProjection;
    readonly VERSION: string;
    readonly FORMAT: string;
    readonly PROJECTIONS: Array<string>;
    readonly MAP_900913?: boolean;
}

export type LayerPerProjection = Record<string, string>;

export interface VectorTiles extends Basemap {
    readonly TYPE: 'MVT';
    readonly STYLE_URL: string;
    readonly SOURCE: string;
    readonly LAYER_EXTENTS: Record<string, [number, number, number, number]>;
    readonly MAX_ZOOM: number;
}

interface Time {
    readonly ALLOW_RANGES: boolean;
}

interface SpatialReferenceConfig {
    readonly NAME: string;
    readonly SRS_STRING: string;
}

interface Project {
    readonly CREATE_TEMPORARY_PROJECT_AT_STARTUP: boolean;
}

export interface CoreConfigStructure extends CommonConfigStructure {
    readonly DEFAULTS: ConfigDefaults;
    readonly DELAYS: Delays;
    readonly MAP: ConfigMap;
    readonly PROJECT: Project;
    readonly SPATIAL_REFERENCES: Array<SpatialReferenceConfig>;
    readonly TIME: Time;
}

export const DEFAULT_CORE_CONFIG: CoreConfigStructure = {
    DEFAULTS: {
        PROJECT: {
            NAME: 'Default',
            TIME: '2014-04-01T12:00:00.000Z',
            TIMESTEP: '1 month',
            PROJECTION: 'EPSG:4326', // TODO: change back to 'EPSG:3857'
        },
        FOCUS_EXTENT: [-180, -90, 180, 90],
    },
    BRANDING: {
        LOGO_URL: 'assets/geoengine.svg',
        LOGO_ICON_URL: 'assets/geoengine-favicon-white.svg',
        LOGO_ALT_URL: 'assets/geoengine-white.svg',
        PAGE_TITLE: 'Geo Engine',
    },
    DELAYS: {
        LOADING: {
            MIN: 500,
        },
        TOOLTIP: 400,
        DEBOUNCE: 400,
        STORAGE_DEBOUNCE: 1500,
        GUEST_LOGIN_HINT: 5000,
    },
    MAP: {
        DEFAULT_BASEMAP: 'Natural Earth Countries 10m',
        BASEMAPS: {
            /* eslint-disable @typescript-eslint/naming-convention */
            'Natural Earth Countries 10m': {
                TYPE: 'MVT',
                URL: 'https://basemap.geoengine.io/natural-earth/{epsg}/{z}/{x}/{y}.pbf',
                STYLE_URL: 'assets/mvt/ne-ge.json',
                SOURCE: 'ne',
                LAYER_EXTENTS: {
                    'EPSG:4326': [-180, -180, 180, 180],
                    'EPSG:3857': [-20037508.3427892, -20037508.3427892, 20037508.3427892, 20037508.3427892],
                },
                MAX_ZOOM: 22,
                ATTRIBUTION: 'Made with Natural Earth. © 2025 Geo Engine GmbH',
            } as const as VectorTiles,
            'Blue Marble (DLR EOC Basemap)': {
                TYPE: 'WMS',
                URL: 'https://geoservice.dlr.de/eoc/basemap/wms',
                LAYER: 'blue_marble:blue_marble',
                VERSION: '1.3.0',
                FORMAT: 'image/png',
                PROJECTIONS: ['EPSG:4326', 'EPSG:3857'],
                ATTRIBUTION: "2004 NASA's Earth Observatory, © 2025 DLR EOC",
            } as const as Wms,
            'Sentinel-2 cloudless layer for 2016 by EOX': {
                TYPE: 'WMS',
                URL: 'https://tiles.maps.eox.at/wms',
                LAYER: {'EPSG:4326': 's2cloudless', 'EPSG:3857': 's2cloudless_3857'},
                VERSION: '1.1.1',
                FORMAT: 'image/png',
                PROJECTIONS: ['EPSG:4326', 'EPSG:3857'],
                MAP_900913: true,
                ATTRIBUTION:
                    'Sentinel-2 cloudless - https://s2maps.eu by EOX IT Services GmbH (Contains modified Copernicus Sentinel data 2016 & 2017)',
            } as const as Wms,
        },
        REFRESH_LAYERS_ON_CHANGE: false,
        VALID_CRS: ['EPSG:4326', 'EPSG:3857'],
    },
    API_URL: '/api',
    TIME: {
        ALLOW_RANGES: true,
    },
    PLOTS: {
        THEME: 'excel',
    },
    SPATIAL_REFERENCES: [
        {
            NAME: 'WGS 84',
            SRS_STRING: 'EPSG:4326',
        },
        {
            NAME: 'WGS 84 / Pseudo-Mercator',
            SRS_STRING: 'EPSG:3857',
        },
        {
            NAME: 'WGS 84 / UTM zone 32N',
            SRS_STRING: 'EPSG:32632',
        },
        {
            NAME: 'WGS 84 / UTM zone 36N',
            SRS_STRING: 'EPSG:32636',
        },
        {
            NAME: 'WGS 84 / UTM zone 36S',
            SRS_STRING: 'EPSG:32736',
        },
        {
            NAME: 'WGS 84 / UTM zone 37N',
            SRS_STRING: 'EPSG:32637',
        },
        {
            NAME: 'WGS 84 / UTM zone 37S',
            SRS_STRING: 'EPSG:32737',
        },
    ],
    PROJECT: {
        CREATE_TEMPORARY_PROJECT_AT_STARTUP: false,
    },
    USER: {
        GUEST: {
            NAME: 'guest',
            PASSWORD: 'guest',
        },
        AUTO_GUEST_LOGIN: true,
        REGISTRATION_AVAILABLE: true,
        LOCAL_LOGIN_AVAILABLE: true,
    },
} as const;

/**
 * A service that provides config entries.
 * Loads a custom file at startup.
 */
@Injectable()
export class CoreConfig extends CommonConfig {
    protected override config!: CoreConfigStructure;

    override get DELAYS(): Delays {
        return this.config.DELAYS;
    }

    get DEFAULTS(): ConfigDefaults {
        return this.config.DEFAULTS;
    }

    get MAP(): ConfigMap {
        return this.config.MAP;
    }

    get TIME(): Time {
        return this.config.TIME;
    }

    get SPATIAL_REFERENCES(): Array<SpatialReferenceConfig> {
        return this.config.SPATIAL_REFERENCES;
    }

    get PROJECT(): Project {
        return this.config.PROJECT;
    }

    /**
     * Initialize the config on app start.
     */
    override async load(defaults: CoreConfigStructure = DEFAULT_CORE_CONFIG): Promise<void> {
        await super.load(defaults);
        this.config = mergeDeepOverrideLists(defaults, {...this.config});
    }
}

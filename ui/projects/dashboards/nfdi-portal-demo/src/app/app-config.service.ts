import {Injectable} from '@angular/core';
import {mergeDeepOverrideLists} from '@geoengine/common';
import {ConfigDefaults, ConfigMap, CoreConfig, CoreConfigStructure, DEFAULT_CORE_CONFIG} from '@geoengine/core';

interface Components {
    readonly PLAYBACK: {
        readonly AVAILABLE: boolean;
    };
}

interface AppConfigStructure extends CoreConfigStructure {
    readonly COMPONENTS: Components;
    readonly DEFAULTS: ConfigDefaults;
    readonly MAP: ConfigMap;
}

const APP_CONFIG_DEFAULTS = mergeDeepOverrideLists(DEFAULT_CORE_CONFIG, {
    COMPONENTS: {
        PLAYBACK: {
            AVAILABLE: false,
        },
    },
    DEFAULTS: {
        PROJECT: {
            NAME: 'Default',
            TIME: '1991-01-01T00:00:00.000Z',
            TIMESTEP: '1 month',
            PROJECTION: 'EPSG:3857',
        },
        FOCUS_EXTENT: [6.98865807458, 47.3024876979, 15.0169958839, 54.983104153],
    },
    MAP: {
        BACKGROUND_LAYER: 'MVT',
        BACKGROUND_LAYER_URL: 'https://basemap.geoengine.io/natural-earth-v2/{epsg}/{z}/{x}/{y}.pbf',
        VECTOR_TILES: {
            STYLE_URL: 'assets/mvt/ne-ge-de.json',
            SOURCE: 'ne',
            BACKGROUND_LAYER_EXTENTS: {
                // eslint-disable-next-line @typescript-eslint/naming-convention
                'EPSG:4326': [-180, -180, 180, 180],
                // eslint-disable-next-line @typescript-eslint/naming-convention
                'EPSG:3857': [-20037508.3427892, -20037508.3427892, 20037508.3427892, 20037508.3427892],
            },
            MAX_ZOOM: 22,
        },
        VALID_CRS: ['EPSG:3857'],
    },
}) as AppConfigStructure;

@Injectable()
export class AppConfig extends CoreConfig {
    protected override config!: AppConfigStructure;

    get COMPONENTS(): Components {
        return this.config.COMPONENTS;
    }

    override get MAP(): ConfigMap {
        return this.config.MAP;
    }

    override get DEFAULTS(): ConfigDefaults {
        return this.config.DEFAULTS;
    }

    override async load(): Promise<void> {
        return super.load(APP_CONFIG_DEFAULTS);
    }
}

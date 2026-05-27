import {Injectable} from '@angular/core';
import {mergeDeepOverrideLists} from '@geoengine/common';
import {CoreConfig, CoreConfigStructure, DEFAULT_CORE_CONFIG} from '@geoengine/core';

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
interface AppConfigStructure extends CoreConfigStructure {}

const APP_CONFIG_DEFAULTS = mergeDeepOverrideLists(DEFAULT_CORE_CONFIG, {
    BRANDING: {
        LOGO_URL: 'assets/logo_code_copernicus_blue.png',
        LOGO_ICON_URL: 'assets/geoengine-favicon-white.svg',
        LOGO_ALT_URL: 'assets/geoengine-white.svg',
        PAGE_TITLE: 'Enhanced Data Viewer | CODE-DE Lab',
    },
    DEFAULTS: {
        PROJECT: {
            NAME: 'Default',
            TIME: '2026-01-01T00:00:00.000Z',
            TIMESTEP: '1 day',
            PROJECTION: 'EPSG:3857',
        },
        FOCUS_EXTENT: [6.98865807458, 47.3024876979, 15.0169958839, 54.983104153],
    },
}) as AppConfigStructure;

@Injectable()
export class AppConfig extends CoreConfig {
    protected override config!: AppConfigStructure;

    override load(): Promise<void> {
        return super.load(APP_CONFIG_DEFAULTS);
    }
}

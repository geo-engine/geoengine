import {Injectable} from '@angular/core';
import {mergeDeepOverrideLists} from '@geoengine/common';
import {CoreConfig, CoreConfigStructure, DEFAULT_CORE_CONFIG} from '@geoengine/core';

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
interface AppConfigStructure extends CoreConfigStructure {}

const APP_CONFIG_DEFAULTS = mergeDeepOverrideLists(DEFAULT_CORE_CONFIG, {
    BRANDING: {
        LOGO_URL: 'assets/CODE-DE-Lab_RGB.svg',
        LOGO_ICON_URL: 'favicon.ico',
        LOGO_ALT_URL: 'assets/CODE-DE-Lab_white_RGB.svg',
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
    MAP: {
        DRAWING: {
            DRAW_STYLE: {
                STROKE_COLOR: 'rgba(62, 163, 220, 0.8)',
                STROKE_CONTRAST_COLOR: '#FFFFFF',
                FILL_COLOR: 'rgba(62, 163, 220, 0.1)',
                WIDTH: 2,
                IMAGE_WIDTH: 4,
                DASH_PATTERN: [8, 8],
            },
            AFTER_DRAW_STYLE: {
                STROKE_COLOR: 'rgba(62, 163, 220, 1)',
                STROKE_CONTRAST_COLOR: '#FFFFFF',
                FILL_COLOR: 'rgba(62, 163, 220, 0.2)',
                WIDTH: 2,
                IMAGE_WIDTH: 4,
                DASH_PATTERN: [8, 8],
            },
        },
    },
});

@Injectable()
export class AppConfig extends CoreConfig {
    protected override config!: AppConfigStructure;

    override load(): Promise<void> {
        return super.load(APP_CONFIG_DEFAULTS);
    }
}

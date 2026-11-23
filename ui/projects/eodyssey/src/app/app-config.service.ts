import {Injectable} from '@angular/core';
import {mergeDeepOverrideLists} from '@geoengine/common';
import {CoreConfig, CoreConfigStructure, DEFAULT_CORE_CONFIG} from '@geoengine/core';
export const DEFAULT_VIEWPORT_LIMITS = {MAX_SOURCE_PIXELS: 20_000_000_000, MAX_STAC_SEARCHES: 64};

interface EOdysseyConfigStructure extends CoreConfigStructure {
    VIEWPORT_LIMITS: typeof DEFAULT_VIEWPORT_LIMITS;
}

const APP_CONFIG_DEFAULTS = mergeDeepOverrideLists(DEFAULT_CORE_CONFIG, {
    VIEWPORT_LIMITS: DEFAULT_VIEWPORT_LIMITS,
    BRANDING: {
        LOGO_URL: 'assets/eodyssey-logo.svg',
        LOGO_ICON_URL: 'assets/eodyssey-icon.svg',
        LOGO_ALT_URL: 'assets/eodyssey-logo-negative.svg',
        PAGE_TITLE: 'EOdyssey | CODE-DE Lab',
    },
    DEFAULTS: {
        PROJECT: {
            NAME: 'Default',
            TIME: '2026-04-01T00:00:00.000Z',
            TIMESTEP: '1 day',
            PROJECTION: 'EPSG:3857',
        },
        FOCUS_EXTENT: [6.98865807458, 47.3024876979, 15.0169958839, 54.983104153],
    },
    MAP: {
        DRAWING: {
            DRAW_STYLE: {
                STROKE_COLOR: 'rgba(255, 130, 37, 0.8)',
                STROKE_CONTRAST_COLOR: '#FFFFFF',
                FILL_COLOR: 'rgba(255, 130, 37, 0.1)',
                WIDTH: 2,
                IMAGE_WIDTH: 4,
                DASH_PATTERN: [8, 8],
            },
            AFTER_DRAW_STYLE: {
                STROKE_COLOR: 'rgba(255, 130, 37, 1)',
                STROKE_CONTRAST_COLOR: '#FFFFFF',
                FILL_COLOR: 'rgba(255, 130, 37, 0.2)',
                WIDTH: 2,
                IMAGE_WIDTH: 4,
                DASH_PATTERN: [8, 8],
            },
        },
    },
});

@Injectable()
export class AppConfig extends CoreConfig {
    protected override config!: EOdysseyConfigStructure;

    get VIEWPORT_LIMITS(): typeof DEFAULT_VIEWPORT_LIMITS {
        return this.config.VIEWPORT_LIMITS;
    }

    override async load(): Promise<void> {
        await super.load(APP_CONFIG_DEFAULTS);
        const limits = this.VIEWPORT_LIMITS;
        if (!limits || ![limits.MAX_SOURCE_PIXELS, limits.MAX_STAC_SEARCHES].every((value) => Number.isSafeInteger(value) && value > 0)) {
            throw new Error('VIEWPORT_LIMITS must contain positive JavaScript-safe integer limits');
        }
    }
}

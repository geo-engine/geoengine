import {Injectable} from '@angular/core';
import {mergeDeepOverrideLists} from '@geoengine/common';
import {CoreConfig, CoreConfigStructure, DEFAULT_CORE_CONFIG} from '@geoengine/core';

interface Components {
    readonly PLAYBACK: {
        readonly AVAILABLE: boolean;
    };
    readonly MAP_RESOLUTION_EXTENT_OVERLAY: {
        readonly AVAILABLE: boolean;
    };
}

interface Routes {
    readonly MANAGER: boolean;
}

interface AppConfigStructure extends CoreConfigStructure {
    readonly COMPONENTS: Components;
    readonly ROUTES: Routes;
}

const APP_CONFIG_DEFAULTS = mergeDeepOverrideLists(DEFAULT_CORE_CONFIG, {
    COMPONENTS: {
        PLAYBACK: {
            AVAILABLE: false,
        },
        REGISTRATION: {
            AVAILABLE: true,
        },
        MAP_RESOLUTION_EXTENT_OVERLAY: {
            AVAILABLE: false,
        },
    },
    BRANDING: {
        LOGO_URL: 'assets/geoengine.svg',
        LOGO_ICON_URL: 'assets/geoengine-favicon-white.svg',
        LOGO_ALT_URL: 'assets/geoengine-white.svg',
        PAGE_TITLE: 'Geo Engine',
    },
    ROUTES: {
        MANAGER: true,
    },
}) as AppConfigStructure;

@Injectable()
export class AppConfig extends CoreConfig {
    protected override config!: AppConfigStructure;

    get COMPONENTS(): Components {
        return this.config.COMPONENTS;
    }

    get ROUTES(): Routes {
        return this.config.ROUTES;
    }

    override load(): Promise<void> {
        return super.load(APP_CONFIG_DEFAULTS);
    }
}

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

interface AppConfigStructure extends CoreConfigStructure {
    readonly COMPONENTS: Components;
}

const APP_CONFIG_DEFAULTS = mergeDeepOverrideLists(DEFAULT_CORE_CONFIG, {
    COMPONENTS: {
        PLAYBACK: {
            AVAILABLE: false,
        },
        MAP_RESOLUTION_EXTENT_OVERLAY: {
            AVAILABLE: true,
        },
    },
}) as AppConfigStructure;

@Injectable()
export class AppConfig extends CoreConfig {
    protected override config!: AppConfigStructure;

    get COMPONENTS(): Components {
        return this.config.COMPONENTS;
    }

    override async load(): Promise<void> {
        return super.load(APP_CONFIG_DEFAULTS);
    }
}

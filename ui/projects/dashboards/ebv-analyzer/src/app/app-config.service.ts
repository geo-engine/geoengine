import {Injectable} from '@angular/core';
import {mergeDeepOverrideLists} from '@geoengine/common';
import {CoreConfig, CoreConfigStructure, DEFAULT_CORE_CONFIG} from '@geoengine/core';

interface Components {
    readonly PLAYBACK: {
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
    },
}) as AppConfigStructure;

@Injectable()
export class AppConfig extends CoreConfig {
    protected override config!: AppConfigStructure;

    get COMPONENTS(): Components {
        return this.config.COMPONENTS;
    }

    override load(): Promise<void> {
        return super.load(APP_CONFIG_DEFAULTS);
    }
}

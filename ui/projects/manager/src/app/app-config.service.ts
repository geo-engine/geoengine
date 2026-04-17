import {Injectable} from '@angular/core';
import {CommonConfig, CommonConfigStructure, DEFAULT_COMMON_CONFIG, mergeDeepOverrideLists} from '@geoengine/common';

/**
 * The structure of the config file containing the URL of the backend API and the branding information.
 */
export interface AppConfigStructure extends CommonConfigStructure {
    readonly BRANDING: Branding;
    readonly DEFAULTS: ConfigDefaults;
}

/**
 * Default values
 */
export interface ConfigDefaults {
    readonly SNACKBAR_DURATION: number;
}

/**
 * Information about the branding of the Geo Engine instance.
 */
interface Branding {
    readonly LOGO_URL: string;
    readonly LOGO_ICON_URL: string;
    readonly LOGO_ALT_URL: string;
    readonly PAGE_TITLE: string;
    readonly HOMEPAGE?: Homepage;
}

/**
 * Specifies a link to a homepage (e.g. project website) and a button image.
 */
interface Homepage {
    readonly URL: string;
    readonly BUTTON_IMAGE_URL: string;
    readonly BUTTON_ALT_TEXT: string;
    readonly BUTTON_TOOLTIP_TEXT: string;
}

const APP_CONFIG_DEFAULTS = mergeDeepOverrideLists(DEFAULT_COMMON_CONFIG, {
    DEFAULTS: {
        SNACKBAR_DURATION: 2000,
    },
    USER: {
        AUTO_GUEST_LOGIN: false,
        LOCAL_LOGIN_AVAILABLE: true,
    },
}) as AppConfigStructure;

@Injectable()
export class AppConfig extends CommonConfig {
    protected override config!: AppConfigStructure;

    get DEFAULTS(): ConfigDefaults {
        return this.config.DEFAULTS;
    }

    // noinspection JSUnusedGlobalSymbols <- function used in parent app
    /**
     * Initialize the config on app start.
     */
    override async load(defaults: AppConfigStructure = APP_CONFIG_DEFAULTS): Promise<void> {
        await super.load(defaults);
        this.config = mergeDeepOverrideLists(defaults, {...this.config});
    }
}

import {Injectable} from '@angular/core';
import {Configuration, DefaultConfig} from '@geoengine/api-client';
import * as Immutable from 'immutable';
import {mergeWith} from 'immutable';

/**
 * The structure of the config file containing the URL of the backend API and the branding information.
 */
export interface CommonConfigStructure {
    readonly API_URL: string;
    readonly DELAYS: Delays;
    readonly PLOTS: Plots;
    readonly USER: User;
    readonly BRANDING: Branding;
}

export interface Branding {
    readonly LOGO_URL: string;
    readonly LOGO_ICON_URL: string;
    readonly LOGO_ALT_URL: string;
    readonly PAGE_TITLE: string;
    readonly HOMEPAGE?: Homepage;
}

export interface Homepage {
    readonly URL: string;
    readonly BUTTON_IMAGE_URL: string;
    readonly BUTTON_ALT_TEXT: string;
    readonly BUTTON_TOOLTIP_TEXT: string;
}

/**
 * Delay values for different actions.
 */
export interface Delays {
    readonly DEBOUNCE: number;
}

interface Plots {
    readonly THEME: 'excel' | 'ggplot2' | 'quartz' | 'vox' | 'dark';
}

export const DEFAULT_COMMON_CONFIG: CommonConfigStructure = {
    API_URL: '/api',
    BRANDING: {
        LOGO_URL: '/assets/geoengine.svg',
        LOGO_ICON_URL: '/assets/geoengine-favicon-white.svg',
        LOGO_ALT_URL: '/assets/geoengine-white.svg',
        PAGE_TITLE: 'Geo Engine',
    },
    DELAYS: {
        DEBOUNCE: 400,
    },
    PLOTS: {
        THEME: 'excel',
    },
    USER: {
        GUEST: {
            NAME: 'guest',
            PASSWORD: 'guest',
        },
        AUTO_GUEST_LOGIN: true,
        REGISTRATION_AVAILABLE: true,
        LOCAL_LOGIN_AVAILABLE: false,
    },
};

interface User {
    readonly GUEST: {
        readonly NAME: string;
        readonly PASSWORD: string;
    };
    readonly AUTO_GUEST_LOGIN: boolean;
    readonly REGISTRATION_AVAILABLE: boolean;
    readonly LOCAL_LOGIN_AVAILABLE: boolean;
}

@Injectable()
export class CommonConfig {
    static readonly CONFIG_FILE = 'assets/config.json';

    protected config!: CommonConfigStructure;

    get API_URL(): string {
        return this.config.API_URL;
    }

    get BRANDING(): Branding {
        return this.config.BRANDING;
    }

    get DELAYS(): Delays {
        return this.config.DELAYS;
    }

    get PLOTS(): Plots {
        return this.config.PLOTS;
    }

    get USER(): User {
        return this.config.USER;
    }

    // noinspection JSUnusedGlobalSymbols <- function used in parent app
    /**
     * Initialize the config on app start.
     */
    public async load(defaults: CommonConfigStructure = DEFAULT_COMMON_CONFIG): Promise<void> {
        const configFileResponse = await fetch(CommonConfig.CONFIG_FILE);

        const appConfig = await configFileResponse.json().catch(() => ({}));
        this.config = mergeDeepOverrideLists(defaults, {...appConfig});

        // we alter the config in the openapi-client so that it uses the correct API_URL
        DefaultConfig.config = new Configuration({
            basePath: this.config.API_URL,
            fetchApi: DefaultConfig.fetchApi,
            middleware: DefaultConfig.middleware,
            queryParamsStringify: DefaultConfig.queryParamsStringify,
            username: DefaultConfig.username,
            password: DefaultConfig.password,
            apiKey: DefaultConfig.apiKey,
            accessToken: DefaultConfig.accessToken,
            headers: DefaultConfig.headers,
            credentials: DefaultConfig.credentials,
        });
    }
}

/**
 * A version of ImmutableJS `mergeDeep` that replaces Lists instead of concatenating them.
 */
export function mergeDeepOverrideLists<C>(a: C, b: Iterable<unknown> | Iterable<[unknown, unknown]> | Record<string, unknown>): C {
    // If b is null, it would overwrite a, even if a is mergeable
    if (b === null) return b;

    if (a && typeof a === 'object' && !Array.isArray(a) && !Immutable.List.isList(a)) {
        return mergeWith(mergeDeepOverrideLists as (a: unknown, b: unknown) => unknown, a, b);
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return b as any;
}

import {computed, effect, inject, Injectable, Signal, signal, WritableSignal} from '@angular/core';
import {Basemap, Basemaps, CoreConfig, VectorTiles, Wms} from '../config.service';

const PATH_PREFIX = window.location.pathname.replace(/\//g, '_').replace(/-/g, '_'); // TODO: de-duplicate with user service?
const STORAGE_KEY = 'basemap';

interface NamedBasemap extends Basemap {
    name: string;
}

@Injectable({providedIn: 'root'})
export class BasemapService {
    protected readonly config = inject(CoreConfig);

    protected readonly selectedBasemap: WritableSignal<NamedBasemap>;
    public readonly basemapProjections: Signal<Array<string>> = computed(() => {
        const currentBasemap = this.selectedBasemap();
        return allowedBasemapProjections(currentBasemap);
    });

    constructor() {
        const defaultBasemap = this.config.MAP.DEFAULT_BASEMAP;
        checkBasemaps(this.basemaps, defaultBasemap);

        const preferredBasemap = loadBasemapPreferenceFromBrowser();

        let basemap: NamedBasemap = {
            name: defaultBasemap,
            ...this.basemaps[defaultBasemap],
        };

        if (preferredBasemap && this.basemaps[preferredBasemap]) {
            basemap = {
                name: preferredBasemap,
                ...this.basemaps[preferredBasemap],
            };
        }

        this.selectedBasemap = signal<NamedBasemap>(basemap);

        effect(() => {
            const currentBasemap = this.selectedBasemap();

            if (!currentBasemap || currentBasemap.name === defaultBasemap) {
                return removeBasemapPreferenceInBrowser();
            }

            saveBasemapPreferenceInBrowser(currentBasemap.name);
        });
    }

    get basemap(): Signal<Basemap> {
        return this.selectedBasemap.asReadonly();
    }

    get selectedBasemapName(): Signal<string> {
        return computed(() => this.selectedBasemap().name);
    }

    get basemaps(): Basemaps {
        return this.config.MAP.BASEMAPS;
    }

    selectBasemap(name: string): void {
        const basemap = this.basemaps[name];
        if (!basemap) {
            throw new Error(`Basemap ${name} is not defined in the configuration`);
        }

        this.selectedBasemap.set({
            name,
            ...basemap,
        } as NamedBasemap);
    }
}

/** minimum type checking
 *
 *  TODO: proper type parsing/checking
 */
function checkBasemaps(basemaps: Basemaps, defaultBasemap: string): void {
    if (!defaultBasemap) {
        throw new Error('CONFIG ERROR: Default basemap undefined');
    }

    if (!basemaps[defaultBasemap]) {
        throw new Error('CONFIG ERROR: Default basemap is not in basemap list');
    }

    for (const basemap of Object.values(basemaps)) {
        const BASEMAP_TYPES = ['WMS', 'MVT'];
        if (!BASEMAP_TYPES.includes(basemap.TYPE)) {
            throw new Error(`CONFIG ERROR: Basemap type ${basemap.TYPE} is not supported (supported types: ${BASEMAP_TYPES.join(', ')})`);
        }
    }
}

function saveBasemapPreferenceInBrowser(preferredBasemap: string): void {
    localStorage.setItem(PATH_PREFIX + STORAGE_KEY, preferredBasemap);
}

const loadBasemapPreferenceFromBrowser = (): string | undefined => localStorage.getItem(PATH_PREFIX + STORAGE_KEY) ?? undefined;

function removeBasemapPreferenceInBrowser(): void {
    localStorage.removeItem(PATH_PREFIX + STORAGE_KEY);
}

export function allowedBasemapProjections(basemap: Basemap): Array<string> {
    switch (basemap.TYPE) {
        case 'MVT': {
            const vectorTilesBasemap = basemap as VectorTiles;
            return Object.keys(vectorTilesBasemap.LAYER_EXTENTS);
        }
        case 'WMS': {
            const wmsBasemap = basemap as Wms;
            return wmsBasemap.PROJECTIONS;
        }
    }
}

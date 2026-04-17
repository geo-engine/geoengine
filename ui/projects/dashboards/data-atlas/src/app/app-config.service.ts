import {Injectable} from '@angular/core';
import {mergeDeepOverrideLists} from '@geoengine/common';
import {CoreConfig, CoreConfigStructure, DEFAULT_CORE_CONFIG} from '@geoengine/core';

interface Data {
    readonly RASTER4D: {
        readonly PROVIDER: string;
        readonly COLLECTION: string;
    };
    readonly RASTER_OTHER: {
        readonly PROVIDER: string;
        readonly COLLECTION: string;
    };
    readonly VECTOR: {
        readonly PROVIDER: string;
        readonly COLLECTION: string;
    };
}

interface AppConfigStructure extends CoreConfigStructure {
    readonly DATA: Data;
}

const APP_CONFIG_DEFAULTS = mergeDeepOverrideLists(DEFAULT_CORE_CONFIG, {
    DEFAULTS: {
        PROJECT: {
            NAME: 'Default',
            TIME: '1950-01-01T00:00:00.000Z',
            TIMESTEP: '100 years',
            PROJECTION: 'EPSG:4326',
        },
        FOCUS_EXTENT: [-10.0, 33.0, 43.0, 720],
    },
    DATA: {
        RASTER4D: {
            PROVIDER: '1690c483-b17f-4d98-95c8-00a64849cd0b',
            COLLECTION: 'root',
        },
        RASTER_OTHER: {
            PROVIDER: 'ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74',
            COLLECTION: 'ddacf46f-b28c-4e05-aae4-58e933cfdec3',
        },
        VECTOR: {
            PROVIDER: 'ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74',
            COLLECTION: '77004f1b-136d-4392-8be9-e88da1528838',
        },
    },
    MAP: {
        BACKGROUND_LAYER: 'MVT',
        BACKGROUND_LAYER_URL: 'https://basemap.geoengine.io/natural-earth-v2/{epsg}/{z}/{x}/{y}.pbf',
        VECTOR_TILES: {
            STYLE_URL: 'assets/mvt/ne-ge-europe.json',
            SOURCE: 'ne',
            BACKGROUND_LAYER_EXTENTS: {
                // eslint-disable-next-line @typescript-eslint/naming-convention
                'EPSG:4326': [-180, -180, 180, 180],
                // eslint-disable-next-line @typescript-eslint/naming-convention
                'EPSG:3857': [-20037508.3427892, -20037508.3427892, 20037508.3427892, 20037508.3427892],
            },
            MAX_ZOOM: 22,
        },
        VALID_CRS: ['EPSG:4326'],
    },
}) as AppConfigStructure;

@Injectable()
export class AppConfig extends CoreConfig {
    protected override config!: AppConfigStructure;

    get DATA(): Data {
        return this.config.DATA;
    }

    override async load(): Promise<void> {
        return super.load(APP_CONFIG_DEFAULTS);
    }
}

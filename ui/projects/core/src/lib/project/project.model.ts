import {UUID, ToDict, STRectangleDict, BBoxDict} from '../backend/backend.model';
import {
    Layer,
    Plot,
    SpatialReference,
    Time,
    TimeStepDuration,
    timeStepDictTotimeStepDuration,
    timeStepDurationToTimeStepDict,
} from '@geoengine/common';
import {Project as ProjectDict, ProjectVersion} from '@geoengine/api-client';

export class Project implements ToDict<ProjectDict> {
    readonly id: UUID;
    readonly name: string;
    readonly description: string;

    readonly version: ProjectVersion;

    readonly _bbox: BBoxDict;

    readonly _spatialReference: SpatialReference;

    readonly _plots: Array<Plot>;
    readonly _layers: Array<Layer>;

    readonly _time: Time;
    readonly _timeStepDuration: TimeStepDuration;

    constructor(config: {
        id: UUID;
        name: string;
        description: string;
        spatialReference: SpatialReference;
        time: Time;
        plots: Array<Plot>;
        layers: Array<Layer>;
        timeStepDuration: TimeStepDuration;
        bbox: BBoxDict;
        version: ProjectVersion;
    }) {
        this.id = config.id;
        this.name = config.name;
        this.description = config.description;
        this._spatialReference = config.spatialReference;
        this._time = config.time;
        this._plots = config.plots;
        this._layers = config.layers;
        this._timeStepDuration = config.timeStepDuration;
        this._bbox = config.bbox;
        this.version = config.version;
    }

    static fromDict(dict: ProjectDict): Project {
        return new Project({
            id: dict.id,
            name: dict.name,
            description: dict.description,
            spatialReference: SpatialReference.fromSrsString(dict.bounds.spatialReference),
            time: Time.fromDict(dict.bounds.timeInterval),
            plots: dict.plots.map(Plot.fromDict),
            layers: dict.layers.map(Layer.fromDict),
            timeStepDuration: timeStepDictTotimeStepDuration(dict.timeStep),
            bbox: dict.bounds.boundingBox,
            version: dict.version,
        });
    }

    updateFields(changes: {
        id?: UUID;
        name?: string;
        description?: string;
        spatialReference?: SpatialReference;
        time?: Time;
        plots?: Array<Plot>;
        layers?: Array<Layer>;
        timeStepDuration?: TimeStepDuration;
        bbox?: BBoxDict;
        version?: ProjectVersion;
    }): Project {
        return new Project({
            id: changes.id ?? this.id,
            name: changes.name ?? this.name,
            description: changes.description ?? this.description,
            spatialReference: changes.spatialReference ?? this.spatialReference,
            time: changes.time ?? this.time,
            plots: changes.plots ?? this.plots,
            layers: changes.layers ?? this.layers,
            timeStepDuration: changes.timeStepDuration ?? this.timeStepDuration,
            bbox: changes.bbox ?? this._bbox,
            version: changes.version ?? this.version,
        });
    }

    get time(): Time {
        return this._time;
    }

    get spatialReference(): SpatialReference {
        return this._spatialReference;
    }

    get plots(): Array<Plot> {
        return this._plots;
    }

    get layers(): Array<Layer> {
        return this._layers;
    }

    get timeStepDuration(): TimeStepDuration {
        return this._timeStepDuration;
    }

    toDict(): ProjectDict {
        return {
            id: this.id,
            name: this.name,
            description: this.description,
            version: this.version, // TODO: get rid of version?
            bounds: this.toBoundsDict(),
            layers: this._layers.map((layer) => layer.toDict()),
            plots: this._plots.map((plot) => plot.toDict()),
            timeStep: timeStepDurationToTimeStepDict(this.timeStepDuration),
        };
    }

    toBoundsDict(): STRectangleDict {
        return {
            spatialReference: this._spatialReference.srsString,
            timeInterval: this._time.toDict(),
            boundingBox: this._bbox,
        };
    }

    toJSON(): string {
        return JSON.stringify(this.toDict());
    }
}

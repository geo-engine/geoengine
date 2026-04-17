import {UUID} from '../datasets/dataset.model';
import {ToDict} from '../time/time.model';
import {Plot as PlotDict} from '@geoengine/api-client';

export type PlotType = 'JSON' | 'PNG';

export class Plot implements HasPlotId, ToDict<PlotDict> {
    readonly id: number;
    readonly name: string;
    readonly workflowId: UUID;

    protected static nextPlotId = 0;

    constructor(config: {id?: number; name: string; workflowId: string}) {
        this.id = config.id ?? Plot.nextPlotId++;

        this.name = config.name;
        this.workflowId = config.workflowId;
    }

    static fromDict(dict: PlotDict): Plot {
        return new Plot({
            name: dict.name,
            workflowId: dict.workflow,
        });
    }

    updateFields(changes: {id?: number; name?: string; workflowId?: string}): Plot {
        return new Plot({
            id: changes.id ?? this.id,
            name: changes.name ?? this.name,
            workflowId: changes.workflowId ?? this.workflowId,
        });
    }

    equals(other: Plot): boolean {
        if (!(other instanceof Plot)) {
            return false;
        }

        return this.id === other.id && this.name === other.name && this.workflowId === other.workflowId;
    }

    toDict(): PlotDict {
        return {
            name: this.name,
            workflow: this.workflowId,
        };
    }
}

export interface HasPlotId {
    readonly id: number;
}

export interface VegaChartData {
    readonly vegaString: string;
    readonly metadata?: {
        readonly selectionName?: string;
    };
}

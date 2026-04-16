import {SpatialGridDescriptor as SpatialGridDescriptorDict, SpatialGridDescriptorState} from '@geoengine/api-client';
import {ToDict} from '../time/time.model';
import {BoundingBox2D} from '../spatial-bounds/bounding-box';
import {SpatialGridDefinition} from './spatial-grid-definition.model';

export class SpatialGridDescriptor implements ToDict<SpatialGridDescriptorDict> {
    readonly spatialGrid: SpatialGridDefinition;
    readonly descriptor: 'derived' | 'source' = 'derived';

    constructor(spatialGrid: SpatialGridDefinition, descriptor: 'derived' | 'source') {
        this.spatialGrid = spatialGrid;
        this.descriptor = descriptor;
    }

    public bbox(): BoundingBox2D {
        return this.spatialGrid.bbox();
    }

    toDict(): SpatialGridDescriptorDict {
        if (this.descriptor == 'source') {
            return {
                spatialGrid: this.spatialGrid.toDict(),
                descriptor: SpatialGridDescriptorState.Source,
            };
        }
        return {
            spatialGrid: this.spatialGrid.toDict(),
            descriptor: SpatialGridDescriptorState.Derived,
        };
    }

    public static fromDict(dict: SpatialGridDescriptorDict): SpatialGridDescriptor {
        const descriptor = dict.descriptor;

        return new SpatialGridDescriptor(SpatialGridDefinition.fromDict(dict.spatialGrid), descriptor);
    }
}

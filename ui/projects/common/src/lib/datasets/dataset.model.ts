import {Measurement} from '../layers/measurement';
import {
    RasterDataType,
    RasterDataTypes,
    VectorColumnDataType,
    VectorColumnDataTypes,
    VectorDataType,
    VectorDataTypes,
} from '../operators/datatype.model';
import {SourceOperatorDict} from '../operators/operator.model';
import {SpatialGridDescriptor} from '../spatial-grid/spatial-grid-descriptor.model';
import {SrsString} from '../spatial-references/spatial-reference.model';
import {Symbology} from '../symbology/symbology.model';
import {
    Dataset as DatasetDict,
    TypedResultDescriptor as TypedResultDescriptorDict,
    TypedVectorResultDescriptor as VectorResultDescriptorDict,
    TypedRasterResultDescriptor as RasterResultDescriptorDict,
    Workflow as WorkflowDict,
    RasterBandDescriptor as RasterBandDescriptor,
    TimeDescriptor,
} from '@geoengine/api-client';

export type UUID = string;

export type NamedDataDict = string;

export class Dataset {
    readonly id: UUID;
    readonly name: string;
    readonly displayName: string;
    readonly description: string;
    readonly resultDescriptor: ResultDescriptor;
    readonly sourceOperator: string;
    readonly symbology?: Symbology;

    constructor(config: DatasetDict) {
        this.id = config.id;
        this.name = config.name;
        this.displayName = config.displayName;
        this.description = config.description;
        this.resultDescriptor = ResultDescriptor.fromDict(config.resultDescriptor);
        this.sourceOperator = config.sourceOperator;
        this.symbology = config.symbology ? Symbology.fromDict(config.symbology) : undefined;
    }

    static fromDict(dict: DatasetDict): Dataset {
        return new Dataset(dict);
    }

    createSourceWorkflow(): WorkflowDict {
        return this.createSourceWorkflowWithOperator({
            type: this.sourceOperator,
            params: {
                data: this.name,
            },
        });
    }

    createSourceWorkflowWithOperator(operator: SourceOperatorDict): WorkflowDict {
        return {
            type: this.resultDescriptor.getTypeString(),
            operator,
        };
    }
}

export abstract class ResultDescriptor {
    readonly spatialReference: SrsString;

    protected constructor(spatialReference: SrsString) {
        this.spatialReference = spatialReference;
    }

    static fromDict(dict: TypedResultDescriptorDict): ResultDescriptor {
        if (dict.type === 'vector') {
            return VectorResultDescriptor.fromDict(dict);
        } else if (dict.type === 'raster') {
            return RasterResultDescriptor.fromDict(dict);
        }

        throw Error('invalid result descriptor type');
    }

    abstract getTypeString(): 'Vector' | 'Raster';
}

export class RasterResultDescriptor extends ResultDescriptor {
    readonly dataType: RasterDataType;
    readonly bands: Array<RasterBandDescriptor>;
    readonly spatialGrid: SpatialGridDescriptor;
    readonly time: TimeDescriptor;

    constructor(config: RasterResultDescriptorDict) {
        super(config.spatialReference);
        this.dataType = RasterDataTypes.fromCode(config.dataType);
        this.bands = config.bands;
        this.spatialGrid = SpatialGridDescriptor.fromDict(config.spatialGrid);
        this.time = config.time;
    }

    static override fromDict(dict: RasterResultDescriptorDict): RasterResultDescriptor {
        return new RasterResultDescriptor(dict);
    }

    getTypeString(): 'Vector' | 'Raster' {
        return 'Raster';
    }
}

export class VectorResultDescriptor extends ResultDescriptor {
    readonly dataType: VectorDataType;
    readonly columns: Map<string, VectorColumnDataType>;
    readonly measurements: Map<string, Measurement>;

    constructor(config: VectorResultDescriptorDict) {
        super(config.spatialReference);
        this.dataType = VectorDataTypes.fromCode(config.dataType);
        this.columns = new Map(Object.entries(config.columns).map(([key, value]) => [key, VectorColumnDataTypes.fromCode(value.dataType)]));
        this.measurements = new Map(Object.entries(config.columns).map(([key, value]) => [key, Measurement.fromDict(value.measurement)]));
    }

    static override fromDict(dict: VectorResultDescriptorDict): ResultDescriptor {
        return new VectorResultDescriptor(dict);
    }

    getTypeString(): 'Vector' | 'Raster' {
        return 'Vector';
    }
}

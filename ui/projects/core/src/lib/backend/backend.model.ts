import {SpatialGridDescriptor, TimeDescriptor} from '@geoengine/api-client';

export type UUID = string;
type TimestampString = string;
export type SrsString = string;

/**
 * Marker dictionary for types that only use primitive types and sub-types.
 */
// eslint-disable-next-line @typescript-eslint/no-empty-object-type
interface SerializableDict {}

interface CoordinateDict {
    x: number;
    y: number;
}

export interface BBoxDict {
    lowerLeftCoordinate: CoordinateDict;
    upperRightCoordinate: CoordinateDict;
}

interface SpatialResolution {
    x: number;
    y: number;
}

/**
 * UNIX time in Milliseconds
 *
 * TODO: For input, allow ISO 8601 strings
 */
export interface TimeIntervalDict {
    start: number;
    end: number;
}

export interface STRectangleDict {
    spatialReference: SrsString;
    boundingBox: BBoxDict;
    timeInterval: TimeIntervalDict;
}

export interface CreateProjectResponseDict {
    id: UUID;
}

export interface ProjectListingDict {
    id: UUID;
    name: string;
    description: string;
    layerNames: Array<string>;
    changed: TimestampString;
}

export type ProjectPermissionDict = 'Read' | 'Write' | 'Owner';

export type ProjectFilterDict = 'None' | {name: {term: string}} | {description: {term: string}};

export type ProjectOrderByDict = 'DateAsc' | 'DateDesc' | 'NameAsc' | 'NameDesc';

export interface PlotDict {
    workflow: UUID;
    name: string;
}

export interface BackendInfoDict {
    buildDate?: Date;
    commitHash?: string;
    version?: string;
    features?: string;
}

export interface ToDict<T> {
    toDict(): T;
}

interface OperatorDict {
    type: string;
    params: OperatorParams | null;
    sources: OperatorSourcesDict;
}

type OperatorSourcesDict = Record<string, OperatorDict | SourceOperatorDict | Array<OperatorDict | SourceOperatorDict> | undefined>;

type ParamTypes = string | number | boolean | Array<ParamTypes> | {[key: string]: ParamTypes} | SerializableDict | undefined;

export type OperatorParams = Record<string, ParamTypes>;

export type NamedDataDict = string;

export interface SourceOperatorDict {
    type: string;
    params: {
        data: NamedDataDict;
    };
}

export interface TimeStepDict {
    step: number;
    granularity: TimeStepGranularityDict;
}

export type TimeStepGranularityDict = 'millis' | 'seconds' | 'minutes' | 'hours' | 'days' | 'months' | 'years';

type DataIdDict = InternalDataIdDict | ExternalDataIdDict;

interface InternalDataIdDict {
    type: 'internal';
    datasetId: UUID;
}
interface ExternalDataIdDict {
    type: 'external';
    providerId: UUID;
    layerId: string;
}

export type DatasetOrderByDict = 'NameAsc' | 'NameDesc';

export interface PlotDataDict {
    plotType: string;
    outputFormat: 'JsonPlain' | 'JsonVega' | 'ImagePng';
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    data: any;
}

interface ResultDescriptorDict {
    type: 'raster' | 'vector' | 'plot';
    spatialReference: SrsString;
    time?: TimeDescriptor;
}

interface RasterBandDescriptorDict {
    name: string;
    measurement: MeasurementDict;
}

export interface RasterResultDescriptorDict extends ResultDescriptorDict {
    type: 'raster';
    dataType: 'U8' | 'U16' | 'U32' | 'U64' | 'I8' | 'I16' | 'I32' | 'I64' | 'F32' | 'F64';
    bands: RasterBandDescriptorDict[];
    time: TimeDescriptor;
    spatialGrid: SpatialGridDescriptor;
    resolution?: SpatialResolution;
}

export interface VectorResultDescriptorDict extends ResultDescriptorDict {
    type: 'vector';
    dataType: VectorDataType;
    columns: Record<string, VectorColumnInfoDict>;
    bbox?: BBoxDict;
}

interface VectorColumnInfoDict {
    dataType: VectorColumnType;
    measurement: MeasurementDict;
}

type VectorColumnType = 'categorical' | 'int' | 'float' | 'text' | 'dateTime' | 'bool';

type VectorDataType = 'Data' | 'MultiPoint' | 'MultiLineString' | 'MultiPolygon';

type MeasurementDict = UnitLessMeasurementDict | ContinuousMeasurementDict | ClassificationMeasurementDict;

interface UnitLessMeasurementDict {
    type: 'unitless';
}

interface ContinuousMeasurementDict {
    type: 'continuous';
    measurement: string;
    unit?: string;
}

interface ClassificationMeasurementDict {
    type: 'classification';
    measurement: string;
    classes: Record<number, string>;
}

export interface UploadResponseDict {
    id: UUID;
}

export interface DatasetNameResponseDict {
    datasetName: string;
}

export interface AutoCreateDatasetDict {
    upload: UUID;
    datasetName: string;
    datasetDescription: string;
    mainFile: string;
    layerName?: string;
}

export interface UploadFilesResponseDict {
    files: Array<string>;
}

export interface UploadFileLayersResponseDict {
    layers: Array<string>;
}

export interface ProvenanceDict {
    citation: string;
    license: string;
    uri: string;
}

export interface ProvenanceEntryDict {
    provenance: ProvenanceDict;
    data: Array<DataIdDict>;
}

export interface SpatialReferenceSpecificationDict {
    name: string;
    spatialReference: SrsString;
    projString: string;
    extent: BBoxDict;
    axisLabels?: [string, string];
}

export interface DataSetProviderListingDict {
    id: UUID;
    typeName: string;
    name: string;
}

export interface GeoEngineErrorDict {
    readonly error: string;
    readonly message: string;
}

export interface LayerCollectionItemDict {
    type: 'collection' | 'layer';
    id: ProviderLayerIdDict | ProviderLayerCollectionIdDict;
    name: string;
    description: string;
    properties: Array<[string, string]>;
}

export interface ProviderLayerIdDict {
    providerId: UUID;
    layerId: string;
}

export interface ProviderLayerCollectionIdDict {
    providerId: UUID;
    collectionId: string;
}

export interface LayerCollectionListingDict extends LayerCollectionItemDict {
    type: 'collection';
    id: ProviderLayerCollectionIdDict;
    entryLabel: string;
}

export interface LayerCollectionLayerDict extends LayerCollectionItemDict {
    type: 'layer';
    id: ProviderLayerIdDict;
}

export interface LayerCollectionDict {
    id: ProviderLayerCollectionIdDict;
    name: string;
    description: string;
    items: LayerCollectionItemDict[];
    properties: Array<[string, string]>;
    entryLabel?: string;
}

export interface WcsParamsDict {
    service: 'WCS';
    request: 'GetCoverage';
    version: '1.1.1';
    identifier: string;
    boundingbox: string;
    format: 'image/tiff';
    gridbasecrs: string;
    gridcs: 'urn:ogc:def:cs:OGC:0.0:Grid2dSquareCS';
    gridtype: 'urn:ogc:def:method:WCS:1.1:2dSimpleGrid';
    gridorigin: string;
    gridoffsets: string;
    time: string;
    nodatavalue?: string;
}

export interface WfsParamsDict {
    workflowId: UUID;
    bbox: BBoxDict;
    time?: TimeIntervalDict;
    srsName?: SrsString;
    namespaces?: string;
    count?: number;
    sortBy?: string;
    resultType?: string;
    filter?: string;
    propertyName?: string;
}

export interface QuotaDict {
    available: number;
    used: number;
}

export type TaskStatusType = 'running' | 'completed' | 'aborted' | 'failed';

export interface TaskStatusDict {
    taskId: UUID;
    status: TaskStatusType;
}

interface Role {
    id: UUID;
    name: string;
}

export interface RoleDescription {
    role: Role;
    individual: boolean;
}

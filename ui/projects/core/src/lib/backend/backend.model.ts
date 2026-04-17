import {SpatialGridDescriptor, TimeDescriptor} from '@geoengine/api-client';

export type UUID = string;
export type TimestampString = string;
export type SrsString = string;

/**
 * Marker dictionary for types that only use primitive types and sub-types.
 */
// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface SerializableDict {}

export interface RegistrationDict {
    id: UUID;
}

export interface UserDict {
    id: UUID;
    email?: string;
    realName?: string;
}

export interface AuthCodeRequestURL {
    url: string;
}

export interface CoordinateDict {
    x: number;
    y: number;
}

export interface BBoxDict {
    lowerLeftCoordinate: CoordinateDict;
    upperRightCoordinate: CoordinateDict;
}

export interface SpatialPartitionDict {
    upperLeftCoordinate: CoordinateDict;
    lowerRightCoordinate: CoordinateDict;
}

export interface SpatialResolution {
    x: number;
    y: number;
}

/**
 * UNIX timestamp in milliseconds.
 *
 * TODO: For input, allow ISO 8601 string
 */
export type TimeInstanceDict = string;

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

export interface ProjectVersion {
    id: UUID;
    changed: TimestampString;
    author: UUID;
}

export type ColorizerDict = LinearGradientDict | LogarithmitGradientDict | PaletteDict | RgbaColorizerDict;

export interface RgbaColorizerDict {
    type: 'rgba';
}

export interface LinearGradientDict {
    type: 'linearGradient';
    breakpoints: Array<BreakpointDict>;
    noDataColor: RgbaColorDict;
    overColor: RgbaColorDict;
    underColor: RgbaColorDict;
}

export interface LogarithmitGradientDict {
    type: 'logarithmicGradient';
    breakpoints: Array<BreakpointDict>;
    noDataColor: RgbaColorDict;
    overColor: RgbaColorDict;
    underColor: RgbaColorDict;
}

export interface PaletteDict {
    type: 'palette';
    colors: Record<string, RgbaColorDict>;
    noDataColor: RgbaColorDict;
    defaultColor: RgbaColorDict;
}

export type RgbaColorDict = [number, number, number, number];

export type SymbologyDict = RasterSymbologyDict | VectorSymbologyDict;
export type VectorSymbologyDict = PointSymbologyDict | LineSymbologyDict | PolygonSymbologyDict;

export interface RasterSymbologyDict {
    type: 'raster';
    opacity: number;
    rasterColorizer: RasterColorizerDict;
}

export type RasterColorizerDict = SingleBandRasterColorizerDict /* TODO: | MultiBandRasterColorizerDict */;

export interface SingleBandRasterColorizerDict {
    type: 'singleBand';
    band: number;
    bandColorizer: ColorizerDict;
}

export interface TextSymbologyDict {
    attribute: string;
    fillColor: ColorParamDict;
    stroke: StrokeParamDict;
}

export interface PointSymbologyDict {
    type: 'point';
    radius: NumberParamDict;
    fillColor: ColorParamDict;
    stroke: StrokeParamDict;
    text?: TextSymbologyDict;
}

export interface LineSymbologyDict {
    type: 'line';
    stroke: StrokeParamDict;
    text?: TextSymbologyDict;
    autoSimplified: boolean;
}

export interface PolygonSymbologyDict {
    type: 'polygon';
    fillColor: ColorParamDict;
    stroke: StrokeParamDict;
    text?: TextSymbologyDict;
    autoSimplified: boolean;
}

export type NumberParamDict = StaticNumberDict | DerivedNumberDict;

export interface StaticNumberDict {
    type: 'static';
    value: number;
}

export interface DerivedNumberDict {
    type: 'derived';
    attribute: string;
    factor: number;
    defaultValue: number;
}

export type ColorParamDict = StaticColorDict | DerivedColorDict;

export interface StaticColorDict {
    type: 'static';
    color: RgbaColorDict;
}

export interface DerivedColorDict {
    type: 'derived';
    attribute: string;
    colorizer: ColorizerDict;
}

export interface StrokeParamDict {
    width: NumberParamDict;
    color: ColorParamDict;
    // TODO: dash
}

export interface BackendInfoDict {
    buildDate?: Date;
    commitHash?: string;
    version?: string;
    features?: string;
}

export interface BreakpointDict {
    value: number;
    color: RgbaColorDict;
}

export interface ErrorDict {
    error: string;
    message: string;
}

export interface ToDict<T> {
    toDict(): T;
}

export interface RegisterWorkflowResultDict {
    id: UUID;
}

export interface WorkflowDict {
    type: 'Vector' | 'Raster' | 'Plot';
    operator: OperatorDict | SourceOperatorDict;
}

export interface OperatorDict {
    type: string;
    params: OperatorParams | null;
    sources: OperatorSourcesDict;
}

export type OperatorSourcesDict = Record<string, OperatorDict | SourceOperatorDict | Array<OperatorDict | SourceOperatorDict> | undefined>;

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

export type DataIdDict = InternalDataIdDict | ExternalDataIdDict;

export interface InternalDataIdDict {
    type: 'internal';
    datasetId: UUID;
}
export interface ExternalDataIdDict {
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

export interface ResultDescriptorDict {
    type: 'raster' | 'vector' | 'plot';
    spatialReference: SrsString;
    time?: TimeDescriptor;
}

export type TypedResultDescriptorDict = VectorResultDescriptorDict | RasterResultDescriptorDict;

export interface RasterBandDescriptorDict {
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

export interface VectorColumnInfoDict {
    dataType: VectorColumnType;
    measurement: MeasurementDict;
}

export type VectorColumnType = 'categorical' | 'int' | 'float' | 'text' | 'dateTime' | 'bool';

type VectorDataType = 'Data' | 'MultiPoint' | 'MultiLineString' | 'MultiPolygon';

export type MeasurementDict = UnitLessMeasurementDict | ContinuousMeasurementDict | ClassificationMeasurementDict;

export interface UnitLessMeasurementDict {
    type: 'unitless';
}

export interface ContinuousMeasurementDict {
    type: 'continuous';
    measurement: string;
    unit?: string;
}

export interface ClassificationMeasurementDict {
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

export interface WorkflowIdResponseDict {
    id: UUID;
}

export interface AddDatasetDict {
    name?: string;
    displayName: string;
    description: string;
    sourceOperator: string;
    symbology?: SymbologyDict;
}

export interface AutoCreateDatasetDict {
    upload: UUID;
    datasetName: string;
    datasetDescription: string;
    mainFile: string;
    layerName?: string;
}

export interface SuggestMetaDataDict {
    dataPath: {upload: UUID} | {volume: string};
    mainFile?: string;
    layerName?: string;
}

export interface UploadFilesResponseDict {
    files: Array<string>;
}

export interface UploadFileLayersResponseDict {
    layers: Array<string>;
}

export interface OgrSourceDatasetDict {
    fileName: string;
    layerName: string;
    dataType?: VectorDataType;
    time: OgrSourceDatasetTimeTypeDict;
    columns?: OgrSourceColumnSpecDict;
    forceOgrTimeFilter: boolean;
    onError: 'ignore' | 'abort';
}

export type OgrSourceDatasetTimeTypeDict =
    | NoneOgrSourceDatasetTimeTypeDict
    | StartOgrSourceDatasetTimeTypeDict
    | StartEndOgrSourceDatasetTimeTypeDict
    | StartDurationOgrSourceDatasetTimeTypeDict;

export interface NoneOgrSourceDatasetTimeTypeDict {
    type: 'none';
}

export interface StartOgrSourceDatasetTimeTypeDict {
    type: 'start';
    startField: string;
    startFormat: OgrSourceTimeFormatDict;
    duration: OgrSourceDurationSpecDict;
}

export interface StartEndOgrSourceDatasetTimeTypeDict {
    type: 'startEnd';
    startField: string;
    startFormat: OgrSourceTimeFormatDict;
    endField: string;
    endFormat: OgrSourceTimeFormatDict;
}

export interface StartDurationOgrSourceDatasetTimeTypeDict {
    type: 'startDuration';
    startField: string;
    startFormat: OgrSourceTimeFormatDict;
    durationField: string;
}

export interface OgrSourceTimeFormatDict {
    format: 'unixTimeStamp' | 'auto' | 'custom';
    customFormat?: string;
    timestampType?: 'epochSeconds' | 'epochMilliseconds';
}

export interface OgrSourceColumnSpecDict {
    x: string;
    y?: string;
    float: Array<string>;
    int: Array<string>;
    text: Array<string>;
}

export type OgrSourceDurationSpecDict = ValueOgrSourceDurationSpecDict | InfiniteOgrSourceDurationSpecDict | ZeroOgrSourceDurationSpecDict;

export interface ValueOgrSourceDurationSpecDict extends TimeStepDict {
    type: 'value';
}

export interface InfiniteOgrSourceDurationSpecDict {
    type: 'infinite';
}

export interface ZeroOgrSourceDurationSpecDict {
    type: 'zero';
}

export interface TypedGeometryDict {
    Data?: '';
    MultiPoint?: MultiPointDict;
    MultiLineString?: MultiLineStringDict;
    MultiPolygon?: MultiPolygonDict;
}

export interface MultiPointDict {
    coordinates: Array<CoordinateDict>;
}

export interface MultiLineStringDict {
    coordinates: Array<Array<CoordinateDict>>;
}

export interface MultiPolygonDict {
    polygons: Array<PolygonDict>;
}

export type PolygonDict = Array<RingDict>;
export type RingDict = Array<CoordinateDict>;

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
export type TaskCleanUpStatusType = 'noCleanUp' | 'running' | 'completed' | 'aborted' | 'failed';

export interface TaskStatusDict {
    taskId: UUID;
    status: TaskStatusType;
}

export interface TaskRunningDict extends TaskStatusDict {
    status: 'running';
    timeStarted: string;
    estimatedTimeRemaining: string;
    pctComplete: string;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    info: any; // TODO: better type in backend
}

export interface TaskCompletedDict extends TaskStatusDict {
    status: 'completed';
    timeStarted: string;
    timeTotal: string;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    info: any; // TODO: better type in backend
}

export interface TaskAbortedDict extends TaskStatusDict {
    status: 'aborted';
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    cleanUp: any; // TODO: better type in backend
}

export interface TaskFailedDict extends TaskStatusDict {
    status: 'failed';
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    cleanUp: any; // TODO: better type in backend
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    error: any; // TODO: better type in backend
}

export interface TaskCleanUpDict {
    status: TaskCleanUpStatusType;
}

export interface TaskCleanUpCompletedDict extends TaskCleanUpDict {
    status: 'completed';
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    info: any; // TODO: better type in backend
}

export interface TaskCleanUpAbortedDict extends TaskCleanUpDict {
    status: 'aborted';
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    info: any; // TODO: better type in backend
}

export interface TaskCleanUpFailedDict extends TaskCleanUpDict {
    status: 'failed';
    error: string;
}

export interface Role {
    id: UUID;
    name: string;
}

export interface RoleDescription {
    role: Role;
    individual: boolean;
}

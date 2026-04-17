import {
    Measurement as MeasurementDict,
    TimeStep as TimeStepDict,
    TimeInterval as TimeIntervalDict,
    TimeGranularity as TimeStepGranularityDict,
    Coordinate2D as Coordinate2DDict,
    LegacyTypedOperatorOperator,
} from '@geoengine/api-client';
import {NamedDataDict} from '../datasets/dataset.model';
import {SrsString} from '../spatial-references/spatial-reference.model';

/**
 * Marker dictionary for types that only use primitive types and sub-types.
 */
// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface SerializableDict {}

type ParamTypes = string | number | boolean | Array<ParamTypes> | {[key: string]: ParamTypes} | SerializableDict | undefined;

export type OperatorParams = Record<string, ParamTypes>;

export interface WorkflowDict {
    type: 'Vector' | 'Raster' | 'Plot';
    operator: OperatorDict | SourceOperatorDict;
}

export type EmptyParams = Record<string, undefined>;

export interface OperatorDict extends LegacyTypedOperatorOperator {
    type: string;
    params: OperatorParams | undefined;
    sources: OperatorSourcesDict;
}

export type OperatorSourcesDict = Record<string, OperatorDict | SourceOperatorDict | Array<OperatorDict | SourceOperatorDict> | undefined>;

export interface SourceOperatorDict {
    type: string;
    params: {
        data: NamedDataDict;
    };
}

export interface ExpressionDict extends OperatorDict {
    type: 'Expression';
    params: {
        expression: string;
        outputType: string;
        outputBand: {
            name: string;
            measurement: MeasurementDict;
        };
        mapNoData: boolean;
    };
    sources: {
        raster: SourceOperatorDict | OperatorDict;
    };
}

export interface BandNeighborhoodAggregateDict extends OperatorDict {
    type: 'BandNeighborhoodAggregate';
    params: {
        aggregate: BandNeighborhoodAggregate;
    };
    sources: {
        raster: SourceOperatorDict | OperatorDict;
    };
}

export type BandNeighborhoodAggregate = BandNeighborhoodAverage | BandNeighborhoodFirstDerivative;

export interface BandNeighborhoodAverage {
    type: 'average';
    windowSize: number;
}

export interface BandNeighborhoodFirstDerivative {
    type: 'firstDerivative';
    bandDistance: {
        type: 'equallySpaced';
        distance: number;
    };
}

export interface BandwiseExpressionDict extends OperatorDict {
    type: 'BandwiseExpression';
    params: {
        expression: string;
        outputType: string;
        mapNoData: boolean;
    };
    sources: {
        raster: SourceOperatorDict | OperatorDict;
    };
}

export interface RgbDict extends OperatorDict {
    type: 'Rgb';
    params: {
        redMin: number;
        redMax: number;
        redScale: number;
        greenMin: number;
        greenMax: number;
        greenScale: number;
        blueMin: number;
        blueMax: number;
        blueScale: number;
    };
    sources: {
        red: OperatorDict | SourceOperatorDict;
        green: OperatorDict | SourceOperatorDict;
        blue: OperatorDict | SourceOperatorDict;
    };
}

export interface NeighborhoodAggregateDict extends OperatorDict {
    type: 'NeighborhoodAggregate';
    params: {
        neighborhood: {type: 'weightsMatrix'; weights: Array<Array<number>>} | {type: 'rectangle'; dimensions: [number, number]};
        aggregateFunction: 'sum' | 'standardDeviation';
    };
    sources: {
        raster: SourceOperatorDict | OperatorDict;
    };
}

export interface FeatureAttributeOverTimeDict extends OperatorDict {
    type: 'FeatureAttributeValuesOverTime';
    params: {
        idColumn: string;
        valueColumn: string;
    };
    sources: {
        vector: OperatorDict | SourceOperatorDict;
    };
}

export interface HistogramParams extends OperatorParams {
    attributeName: string;
    bounds:
        | {
              min: number;
              max: number;
          }
        | 'data';
    buckets:
        | {
              type: 'number';
              value: number;
          }
        | {
              type: 'squareRootChoiceRule';
              maxNumberOfBuckets: number;
          };
    interactive?: boolean;
}

export interface HistogramDict extends OperatorDict {
    type: 'Histogram';
    params: HistogramParams;
    sources: {
        source: SourceOperatorDict | OperatorDict;
    };
}

export interface PieChartDict extends OperatorDict {
    type: 'PieChart';
    params: PieChartCountParams;
    sources: {
        vector: SourceOperatorDict | OperatorDict;
    };
}

export interface PieChartCountParams extends OperatorParams {
    type: 'count';
    columnName: string;
    donut?: boolean;
}

export interface ClassHistogramParams extends OperatorParams {
    columnName?: string;
}

export interface ClassHistogramDict extends OperatorDict {
    type: 'ClassHistogram';
    params: ClassHistogramParams;
    sources: {
        source: SourceOperatorDict | OperatorDict;
    };
}

export interface BoxPlotParams extends OperatorParams {
    columnNames: Array<string>;
    includeNoData: boolean;
}

export interface BoxPlotDict extends OperatorDict {
    type: 'BoxPlot';
    params: BoxPlotParams;
    sources: {
        source: SourceOperatorDict | OperatorDict | Array<SourceOperatorDict | OperatorDict>;
    };
}

export interface ScatterPlotParams extends OperatorParams {
    columnX: string;
    columnY: string;
}

export interface ScatterPlotDict extends OperatorDict {
    type: 'ScatterPlot';
    params: ScatterPlotParams;
    sources: {
        vector: SourceOperatorDict | OperatorDict;
    };
}

export interface MeanRasterPixelValuesOverTimeParams extends OperatorParams {
    timePosition: 'start' | 'center' | 'end';
    area: boolean;
}

export interface MeanRasterPixelValuesOverTimeDict extends OperatorDict {
    type: 'MeanRasterPixelValuesOverTime';
    params: MeanRasterPixelValuesOverTimeParams;
    sources: {
        raster: SourceOperatorDict | OperatorDict;
    };
}

export interface PointInPolygonFilterDict extends OperatorDict {
    type: 'PointInPolygonFilter';
    params: EmptyParams;
    sources: {
        points: SourceOperatorDict | OperatorDict;
        polygons: SourceOperatorDict | OperatorDict;
    };
}

export interface RasterizationDict extends OperatorDict {
    type: 'Rasterization';
    params: GridRasterizationDict | DensityRasterizationDict;
    sources: {
        vector: SourceOperatorDict | OperatorDict;
    };
}

export interface GridRasterizationDict extends OperatorParams {
    type: 'grid';
    spatialResolution: {
        x: number;
        y: number;
    };
    originCoordinate: {
        x: number;
        y: number;
    };
    gridSizeMode: 'fixed' | 'relative';
}

export interface DensityRasterizationDict extends OperatorParams {
    type: 'density';
    cutoff: number;
    stddev: number;
}

export interface ColumnRangeFilterDict extends OperatorDict {
    type: 'ColumnRangeFilter';
    params: {
        column: string;
        ranges: Array<number[]> | Array<string[]>;
        keepNulls: boolean;
    };
    sources: {
        vector: SourceOperatorDict | OperatorDict;
    };
}

export interface RasterVectorJoinParams extends OperatorParams {
    names: ColumnNamesDict;
    temporalAggregation: 'none' | 'first' | 'mean';
    temporalAggregationIgnoreNoData?: boolean;
    featureAggregation: 'first' | 'mean';
    featureAggregationIgnoreNoData?: boolean;
}

export type ColumnNamesDict = ColumnNamesDefaultDict | ColumnNamesSuffixDict | ColumnNamesNamesDict;

export interface ColumnNamesDefaultDict {
    type: 'default';
}

export interface ColumnNamesSuffixDict {
    type: 'suffix';
    values: Array<string>;
}

export interface ColumnNamesNamesDict {
    type: 'names';
    values: Array<string>;
}

export interface RasterVectorJoinDict extends OperatorDict {
    type: 'RasterVectorJoin';
    params: RasterVectorJoinParams;
    sources: {
        vector: SourceOperatorDict | OperatorDict;
        rasters: Array<SourceOperatorDict | OperatorDict>;
    };
}

export interface ReprojectionDict extends OperatorDict {
    type: 'Reprojection';
    params: {
        targetSpatialReference: SrsString;
    };
    sources: {
        source: SourceOperatorDict | OperatorDict;
    };
}

export interface StatisticsParams extends OperatorParams {
    columnNames: Array<string>;
    percentiles: Array<number>;
}

export interface StatisticsDict extends OperatorDict {
    type: 'Statistics';
    params: StatisticsParams;
    sources: {
        source: SourceOperatorDict | OperatorDict | Array<SourceOperatorDict | OperatorDict>;
    };
}

export interface VectorExpressionDict extends OperatorDict {
    type: 'VectorExpression';
    params: VectorExpressionParams;
    sources: {
        vector: SourceOperatorDict | OperatorDict;
    };
}

export interface VectorExpressionParams extends OperatorParams {
    inputColumns: Array<string>;
    expression: string;
    outputColumn: GeometryOutputColumn | ColumnOutputColumn;
    outputMeasurement: MeasurementDict;
    geometryColumnName?: string;
}

export interface OutputColumn {
    type: 'geometry' | 'column';
    // eslint-disable-next-line @typescript-eslint/no-redundant-type-constituents
    value: string | ('MultiPoint' | 'MultiLineString' | 'MultiPolygon');
}

export interface ColumnOutputColumn extends OutputColumn {
    type: 'column';
    value: string;
}

export interface GeometryOutputColumn extends OutputColumn {
    type: 'geometry';
    value: 'MultiPoint' | 'MultiLineString' | 'MultiPolygon';
}

export interface TemporalRasterAggregationDict extends OperatorDict {
    type: 'TemporalRasterAggregation';
    params: {
        aggregation: {
            type: TemporalRasterAggregationDictAgregationType;
            ignoreNoData?: boolean;
            percentile?: number;
        };
        window: TimeStepDict;
        windowReference?: TimeInstanceDict;
        outputType?: 'U8' | 'U16' | 'U32' | 'U64' | 'I8' | 'I16' | 'I32' | 'I64' | 'F32' | 'F64';
    };
}

/*
 * UNIX timestamp in milliseconds.
 *
 * TODO: For input, allow ISO 8601 string
 */
export type TimeInstanceDict = string;

export type TemporalRasterAggregationDictAgregationType =
    | 'min'
    | 'max'
    | 'first'
    | 'last'
    | 'mean'
    | 'sum'
    | 'count'
    | 'percentileEstimate';

export interface RasterStackerDict extends OperatorDict {
    type: 'RasterStacker';
    params: {
        renameBands: RenameBandsDict;
    };
}

export type RenameBandsDict = RenameBandsDefaultDict | RenameBandsSuffixDict | RenameBandsRenameDict;

export interface RenameBandsDefaultDict {
    type: 'default';
}

export interface RenameBandsSuffixDict {
    type: 'suffix';
    values: Array<string>;
}

export interface RenameBandsRenameDict {
    type: 'rename';
    values: Array<string>;
}

export interface RasterTypeConversionDict extends OperatorDict {
    type: 'RasterTypeConversion';
    params: {
        outputDataType: string;
    };
}

export interface VisualPointClusteringParams extends OperatorParams {
    minRadiusPx: number;
    deltaPx: number;
    resolution: number;
    radiusColumn: string;
    countColumn: string;
    columnAggregates: Record<
        string,
        {
            columnName: string;
            aggregateType: 'meanNumber' | 'stringSample' | 'null';
        }
    >;
}

export interface OgrSourceDict extends SourceOperatorDict {
    type: 'OgrSource';
    params: {
        data: NamedDataDict;
        attributeProjection?: Array<string>;
        attributeFilters?: Array<AttributeFilterDict>;
    };
}

export interface AttributeFilterDict {
    attribute: string;
    ranges: Array<[number, number] | [string, string]>;
    keepNulls?: boolean;
}

export interface TimeProjectionDict extends OperatorDict {
    type: 'TimeProjection';
    params: {
        step: TimeStepDict;
        stepReference?: TimeInstanceDict;
    };
}

export interface TimeShiftDict extends OperatorDict {
    type: 'TimeShift';
    params: AbsoluteTimeShiftDictParams | RelativeTimeShiftDictParams;
}

export interface AbsoluteTimeShiftDictParams extends OperatorParams {
    type: 'absolute';
    timeInterval: TimeIntervalDict;
}

export interface RelativeTimeShiftDictParams extends OperatorParams {
    type: 'relative';
    granularity: TimeStepGranularityDict;
    value: number;
}

export interface InterpolationDict extends OperatorDict {
    type: 'Interpolation';
    params: {
        interpolation: 'nearestNeighbor' | 'biLinear';
        outputResolution: OutputResolutionDict;
        outputOriginReference?: Coordinate2DDict;
    };
}

export interface DownsamplingDict extends OperatorDict {
    type: 'Downsampling';
    params: {
        samplingMethod: 'nearestNeighbor';
        outputResolution: OutputResolutionDict;
        outputOriginReference?: Coordinate2DDict;
    };
}

export type OutputResolutionDict = {type: 'resolution'; x: number; y: number} | {type: 'fraction'; x: number; y: number};

export interface RasterUnScalingDict extends OperatorDict {
    type: 'RasterScaling';
    params: {
        slope: RasterMetadataKey | {type: 'constant'; value: number} | {type: 'auto'};
        offset: RasterMetadataKey | {type: 'constant'; value: number} | {type: 'auto'};
        outputMeasurement?: string;
        scalingMode: 'mulSlopeAddOffset' | 'subOffsetDivSlope';
    };
}

export interface RasterMetadataKey {
    type: 'metadataKey';
    domain?: string;
    key: string;
}

export interface LineSimplificationDict extends OperatorDict {
    type: 'LineSimplification';
    params: {
        algorithm: 'douglasPeucker' | 'visvalingam';
        epsilon: number | undefined;
    };
}

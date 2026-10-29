use geoengine_datatypes::dataset::NamedData;
use geoengine_datatypes::primitives::{MultiLineString, MultiPoint, MultiPolygon, NoGeometry};
use serde::{Deserialize, Serialize};
use tracing::debug;

use super::{
    CreateSpan, ExecutionContext, InitializedPlotOperator, InitializedRasterOperator,
    InitializedVectorOperator, OperatorData, PlotOperator, ProcessingGraphOperatorPath,
    RasterOperator, VectorOperator,
};
use crate::machine_learning::onnx::Onnx;
use crate::mock::{
    MockDatasetDataSource, MockFeatureCollectionSource, MockPointSource, MockRasterSource,
};
use crate::plot::{
    BoxPlot, ClassHistogram, FeatureAttributeValuesOverTime, Histogram,
    MeanRasterPixelValuesOverTime, PieChart, ScatterPlot, Statistics,
};
use crate::processing::{
    BandFilter, BandNeighborhoodAggregate, BandwiseExpression, ColumnRangeFilter, Downsampling,
    Expression, Interpolation, LineSimplification, NeighborhoodAggregate, PointInPolygonFilter,
    Radiance, RasterScaling, RasterStacker, RasterTypeConversion, RasterVectorJoin, Rasterization,
    Reflectance, Reprojection, Temperature, TemporalRasterAggregation, TimeProjection, TimeShift,
    VectorExpression, VectorJoin, VisualPointClustering,
};
use crate::source::{CsvSource, GdalSource, MultiBandGdalSource, OgrSource};
use crate::util::Result;

/// Generates an enum over all operators of one output type.
///
/// The variant names are the `type` tags of the serialized operators.
/// They must not change, since serialized operators are persisted and hashed.
macro_rules! typed_operator {
    (
        $(#[$meta:meta])*
        $name:ident: $operator_trait:ident => $initialized:ident {
            $($variant:ident($operator:ty)),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Debug, Serialize, Deserialize)]
        #[serde(tag = "type")]
        pub enum $name {
            $($variant(Box<$operator>),)+
        }

        impl $name {
            /// Initialize the operator
            pub async fn initialize(
                self,
                path: ProcessingGraphOperatorPath,
                context: &dyn ExecutionContext,
            ) -> Result<Box<dyn $initialized>> {
                debug!("Initialize {}, path: {}", self.type_name(), &path);
                match self {
                    $(Self::$variant(operator) => $operator_trait::initialize(operator, path, context).await,)+
                }
            }

            /// The name of the operator, i.e., its `type` tag
            pub fn type_name(&self) -> &'static str {
                match self {
                    $(Self::$variant(_) => stringify!($variant),)+
                }
            }

            pub fn span(&self) -> CreateSpan {
                match self {
                    $(Self::$variant(operator) => $operator_trait::span(operator.as_ref()),)+
                }
            }

            /// Pairs of variant names and the `OperatorName::TYPE_NAME` of their operators
            #[cfg(test)]
            fn variant_and_operator_names() -> &'static [(&'static str, &'static str)] {
                &[$((stringify!($variant), <$operator as crate::engine::OperatorName>::TYPE_NAME),)+]
            }
        }

        impl OperatorData for $name {
            fn data_names_collect(&self, data_names: &mut Vec<NamedData>) {
                match self {
                    $(Self::$variant(operator) => operator.data_names_collect(data_names),)+
                }
            }
        }

        $(
            impl From<$operator> for $name {
                fn from(operator: $operator) -> Self {
                    Self::$variant(Box::new(operator))
                }
            }
        )+
    };
}

typed_operator! {
    /// An operator that produces raster data
    TypedRasterOperator: RasterOperator => InitializedRasterOperator {
        BandFilter(BandFilter),
        BandNeighborhoodAggregate(BandNeighborhoodAggregate),
        BandwiseExpression(BandwiseExpression),
        Downsampling(Downsampling),
        Expression(Expression),
        GdalSource(GdalSource),
        Interpolation(Interpolation),
        MultiBandGdalSource(MultiBandGdalSource),
        NeighborhoodAggregate(NeighborhoodAggregate),
        Onnx(Onnx),
        Radiance(Radiance),
        RasterScaling(RasterScaling),
        RasterStacker(RasterStacker),
        RasterTypeConversion(RasterTypeConversion),
        Rasterization(Rasterization),
        Reflectance(Reflectance),
        Reprojection(Reprojection),
        Temperature(Temperature),
        TemporalRasterAggregation(TemporalRasterAggregation),
        TimeShift(TimeShift),
        MockRasterSourceu8(MockRasterSource<u8>),
        MockRasterSourceu16(MockRasterSource<u16>),
        MockRasterSourceu32(MockRasterSource<u32>),
        MockRasterSourceu64(MockRasterSource<u64>),
        MockRasterSourcei8(MockRasterSource<i8>),
        MockRasterSourcei16(MockRasterSource<i16>),
        MockRasterSourcei32(MockRasterSource<i32>),
        MockRasterSourcei64(MockRasterSource<i64>),
        MockRasterSourcef32(MockRasterSource<f32>),
        MockRasterSourcef64(MockRasterSource<f64>),
    }
}

typed_operator! {
    /// An operator that produces vector data
    TypedVectorOperator: VectorOperator => InitializedVectorOperator {
        ColumnRangeFilter(ColumnRangeFilter),
        CsvSource(CsvSource),
        LineSimplification(LineSimplification),
        OgrSource(OgrSource),
        PointInPolygonFilter(PointInPolygonFilter),
        RasterVectorJoin(RasterVectorJoin),
        Reprojection(Reprojection),
        TimeProjection(TimeProjection),
        TimeShift(TimeShift),
        VectorExpression(VectorExpression),
        VectorJoin(VectorJoin),
        VisualPointClustering(VisualPointClustering),
        MockDatasetDataSource(MockDatasetDataSource),
        MockPointSource(MockPointSource),
        MockFeatureCollectionSourceNoGeometry(MockFeatureCollectionSource<NoGeometry>),
        MockFeatureCollectionSourceMultiPoint(MockFeatureCollectionSource<MultiPoint>),
        MockFeatureCollectionSourceMultiLineString(MockFeatureCollectionSource<MultiLineString>),
        MockFeatureCollectionSourceMultiPolygon(MockFeatureCollectionSource<MultiPolygon>),
    }
}

typed_operator! {
    /// An operator that produces plot data
    TypedPlotOperator: PlotOperator => InitializedPlotOperator {
        BoxPlot(BoxPlot),
        ClassHistogram(ClassHistogram),
        FeatureAttributeValuesOverTime(FeatureAttributeValuesOverTime),
        Histogram(Histogram),
        MeanRasterPixelValuesOverTime(MeanRasterPixelValuesOverTime),
        PieChart(PieChart),
        ScatterPlot(ScatterPlot),
        Statistics(Statistics),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The variant names are the serialized `type` tags, while `OperatorName::TYPE_NAME` is used
    /// for logging and tracing. They must not drift apart.
    #[test]
    fn it_uses_the_operator_names_as_variant_names() {
        // generic mocks have one variant per type parameter, suffixed with the parameter
        const GENERIC_MOCKS: [&str; 2] = ["MockRasterSource", "MockFeatureCollectionSource"];

        for (variant, operator_name) in TypedRasterOperator::variant_and_operator_names()
            .iter()
            .chain(TypedVectorOperator::variant_and_operator_names())
            .chain(TypedPlotOperator::variant_and_operator_names())
        {
            if GENERIC_MOCKS.contains(operator_name) {
                assert!(
                    variant.starts_with(operator_name),
                    "variant {variant} must start with {operator_name}"
                );
            } else {
                assert_eq!(variant, operator_name);
            }
        }
    }
}

//! Footprints: when and where the data of a raster processing graph exists.

use crate::{
    api::handlers::processing_graphs::{
        MAX_FOOTPRINT_TIME_STEPS, RasterFootprints, TimeStepFootprint, TimeStepFootprintStatus,
        error,
    },
    contexts::SessionContext,
    error::Result,
    processing_graphs::{processing_graph::ProcessingGraphId, registry::ProcessingGraphRegistry},
    quota::ComputationId,
};
use futures::{StreamExt, TryStreamExt};
use geoengine_datatypes::{
    error::BoxedResultExt,
    primitives::{BandSelection, RasterQueryRectangle, SpatialPartition2D, TimeInterval},
    raster::GeoTransform,
    util::Identifier,
};
use geoengine_operators::{
    call_on_generic_raster_processor,
    engine::{
        ExecutionContext, Footprint, FootprintBounds, FootprintPrecision,
        ProcessingGraphOperatorPath, QueryContext, TypedRasterQueryProcessor,
    },
};

/// Tells for at most `limit` time steps within `time_interval` where the graph may have data.
pub async fn raster_footprints<C: SessionContext>(
    ctx: C,
    processing_graph_id: ProcessingGraphId,
    spatial_bounds: SpatialPartition2D,
    time_interval: TimeInterval,
    bands: BandSelection,
    limit: usize,
    precision: FootprintPrecision,
) -> Result<RasterFootprints> {
    let operator = ctx
        .db()
        .load_processing_graph(&processing_graph_id)
        .await?
        .operator
        .get_raster()
        .boxed_context(error::ProcessingGraphMustBeOfTypeRaster)?;

    let execution_context = ctx.execution_context()?;

    let initialized_operator = operator
        .initialize(
            ProcessingGraphOperatorPath::initialize_root(),
            &execution_context,
        )
        .await?;

    let spatial_reference = initialized_operator.result_descriptor().spatial_reference;
    let geo_transform = initialized_operator
        .result_descriptor()
        .tiling_grid_definition(execution_context.tiling_specification())
        .tiling_geo_transform();
    let query_bounds = geo_transform.spatial_to_grid_bounds(&spatial_bounds);

    let query_ctx = ctx.query_context(processing_graph_id, ComputationId::new())?;
    let processor = initialized_operator.query_processor()?;

    let limit = limit.clamp(1, MAX_FOOTPRINT_TIME_STEPS);
    let (time_steps, next) =
        bounded_time_steps(&processor, time_interval, limit, &query_ctx).await?;

    let (Some(first), Some(last)) = (time_steps.first(), time_steps.last()) else {
        return Ok(RasterFootprints {
            spatial_reference: spatial_reference.into(),
            steps: Vec::new(),
            next: None,
            too_large: false,
        });
    };

    let query = RasterQueryRectangle::new(
        query_bounds,
        TimeInterval::new(first.start(), last.end())?,
        bands,
    );

    let footprints = match query_footprints(&processor, query, precision, &query_ctx).await {
        Ok(footprints) => footprints,
        Err(geoengine_operators::error::Error::FootprintQueryTooLarge { .. }) => {
            return Ok(RasterFootprints {
                spatial_reference: spatial_reference.into(),
                steps: Vec::new(),
                next: None,
                too_large: true,
            });
        }
        Err(error) => return Err(error.into()),
    };

    Ok(RasterFootprints {
        spatial_reference: spatial_reference.into(),
        steps: footprints
            .iter()
            .map(|footprint| time_step_footprint(footprint, geo_transform))
            .collect(),
        next: next.map(Into::into),
        too_large: false,
    })
}

/// The first `limit` time steps. Asking for them first keeps a long time range from
/// making the footprint query itself look at all of its time steps.
async fn bounded_time_steps(
    processor: &TypedRasterQueryProcessor,
    time_interval: TimeInterval,
    limit: usize,
    query_ctx: &dyn QueryContext,
) -> Result<(Vec<TimeInterval>, Option<TimeInterval>), geoengine_operators::error::Error> {
    let mut time_steps: Vec<TimeInterval> = call_on_generic_raster_processor!(
        processor, p => p.time_query(time_interval, query_ctx).await
    )?
    .take(limit + 1)
    .try_collect()
    .await?;

    let next = (time_steps.len() > limit)
        .then(|| time_steps.pop())
        .flatten();

    Ok((time_steps, next))
}

async fn query_footprints(
    processor: &TypedRasterQueryProcessor,
    query: RasterQueryRectangle,
    precision: FootprintPrecision,
    query_ctx: &dyn QueryContext,
) -> Result<Vec<Footprint>, geoengine_operators::error::Error> {
    call_on_generic_raster_processor!(
        processor, p => p.footprint_query(query, precision, query_ctx).await
    )?
    .try_collect()
    .await
}

fn time_step_footprint(footprint: &Footprint, geo_transform: GeoTransform) -> TimeStepFootprint {
    let (status, bounds) = match &footprint.bounds {
        FootprintBounds::Unknown => (TimeStepFootprintStatus::Unknown, Vec::new()),
        FootprintBounds::Boxes(boxes) if boxes.is_empty() => {
            (TimeStepFootprintStatus::Empty, Vec::new())
        }
        FootprintBounds::Boxes(boxes) => (
            TimeStepFootprintStatus::Data,
            boxes
                .iter()
                .map(|grid_box| geo_transform.grid_to_spatial_bounds(grid_box).into())
                .collect(),
        ),
    };

    TimeStepFootprint {
        time: footprint.time.into(),
        status,
        bounds,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::raster::GridBoundingBox2D;

    #[test]
    fn it_maps_bounds_to_statuses() {
        let geo_transform = GeoTransform::new_unchecked((0., 0.).into(), 1., -1.);
        let time = TimeInterval::new_unchecked(0, 1);
        let status =
            |bounds| time_step_footprint(&Footprint { time, bounds }, geo_transform).status;

        assert!(matches!(
            status(FootprintBounds::Unknown),
            TimeStepFootprintStatus::Unknown
        ));
        assert!(matches!(
            status(FootprintBounds::empty()),
            TimeStepFootprintStatus::Empty
        ));

        let footprint = time_step_footprint(
            &Footprint {
                time,
                bounds: FootprintBounds::from_boxes([
                    GridBoundingBox2D::new_min_max(0, 1, 2, 3).unwrap()
                ]),
            },
            geo_transform,
        );
        assert!(matches!(footprint.status, TimeStepFootprintStatus::Data));
        assert_eq!(
            footprint.bounds,
            vec![SpatialPartition2D::new_unchecked((2., 0.).into(), (4., -2.).into()).into()]
        );
    }
}

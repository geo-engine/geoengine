use super::query_processor::{TypedRasterQueryProcessor, TypedVectorQueryProcessor};
use crate::{error, util::Result};
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::{projection::ProjectionOption, raster::RasterDataType};
use serde::{Deserialize, Serialize};
use std::ops::Range;

/// Common methods for `Operator`s
pub trait Operator: std::fmt::Debug + Send + Sync {
    /// Get the sources of the `Operator`
    fn raster_sources(&self) -> &[Box<dyn RasterOperator>];

    /// Get the sources of the `Operator`
    fn vector_sources(&self) -> &[Box<dyn VectorOperator>];

    /// Get the sources of the `Operator`
    fn raster_sources_mut(&mut self) -> &mut [Box<dyn RasterOperator>];

    /// Get the sources of the `Operator`
    fn vector_sources_mut(&mut self) -> &mut [Box<dyn VectorOperator>];

    fn initialize(&mut self) -> Result<()> {
        for ro in self.raster_sources_mut() {
            ro.initialize()?
        }
        for vo in self.vector_sources_mut() {
            vo.initialize()?
        }
        Ok(())
    }

    fn validate_children(
        &self,
        expected_projection: ProjectionOption,
        number_of_raster_sources: Range<usize>,
        number_of_vector_sources: Range<usize>,
    ) -> Result<()> {
        if !number_of_raster_sources.contains(&self.raster_sources().len()) {}
        if !number_of_vector_sources.contains(&self.vector_sources().len()) {}

        for proj in self
            .raster_sources()
            .iter()
            .map(|o| o.result_descriptor().projection)
            .chain(
                self.vector_sources()
                    .iter()
                    .map(|o| o.result_descriptor().projection),
            )
        {
            if proj != expected_projection {
                return Err(error::Error::InvalidProjection {
                    expected: expected_projection,
                    found: proj,
                });
            }
        }

        Ok(())
    }
}

/// Common methods for `VectorOperator`s
#[typetag::serde(tag = "type")]
pub trait VectorOperator: Operator {
    /// Get the result type of the `Operator`
    fn result_descriptor(&self) -> VectorResultDescriptor;

    /// Instantiate a `TypedVectorQueryProcessor` from a `RasterOperator`
    fn vector_processor(&self) -> Result<TypedVectorQueryProcessor>;

    /// Wrap a box around a `VectorOperator`
    fn boxed(self) -> Box<dyn VectorOperator>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

/// Common methods for `RasterOperator`s
#[typetag::serde(tag = "type")]
pub trait RasterOperator: Operator {
    /// Get the result type of the `Operator`
    fn result_descriptor(&self) -> RasterResultDescriptor;

    /// Instantiate a `TypedRasterQueryProcessor` from a `RasterOperator`
    fn raster_processor(&self) -> Result<TypedRasterQueryProcessor>;

    /// Wrap a box around a `RasterOperator`
    fn boxed(self) -> Box<dyn RasterOperator>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub struct RasterResultDescriptor {
    pub data_type: RasterDataType,
    pub projection: ProjectionOption,
}

impl RasterResultDescriptor {
    pub fn map<F>(self, f: F) -> Self
    where
        F: Fn(Self) -> Self,
    {
        f(self)
    }

    pub fn map_data_type<F>(mut self, f: F) -> Self
    where
        F: Fn(RasterDataType) -> RasterDataType,
    {
        self.data_type = f(self.data_type);
        self
    }

    pub fn map_projection<F>(mut self, f: F) -> Self
    where
        F: Fn(ProjectionOption) -> ProjectionOption,
    {
        self.projection = f(self.projection);
        self
    }
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub struct VectorResultDescriptor {
    pub data_type: VectorDataType,
    pub projection: ProjectionOption,
}

impl VectorResultDescriptor {
    pub fn map<F>(self, f: F) -> Self
    where
        F: Fn(Self) -> Self,
    {
        f(self)
    }

    pub fn map_data_type<F>(mut self, f: F) -> Self
    where
        F: Fn(VectorDataType) -> VectorDataType,
    {
        self.data_type = f(self.data_type);
        self
    }

    pub fn map_projection<F>(mut self, f: F) -> Self
    where
        F: Fn(ProjectionOption) -> ProjectionOption,
    {
        self.projection = f(self.projection);
        self
    }
}

/// An enum to differentiate between `Operator` variants
#[derive(Debug, Serialize, Deserialize)]
pub enum TypedOperator {
    Vector(Box<dyn VectorOperator>),
    Raster(Box<dyn RasterOperator>),
}

impl Into<TypedOperator> for Box<dyn VectorOperator> {
    fn into(self) -> TypedOperator {
        TypedOperator::Vector(self)
    }
}

impl Into<TypedOperator> for Box<dyn RasterOperator> {
    fn into(self) -> TypedOperator {
        TypedOperator::Raster(self)
    }
}

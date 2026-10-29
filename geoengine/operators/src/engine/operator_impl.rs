use geoengine_datatypes::dataset::NamedData;
use serde::{Deserialize, Serialize};

use crate::util::input::{MultiRasterOrVectorOperator, RasterOrVectorOperator};

use super::{OperatorData, TypedRasterOperator, TypedVectorOperator};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Operator<Params, Sources> {
    pub params: Params,
    pub sources: Sources,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SourceOperator<Params> {
    pub params: Params,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SingleRasterSource {
    pub raster: TypedRasterOperator,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SingleVectorSource {
    pub vector: TypedVectorOperator,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SingleRasterOrVectorSource {
    pub source: RasterOrVectorOperator,
}

impl SingleRasterOrVectorSource {
    pub fn raster(self) -> Option<SingleRasterSource> {
        match self.source {
            RasterOrVectorOperator::Raster(r) => Some(SingleRasterSource { raster: r }),
            RasterOrVectorOperator::Vector(_) => None,
        }
    }

    pub fn vector(self) -> Option<SingleVectorSource> {
        match self.source {
            RasterOrVectorOperator::Raster(_) => None,
            RasterOrVectorOperator::Vector(v) => Some(SingleVectorSource { vector: v }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MultipleRasterOrSingleVectorSource {
    pub source: MultiRasterOrVectorOperator,
}

impl MultipleRasterOrSingleVectorSource {
    pub fn raster(self) -> Option<MultipleRasterSources> {
        match self.source {
            MultiRasterOrVectorOperator::Raster(r) => Some(MultipleRasterSources { rasters: r }),
            MultiRasterOrVectorOperator::Vector(_) => None,
        }
    }

    pub fn vector(self) -> Option<SingleVectorSource> {
        match self.source {
            MultiRasterOrVectorOperator::Raster(_) => None,
            MultiRasterOrVectorOperator::Vector(v) => Some(SingleVectorSource { vector: v }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MultipleRasterSources {
    pub rasters: Vec<TypedRasterOperator>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MultipleVectorSources {
    pub vectors: Vec<TypedVectorOperator>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SingleVectorMultipleRasterSources {
    pub vector: TypedVectorOperator,
    pub rasters: Vec<TypedRasterOperator>,
}

impl From<TypedVectorOperator> for SingleVectorSource {
    fn from(vector: TypedVectorOperator) -> Self {
        Self { vector }
    }
}

impl From<TypedRasterOperator> for SingleRasterSource {
    fn from(raster: TypedRasterOperator) -> Self {
        Self { raster }
    }
}

impl From<Vec<TypedRasterOperator>> for MultipleRasterSources {
    fn from(rasters: Vec<TypedRasterOperator>) -> Self {
        Self { rasters }
    }
}

impl From<Vec<TypedVectorOperator>> for MultipleVectorSources {
    fn from(vectors: Vec<TypedVectorOperator>) -> Self {
        Self { vectors }
    }
}

impl From<TypedVectorOperator> for SingleRasterOrVectorSource {
    fn from(vector: TypedVectorOperator) -> Self {
        Self {
            source: RasterOrVectorOperator::Vector(vector),
        }
    }
}

impl From<TypedRasterOperator> for SingleRasterOrVectorSource {
    fn from(raster: TypedRasterOperator) -> Self {
        Self {
            source: RasterOrVectorOperator::Raster(raster),
        }
    }
}

impl From<TypedVectorOperator> for MultipleRasterOrSingleVectorSource {
    fn from(vector: TypedVectorOperator) -> Self {
        Self {
            source: MultiRasterOrVectorOperator::Vector(vector),
        }
    }
}

impl From<TypedRasterOperator> for MultipleRasterOrSingleVectorSource {
    fn from(raster: TypedRasterOperator) -> Self {
        Self {
            source: MultiRasterOrVectorOperator::Raster(vec![raster]),
        }
    }
}

impl From<Vec<TypedRasterOperator>> for MultipleRasterOrSingleVectorSource {
    fn from(raster: Vec<TypedRasterOperator>) -> Self {
        Self {
            source: MultiRasterOrVectorOperator::Raster(raster),
        }
    }
}

impl<Params, Sources> OperatorData for Operator<Params, Sources>
where
    Sources: OperatorData,
{
    fn data_names_collect(&self, data_names: &mut Vec<NamedData>) {
        self.sources.data_names_collect(data_names);
    }
}

impl<Params> OperatorData for SourceOperator<Params>
where
    Params: OperatorData,
{
    fn data_names_collect(&self, data_names: &mut Vec<NamedData>) {
        self.params.data_names_collect(data_names);
    }
}

impl OperatorData for SingleRasterOrVectorSource {
    fn data_names_collect(&self, data_names: &mut Vec<NamedData>) {
        self.source.data_names_collect(data_names);
    }
}

impl OperatorData for MultipleRasterOrSingleVectorSource {
    fn data_names_collect(&self, data_names: &mut Vec<NamedData>) {
        self.source.data_names_collect(data_names);
    }
}

impl OperatorData for SingleVectorSource {
    fn data_names_collect(&self, data_names: &mut Vec<NamedData>) {
        self.vector.data_names_collect(data_names);
    }
}

impl OperatorData for SingleRasterSource {
    fn data_names_collect(&self, data_names: &mut Vec<NamedData>) {
        self.raster.data_names_collect(data_names);
    }
}

impl OperatorData for MultipleRasterSources {
    fn data_names_collect(&self, data_names: &mut Vec<NamedData>) {
        for source in &self.rasters {
            source.data_names_collect(data_names);
        }
    }
}

impl OperatorData for MultipleVectorSources {
    fn data_names_collect(&self, data_names: &mut Vec<NamedData>) {
        for source in &self.vectors {
            source.data_names_collect(data_names);
        }
    }
}

impl OperatorData for SingleVectorMultipleRasterSources {
    fn data_names_collect(&self, data_names: &mut Vec<NamedData>) {
        self.vector.data_names_collect(data_names);
        for source in &self.rasters {
            source.data_names_collect(data_names);
        }
    }
}

use geoengine_datatypes::dataset::DataId;
use serde::{Deserialize, Serialize};

use crate::util::input::{MultiRasterOrVectorOperator, RasterOrVectorOperator};

use super::{OperatorData, RasterOperator, VectorOperator};

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
    pub raster: Box<dyn RasterOperator>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SingleVectorSource {
    pub vector: Box<dyn VectorOperator>,
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
    pub rasters: Vec<Box<dyn RasterOperator>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MultipleVectorSources {
    pub vectors: Vec<Box<dyn VectorOperator>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SingleVectorMultipleRasterSources {
    pub vector: Box<dyn VectorOperator>,
    pub rasters: Vec<Box<dyn RasterOperator>>,
}

impl From<Box<dyn VectorOperator>> for SingleVectorSource {
    fn from(vector: Box<dyn VectorOperator>) -> Self {
        Self { vector }
    }
}

impl From<Box<dyn RasterOperator>> for SingleRasterSource {
    fn from(raster: Box<dyn RasterOperator>) -> Self {
        Self { raster }
    }
}

impl From<Vec<Box<dyn RasterOperator>>> for MultipleRasterSources {
    fn from(rasters: Vec<Box<dyn RasterOperator>>) -> Self {
        Self { rasters }
    }
}

impl From<Vec<Box<dyn VectorOperator>>> for MultipleVectorSources {
    fn from(vectors: Vec<Box<dyn VectorOperator>>) -> Self {
        Self { vectors }
    }
}

impl From<Box<dyn VectorOperator>> for SingleRasterOrVectorSource {
    fn from(vector: Box<dyn VectorOperator>) -> Self {
        Self {
            source: RasterOrVectorOperator::Vector(vector),
        }
    }
}

impl From<Box<dyn RasterOperator>> for SingleRasterOrVectorSource {
    fn from(raster: Box<dyn RasterOperator>) -> Self {
        Self {
            source: RasterOrVectorOperator::Raster(raster),
        }
    }
}

impl From<Box<dyn VectorOperator>> for MultipleRasterOrSingleVectorSource {
    fn from(vector: Box<dyn VectorOperator>) -> Self {
        Self {
            source: MultiRasterOrVectorOperator::Vector(vector),
        }
    }
}

impl From<Box<dyn RasterOperator>> for MultipleRasterOrSingleVectorSource {
    fn from(raster: Box<dyn RasterOperator>) -> Self {
        Self {
            source: MultiRasterOrVectorOperator::Raster(vec![raster]),
        }
    }
}

impl From<Vec<Box<dyn RasterOperator>>> for MultipleRasterOrSingleVectorSource {
    fn from(raster: Vec<Box<dyn RasterOperator>>) -> Self {
        Self {
            source: MultiRasterOrVectorOperator::Raster(raster),
        }
    }
}

impl<Params, Sources> OperatorData for Operator<Params, Sources>
where
    Sources: OperatorData,
{
    fn data_ids_collect(&self, data_ids: &mut Vec<DataId>) {
        self.sources.data_ids_collect(data_ids);
    }
}

impl<Params> OperatorData for SourceOperator<Params>
where
    Params: OperatorData,
{
    fn data_ids_collect(&self, data_ids: &mut Vec<DataId>) {
        self.params.data_ids_collect(data_ids);
    }
}

impl OperatorData for SingleRasterOrVectorSource {
    fn data_ids_collect(&self, data_ids: &mut Vec<DataId>) {
        self.source.data_ids_collect(data_ids);
    }
}

impl OperatorData for MultipleRasterOrSingleVectorSource {
    fn data_ids_collect(&self, data_ids: &mut Vec<DataId>) {
        self.source.data_ids_collect(data_ids);
    }
}

impl OperatorData for SingleVectorSource {
    fn data_ids_collect(&self, data_ids: &mut Vec<DataId>) {
        self.vector.data_ids_collect(data_ids);
    }
}

impl OperatorData for SingleRasterSource {
    fn data_ids_collect(&self, data_ids: &mut Vec<DataId>) {
        self.raster.data_ids_collect(data_ids);
    }
}

impl OperatorData for MultipleRasterSources {
    fn data_ids_collect(&self, data_ids: &mut Vec<DataId>) {
        for source in &self.rasters {
            source.data_ids_collect(data_ids);
        }
    }
}

impl OperatorData for MultipleVectorSources {
    fn data_ids_collect(&self, data_ids: &mut Vec<DataId>) {
        for source in &self.vectors {
            source.data_ids_collect(data_ids);
        }
    }
}

impl OperatorData for SingleVectorMultipleRasterSources {
    fn data_ids_collect(&self, data_ids: &mut Vec<DataId>) {
        self.vector.data_ids_collect(data_ids);
        for source in &self.rasters {
            source.data_ids_collect(data_ids);
        }
    }
}

use geoengine_datatypes::dataset::DatasetId;
use serde::{Deserialize, Serialize};

use crate::util::input::RasterOrVectorOperator;

use super::{OperatorDatasets, RasterOperator, VectorOperator};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Operator<Params, Sources> {
    pub params: Params,
    pub sources: Sources,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
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

impl<Params, Sources> OperatorDatasets for Operator<Params, Sources>
where
    Sources: OperatorDatasets,
{
    fn datasets_collect(&self, datasets: &mut Vec<DatasetId>) {
        self.sources.datasets_collect(datasets);
    }
}

impl<Params> OperatorDatasets for SourceOperator<Params>
where
    Params: OperatorDatasets,
{
    fn datasets_collect(&self, datasets: &mut Vec<DatasetId>) {
        self.params.datasets_collect(datasets);
    }
}

impl OperatorDatasets for SingleRasterOrVectorSource {
    fn datasets_collect(&self, datasets: &mut Vec<DatasetId>) {
        self.source.datasets_collect(datasets)
    }
}

impl OperatorDatasets for SingleVectorSource {
    fn datasets_collect(&self, datasets: &mut Vec<DatasetId>) {
        self.vector.datasets_collect(datasets)
    }
}

impl OperatorDatasets for SingleRasterSource {
    fn datasets_collect(&self, datasets: &mut Vec<DatasetId>) {
        self.raster.datasets_collect(datasets)
    }
}

impl OperatorDatasets for MultipleRasterSources {
    fn datasets_collect(&self, datasets: &mut Vec<DatasetId>) {
        for source in &self.rasters {
            source.datasets_collect(datasets);
        }
    }
}

impl OperatorDatasets for MultipleVectorSources {
    fn datasets_collect(&self, datasets: &mut Vec<DatasetId>) {
        for source in &self.vectors {
            source.datasets_collect(datasets);
        }
    }
}

impl OperatorDatasets for SingleVectorMultipleRasterSources {
    fn datasets_collect(&self, datasets: &mut Vec<DatasetId>) {
        self.vector.datasets_collect(datasets);
        for source in &self.rasters {
            source.datasets_collect(datasets);
        }
    }
}

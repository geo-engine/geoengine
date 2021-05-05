use serde::{Deserialize, Serialize};

use crate::util::input::RasterOrVectorOperator;

use super::{RasterOperator, VectorOperator};

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
pub struct OneVectorManyRasterSources {
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

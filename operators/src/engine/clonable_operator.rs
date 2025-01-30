use super::{
    InitializedPlotOperator, InitializedRasterOperator, InitializedVectorOperator, PlotOperator,
    RasterOperator, VectorOperator,
};

/// Helper trait for making boxed `RasterOperator`s cloneable
pub trait CloneableRasterOperator {
    fn clone_boxed_raster(&self) -> Box<dyn RasterOperator>;
}

/// Helper trait for making boxed `VectorOperator`s cloneable
pub trait CloneableVectorOperator {
    fn clone_boxed_vector(&self) -> Box<dyn VectorOperator>;
}

/// Helper trait for making boxed `PlotOperator`s cloneable
pub trait CloneablePlotOperator {
    fn clone_boxed_plot(&self) -> Box<dyn PlotOperator>;
}

impl<T> CloneableRasterOperator for T
where
    T: 'static + RasterOperator + Clone,
{
    fn clone_boxed_raster(&self) -> Box<dyn RasterOperator> {
        Box::new(self.clone())
    }
}

impl<T> CloneableVectorOperator for T
where
    T: 'static + VectorOperator + Clone,
{
    fn clone_boxed_vector(&self) -> Box<dyn VectorOperator> {
        Box::new(self.clone())
    }
}

impl<T> CloneablePlotOperator for T
where
    T: 'static + PlotOperator + Clone,
{
    fn clone_boxed_plot(&self) -> Box<dyn PlotOperator> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn RasterOperator> {
    fn clone(&self) -> Box<dyn RasterOperator> {
        self.clone_boxed_raster()
    }
}

impl Clone for Box<dyn VectorOperator> {
    fn clone(&self) -> Box<dyn VectorOperator> {
        self.clone_boxed_vector()
    }
}

impl Clone for Box<dyn PlotOperator> {
    fn clone(&self) -> Box<dyn PlotOperator> {
        self.clone_boxed_plot()
    }
}

/// Helper trait for making boxed `InitializedRasterOperator`s cloneable
pub trait CloneableInitializedRasterOperator {
    fn clone_boxed_raster(&self) -> Box<dyn InitializedRasterOperator>;
}

/// Helper trait for making boxed `InitializedVectorOperator`s cloneable
pub trait CloneableInitializedVectorOperator {
    fn clone_boxed_vector(&self) -> Box<dyn InitializedVectorOperator>;
}

/// Helper trait for making boxed `InitializedPlotOperator`s cloneable
pub trait CloneableInitializedPlotOperator {
    fn clone_boxed_plot(&self) -> Box<dyn InitializedPlotOperator>;
}

impl<T> CloneableInitializedRasterOperator for T
where
    T: 'static + InitializedRasterOperator + Clone,
{
    fn clone_boxed_raster(&self) -> Box<dyn InitializedRasterOperator> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn InitializedRasterOperator> {
    fn clone(&self) -> Box<dyn InitializedRasterOperator> {
        self.clone_boxed_raster()
    }
}

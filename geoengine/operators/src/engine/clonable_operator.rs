use super::{InitializedPlotOperator, InitializedRasterOperator, InitializedVectorOperator};

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

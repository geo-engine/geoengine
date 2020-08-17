use super::{
    InitializedOperator, InitializedRasterOperator, InitializedVectorOperator, Operator,
    RasterOperator, VectorOperator,
};

/// Helper trait for making boxed `Operator`s cloneable
pub trait CloneableOperator {
    fn clone_boxed(&self) -> Box<dyn Operator>;
}

/// Helper trait for making boxed `RasterOperator`s cloneable
pub trait CloneableRasterOperator {
    fn clone_boxed_raster(&self) -> Box<dyn RasterOperator>;
}

/// Helper trait for making boxed `VectorOperator`s cloneable
pub trait CloneableVectorOperator {
    fn clone_boxed_vector(&self) -> Box<dyn VectorOperator>;
}

impl<T> CloneableOperator for T
where
    T: 'static + Operator + Clone,
{
    fn clone_boxed(&self) -> Box<dyn Operator> {
        Box::new(self.clone())
    }
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

impl Clone for Box<dyn Operator> {
    fn clone(&self) -> Box<dyn Operator> {
        self.clone_boxed()
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

/// Helper trait for making boxed `InitializedOperator`s cloneable
pub trait CloneableInitializedOperator {
    fn clone_boxed(&self) -> Box<dyn InitializedOperator>;
}

/// Helper trait for making boxed `InitializedRasterOperator`s cloneable
pub trait CloneableInitializedRasterOperator {
    fn clone_boxed_raster(&self) -> Box<InitializedRasterOperator>;
}

/// Helper trait for making boxed `InitializedVectorOperator`s cloneable
pub trait CloneableInitializedVectorOperator {
    fn clone_boxed_vector(&self) -> Box<InitializedVectorOperator>;
}

impl<T> CloneableInitializedOperator for T
where
    T: 'static + InitializedOperator + Clone,
{
    fn clone_boxed(&self) -> Box<dyn InitializedOperator> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn InitializedOperator> {
    fn clone(&self) -> Box<dyn InitializedOperator> {
        self.clone_boxed()
    }
}

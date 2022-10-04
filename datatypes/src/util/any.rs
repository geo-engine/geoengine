use std::{any::Any, sync::Arc};

/// Easy `Any`-casting by propagating the call to the underlying implementor
pub trait AsAny: Any {
    fn as_any(&self) -> &dyn std::any::Any;
}

impl<T> AsAny for T
where
    T: Any,
{
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Easy `Any`-casting for `Arc`s by propagating the call to the underlying implementor
pub trait AsAnyArc {
    /// Returns the required Arc type for calling `Arc::downcast`
    fn as_any_arc(self: Arc<Self>) -> Arc<(dyn Any + Send + Sync)>;
}

impl<T> AsAnyArc for T
where
    T: Any + Send + Sync,
{
    fn as_any_arc(self: Arc<Self>) -> Arc<(dyn Any + Send + Sync)> {
        self
    }
}

// TODO: test that everything works as expected

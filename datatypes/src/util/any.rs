use std::any::Any;

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

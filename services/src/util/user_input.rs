use crate::error::Result;

pub trait UserInput: Clone {
    /// Validates user input and returns itself
    ///
    /// # Errors
    ///
    /// Fails if the user input is invalid
    ///
    fn validate(&self) -> Result<()>;

    /// Validates user input and returns itself
    ///
    /// # Errors
    ///
    /// Fails if the user input is invalid
    ///
    fn validated(self) -> Result<Validated<Self>>
    where
        Self: Sized,
    {
        self.validate().map(|_| Validated { user_input: self })
    }
}

#[derive(Debug, Clone)]
pub struct Validated<T: UserInput + Clone> {
    pub user_input: T,
}

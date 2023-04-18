/// A path to an operator within an operator graph (workflow).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkflowOperatorPath {
    id: Vec<u8>,
}

impl WorkflowOperatorPath {
    /// Creates a new root path.    
    pub fn initialize_root() -> Self {
        Self { id: Vec::new() }
    }

    /// returns the inner Vec<u8> of the path
    pub fn inner(self) -> Vec<u8> {
        self.id
    }

    /// clone the path and extend it with the given suffix
    #[must_use]
    pub fn clone_and_extend(&self, suffix: &[u8]) -> Self {
        let mut id = self.id.clone();
        id.extend(suffix);
        Self { id }
    }

    /// clone the path and append the given suffix
    #[must_use]
    pub fn clone_and_append(&self, suffix: u8) -> Self {
        let mut id = self.id.clone();
        id.push(suffix);
        Self { id }
    }

    /// checks if the path starts with the given prefix
    pub fn starts_with(&self, prefix: &[u8]) -> bool {
        self.id.starts_with(prefix)
    }
}

impl AsRef<[u8]> for WorkflowOperatorPath {
    fn as_ref(&self) -> &[u8] {
        &self.id
    }
}

impl From<&[u8]> for WorkflowOperatorPath {
    fn from(id: &[u8]) -> Self {
        Self { id: id.to_vec() }
    }
}

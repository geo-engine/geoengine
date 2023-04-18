#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct WorkflowOperatorPath {
    id: Vec<u8>,
}

impl WorkflowOperatorPath {
    pub fn new(id: Vec<u8>) -> Self {
        Self { id }
    }

    pub fn inner(self) -> Vec<u8> {
        self.id
    }

    #[must_use]
    pub fn clone_and_extend(&self, suffix: &[u8]) -> Self {
        let mut id = self.id.clone();
        id.extend(suffix);
        Self { id }
    }

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

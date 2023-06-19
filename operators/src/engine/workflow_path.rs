use std::fmt::{Display, Formatter};

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

    /// checks if the path is the root path
    pub fn is_root(&self) -> bool {
        self.id.is_empty()
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

impl Display for WorkflowOperatorPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let sep = ", ";

        write!(f, "[")?;

        for (i, id) in self.id.iter().enumerate() {
            if i > 0 {
                write!(f, "{sep}")?;
            }
            write!(f, "{id}")?;
        }

        write!(f, "]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let path = WorkflowOperatorPath::initialize_root();
        assert_eq!(path.to_string(), "[]");

        let path = path.clone_and_append(1);
        assert_eq!(path.to_string(), "[1]");

        let path = path.clone_and_extend(&[2, 3]);
        assert_eq!(path.to_string(), "[1, 2, 3]");
    }
}

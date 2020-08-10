use serde::{Deserialize, Serialize};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE")]
pub enum ProjectionAuthority {
    Epsg,
    SrOrg,
    Iau2000,
    Esri,
}

impl std::fmt::Display for ProjectionAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_string(&self).expect("must not fail")
        )
    }
}

// TODO: serialize as "auth:code"
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Projection {
    authority: ProjectionAuthority,
    code: u32,
}

impl Projection {
    pub fn new(authority: ProjectionAuthority, code: u32) -> Self {
        Projection { authority, code }
    }

    /// the WGS 84 projection
    pub fn wgs84() -> Self {
        Projection::new(ProjectionAuthority::Epsg, 4326)
    }
}

impl std::fmt::Display for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.authority, self.code)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum ProjectionOption {
    Projection(Projection),
    None,
}

impl std::fmt::Display for ProjectionOption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProjectionOption::Projection(p) => write!(f, "{}", p),
            ProjectionOption::None => Ok(()),
        }
    }
}

impl Into<ProjectionOption> for Projection {
    fn into(self) -> ProjectionOption {
        ProjectionOption::Projection(self)
    }
}

impl From<Option<Projection>> for ProjectionOption {
    fn from(option: Option<Projection>) -> Self {
        match option {
            Some(p) => ProjectionOption::Projection(p),
            None => ProjectionOption::None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_1() {
        let proj = Projection {
            authority: ProjectionAuthority::Epsg,
            code: 4326,
        };

        assert_eq!(
            serde_json::to_string(&proj).unwrap(),
            "{\"authority\":\"EPSG\",\"code\":4326}"
        );
    }
}

use crate::error::Result;
use crate::projects::{Project, ProjectDb, ProjectId, ProjectVersion};
use crate::{
    pro::users::{UserId, UserSession},
    projects::{OrderBy, ProjectFilter},
};
use async_trait::async_trait;
#[cfg(feature = "postgres")]
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::LoadVersion;

/// Storage of user projects
#[async_trait]
pub trait ProProjectDb: ProjectDb<UserSession> {
    /// Load the the `version` of the `project` for the `user`
    async fn load_version(
        &self,
        session: &UserSession,
        project: ProjectId,
        version: LoadVersion,
    ) -> Result<Project>;

    /// List all versions of the `project` if given `user` has at least read permission
    async fn versions(
        &self,
        session: &UserSession,
        project: ProjectId,
    ) -> Result<Vec<ProjectVersion>>;

    /// List all permissions of users for the `project` if the `user` is an owner
    async fn list_permissions(
        &self,
        session: &UserSession,
        project: ProjectId,
    ) -> Result<Vec<UserProjectPermission>>;

    /// Add a `permission` if the `user` is owner of the permission's target project
    async fn add_permission(
        &self,
        session: &UserSession,
        permission: UserProjectPermission,
    ) -> Result<()>;

    /// Remove a `permission` if the `user` is owner of the target project
    async fn remove_permission(
        &self,
        session: &UserSession,
        permission: UserProjectPermission,
    ) -> Result<()>;
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, ToSchema)]
#[cfg_attr(feature = "postgres", derive(ToSql, FromSql))]
pub enum ProjectPermission {
    Read,
    Write,
    Owner,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, ToSchema)]
#[schema(example = json!({
    "user": "3cbe632e-c50a-46d0-8490-f12621347bb1",
    "project": "aaed86a1-49d4-482d-b993-39159bb853df",
    "permission": "Read"
}))]
pub struct UserProjectPermission {
    pub project: ProjectId,
    pub permission: ProjectPermission,
    pub user: UserId,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct ProjectListOptions {
    #[serde(default)]
    pub filter: ProjectFilter,
    pub order: OrderBy,
    pub offset: u32,
    pub limit: u32,
}

// /// Instead of parsing list params, deserialize `ProjectPermission`s as JSON list.
// pub fn permissions_from_json_str<'de, D>(
//     deserializer: D,
// ) -> Result<Vec<ProjectPermission>, D::Error>
// where
//     D: serde::Deserializer<'de>,
// {
//     use serde::de::Visitor;

//     struct PermissionsFromJsonStrVisitor;
//     impl<'de> Visitor<'de> for PermissionsFromJsonStrVisitor {
//         type Value = Vec<ProjectPermission>;

//         fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
//             formatter.write_str("a JSON array of type `ProjectPermission`")
//         }

//         fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
//         where
//             E: serde::de::Error,
//         {
//             serde_json::from_str(v).map_err(|error| E::custom(error.to_string()))
//         }
//     }

//     deserializer.deserialize_str(PermissionsFromJsonStrVisitor)
// }

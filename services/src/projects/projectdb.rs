use crate::projects::project::{ProjectListing, ProjectId, Project, CreateProject, UpdateProject, UserProjectPermission, ProjectListOptions};
use crate::users::user::{UserIdentification, Validated};
use crate::error::Result;

pub trait ProjectDB: Send + Sync {
    // TODO: pass session instead of UserIdentification?
    fn list(&self, user: UserIdentification, options: Validated<ProjectListOptions>) -> Vec<ProjectListing>;    
    fn load(&self, user: UserIdentification, project: ProjectId) -> Result<Project>;
    fn create(&mut self, user: UserIdentification, project: Validated<CreateProject>) -> ProjectId;
    fn update(&mut self, user: UserIdentification, project: Validated<UpdateProject>) -> Result<()>;
    fn delete(&mut self, user: UserIdentification, project: ProjectId) -> Result<()>;

    fn list_permissions(&mut self, user: UserIdentification, project: ProjectId) -> Result<Vec<UserProjectPermission>>;
    fn add_permission(&mut self, user: UserIdentification, permission: UserProjectPermission) -> Result<()>;
    fn remove_permission(&mut self, user: UserIdentification, permission: UserProjectPermission) -> Result<()>;
}

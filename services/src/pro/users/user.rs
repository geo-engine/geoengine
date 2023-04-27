use pwhash::bcrypt;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use validator::Validate;

use crate::error::Result;
use crate::identifier;
use crate::pro::permissions::{Role, RoleId};
use geoengine_datatypes::util::Identifier;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, ToSchema, Validate)]
#[serde(rename_all = "camelCase")]
#[schema(example = json!({
    "email": "foo@example.com",
    "password": "secret123",
    "realName": "Foo Bar"
}))]
pub struct UserRegistration {
    #[validate(email)]
    pub email: String,
    #[validate(length(min = 8))]
    pub password: String,
    #[validate(length(min = 1))]
    pub real_name: String,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, ToSchema, Validate)]
#[schema(example = json!({
    "email": "foo@example.com",
    "password": "secret123",
}))]
pub struct UserCredentials {
    #[validate(email)]
    pub email: String,
    #[validate(length(min = 8))]
    pub password: String,
}

identifier!(UserId);

#[derive(Clone)]
pub struct User {
    pub id: UserId,
    pub email: String,
    pub password_hash: String,
    pub real_name: String,
    pub active: bool,
    pub roles: Vec<RoleId>,
}

impl From<UserRegistration> for User {
    fn from(user_registration: UserRegistration) -> Self {
        let id = UserId::new();
        Self {
            id,
            email: user_registration.email,
            password_hash: bcrypt::hash(&user_registration.password).unwrap(),
            real_name: user_registration.real_name,
            active: true,
            roles: vec![id.into(), Role::registered_user_role_id()],
        }
    }
}

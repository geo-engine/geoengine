use crate::users::userdb::UserDB;
use crate::projects::projectdb::ProjectDB;
use crate::handlers::{DB, authenticate};
use warp::Filter;
use std::sync::Arc;
use crate::users::session::Session;
use crate::projects::project::{CreateProject, ProjectListOptions, ProjectId, UpdateProject, UserProjectPermission};
use crate::users::user::UserInput;

pub fn create_project_handler<T: UserDB, R: ProjectDB>(user_db: DB<T>, project_db: DB<R>) -> impl Filter<Extract=impl warp::Reply, Error=warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("project" / "create"))
        .and(authenticate(user_db))
        .and(warp::body::json())
        .and(warp::any().map(move || Arc::clone(&project_db)))
        .and_then(create_project)
}

// TODO: move into handler once async closures are available?
async fn create_project<T: ProjectDB>(session: Session, create: CreateProject, project_db: DB<T>) -> Result<impl warp::Reply, warp::Rejection> {
    let create = create.validated().map_err(warp::reject::custom)?;
    let id = project_db.write().await.create(session.user, create);
    Ok(warp::reply::json(&id))
}

pub fn list_projects_handler<T: UserDB, R: ProjectDB>(user_db: DB<T>, project_db: DB<R>) -> impl Filter<Extract=impl warp::Reply, Error=warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("project" / "list"))
        .and(authenticate(user_db))
        .and(warp::body::json())
        .and(warp::any().map(move || Arc::clone(&project_db)))
        .and_then(list_projects)
}

// TODO: move into handler once async closures are available?
async fn list_projects<T: ProjectDB>(session: Session, options: ProjectListOptions, project_db: DB<T>) -> Result<impl warp::Reply, warp::Rejection> {
    let options = options.validated().map_err(warp::reject::custom)?;
    let listing = project_db.read().await.list(session.user, options);
    Ok(warp::reply::json(&listing))
}

pub fn load_project_handler<T: UserDB, R: ProjectDB>(user_db: DB<T>, project_db: DB<R>) -> impl Filter<Extract=impl warp::Reply, Error=warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("project" / "load"))
        .and(authenticate(user_db))
        .and(warp::body::json())
        .and(warp::any().map(move || Arc::clone(&project_db)))
        .and_then(load_project)
}

// TODO: move into handler once async closures are available?
async fn load_project<T: ProjectDB>(session: Session, project: ProjectId, project_db: DB<T>) -> Result<impl warp::Reply, warp::Rejection> {
    let id = project_db.read().await.load(session.user, project).map_err(warp::reject::custom)?;
    Ok(warp::reply::json(&id))
}

pub fn update_project_handler<T: UserDB, R: ProjectDB>(user_db: DB<T>, project_db: DB<R>) -> impl Filter<Extract=impl warp::Reply, Error=warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("project" / "update"))
        .and(authenticate(user_db))
        .and(warp::body::json())
        .and(warp::any().map(move || Arc::clone(&project_db)))
        .and_then(update_project)
}

// TODO: move into handler once async closures are available?
async fn update_project<T: ProjectDB>(session: Session, update: UpdateProject, project_db: DB<T>) -> Result<impl warp::Reply, warp::Rejection> {
    let update = update.validated().map_err(warp::reject::custom)?;
    project_db.write().await.update(session.user, update).map_err(warp::reject::custom)?;
    Ok(warp::reply())
}

pub fn delete_project_handler<T: UserDB, R: ProjectDB>(user_db: DB<T>, project_db: DB<R>) -> impl Filter<Extract=impl warp::Reply, Error=warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("project" / "delete"))
        .and(authenticate(user_db))
        .and(warp::body::json())
        .and(warp::any().map(move || Arc::clone(&project_db)))
        .and_then(delete_project)
}

// TODO: move into handler once async closures are available?
async fn delete_project<T: ProjectDB>(session: Session, project: ProjectId, project_db: DB<T>) -> Result<impl warp::Reply, warp::Rejection> {
    project_db.write().await.delete(session.user, project).map_err(warp::reject::custom)?;
    Ok(warp::reply())
}

pub fn add_permission_handler<T: UserDB, R: ProjectDB>(user_db: DB<T>, project_db: DB<R>) -> impl Filter<Extract=impl warp::Reply, Error=warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("project" / "permission" / "add"))
        .and(authenticate(user_db))
        .and(warp::body::json())
        .and(warp::any().map(move || Arc::clone(&project_db)))
        .and_then(add_permission)
}

// TODO: move into handler once async closures are available?
async fn add_permission<T: ProjectDB>(session: Session, permission: UserProjectPermission, project_db: DB<T>) -> Result<impl warp::Reply, warp::Rejection> {
    project_db.write().await.add_permission(session.user, permission).map_err(warp::reject::custom)?;
    Ok(warp::reply())
}

pub fn remove_permission_handler<T: UserDB, R: ProjectDB>(user_db: DB<T>, project_db: DB<R>) -> impl Filter<Extract=impl warp::Reply, Error=warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("project" / "permission" / "remove"))
        .and(authenticate(user_db))
        .and(warp::body::json())
        .and(warp::any().map(move || Arc::clone(&project_db)))
        .and_then(remove_permission)
}

// TODO: move into handler once async closures are available?
async fn remove_permission<T: ProjectDB>(session: Session, permission: UserProjectPermission, project_db: DB<T>) -> Result<impl warp::Reply, warp::Rejection> {
    project_db.write().await.remove_permission(session.user, permission).map_err(warp::reject::custom)?;
    Ok(warp::reply())
}

pub fn list_permissions_handler<T: UserDB, R: ProjectDB>(user_db: DB<T>, project_db: DB<R>) -> impl Filter<Extract=impl warp::Reply, Error=warp::Rejection> + Clone {
    warp::post()
        .and(warp::path!("project" / "permission" / "list"))
        .and(authenticate(user_db))
        .and(warp::body::json())
        .and(warp::any().map(move || Arc::clone(&project_db)))
        .and_then(list_permissions)
}

// TODO: move into handler once async closures are available?
async fn list_permissions<T: ProjectDB>(session: Session, project: ProjectId, project_db: DB<T>) -> Result<impl warp::Reply, warp::Rejection> {
    let permissions = project_db.write().await.list_permissions(session.user, project).map_err(warp::reject::custom)?;
    Ok(warp::reply::json(&permissions))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::RwLock;
    use crate::projects::projectdb::HashMapProjectDB;
    use crate::users::userdb::HashMapUserDB;
    use crate::projects::project::{ProjectId, ProjectOwner, ProjectFilter, OrderBy, ProjectListing, Project, UpdateProject, ProjectPermission};
    use crate::users::user::{UserRegistration, UserCredentials};

    #[tokio::test]
    async fn create() {
        let user_db = Arc::new(RwLock::new(HashMapUserDB::default()));
        let project_db = Arc::new(RwLock::new(HashMapProjectDB::default()));

        user_db.write().await.register(UserRegistration {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
            real_name: "Foo Bar".to_string()
        }.validated().unwrap()).unwrap();

        let session = user_db.write().await.login(UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
        }).unwrap();

        let create = CreateProject {
            name: "Test".to_string(),
            description: "Foo".to_string()
        };

        let res = warp::test::request()
            .method("POST")
            .path("/project/create")
            .header("Content-Length", "0")
            .header("Authorization", session.token.to_string())
            .json(&create)
            .reply(&create_project_handler(user_db.clone(), project_db.clone()))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        assert!(serde_json::from_str::<ProjectId>(&body).is_ok());
    }

    #[tokio::test]
    async fn list() {
        let user_db = Arc::new(RwLock::new(HashMapUserDB::default()));
        let project_db = Arc::new(RwLock::new(HashMapProjectDB::default()));

        user_db.write().await.register(UserRegistration {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
            real_name: "Foo Bar".to_string()
        }.validated().unwrap()).unwrap();

        let session = user_db.write().await.login(UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
        }).unwrap();

        for i in 0..10 {
            let create = CreateProject {
                name: format!("Test{}", i),
                description: format!("Test{}", 10 - i),
            }.validated().unwrap();
            project_db.write().await.create(session.user, create);
        }

        let options = ProjectListOptions {
            owner: ProjectOwner::Any,
            filter: ProjectFilter::None,
            order: OrderBy::NameDesc,
            offset: 0,
            limit: 2,
        };

        let res = warp::test::request()
            .method("POST")
            .path("/project/list")
            .header("Content-Length", "0")
            .header("Authorization", session.token.to_string())
            .json(&options)
            .reply(&list_projects_handler(user_db.clone(), project_db.clone()))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        let result = serde_json::from_str::<Vec<ProjectListing>>(&body);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn load() {
        let user_db = Arc::new(RwLock::new(HashMapUserDB::default()));
        let project_db = Arc::new(RwLock::new(HashMapProjectDB::default()));

        user_db.write().await.register(UserRegistration {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
            real_name: "Foo Bar".to_string()
        }.validated().unwrap()).unwrap();

        let session = user_db.write().await.login(UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
        }).unwrap();

        let project = project_db.write().await.create(session.user, CreateProject {
            name: "Test".to_string(),
            description: "Foo".to_string(),
        }.validated().unwrap());

        let res = warp::test::request()
            .method("POST")
            .path("/project/load")
            .header("Content-Length", "0")
            .header("Authorization", session.token.to_string())
            .json(&project)
            .reply(&load_project_handler(user_db.clone(), project_db.clone()))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        assert!(serde_json::from_str::<Project>(&body).is_ok());
    }

    #[tokio::test]
    async fn update() {
        let user_db = Arc::new(RwLock::new(HashMapUserDB::default()));
        let project_db = Arc::new(RwLock::new(HashMapProjectDB::default()));

        user_db.write().await.register(UserRegistration {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
            real_name: "Foo Bar".to_string()
        }.validated().unwrap()).unwrap();

        let session = user_db.write().await.login(UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
        }).unwrap();

        let project = project_db.write().await.create(session.user, CreateProject {
            name: "Test".to_string(),
            description: "Foo".to_string(),
        }.validated().unwrap());

        let update = UpdateProject {
            id: project,
            name: Some("TestUpdate".to_string()),
            description: None,
            layers: None
        };

        let res = warp::test::request()
            .method("POST")
            .path("/project/update")
            .header("Content-Length", "0")
            .header("Authorization", session.token.to_string())
            .json(&update)
            .reply(&update_project_handler(user_db.clone(), project_db.clone()))
            .await;

        assert_eq!(res.status(), 200);

        assert_eq!(project_db.read().await.load(session.user, project).unwrap().name, "TestUpdate");
    }

    #[tokio::test]
    async fn delete() {
        let user_db = Arc::new(RwLock::new(HashMapUserDB::default()));
        let project_db = Arc::new(RwLock::new(HashMapProjectDB::default()));

        user_db.write().await.register(UserRegistration {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
            real_name: "Foo Bar".to_string()
        }.validated().unwrap()).unwrap();

        let session = user_db.write().await.login(UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
        }).unwrap();

        let project = project_db.write().await.create(session.user, CreateProject {
            name: "Test".to_string(),
            description: "Foo".to_string(),
        }.validated().unwrap());

        let res = warp::test::request()
            .method("POST")
            .path("/project/delete")
            .header("Content-Length", "0")
            .header("Authorization", session.token.to_string())
            .json(&project)
            .reply(&delete_project_handler(user_db.clone(), project_db.clone()))
            .await;

        assert_eq!(res.status(), 200);

        assert!(project_db.read().await.load(session.user, project).is_err());

        let res = warp::test::request()
            .method("POST")
            .path("/project/delete")
            .header("Content-Length", "0")
            .header("Authorization", session.token.to_string())
            .json(&project)
            .reply(&delete_project_handler(user_db.clone(), project_db.clone()))
            .await;

        assert_eq!(res.status(), 500);
    }

    #[tokio::test]
    async fn add_permission() {
        let user_db = Arc::new(RwLock::new(HashMapUserDB::default()));
        let project_db = Arc::new(RwLock::new(HashMapProjectDB::default()));

        user_db.write().await.register(UserRegistration {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
            real_name: "Foo Bar".to_string()
        }.validated().unwrap()).unwrap();

        let target_user = user_db.write().await.register(UserRegistration {
            email: "foo2@bar.de".to_string(),
            password: "secret1234".to_string(),
            real_name: "Foo2 Bar".to_string()
        }.validated().unwrap()).unwrap();

        let session = user_db.write().await.login(UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
        }).unwrap();

        let project = project_db.write().await.create(session.user, CreateProject {
            name: "Test".to_string(),
            description: "Foo".to_string(),
        }.validated().unwrap());

        let permission = UserProjectPermission {
            user: target_user,
            project,
            permission: ProjectPermission::Read
        };

        let res = warp::test::request()
            .method("POST")
            .path("/project/permission/add")
            .header("Content-Length", "0")
            .header("Authorization", session.token.to_string())
            .json(&permission)
            .reply(&add_permission_handler(user_db.clone(), project_db.clone()))
            .await;

        assert_eq!(res.status(), 200);

        assert!(project_db.write().await.load(target_user, project).is_ok());
    }

    #[tokio::test]
    async fn remove_permission() {
        let user_db = Arc::new(RwLock::new(HashMapUserDB::default()));
        let project_db = Arc::new(RwLock::new(HashMapProjectDB::default()));

        user_db.write().await.register(UserRegistration {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
            real_name: "Foo Bar".to_string()
        }.validated().unwrap()).unwrap();

        let target_user = user_db.write().await.register(UserRegistration {
            email: "foo2@bar.de".to_string(),
            password: "secret1234".to_string(),
            real_name: "Foo2 Bar".to_string()
        }.validated().unwrap()).unwrap();

        let session = user_db.write().await.login(UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
        }).unwrap();

        let project = project_db.write().await.create(session.user, CreateProject {
            name: "Test".to_string(),
            description: "Foo".to_string(),
        }.validated().unwrap());

        let permission = UserProjectPermission {
            user: target_user,
            project,
            permission: ProjectPermission::Read
        };

        project_db.write().await.add_permission(session.user, permission.clone()).unwrap();

        let res = warp::test::request()
            .method("POST")
            .path("/project/permission/remove")
            .header("Content-Length", "0")
            .header("Authorization", session.token.to_string())
            .json(&permission)
            .reply(&remove_permission_handler(user_db.clone(), project_db.clone()))
            .await;

        assert_eq!(res.status(), 200);

        assert!(project_db.write().await.load(target_user, project).is_err());
    }

    #[tokio::test]
    async fn list_permissions() {
        let user_db = Arc::new(RwLock::new(HashMapUserDB::default()));
        let project_db = Arc::new(RwLock::new(HashMapProjectDB::default()));

        user_db.write().await.register(UserRegistration {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
            real_name: "Foo Bar".to_string()
        }.validated().unwrap()).unwrap();

        let target_user = user_db.write().await.register(UserRegistration {
            email: "foo2@bar.de".to_string(),
            password: "secret1234".to_string(),
            real_name: "Foo2 Bar".to_string()
        }.validated().unwrap()).unwrap();

        let session = user_db.write().await.login(UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
        }).unwrap();

        let project = project_db.write().await.create(session.user, CreateProject {
            name: "Test".to_string(),
            description: "Foo".to_string(),
        }.validated().unwrap());

        let permission = UserProjectPermission {
            user: target_user,
            project,
            permission: ProjectPermission::Read
        };

        project_db.write().await.add_permission(session.user, permission.clone()).unwrap();

        let res = warp::test::request()
            .method("POST")
            .path("/project/permission/list")
            .header("Content-Length", "0")
            .header("Authorization", session.token.to_string())
            .json(&project)
            .reply(&list_permissions_handler(user_db.clone(), project_db.clone()))
            .await;

        assert_eq!(res.status(), 200);

        let body: String = String::from_utf8(res.body().to_vec()).unwrap();
        let result = serde_json::from_str::<Vec<UserProjectPermission>>(&body);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }
}

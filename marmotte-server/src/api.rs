use actix_web::{HttpResponse, Responder, web};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;

use crate::document::Document;
use crate::model::models::Database;

pub struct AppState {
    databases_root: PathBuf,
    storage_lock: Mutex<()>,
}

impl AppState {
    pub fn new(databases_root: PathBuf) -> Self {
        Self {
            databases_root,
            storage_lock: Mutex::new(()),
        }
    }
}

#[derive(Deserialize)]
struct NamedResourceRequest {
    name: String,
}

#[derive(Serialize)]
struct NamedResourceResponse {
    name: String,
}

#[derive(Serialize)]
struct ApiError {
    error: &'static str,
}

fn error(status: actix_web::http::StatusCode, message: &'static str) -> HttpResponse {
    HttpResponse::build(status).json(ApiError { error: message })
}

fn is_valid_resource_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 255 {
        return false;
    }

    let mut components = Path::new(name).components();
    matches!(components.next(), Some(Component::Normal(part)) if part == name)
        && components.next().is_none()
}

fn database(state: &AppState, name: &str) -> Result<Database, HttpResponse> {
    if !is_valid_resource_name(name) {
        return Err(error(
            actix_web::http::StatusCode::BAD_REQUEST,
            "Invalid database name",
        ));
    }

    Ok(Database::new(
        state.databases_root.to_string_lossy().into_owned(),
        name.to_owned(),
    ))
}

async fn create_database(
    state: web::Data<AppState>,
    request: web::Json<NamedResourceRequest>,
) -> impl Responder {
    let db = match database(&state, &request.name) {
        Ok(db) => db,
        Err(response) => return response,
    };
    let _guard = match state.storage_lock.lock() {
        Ok(guard) => guard,
        Err(_) => {
            return error(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Storage lock is unavailable",
            );
        }
    };

    match db.create() {
        Ok(()) => HttpResponse::Created().json(NamedResourceResponse {
            name: request.name.clone(),
        }),
        Err(err) if err.kind() == ErrorKind::AlreadyExists => error(
            actix_web::http::StatusCode::CONFLICT,
            "Database already exists",
        ),
        Err(_) => error(
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to create database",
        ),
    }
}

async fn create_collection(
    state: web::Data<AppState>,
    database_name: web::Path<String>,
    request: web::Json<NamedResourceRequest>,
) -> impl Responder {
    let db = match database(&state, &database_name) {
        Ok(db) => db,
        Err(response) => return response,
    };
    if !is_valid_resource_name(&request.name) {
        return error(
            actix_web::http::StatusCode::BAD_REQUEST,
            "Invalid collection name",
        );
    }
    let _guard = match state.storage_lock.lock() {
        Ok(guard) => guard,
        Err(_) => {
            return error(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Storage lock is unavailable",
            );
        }
    };

    match db.create_collection(&request.name) {
        Ok(_) => HttpResponse::Created().json(NamedResourceResponse {
            name: request.name.clone(),
        }),
        Err(err) if err.kind() == ErrorKind::NotFound => error(
            actix_web::http::StatusCode::NOT_FOUND,
            "Database does not exist",
        ),
        Err(err) if err.kind() == ErrorKind::AlreadyExists => error(
            actix_web::http::StatusCode::CONFLICT,
            "Collection already exists",
        ),
        Err(_) => error(
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to create collection",
        ),
    }
}

async fn add_document(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
    payload: web::Json<Value>,
) -> impl Responder {
    let (database_name, collection_name) = path.into_inner();
    let db = match database(&state, &database_name) {
        Ok(db) => db,
        Err(response) => return response,
    };
    if !is_valid_resource_name(&collection_name) {
        return error(
            actix_web::http::StatusCode::BAD_REQUEST,
            "Invalid collection name",
        );
    }
    if !db.get_database_folder().is_dir() {
        return error(
            actix_web::http::StatusCode::NOT_FOUND,
            "Database does not exist",
        );
    }
    if !db.get_database_folder().join(&collection_name).is_dir() {
        return error(
            actix_web::http::StatusCode::NOT_FOUND,
            "Collection does not exist",
        );
    }

    let document = Document::new(payload.into_inner());
    let _guard = match state.storage_lock.lock() {
        Ok(guard) => guard,
        Err(_) => {
            return error(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Storage lock is unavailable",
            );
        }
    };

    match db.store_document(&collection_name, &document) {
        Ok(location) => HttpResponse::Created().json(location),
        Err(_) => error(
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to store document",
        ),
    }
}

pub fn configure(config: &mut web::ServiceConfig) {
    config
        .route("/databases", web::post().to(create_database))
        .route(
            "/databases/{database}/collections",
            web::post().to(create_collection),
        )
        .route(
            "/databases/{database}/collections/{collection}/documents",
            web::post().to(add_document),
        );
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{App, http::StatusCode, test};
    use serde_json::json;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TestRoot {
        temp_root: PathBuf,
        path: PathBuf,
    }

    impl TestRoot {
        fn new() -> Self {
            let temp_root = std::env::temp_dir().canonicalize().unwrap();
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = temp_root.join(format!("marmotte-api-{}-{stamp}", std::process::id()));
            fs::create_dir(&path).unwrap();
            Self { temp_root, path }
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            if let Ok(path) = self.path.canonicalize() {
                assert_eq!(path.parent(), Some(self.temp_root.as_path()));
                assert!(
                    path.file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .starts_with("marmotte-api-")
                );
                let _ = fs::remove_dir_all(path);
            }
        }
    }

    #[actix_web::test]
    async fn database_collection_and_document_can_be_created() {
        let root = TestRoot::new();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(AppState::new(root.path.clone())))
                .configure(configure),
        )
        .await;

        let request = test::TestRequest::post()
            .uri("/databases")
            .set_json(json!({ "name": "test" }))
            .to_request();
        assert_eq!(
            test::call_service(&app, request).await.status(),
            StatusCode::CREATED
        );

        let request = test::TestRequest::post()
            .uri("/databases/test/collections")
            .set_json(json!({ "name": "test_collection" }))
            .to_request();
        assert_eq!(
            test::call_service(&app, request).await.status(),
            StatusCode::CREATED
        );

        let request = test::TestRequest::post()
            .uri("/databases/test/collections/test_collection/documents")
            .set_json(json!({ "name": "John Doe", "age": 43, "id": 468 }))
            .to_request();
        let response = test::call_service(&app, request).await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let first_location: Value = test::read_body_json(response).await;

        let request = test::TestRequest::post()
            .uri("/databases/test/collections/test_collection/documents")
            .set_json(json!({ "name": "Jane Doe", "age": 39, "id": 469 }))
            .to_request();
        let response = test::call_service(&app, request).await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let second_location: Value = test::read_body_json(response).await;

        assert!(second_location["offset"].as_u64() > first_location["offset"].as_u64());
        assert!(
            root.path
                .join("test/test_collection/0000001.data")
                .is_file()
        );
    }

    #[actix_web::test]
    async fn path_traversal_names_are_rejected() {
        let root = TestRoot::new();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(AppState::new(root.path.clone())))
                .configure(configure),
        )
        .await;
        let request = test::TestRequest::post()
            .uri("/databases")
            .set_json(json!({ "name": "../outside" }))
            .to_request();

        assert_eq!(
            test::call_service(&app, request).await.status(),
            StatusCode::BAD_REQUEST
        );
    }
}

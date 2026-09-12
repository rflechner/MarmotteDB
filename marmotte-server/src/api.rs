use actix_web::{HttpResponse, Responder, web};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use crate::document::Document;
use crate::model::models::{Database, RecordLocation};

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

#[derive(Deserialize, ToSchema)]
struct NamedResourceRequest {
    /// Name of the resource to create.
    #[schema(example = "customers")]
    name: String,
}

#[derive(Serialize, ToSchema)]
struct NamedResourceResponse {
    /// Name of the created resource.
    #[schema(example = "customers")]
    name: String,
}

/// Location of a stored document inside a collection page file.
#[derive(Serialize, ToSchema)]
struct DocumentLocationResponse {
    /// Page file the document was written to.
    #[schema(example = "0000001.data")]
    page_file: String,
    /// Byte offset of the document record inside the page file.
    #[schema(example = 2048)]
    offset: u64,
}

impl From<RecordLocation> for DocumentLocationResponse {
    fn from(location: RecordLocation) -> Self {
        Self {
            page_file: location.page_file,
            offset: location.offset,
        }
    }
}

#[derive(Serialize, ToSchema)]
struct ApiError {
    /// Human readable reason why the request was rejected.
    #[schema(value_type = String, example = "Invalid database name")]
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

/// Create a database.
#[utoipa::path(
    post,
    path = "/databases",
    tag = "databases",
    request_body = NamedResourceRequest,
    responses(
        (status = 201, description = "Database created", body = NamedResourceResponse),
        (status = 400, description = "Invalid database name", body = ApiError),
        (status = 409, description = "Database already exists", body = ApiError),
        (status = 500, description = "Database could not be created", body = ApiError),
    )
)]
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

/// Create a collection inside an existing database.
#[utoipa::path(
    post,
    path = "/databases/{database}/collections",
    tag = "collections",
    params(("database" = String, Path, description = "Name of an existing database")),
    request_body = NamedResourceRequest,
    responses(
        (status = 201, description = "Collection created", body = NamedResourceResponse),
        (status = 400, description = "Invalid database or collection name", body = ApiError),
        (status = 404, description = "Database does not exist", body = ApiError),
        (status = 409, description = "Collection already exists", body = ApiError),
        (status = 500, description = "Collection could not be created", body = ApiError),
    )
)]
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

/// Append a JSON document to an existing collection.
#[utoipa::path(
    post,
    path = "/databases/{database}/collections/{collection}/documents",
    tag = "documents",
    params(
        ("database" = String, Path, description = "Name of an existing database"),
        ("collection" = String, Path, description = "Name of an existing collection"),
    ),
    request_body(
        content = Object,
        description = "Arbitrary JSON document",
        content_type = "application/json",
        example = json!({ "name": "John Doe", "age": 43, "id": 468 }),
    ),
    responses(
        (status = 201, description = "Document stored", body = DocumentLocationResponse),
        (status = 400, description = "Invalid database or collection name", body = ApiError),
        (status = 404, description = "Database or collection does not exist", body = ApiError),
        (status = 500, description = "Document could not be stored", body = ApiError),
    )
)]
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
        Ok(location) => HttpResponse::Created().json(DocumentLocationResponse::from(location)),
        Err(_) => error(
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to store document",
        ),
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Marmotte DB",
        description = "REST API of the Marmotte DB document store.",
    ),
    paths(create_database, create_collection, add_document),
    components(schemas(
        NamedResourceRequest,
        NamedResourceResponse,
        DocumentLocationResponse,
        ApiError
    )),
    tags(
        (name = "databases", description = "Database lifecycle"),
        (name = "collections", description = "Collection lifecycle"),
        (name = "documents", description = "Document storage"),
    )
)]
pub struct ApiDoc;

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
        )
        .service(
            SwaggerUi::new("/swagger-ui/{_:.*}").url("/api-docs/openapi.json", ApiDoc::openapi()),
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
    async fn openapi_document_describes_every_route() {
        let root = TestRoot::new();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(AppState::new(root.path.clone())))
                .configure(configure),
        )
        .await;

        let request = test::TestRequest::get()
            .uri("/api-docs/openapi.json")
            .to_request();
        let response = test::call_service(&app, request).await;
        assert_eq!(response.status(), StatusCode::OK);

        let spec: Value = test::read_body_json(response).await;
        assert!(spec["paths"]["/databases"]["post"].is_object());
        assert!(spec["paths"]["/databases/{database}/collections"]["post"].is_object());
        assert!(
            spec["paths"]["/databases/{database}/collections/{collection}/documents"]["post"]
                .is_object()
        );
        assert!(spec["components"]["schemas"]["DocumentLocationResponse"].is_object());
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

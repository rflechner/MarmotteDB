use crate::document::Document;
use crate::storage::disk_writer::DiskWriter;
use std::path::PathBuf;

#[derive(Debug)]
pub struct DatabaseCollection {
    pub name: String,
}

#[derive(Debug)]
pub struct Database {
    pub storage_root_folder: String,
    pub name: String,
    pub collections: Vec<DatabaseCollection>,
}

#[derive(Debug)]
pub struct RecordLocation {
    pub page_file: String,
    pub offset: u64,
}

impl Database {
    pub(crate) fn new(storage_root_folder: String, name: String) -> Self {
        Self {
            storage_root_folder,
            name,
            collections: Vec::new(),
        }
    }

    pub fn add_collection(&mut self, collection: DatabaseCollection) {
        self.collections.push(collection);
    }

    pub fn add_collections(&mut self, collections: Vec<DatabaseCollection>) {
        self.collections.extend(collections);
    }

    pub fn add_collection_by_name(&mut self, name: String) {
        self.collections.push(DatabaseCollection { name });
    }

    pub fn delete_collection_by_name(&mut self, name: String) {
        self.collections
            .retain(|collection| collection.name != name);
    }

    pub fn get_database_folder(&self) -> PathBuf {
        PathBuf::from(&self.storage_root_folder).join(&self.name)
    }

    pub fn store_document(
        &self,
        collection_name: &str,
        document: &Document,
    ) -> Result<RecordLocation, &str> {
        let collection_folder = self.get_database_folder().join(collection_name);
        // ensure collection folder exists
        std::fs::create_dir_all(&collection_folder)
            .map_err(|_| "Failed to create collection folder")?;
        let data_file_path = collection_folder.join("0000001.data");
        let mut data_writer = DiskWriter::new(data_file_path.to_str().unwrap(), 2048);

        let binary_content = document.as_bytes().map_err(|_| "Failed to add record")?;
        let offset = data_writer.add_record(&binary_content);

        Ok(RecordLocation {
            page_file: data_file_path.to_str().unwrap().to_string(),
            offset,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binary_serializer::BinarySerializer;
    use crate::storage::disk_reader::{DiskReader, DiskReaderOptions};
    use crate::storage::disk_writer::RecordsFileMeta;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TestDatabaseRoot {
        temp_root: PathBuf,
        path: PathBuf,
    }

    impl TestDatabaseRoot {
        fn new() -> Self {
            let temp_root = std::env::temp_dir().canonicalize().unwrap();
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = temp_root.join(format!("marmotte-database-{}-{stamp}", std::process::id()));
            fs::create_dir(&path).unwrap();

            Self { temp_root, path }
        }
    }

    impl Drop for TestDatabaseRoot {
        fn drop(&mut self) {
            if let Ok(path) = self.path.canonicalize() {
                assert_eq!(path.parent(), Some(self.temp_root.as_path()));
                assert!(path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with("marmotte-database-"));
                let _ = fs::remove_dir_all(path);
            }
        }
    }

    #[test]
    fn store_document_should_write_it_to_the_collection_page() {
        let databases_folder = TestDatabaseRoot::new();
        let db = Database::new(
            databases_folder.path.to_str().unwrap().to_string(),
            "test".to_string(),
        );
        let collection_name = "test_collection";
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "id": 468
        }"#;
        let document = Document::from_slice(data.as_bytes()).unwrap();

        let record_location = db.store_document(collection_name, &document).unwrap();

        let expected_page = databases_folder
            .path
            .join("test")
            .join(collection_name)
            .join("0000001.data");
        assert_eq!(PathBuf::from(&record_location.page_file), expected_page);
        assert!(expected_page.is_file());

        let mut reader = DiskReader::new(
            record_location.page_file.as_str(),
            DiskReaderOptions::create_default(),
        );
        let stored_record = reader.next().unwrap();
        let stored_value = BinarySerializer::deserialize_json(&stored_record.content).unwrap();

        assert_eq!(document.value(), &stored_value);
        assert_eq!(record_location.offset, RecordsFileMeta::size() as u64);
    }
}

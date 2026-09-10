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

    pub fn store_document(&self, collection_name: &str, document: &Document) -> Result<RecordLocation, &str> {
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

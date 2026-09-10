use std::path::PathBuf;

pub struct DatabaseCollection {
    pub name: String,
}

pub struct Database {
    pub storage_root_folder: String,
    pub name: String,
    pub collections: Vec<DatabaseCollection>,
}

impl Database {
    fn new(storage_root_folder: String, name: String) -> Self {
        Self {
            storage_root_folder,
            name,
            collections: Vec::new(),
        }
    }

    fn add_collection(&mut self, collection: DatabaseCollection) {
        self.collections.push(collection);
    }

    fn add_collections(&mut self, collections: Vec<DatabaseCollection>) {
        self.collections.extend(collections);
    }

    fn add_collection_by_name(&mut self, name: String) {
        self.collections.push(DatabaseCollection { name });
    }

    fn delete_collection_by_name(&mut self, name: String) {
        self.collections
            .retain(|collection| collection.name != name);
    }

    fn get_database_folder(&self) -> PathBuf {
        PathBuf::from(&self.storage_root_folder).join(&self.name)
    }
}

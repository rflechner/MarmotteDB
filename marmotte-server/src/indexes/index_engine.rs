use serde_json::Value;
use crate::binary_serializer::TypeFlag;

pub enum IndexAlgo {
    SortedTable,
}

pub struct IndexDeclaration {
    pub database: String,
    pub collection: String,
    pub property_path: String,
    pub type_flag: TypeFlag,
    pub algo: IndexAlgo,
    pub asynchronous: bool,
}

trait IndexEngine {

    fn index_document(&self, index_declaration: IndexDeclaration, document: &Value);

}

struct IndexEngineImpl;

impl IndexEngine for IndexEngineImpl {
    fn index_document(&self, index_declaration: IndexDeclaration, document: &Value) {
        todo!()
    }
}

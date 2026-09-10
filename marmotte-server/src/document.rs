use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Document {
    value: Value,
}

impl Document {
    pub fn new(value: Value) -> Self {
        Self { value }
    }

    pub fn from_slice(payload: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(payload)
    }

    pub fn value(&self) -> &Value {
        &self.value
    }

    pub fn into_value(self) -> Value {
        self.value
    }

    pub fn id(&self) -> Option<String> {
        match self.value.get("id")? {
            Value::String(id) => Some(id.clone()),
            Value::Number(id) => Some(id.to_string()),
            _ => None,
        }
    }

    pub fn property_values(&self, path: &str) -> Vec<&Value> {
        fn match_property_level<'a>(values: Vec<&'a Value>, part: &str) -> Vec<&'a Value> {
            values
                .into_iter()
                .flat_map(|value| match value {
                    Value::Array(items) => match_property_level(items.iter().collect(), part),
                    Value::Object(properties) => properties.get(part).into_iter().collect(),
                    _ => Vec::new(),
                })
                .collect()
        }

        path.split('.').fold(vec![&self.value], |values, part| {
            match_property_level(values, part)
        })
    }
}

impl From<Value> for Document {
    fn from(value: Value) -> Self {
        Self::new(value)
    }
}

impl From<Document> for Value {
    fn from(document: Document) -> Self {
        document.into_value()
    }
}

impl AsRef<Value> for Document {
    fn as_ref(&self) -> &Value {
        self.value()
    }
}

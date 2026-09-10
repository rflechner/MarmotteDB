pub mod document {

    use serde_json::Value;
    use bytes::BytesMut;

    pub fn find_id(payload: BytesMut) -> Option<String> {
        match serde_json::from_slice::<Value>(&payload) {
            serde_json::Result::Ok(v) => find_id_of_document(v),
            _ => None
        }
    }

    pub fn find_id_of_document(v: Value) -> Option<String> {
        match &v["id"] {
            Value::String(id) => {
                Some(id.clone())
            },
            Value::Number(id) => {
                Some(id.to_string())
            }
            _ => { None }
        }
    }

    pub fn get_property_value(v: Value, path: String) -> Vec<Value> {

        fn match_property_level(current_level:Vec<Value>, part: &str) -> Vec<Value> {
            current_level.iter().map(|v| {
                if let Value::Array(items) = v {
                    items.iter().map(move |l| {
                        match_property_level([l.clone()].to_vec(), part)
                    }).flatten().collect()
                } else {
                    match &v[part] {
                        Value::Null => [].to_vec(),
                        Value::Bool(b) => [Value::Bool(*b)].to_vec(),
                        Value::Number(n) => [Value::Number(n.clone())].to_vec(),
                        Value::String(s) => [Value::String(s.clone())].to_vec(),
                        Value::Array(values) => [Value::Array(values.clone())].to_vec(),
                        Value::Object(o) => [Value::Object(o.clone())].to_vec(),
                    }
                }
            })
                .flatten()
                .collect()
        }

        let parts: Vec<&str> = path.split('.').collect();
        let init:Vec<Value> = [v].to_vec();

        let result = parts.iter().fold(init, |current_level, part| {
            match_property_level(current_level, part)
        });

        result
    }

}

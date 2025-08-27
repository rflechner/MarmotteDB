use bytes::{BytesMut, Bytes};
use serde_json::Map;

use crate::binary::*;

use serde_json::Value;

/*
## Datagram:

### Property encoding:

```text
| Name (Text) | Value (bytes) |
```

### Values encoding

```text
| TypeFlag (1 byte) | Data (bytes) |
```

#### Bool:

```text
| TypeFlag | Data   |
| 0        | 1 byte |
```

#### Number:

```text
| TypeFlag | Data     |
| 1        | 64 bytes |
```

#### Text:

```text
| TypeFlag | Length prefix | Data     |
| 2        | 64 bytes      | bytes    |
```

#### Array:

```text
| TypeFlag | Length prefix | Data     |
| 3        | 64 bytes      | bytes    |
```

*/

#[derive(PartialEq)]
pub enum TypeFlag {
    Null,
    Bool,
    Int64,
    Float,
    Text,
    Array,
    Object
}

impl TypeFlag {

    fn to_bin(&self) -> u8 {
        match &self {
            TypeFlag::Null => 0,
            TypeFlag::Bool => 1,
            TypeFlag::Int64 => 2,
            TypeFlag::Float => 3,
            TypeFlag::Text => 4,
            TypeFlag::Array => 5,
            TypeFlag::Object => 6
        }
    }

    fn From(v: u8) -> Result<TypeFlag, String>{
        match v {
            0 => Ok(TypeFlag::Null),
            1 => Ok(TypeFlag::Bool),
            2 => Ok(TypeFlag::Int64),
            3 => Ok(TypeFlag::Float),
            4 => Ok(TypeFlag::Text),
            5 => Ok(TypeFlag::Array),
            6 => Ok(TypeFlag::Object),
            n => Err(format!("{} is not a valid type flag.", n))
        }
    }

}

pub struct BinarySerializer {
    pub writer : Box<BinaryWriter>
}

impl BinarySerializer {

    pub fn new() -> BinarySerializer {
        let wr = BinaryWriter { buffer: BytesMut::new() };
        BinarySerializer { writer:Box::new(wr) }
    }

    pub fn serialize_json<'s>(json:&String) -> Result<Bytes, &'s str> {

        match serde_json::from_str::<Value>(json) {
            Err(_) => Err(&"Could not parse JSON"),
            Ok(value) => {
                let wr = BinaryWriter { buffer: BytesMut::with_capacity(json.len()) };
                let mut serializer = BinarySerializer { writer:Box::new(wr) };
                match serializer.serialize_json_value(&value, json.len()) {
                    Ok(_) => {
                        let b = serializer.writer.buffer;
                        let f = b.freeze();
                        Ok(f)
                    },
                    Err(e) => Err(e)
                }
            }
        }
    }

    pub fn serialize_json_value<'s>(&mut self, json: &Value, max_capacity: usize) -> Result<(), &'s str> {
        //let mut callstack: LinkedList<&Value> = LinkedList::new();
        match json {
            Value::Object(o) => {
                self.writer.write_u8(TypeFlag::Object.to_bin());
                let len = o.len() as u64;
                self.writer.write_bytes(&len.to_be_bytes());
                for key in o.keys() {
                    self.writer.write_string(key);
                    self.serialize_json_value(&o[key], max_capacity).expect("cannot serialize json array");
                }
                Ok(())
            },
            Value::Null => {
                self.writer.write_u8(TypeFlag::Null.to_bin());
                Ok(())
            },
            Value::Bool(b) => {
                self.writer.write_u8(TypeFlag::Bool.to_bin());
                self.writer.write_bool(*b);
                Ok(())
            },
            Value::Number(number) => {
                match number.as_i64() {
                    Some(n) => {
                        self.writer.write_u8(TypeFlag::Int64.to_bin());
                        self.writer.write_i64(n);
                    },
                    None => {
                        match number.as_f64() {
                            None => {},
                            Some(f) => {
                                self.writer.write_u8(TypeFlag::Float.to_bin());
                                self.writer.write_f64(f);
                            }
                        }
                    }
                }
                Ok(())
            },
            Value::String(s) => {
                self.writer.write_u8(TypeFlag::Text.to_bin());
                self.writer.write_string(s);
                Ok(())
            },
            Value::Array(a) => {
                self.writer.write_u8(TypeFlag::Array.to_bin());
                let len = a.len().to_be_bytes();
                self.writer.write_bytes(&len);
                for item in a {
                    //callstack.push_back(item);
                    self.serialize_json_value(&item, max_capacity).expect("cannot serialize json array");
                }
                Ok(())
            },
            _ => Err(&"cannot convert this kind of document.")
        }

    }

    pub fn read_json_object_properties(reader: &mut BinaryReader) -> Result<Value, String> {
        let property_count = reader.read_u64()?;
        let mut properties: Map<String, Value> = Map::new();

        for _ in 0..property_count {
            let name = reader.read_string()
                .or_else(|e| {
                    Err(format!("deserialize_json: cannot read property name : {}", e))
                })?;
            let flag_data = reader.read_u8()?;
            let flag = TypeFlag::From(flag_data).or_else(|e| { Err(format!("cannot read property type : {}", e)) })?;
            let value = BinarySerializer::read_value(flag, reader)?;

            properties.insert(name, value);
        }

        Ok(Value::Object(properties))
    }

    pub fn read_json_object(reader: &mut BinaryReader) -> Result<Value, String> {

        let flag_data = reader.read_u8()?;
        let flag = TypeFlag::From(flag_data).or_else(|e| { Err(format!("cannot read property type : {}", e)) })?;

        BinarySerializer::read_json_object_properties(reader)
    }

    pub fn read_value(t: TypeFlag, reader: &mut BinaryReader) -> Result<Value, String> {
        match t {
            TypeFlag::Null => {
                reader.read_bool();
                Ok(Value::Null)
            },
            TypeFlag::Bool => Ok(Value::Bool(reader.read_bool().map_err(String::from)?)),
            TypeFlag::Text => Ok(Value::String(reader.read_string().map_err(String::from)?)),
            TypeFlag::Int64 => {
                let v = reader.read_i64().map_err(String::from)?;
                Ok(serde_json::to_value(v).or_else(|_| { Err(format!("cannot read Int64 {}", v)) })?)
            },
            TypeFlag::Float => {
                let v = reader.read_f64().map_err(String::from)?;
                Ok(serde_json::to_value(v).or_else(|_| { Err(format!("cannot read Float {}", v)) })?)
            },
            TypeFlag::Array => {
                let count = reader.read_i64().map_err(String::from)?;
                let mut items: Vec<Value> = Vec::new();
                for _ in 0..count {
                    let flag_data = reader.read_u8()?;
                    let flag = TypeFlag::From(flag_data).or_else(|e| { Err(format!("cannot read property type : {}", e)) })?;
                    let value = BinarySerializer::read_value(flag, reader)?;
                    items.push(value);
                }
                Ok(Value::Array(items))
            },
            TypeFlag::Object => {
                BinarySerializer::read_json_object_properties(reader)
            },
            _ => Err(String::from("not implemented."))
        }
    }

    pub fn deserialize_json(src: &[u8]) -> Result<Value, String> {
        let bytes = BytesMut::from(src);
        let mut reader = BinaryReader::from(bytes);
        BinarySerializer::read_json_object(&mut reader)
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_flag_to_bin_should_return_valid_value() -> Result<(), String> {
        assert_eq!(0, TypeFlag::Null.to_bin());
        assert_eq!(1, TypeFlag::Bool.to_bin());
        assert_eq!(2, TypeFlag::Int64.to_bin());
        assert_eq!(3, TypeFlag::Float.to_bin());
        assert_eq!(4, TypeFlag::Text.to_bin());
        assert_eq!(5, TypeFlag::Array.to_bin());

        Ok(())
    }

    #[test]
    fn type_flag_from_bin_should_return_valid_value() -> Result<(), String> {
        assert_eq!(TypeFlag::From(0).unwrap().to_bin(), TypeFlag::Null.to_bin());
        assert_eq!(TypeFlag::From(1).unwrap().to_bin(), TypeFlag::Bool.to_bin());
        assert_eq!(TypeFlag::From(2).unwrap().to_bin(), TypeFlag::Int64.to_bin());
        assert_eq!(TypeFlag::From(3).unwrap().to_bin(), TypeFlag::Float.to_bin());
        assert_eq!(TypeFlag::From(4).unwrap().to_bin(), TypeFlag::Text.to_bin());
        assert_eq!(TypeFlag::From(5).unwrap().to_bin(), TypeFlag::Array.to_bin());

        Ok(())
    }

    #[test]
    fn serialize_simple_payload_should_success() -> Result<(), String> {
        let payload = r#"
        {
            "name": "John Doe",
            "age": 48,
            "activated": true
        }"#;
        //let json = serde_json::from_str::<Value>(&payload).unwrap();
        let bin:Bytes = BinarySerializer::serialize_json(&String::from(payload))?;
        let doc = BinarySerializer::deserialize_json(&bin)?;

        assert_eq!(doc["name"], "John Doe");
        assert_eq!(doc["age"], 48);
        assert_eq!(doc["activated"], true);
        assert_eq!(doc["activated"], Value::Bool(true));

        Ok(())
    }

    #[test]
    fn serialize_payload_with_int_array_should_success() -> Result<(), String> {
        let payload = r#"
        {
            "name": "John Doe",
            "age": 48,
            "activated": true,
            "messageIds": [1234, 998]
        }"#;
        //let json = serde_json::from_str::<Value>(&payload).unwrap();
        let bin:Bytes = BinarySerializer::serialize_json(&String::from(payload))?;
        let doc = BinarySerializer::deserialize_json(&bin)?;

        assert_eq!(doc["name"], "John Doe");
        assert_eq!(doc["age"], 48);
        assert_eq!(doc["activated"], true);
        assert_eq!(doc["activated"], Value::Bool(true));

        let v1 = serde_json::to_value(1234).or_else(|e| { Err(format!("cannot create a value : {}", e)) })?;
        let v2 = serde_json::to_value(998).or_else(|e| { Err(format!("cannot create a value : {}", e)) })?;

        assert_eq!(doc["messageIds"], Value::Array(vec![ v1, v2 ]));

        Ok(())
    }

    #[test]
    fn serialize_complex_payload_with_objects_array_should_success() -> Result<(), String> {
        let payload = r#"
        {
          "id": 9800,
          "Name": "John Doe",
          "Age": 35,
          "messages": [
              { "title": "Hello", "text": "ca va" }
          ]
        }"#;
        //let json = serde_json::from_str::<Value>(&payload).unwrap();
        let bin:Bytes = BinarySerializer::serialize_json(&String::from(payload))?;
        let doc = BinarySerializer::deserialize_json(&bin)?;

        assert_eq!(doc["Name"], "John Doe");
        assert_eq!(doc["Age"], 35);
        assert_eq!(doc["id"], 9800);

        let messages = &doc["messages"];
        match messages {
            Value::Array(values) => {
                assert_eq!(1, values.len());
                match &values[0] {
                    Value::Object(message) => {
                        assert_eq!(message.len(), 2);
                        assert_eq!(message["title"], "Hello");
                        assert_eq!(message["text"], "ca va");
                    },
                    _ => panic!("should be an array")
                }
            },
            _ => panic!("should be an array"),
        }

        Ok(())
    }

    #[test]
    fn serialize_complex_payload_with_objects_array_of_2_items_should_success() -> Result<(), String> {
        let payload = r#"
        {
          "id": 9800,
          "Name": "John Doe",
          "Age": 35,
          "messages": [
              { "title": "Hello", "text": "ca va" },
              { "title": "Bye", "text": "yes" }
          ]
        }"#;
        //let json = serde_json::from_str::<Value>(&payload).unwrap();
        let bin:Bytes = BinarySerializer::serialize_json(&String::from(payload))?;
        let doc = BinarySerializer::deserialize_json(&bin)?;

        assert_eq!(doc["Name"], "John Doe");
        assert_eq!(doc["Age"], 35);
        assert_eq!(doc["id"], 9800);

        let messages = &doc["messages"];
        match messages {
            Value::Array(values) => {
                assert_eq!(2, values.len());

                match &values[0] {
                    Value::Object(message) => {
                        assert_eq!(message.len(), 2);
                        assert_eq!(message["title"], "Hello");
                        assert_eq!(message["text"], "ca va");
                    },
                    _ => panic!("should be an object")
                }

                match &values[1] {
                    Value::Object(message) => {
                        assert_eq!(message.len(), 2);
                        assert_eq!(message["title"], "Bye");
                        assert_eq!(message["text"], "yes");
                    },
                    _ => panic!("should be an object")
                }

            },
            _ => panic!("should be an array"),
        }

        Ok(())
    }

}

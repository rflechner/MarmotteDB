use bytes::BytesMut;
use serde_json::Value;

use crate::document::document;

fn parse_json(json: &str) -> Value {
    let payload = BytesMut::from(json);
    serde_json::from_slice::<Value>(&payload).unwrap()
}

#[test]
fn property_value_should_be_string() -> Result<(), String> {
    let json = parse_json(
        r#"
    {
        "name": "John Doe",
        "age": 43,
        "id": "id-4687"
    }"#,
    );
    let r = document::get_property_value(json, String::from("name"));
    assert_eq!([Value::String("John Doe".to_string())].to_vec(), r);
    Ok(())
}

#[test]
fn property_value_of_level2_should_be_string() -> Result<(), String> {
    let json = parse_json(
        r#"
    {
        "name": "John Doe",
        "message": {
          "title": "hello !",
          "text": "How are you ?"
        },
        "age": 43,
        "id": "id-4687"
    }"#,
    );
    let r = document::get_property_value(json, String::from("message.title"));
    assert_eq!([Value::String("hello !".to_string())].to_vec(), r);
    Ok(())
}

#[test]
fn property_value_of_level2_should_be_string_array() -> Result<(), String> {
    let json = parse_json(
        r#"
    {
        "name": "John Doe",
        "messages": [
          {
            "id": 1,
            "title": "hello !",
            "text": "How are you ?"
          },
          {
            "id": 2,
            "title": "hello 2 !",
            "text": "How are you 2 ?"
          },
          {
            "id": 3,
            "text": "How are you 3 ?"
          },
          {
            "id": 4,
            "title": "hello 4 !",
            "text": "How are you 4 ?"
          }
        ],
        "age": 43,
        "id": "id-4687"
    }"#,
    );
    let r = document::get_property_value(json, String::from("messages.title"));
    assert_eq!(
        [
            Value::String("hello !".to_string()),
            Value::String("hello 2 !".to_string()),
            Value::String("hello 4 !".to_string()),
        ]
        .to_vec(),
        r
    );
    Ok(())
}

#[test]
fn property_value_of_level3_should_be_bool() -> Result<(), String> {
    let json = parse_json(
        r#"
    {
        "name": "John Doe",
        "message": {
          "title": "hello !",
          "text": "How are you ?",
          "meta": {
            "deleted": true,
            "readcount": 2
          }
        },
        "age": 43,
        "id": "id-4687"
    }"#,
    );
    let r = document::get_property_value(json, String::from("message.meta.deleted"));
    assert_eq!([Value::Bool(true)].to_vec(), r);
    Ok(())
}

#[test]
fn find_id_should_return_string_id() -> Result<(), String> {
    let data = r#"
    {
        "name": "John Doe",
        "age": 43,
        "id": "id-4687"
    }"#;
    let r = document::find_id(BytesMut::from(data));
    assert_eq!(Some(String::from("id-4687")), r);
    Ok(())
}

#[test]
fn find_id_should_return_number_id() -> Result<(), String> {
    let data = r#"
    {
        "name": "John Doe",
        "age": 43,
        "id": 4687
    }"#;
    let r = document::find_id(BytesMut::from(data));
    assert_eq!(Some(String::from("4687")), r);
    Ok(())
}

#[test]
fn find_id_receiving_invalid_json_should_return_none() -> Result<(), String> {
    let data = r#"
    {
        "name": "John Doe",
        "age": 43,
        "id": 4687sa
    }"#;
    let r = document::find_id(BytesMut::from(data));
    assert_eq!(None, r);
    Ok(())
}

#[test]
fn find_id_receiving_json_without_id_should_return_none() -> Result<(), String> {
    let data = r#"
    {
        "name": "John Doe",
        "age": 43
    }"#;
    let r = document::find_id(BytesMut::from(data));
    assert_eq!(None, r);
    Ok(())
}

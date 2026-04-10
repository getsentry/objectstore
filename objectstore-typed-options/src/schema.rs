use std::path::Path;

use serde_json::{Map, Value};

/// Converts the JSON Schema produced by schemars for `T` into sentry-options schema format.
///
/// The sentry-options format has strict per-level rules about which keys are allowed:
///
/// - **Root**: `version` (added here), `type`, `properties`
/// - **Top-level property** (inside `properties`): `type`, `description`, `default`, `items`,
///   `additionalProperties`
/// - **Items** (array element schema): `type`, `properties`, `additionalProperties`
/// - **Nested property** (inside an items object's `properties`): `type`,
///   `additionalProperties`, `optional`
///
/// `$ref` references are inlined from `$defs`. Nullable types (`"type": ["T", "null"]` or
/// `"anyOf": [T, null]`) are unwrapped to the base type; fields detected as nullable are
/// marked `"optional": true` at the nested-property level.
pub fn generate_sentry_schema<T: schemars::JsonSchema>() -> Value {
    let schema = schemars::schema_for!(T);
    let json = serde_json::to_value(&schema).expect("schema serialization cannot fail");

    let defs = match json.get("$defs") {
        Some(Value::Object(map)) => map.clone(),
        _ => Map::new(),
    };

    let converted = convert_schema(&json, &defs, Level::Root);

    let mut root = Map::new();
    root.insert("version".to_string(), Value::String("1.0".to_string()));
    if let Value::Object(map) = converted {
        for (key, value) in map {
            root.insert(key, value);
        }
    }

    Value::Object(root)
}

/// Asserts that the sentry-options schema for `T` matches the golden file at `schema_path`.
///
/// If the `UPDATE_SCHEMA` environment variable is set to `"1"`, the golden file is
/// regenerated instead of compared.
pub fn assert_schema_matches_golden_file<T: schemars::JsonSchema>(schema_path: &Path) {
    let generated = generate_sentry_schema::<T>();
    let generated_str =
        serde_json::to_string_pretty(&generated).expect("schema serialization cannot fail") + "\n";

    if std::env::var("UPDATE_SCHEMA").as_deref() == Ok("1") {
        std::fs::write(schema_path, &generated_str).expect("write schema golden file");
        return;
    }

    let existing = std::fs::read_to_string(schema_path).expect("read schema golden file");
    if existing != generated_str {
        panic!(
            "schema golden file is out of date:\n{}\nRun with UPDATE_SCHEMA=1 to regenerate.",
            render_diff(&existing, &generated_str),
        );
    }
}

fn render_diff(old: &str, new: &str) -> String {
    use similar::{ChangeTag, TextDiff};

    let diff = TextDiff::from_lines(old, new);
    let mut out = String::new();

    for change in diff.iter_all_changes() {
        let (prefix, color_open, color_close) = match change.tag() {
            ChangeTag::Equal => ("  ", "", ""),
            ChangeTag::Delete => ("- ", "\x1b[31m", "\x1b[0m"),
            ChangeTag::Insert => ("+ ", "\x1b[32m", "\x1b[0m"),
        };
        out.push_str(&format!(
            "{}{}{}{}",
            color_open, prefix, change, color_close
        ));
    }

    out
}

// --- conversion internals ---

/// Tracks which structural level of the sentry-options schema format is being produced.
#[derive(Clone, Copy)]
enum Level {
    /// The root object (`{ "version", "type", "properties" }`).
    Root,
    /// A key directly inside the root `properties` map. Allowed: `type`, `description`,
    /// `default`, `items`, `additionalProperties`.
    TopLevelProp,
    /// The `items` schema of an array-typed top-level property. Allowed: `type`, `properties`,
    /// `additionalProperties`.
    Items,
    /// A key inside the `properties` of an items object. Allowed: `type`,
    /// `additionalProperties`, `optional`.
    NestedProp,
}

fn convert_schema(schema: &Value, defs: &Map<String, Value>, level: Level) -> Value {
    match schema {
        Value::Object(map) => convert_object(map, defs, level),
        other => other.clone(),
    }
}

fn convert_object(map: &Map<String, Value>, defs: &Map<String, Value>, level: Level) -> Value {
    // Inline $ref references before any level-specific handling.
    if let Some(def_schema) = map
        .get("$ref")
        .and_then(|v| v.as_str())
        .and_then(|s| s.strip_prefix("#/$defs/"))
        .and_then(|name| defs.get(name))
    {
        return convert_schema(def_schema, defs, level);
    }

    match level {
        Level::Root => {
            let mut out = Map::new();
            for key in ["type", "properties"] {
                let Some(value) = map.get(key) else {
                    continue;
                };
                let converted = if key == "properties" {
                    convert_properties(value, defs, Level::TopLevelProp)
                } else {
                    value.clone()
                };
                out.insert(key.to_string(), converted);
            }
            Value::Object(out)
        }

        Level::TopLevelProp => {
            let mut out = Map::new();
            out.insert("type".to_string(), scalar_type(map));
            for key in ["description", "default", "items", "additionalProperties"] {
                let Some(value) = map.get(key) else {
                    continue;
                };
                let converted = match key {
                    "items" => convert_schema(value, defs, Level::Items),
                    "additionalProperties" => convert_schema(value, defs, Level::NestedProp),
                    _ => value.clone(),
                };
                out.insert(key.to_string(), converted);
            }
            Value::Object(out)
        }

        Level::Items => {
            let mut out = Map::new();
            for key in ["type", "properties", "additionalProperties"] {
                let Some(value) = map.get(key) else {
                    continue;
                };
                let converted = match key {
                    "properties" => convert_properties(value, defs, Level::NestedProp),
                    "additionalProperties" => convert_schema(value, defs, Level::NestedProp),
                    _ => value.clone(),
                };
                out.insert(key.to_string(), converted);
            }
            Value::Object(out)
        }

        Level::NestedProp => {
            let is_optional = is_optional(map);
            let mut out = Map::new();
            out.insert("type".to_string(), scalar_type(map));
            if let Some(value) = map.get("additionalProperties") {
                out.insert(
                    "additionalProperties".to_string(),
                    convert_schema(value, defs, Level::NestedProp),
                );
            }
            if is_optional {
                out.insert("optional".to_string(), Value::Bool(true));
            }
            Value::Object(out)
        }
    }
}

/// Extracts the scalar type string, unwrapping nullable array types like `["string", "null"]`.
fn scalar_type(map: &Map<String, Value>) -> Value {
    match map.get("type") {
        Some(Value::Array(types)) => {
            let non_null: Vec<&Value> = types
                .iter()
                .filter(|t| t.as_str() != Some("null"))
                .collect();
            if non_null.len() == 1 {
                return (*non_null[0]).clone();
            }
            Value::Array(types.clone())
        }
        Some(t) => t.clone(),
        None => Value::Null,
    }
}

/// Returns `true` if the field is optional: nullable type, anyOf with null, or has a default.
fn is_optional(map: &Map<String, Value>) -> bool {
    if map.contains_key("default") {
        return true;
    }
    if matches!(map.get("type"), Some(Value::Array(types)) if types.iter().any(|t| t.as_str() == Some("null")))
    {
        return true;
    }
    if matches!(
        map.get("anyOf"),
        Some(Value::Array(variants))
            if variants.iter().any(|v| v.get("type").is_some_and(|t| t == "null"))
    ) {
        return true;
    }
    false
}

fn convert_properties(props: &Value, defs: &Map<String, Value>, level: Level) -> Value {
    let Value::Object(map) = props else {
        return props.clone();
    };
    let converted = map
        .iter()
        .map(|(k, v)| (k.clone(), convert_schema(v, defs, level)))
        .collect();
    Value::Object(converted)
}

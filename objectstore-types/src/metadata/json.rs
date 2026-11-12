//! The JSON representation of object metadata in the public API.
//!
//! Defines [`InsertMetadata`] and [`ObjectMetadata`] and their conversions to and from
//! [`Metadata`]. The format is described in the [parent module](super#json-in-the-public-api).

use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;

use serde::de::value::MapAccessDeserializer;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{
    Compression, DEFAULT_CONTENT_TYPE, Error, ExpirationPolicy, Metadata, normalize_custom_key,
    validate_content_type,
};
use crate::time::{Rfc3339Timestamp, Timestamp};

/// Writable object metadata, as sent by clients when inserting an object.
///
/// Deserialization is strict: unknown fields, read-only fields (`time_created`, `time_expires`,
/// `size`), duplicate fields, and invalid values, including `null`, are errors. Duplicate keys
/// inside [`custom`](Self::custom) are not an error; the last value wins. All fields are optional,
/// and unset fields are omitted when serializing. See the [field
/// table](super#json-in-the-public-api).
///
/// Convert to [`Metadata`] with [`into_metadata`](Self::into_metadata), which stamps the creation
/// time and resolves the expiration deadline.
///
/// ```
/// use objectstore_types::metadata::InsertMetadata;
/// use objectstore_types::time::Timestamp;
///
/// let json = r#"{"content_type":"text/plain","expiration":"ttl:1h"}"#;
/// let insert: InsertMetadata = serde_json::from_str(json)?;
///
/// let now = Timestamp::from_unix_secs(1_700_000_000)?;
/// let metadata = insert.into_metadata(now)?;
/// assert_eq!(metadata.content_type, "text/plain");
/// assert_eq!(metadata.time_expires, Some(now + std::time::Duration::from_secs(3600)));
///
/// // Read-only fields are rejected.
/// assert!(serde_json::from_str::<InsertMetadata>(r#"{"size":12}"#).is_err());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
// `remote = "Self"` turns the derives into inherent functions; see `ObjectOnly`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "Self", deny_unknown_fields)]
pub struct InsertMetadata {
    /// IANA media type of the object.
    ///
    /// Validated during deserialization. Defaults to [`DEFAULT_CONTENT_TYPE`] when converted to
    /// [`Metadata`].
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_content_type"
    )]
    pub content_type: Option<String>,

    /// The compression algorithm of the payload, `"zstd"` or `"gzip"`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "optional_string::serialize",
        deserialize_with = "present_string"
    )]
    pub content_encoding: Option<Compression>,

    /// The expiration policy, for example `"ttl:30d"` or `"tti:1h"`.
    ///
    /// Defaults to [`ExpirationPolicy::Manual`], which is omitted when serializing.
    #[serde(
        default,
        skip_serializing_if = "ExpirationPolicy::is_manual",
        with = "string"
    )]
    pub expiration: ExpirationPolicy,

    /// The origin of the object, typically the IP address of the original source.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present"
    )]
    pub origin: Option<String>,

    /// The filename returned when downloading the object.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present"
    )]
    pub filename: Option<String>,

    /// Arbitrary user-provided key-value metadata.
    ///
    /// Keys are validated and lowercased with [`normalize_custom_key`] during deserialization. If
    /// two keys are equal after lowercasing, the last one wins.
    #[serde(
        default,
        skip_serializing_if = "BTreeMap::is_empty",
        deserialize_with = "deserialize_custom"
    )]
    pub custom: BTreeMap<String, String>,
}

impl InsertMetadata {
    /// Converts into [`Metadata`] for an object written at `access_time`.
    ///
    /// Sets [`time_created`](Metadata::time_created) to `access_time` and resolves
    /// [`time_expires`](Metadata::time_expires) from the expiration policy, exactly like
    /// [`Metadata::from_insert_headers`]. A missing content type becomes
    /// [`DEFAULT_CONTENT_TYPE`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentType`] if the content type is not a valid media type, and
    /// [`Error::ExpirationTime`] if the expiration deadline is outside the supported range.
    pub fn into_metadata(self, access_time: Timestamp) -> Result<Metadata, Error> {
        let Self {
            content_type,
            content_encoding,
            expiration,
            origin,
            filename,
            custom,
        } = self;

        let content_type = match content_type {
            Some(content_type) => {
                validate_content_type(&content_type)?;
                content_type.into()
            }
            None => DEFAULT_CONTENT_TYPE.into(),
        };

        let mut metadata = Metadata {
            expiration_policy: expiration,
            time_created: None,
            time_expires: None,
            content_type,
            compression: content_encoding,
            origin,
            filename,
            size: None,
            custom,
        };
        metadata.materialize_write_times(access_time)?;

        Ok(metadata)
    }
}

impl Serialize for InsertMetadata {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Self::serialize(self, serializer)
    }
}

impl<'de> Deserialize<'de> for InsertMetadata {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_object(deserializer)
    }
}

impl ObjectOnly for InsertMetadata {
    const EXPECTING: &'static str = "an object of insert metadata";

    fn deserialize_fields<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::deserialize(deserializer)
    }
}

impl From<&Metadata> for InsertMetadata {
    /// Copies the writable fields of `metadata`, dropping the read-only ones.
    ///
    /// Leaves [`content_type`](Self::content_type) unset if it is [`DEFAULT_CONTENT_TYPE`], so
    /// that the server can fall back to the content type of the payload.
    fn from(metadata: &Metadata) -> Self {
        let content_type = (metadata.content_type != DEFAULT_CONTENT_TYPE)
            .then(|| metadata.content_type.clone().into_owned());

        Self {
            content_type,
            content_encoding: metadata.compression,
            expiration: metadata.expiration_policy,
            origin: metadata.origin.clone(),
            filename: metadata.filename.clone(),
            custom: metadata.custom.clone(),
        }
    }
}

/// Complete object metadata, as returned by the server for an existing object.
///
/// Includes the read-only fields `time_created`, `time_expires`, and `size`. Unset fields are
/// omitted when serializing, except [`content_type`](Self::content_type), which is always
/// present. Unknown fields are ignored when deserializing. See the [field
/// table](super#json-in-the-public-api).
///
/// Converts losslessly to and from [`Metadata`]. Prefer converting from an owned [`Metadata`],
/// which moves its strings instead of cloning them.
///
/// ```
/// use objectstore_types::metadata::{Metadata, ObjectMetadata};
///
/// let object = ObjectMetadata::from(&Metadata::default());
/// let json = serde_json::to_string(&object)?;
/// assert_eq!(json, r#"{"content_type":"application/octet-stream"}"#);
///
/// let object: ObjectMetadata = serde_json::from_str(&json)?;
/// assert_eq!(Metadata::from(object), Metadata::default());
/// # Ok::<(), serde_json::Error>(())
/// ```
// `remote = "Self"` turns the derives into inherent functions; see `ObjectOnly`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "Self")]
pub struct ObjectMetadata {
    /// IANA media type of the object.
    ///
    /// Defaults to [`DEFAULT_CONTENT_TYPE`] if missing during deserialization.
    #[serde(default = "default_content_type")]
    pub content_type: String,

    /// The compression algorithm of the stored payload, `"zstd"` or `"gzip"`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "optional_string"
    )]
    pub content_encoding: Option<Compression>,

    /// The expiration policy, for example `"ttl:30d"` or `"tti:1h"`.
    ///
    /// Omitted when [`ExpirationPolicy::Manual`].
    #[serde(
        default,
        skip_serializing_if = "ExpirationPolicy::is_manual",
        with = "string"
    )]
    pub expiration: ExpirationPolicy,

    /// The origin of the object, typically the IP address of the original source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,

    /// The filename returned when downloading the object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,

    /// Arbitrary user-provided key-value metadata.
    ///
    /// Keys are not validated during deserialization: objects stored before the [key
    /// rules](super::normalize_custom_key) were explicit may carry other keys.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub custom: BTreeMap<String, String>,

    /// The creation or last replacement time of the object, as an RFC 3339 timestamp.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "optional_rfc3339"
    )]
    pub time_created: Option<Timestamp>,

    /// The resolved expiration deadline, as an RFC 3339 timestamp.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "optional_rfc3339"
    )]
    pub time_expires: Option<Timestamp>,

    /// Size of the stored data in bytes; for a compressed object, the compressed size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<usize>,
}

impl Serialize for ObjectMetadata {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Self::serialize(self, serializer)
    }
}

impl<'de> Deserialize<'de> for ObjectMetadata {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_object(deserializer)
    }
}

impl ObjectOnly for ObjectMetadata {
    const EXPECTING: &'static str = "an object of object metadata";

    fn deserialize_fields<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::deserialize(deserializer)
    }
}

impl From<Metadata> for ObjectMetadata {
    fn from(metadata: Metadata) -> Self {
        let Metadata {
            expiration_policy,
            time_created,
            time_expires,
            content_type,
            compression,
            origin,
            filename,
            size,
            custom,
        } = metadata;

        Self {
            content_type: content_type.into_owned(),
            content_encoding: compression,
            expiration: expiration_policy,
            origin,
            filename,
            custom,
            time_created,
            time_expires,
            size,
        }
    }
}

impl From<&Metadata> for ObjectMetadata {
    fn from(metadata: &Metadata) -> Self {
        Self::from(metadata.clone())
    }
}

impl From<ObjectMetadata> for Metadata {
    fn from(object: ObjectMetadata) -> Self {
        let ObjectMetadata {
            content_type,
            content_encoding,
            expiration,
            origin,
            filename,
            custom,
            time_created,
            time_expires,
            size,
        } = object;

        Self {
            expiration_policy: expiration,
            time_created,
            time_expires,
            content_type: content_type.into(),
            compression: content_encoding,
            origin,
            filename,
            size,
            custom,
        }
    }
}

/// A metadata type that deserializes only from a map, such as a JSON object.
///
/// Serde's derived visitor for a struct also accepts a sequence of positional fields, so `[]`
/// would deserialize into a value with all fields defaulted. Types derive with
/// `#[serde(remote = "Self")]`, which turns the derives into inherent functions, and forward their
/// `Deserialize` impl to [`deserialize_object`].
trait ObjectOnly: Sized {
    /// Describes the expected input in error messages.
    const EXPECTING: &'static str;

    /// Deserializes the fields with the derived implementation.
    fn deserialize_fields<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error>;
}

/// Deserializes `T` from a map only, rejecting sequences and other values.
fn deserialize_object<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: ObjectOnly,
    D: Deserializer<'de>,
{
    struct ObjectVisitor<T>(PhantomData<T>);

    impl<'de, T: ObjectOnly> Visitor<'de> for ObjectVisitor<T> {
        type Value = T;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str(T::EXPECTING)
        }

        fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
            T::deserialize_fields(MapAccessDeserializer::new(map))
        }
    }

    deserializer.deserialize_map(ObjectVisitor(PhantomData))
}

fn default_content_type() -> String {
    DEFAULT_CONTENT_TYPE.to_owned()
}

/// Deserializes a present, non-null field.
///
/// Used with `#[serde(default)]`, so that a missing field is `None` but `null` is an error.
fn present<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    T::deserialize(deserializer).map(Some)
}

/// Like [`present`], for values in their [`string`] syntax.
fn present_string<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where
    T: FromStr,
    T::Err: fmt::Display,
    D: Deserializer<'de>,
{
    string::deserialize(deserializer).map(Some)
}

/// Like [`present`], and validates the value as a media type.
fn deserialize_content_type<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let content_type = String::deserialize(deserializer)?;
    validate_content_type(&content_type).map_err(serde::de::Error::custom)?;
    Ok(Some(content_type))
}

/// Deserializes custom metadata, normalizing every key with [`normalize_custom_key`].
///
/// Entries are inserted in input order, so the last of several keys that are equal after
/// lowercasing wins.
fn deserialize_custom<'de, D>(deserializer: D) -> Result<BTreeMap<String, String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct CustomVisitor;

    impl<'de> Visitor<'de> for CustomVisitor {
        type Value = BTreeMap<String, String>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("an object of string values")
        }

        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let mut custom = BTreeMap::new();
            while let Some((key, value)) = map.next_entry::<String, String>()? {
                let key = normalize_custom_key(&key).map_err(serde::de::Error::custom)?;
                custom.insert(key, value);
            }
            Ok(custom)
        }
    }

    deserializer.deserialize_map(CustomVisitor)
}

/// Serde helpers for values whose JSON form is their [`Display`](fmt::Display) and
/// [`FromStr`] syntax.
mod string {
    use super::*;

    pub fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: fmt::Display,
        S: Serializer,
    {
        serializer.collect_str(value)
    }

    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
    where
        T: FromStr,
        T::Err: fmt::Display,
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

/// Like [`string`], for optional values.
mod optional_string {
    use super::*;

    pub fn serialize<T, S>(value: &Option<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: fmt::Display,
        S: Serializer,
    {
        match value {
            Some(value) => serializer.collect_str(value),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
    where
        T: FromStr,
        T::Err: fmt::Display,
        D: Deserializer<'de>,
    {
        let value = Option::<String>::deserialize(deserializer)?;
        value
            .map(|value| value.parse().map_err(serde::de::Error::custom))
            .transpose()
    }
}

/// Serde helpers for optional [`Timestamp`]s as RFC 3339 strings.
mod optional_rfc3339 {
    use super::*;

    pub fn serialize<S>(value: &Option<Timestamp>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.map(Timestamp::as_rfc3339).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Timestamp>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Option::<Rfc3339Timestamp>::deserialize(deserializer)?;
        Ok(value.map(Rfc3339Timestamp::into_inner))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use super::*;

    fn parse_insert(json: &str) -> Result<InsertMetadata, serde_json::Error> {
        serde_json::from_str(json)
    }

    fn full_metadata() -> Metadata {
        Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_hours(30 * 24)),
            time_created: Some(Timestamp::from_unix_secs(1_700_000_000).unwrap()),
            time_expires: Some(Timestamp::from_unix_secs(1_702_592_000).unwrap()),
            content_type: "image/png".into(),
            compression: Some(Compression::Zstd),
            origin: Some("203.0.113.42".into()),
            filename: Some("résumé.png".into()),
            size: Some(1024),
            custom: BTreeMap::from([("build".into(), "123".into())]),
        }
    }

    #[test]
    fn insert_parses_all_fields() {
        let json = r#"{
            "content_type": "image/png",
            "content_encoding": "zstd",
            "expiration": "ttl:30d",
            "origin": "203.0.113.42",
            "filename": "résumé.png",
            "custom": {"build": "123"}
        }"#;

        let metadata = parse_insert(json).unwrap();
        assert_eq!(
            metadata,
            InsertMetadata {
                content_type: Some("image/png".into()),
                content_encoding: Some(Compression::Zstd),
                expiration: ExpirationPolicy::TimeToLive(Duration::from_hours(30 * 24)),
                origin: Some("203.0.113.42".into()),
                filename: Some("résumé.png".into()),
                custom: BTreeMap::from([("build".into(), "123".into())]),
            }
        );
    }

    #[test]
    fn insert_parses_empty_object_as_default() {
        assert_eq!(parse_insert("{}").unwrap(), InsertMetadata::default());
    }

    #[test]
    fn insert_parses_every_policy_and_encoding() {
        let cases = [
            (r#"{"expiration":"manual"}"#, ExpirationPolicy::Manual),
            (
                r#"{"expiration":"tti:1h"}"#,
                ExpirationPolicy::TimeToIdle(Duration::from_hours(1)),
            ),
        ];
        for (json, expected) in cases {
            assert_eq!(parse_insert(json).unwrap().expiration, expected, "{json}");
        }

        for (json, expected) in [
            (r#"{"content_encoding":"zstd"}"#, Compression::Zstd),
            (r#"{"content_encoding":"gzip"}"#, Compression::Gzip),
        ] {
            assert_eq!(
                parse_insert(json).unwrap().content_encoding,
                Some(expected),
                "{json}"
            );
        }
    }

    #[test]
    fn insert_accepts_content_type_parameters() {
        let json = r#"{"content_type":"text/plain; charset=utf-8"}"#;
        let metadata = parse_insert(json).unwrap();
        assert_eq!(
            metadata.content_type.as_deref(),
            Some("text/plain; charset=utf-8")
        );
        assert_eq!(serde_json::to_string(&metadata).unwrap(), json);
    }

    #[test]
    fn insert_keeps_strings_verbatim() {
        // JSON strings are never percent-decoded, unlike header values.
        let json = r#"{"filename":"50%41.txt","origin":"%C3%9C","custom":{"k":"100%25"}}"#;
        let metadata = parse_insert(json).unwrap();
        assert_eq!(metadata.filename.as_deref(), Some("50%41.txt"));
        assert_eq!(metadata.origin.as_deref(), Some("%C3%9C"));
        assert_eq!(metadata.custom["k"], "100%25");
    }

    #[test]
    fn insert_keeps_last_duplicate_custom_key() {
        let metadata = parse_insert(r#"{"custom":{"k":"first","k":"last"}}"#).unwrap();
        assert_eq!(
            metadata.custom,
            BTreeMap::from([("k".into(), "last".into())])
        );
    }

    #[test]
    fn insert_lowercases_custom_keys() {
        let metadata = parse_insert(r#"{"custom":{"Build-ID":"123"}}"#).unwrap();
        assert_eq!(
            metadata.custom,
            BTreeMap::from([("build-id".into(), "123".into())])
        );
    }

    #[test]
    fn insert_accepts_custom_key_symbols() {
        for symbol in [':', '%', '$', '#', '+', '|', '~', '.', '_', '-'] {
            let key = format!("k{symbol}k");
            let json = serde_json::json!({ "custom": { &key: "v" } }).to_string();
            let metadata = parse_insert(&json).unwrap();
            assert_eq!(metadata.custom.keys().collect::<Vec<_>>(), [&key], "{json}");
        }
    }

    #[test]
    fn insert_keeps_last_custom_key_after_lowercasing() {
        let cases = [
            r#"{"custom":{"Build":"first","build":"last"}}"#,
            r#"{"custom":{"build":"first","Build":"last"}}"#,
            r#"{"custom":{"Build":"first","BUILD":"last"}}"#,
        ];
        for json in cases {
            let metadata = parse_insert(json).unwrap();
            assert_eq!(
                metadata.custom,
                BTreeMap::from([("build".into(), "last".into())]),
                "{json}"
            );
        }
    }

    #[test]
    fn insert_rejects_invalid_custom_keys() {
        let keys = [
            "", "a b", "ü", "a/b", "(a)", "a!b", "a'b", "a*b", "a^b", "a`b", "a&b", "a\"b", "a\tb",
        ];
        for key in keys {
            let json = serde_json::json!({ "custom": { key: "v" } }).to_string();
            assert!(parse_insert(&json).is_err(), "unexpectedly accepted {json}");
        }
    }

    #[test]
    fn insert_custom_keys_survive_headers() {
        let json = r#"{"custom":{"Build:ID":"1","100%":"2","a$#+|~.b":"3","ok":"ü 100%"}}"#;
        let metadata = parse_insert(json)
            .unwrap()
            .into_metadata(Timestamp::UNIX_EPOCH)
            .unwrap();
        assert_eq!(
            metadata.custom.keys().collect::<Vec<_>>(),
            ["100%", "a$#+|~.b", "build:id", "ok"]
        );

        let headers = metadata.to_headers("").unwrap();
        let roundtripped = Metadata::from_headers(&headers, "").unwrap();
        assert_eq!(roundtripped.custom, metadata.custom);
    }

    #[test]
    fn insert_rejects_invalid_documents() {
        let cases = [
            // unknown fields, including typos
            r#"{"expiraton":"ttl:30d"}"#,
            r#"{"compression":"zstd"}"#,
            // read-only fields
            r#"{"time_created":"2026-11-10T12:00:00Z"}"#,
            r#"{"time_expires":"2026-11-10T12:00:00Z"}"#,
            r#"{"size":12}"#,
            // duplicate fields
            r#"{"content_type":"text/plain","content_type":"image/png"}"#,
            r#"{"expiration":"ttl:1d","expiration":"tti:1d"}"#,
            r#"{"custom":{},"custom":{}}"#,
            // invalid values
            r#"{"content_type":"not a media type!"}"#,
            r#"{"content_type":""}"#,
            r#"{"content_type":42}"#,
            r#"{"content_encoding":"brotli"}"#,
            r#"{"content_encoding":"Zstd"}"#,
            r#"{"expiration":"garbage"}"#,
            r#"{"expiration":"ttl:"}"#,
            r#"{"expiration":{"TimeToLive":{"secs":30,"nanos":0}}}"#,
            r#"{"origin":1}"#,
            r#"{"filename":["a"]}"#,
            r#"{"custom":{"k":1}}"#,
            r#"{"custom":["k"]}"#,
            // null is not a value for any field
            r#"{"content_type":null}"#,
            r#"{"content_encoding":null}"#,
            r#"{"expiration":null}"#,
            r#"{"origin":null}"#,
            r#"{"filename":null}"#,
            r#"{"custom":null}"#,
            r#"{"custom":{"k":null}}"#,
            // not an object, including positional fields
            r#"[]"#,
            r#"["text/plain"]"#,
            r#"null"#,
            r#""content_type""#,
        ];

        for json in cases {
            assert!(parse_insert(json).is_err(), "unexpectedly accepted {json}");
        }
    }

    #[test]
    fn insert_serializes_only_set_fields() {
        let json = serde_json::to_string(&InsertMetadata::default()).unwrap();
        assert_eq!(json, "{}");

        let metadata = InsertMetadata {
            content_type: Some("image/png".into()),
            content_encoding: Some(Compression::Zstd),
            expiration: ExpirationPolicy::TimeToLive(Duration::from_hours(30 * 24)),
            custom: BTreeMap::from([("build".into(), "123".into())]),
            ..Default::default()
        };
        let json = serde_json::to_string(&metadata).unwrap();
        assert_eq!(
            json,
            r#"{"content_type":"image/png","content_encoding":"zstd","expiration":"ttl:30d","custom":{"build":"123"}}"#
        );
        assert_eq!(parse_insert(&json).unwrap(), metadata);
    }

    #[test]
    fn insert_into_metadata_resolves_ttl() {
        let access_time = Timestamp::from_unix_secs(1_700_000_000).unwrap();
        let insert = InsertMetadata {
            expiration: ExpirationPolicy::TimeToLive(Duration::from_secs(30)),
            ..Default::default()
        };

        let metadata = insert.into_metadata(access_time).unwrap();
        assert_eq!(metadata.time_created, Some(access_time));
        assert_eq!(
            metadata.time_expires,
            Some(access_time + Duration::from_secs(30))
        );
        assert!(metadata.validate().is_ok());
    }

    #[test]
    fn insert_into_metadata_resolves_tti() {
        let access_time = Timestamp::from_unix_secs(1_700_000_000).unwrap();
        let insert = InsertMetadata {
            expiration: ExpirationPolicy::TimeToIdle(Duration::from_hours(1)),
            ..Default::default()
        };

        let metadata = insert.into_metadata(access_time).unwrap();
        assert_eq!(metadata.time_created, Some(access_time));
        assert_eq!(
            metadata.time_expires,
            Some(access_time + Duration::from_hours(1))
        );
        assert!(metadata.validate().is_ok());
    }

    #[test]
    fn insert_into_metadata_manual_has_no_expiry() {
        let access_time = Timestamp::from_unix_secs(1_700_000_000).unwrap();
        let metadata = InsertMetadata::default()
            .into_metadata(access_time)
            .unwrap();

        assert_eq!(metadata.expiration_policy, ExpirationPolicy::Manual);
        assert_eq!(metadata.time_created, Some(access_time));
        assert_eq!(metadata.time_expires, None);
        assert!(metadata.validate().is_ok());
    }

    #[test]
    fn insert_into_metadata_rejects_out_of_range_expiry() {
        let access_time = Timestamp::from_rfc3339("9999-12-31T00:00:00Z").unwrap();
        let insert = InsertMetadata {
            expiration: ExpirationPolicy::TimeToLive(Duration::from_hours(48)),
            ..Default::default()
        };

        let err = insert.into_metadata(access_time).unwrap_err();
        assert!(matches!(err, Error::ExpirationTime(_)));
    }

    #[test]
    fn insert_into_metadata_copies_writable_fields() {
        let access_time = Timestamp::from_unix_secs(1_700_000_000).unwrap();
        let insert = InsertMetadata {
            content_type: Some("image/png".into()),
            content_encoding: Some(Compression::Gzip),
            expiration: ExpirationPolicy::Manual,
            origin: Some("203.0.113.42".into()),
            filename: Some("résumé.png".into()),
            custom: BTreeMap::from([("build".into(), "123".into())]),
        };

        let metadata = insert.into_metadata(access_time).unwrap();
        assert_eq!(
            metadata,
            Metadata {
                expiration_policy: ExpirationPolicy::Manual,
                time_created: Some(access_time),
                time_expires: None,
                content_type: "image/png".into(),
                compression: Some(Compression::Gzip),
                origin: Some("203.0.113.42".into()),
                filename: Some("résumé.png".into()),
                size: None,
                custom: BTreeMap::from([("build".into(), "123".into())]),
            }
        );
    }

    #[test]
    fn insert_into_metadata_defaults_content_type() {
        let metadata = InsertMetadata::default()
            .into_metadata(Timestamp::UNIX_EPOCH)
            .unwrap();
        assert_eq!(metadata.content_type, DEFAULT_CONTENT_TYPE);
    }

    #[test]
    fn insert_into_metadata_rejects_invalid_content_type() {
        let insert = InsertMetadata {
            content_type: Some("not a media type!".into()),
            ..Default::default()
        };

        let err = insert.into_metadata(Timestamp::UNIX_EPOCH).unwrap_err();
        assert!(matches!(err, Error::ContentType(_)));
    }

    #[test]
    fn insert_matches_insert_headers() {
        let access_time = Timestamp::from_unix_secs(1_700_000_000).unwrap();
        for expiration in ["manual", "ttl:30d", "tti:1h"] {
            let mut headers = http::HeaderMap::new();
            headers.insert("content-type", "text/plain".parse().unwrap());
            headers.insert(
                crate::metadata::HEADER_EXPIRATION,
                expiration.parse().unwrap(),
            );
            let from_headers = Metadata::from_insert_headers(&headers, "", access_time).unwrap();

            let json = format!(r#"{{"content_type":"text/plain","expiration":"{expiration}"}}"#);
            let from_json = parse_insert(&json)
                .unwrap()
                .into_metadata(access_time)
                .unwrap();

            assert_eq!(from_json, from_headers, "{expiration}");
        }
    }

    #[test]
    fn insert_from_metadata_drops_read_only_fields() {
        let insert = InsertMetadata::from(&full_metadata());
        assert_eq!(
            insert,
            InsertMetadata {
                content_type: Some("image/png".into()),
                content_encoding: Some(Compression::Zstd),
                expiration: ExpirationPolicy::TimeToLive(Duration::from_hours(30 * 24)),
                origin: Some("203.0.113.42".into()),
                filename: Some("résumé.png".into()),
                custom: BTreeMap::from([("build".into(), "123".into())]),
            }
        );
    }

    #[test]
    fn insert_from_metadata_omits_default_content_type() {
        let insert = InsertMetadata::from(&Metadata::default());
        assert_eq!(insert.content_type, None);
        assert_eq!(serde_json::to_string(&insert).unwrap(), "{}");

        let metadata = Metadata {
            content_type: "text/plain".into(),
            ..Default::default()
        };
        let insert = InsertMetadata::from(&metadata);
        assert_eq!(insert.content_type.as_deref(), Some("text/plain"));
    }

    #[test]
    fn insert_roundtrips_through_metadata() {
        let original = full_metadata();
        let access_time = original.time_created.unwrap();

        let insert = InsertMetadata::from(&original);
        let json = serde_json::to_string(&insert).unwrap();
        let restored = parse_insert(&json)
            .unwrap()
            .into_metadata(access_time)
            .unwrap();

        assert_eq!(
            restored,
            Metadata {
                size: None,
                ..original
            }
        );
    }

    #[test]
    fn object_serializes_all_fields() {
        let object = ObjectMetadata::from(&full_metadata());
        let json = serde_json::to_string_pretty(&object).unwrap();

        insta::assert_snapshot!(json, @r#"
        {
          "content_type": "image/png",
          "content_encoding": "zstd",
          "expiration": "ttl:30d",
          "origin": "203.0.113.42",
          "filename": "résumé.png",
          "custom": {
            "build": "123"
          },
          "time_created": "2023-11-14T22:13:20Z",
          "time_expires": "2023-12-14T22:13:20Z",
          "size": 1024
        }
        "#);
    }

    #[test]
    fn object_from_owned_metadata_matches_borrowed() {
        assert_eq!(
            ObjectMetadata::from(full_metadata()),
            ObjectMetadata::from(&full_metadata())
        );
        assert_eq!(
            ObjectMetadata::from(Metadata::default()),
            ObjectMetadata::from(&Metadata::default())
        );
    }

    #[test]
    fn object_omits_unset_fields() {
        let object = ObjectMetadata::from(&Metadata::default());
        let json = serde_json::to_string(&object).unwrap();
        assert_eq!(json, r#"{"content_type":"application/octet-stream"}"#);
    }

    #[test]
    fn object_roundtrips_through_metadata() {
        let original = full_metadata();

        let json = serde_json::to_string(&ObjectMetadata::from(&original)).unwrap();
        let object: ObjectMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(Metadata::from(object), original);

        let json = serde_json::to_string(&ObjectMetadata::from(&Metadata::default())).unwrap();
        let object: ObjectMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(Metadata::from(object), Metadata::default());
    }

    #[test]
    fn object_ignores_unknown_fields() {
        let json = r#"{"content_type":"text/plain","size":12,"checksum":"abc","future":{"a":1}}"#;
        let object: ObjectMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(object.content_type, "text/plain");
        assert_eq!(object.size, Some(12));
    }

    #[test]
    fn object_rounds_fractional_timestamps_up() {
        let json = r#"{"content_type":"text/plain","time_created":"2023-11-14T22:13:20.5Z"}"#;
        let object: ObjectMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(
            object.time_created,
            Some(Timestamp::from_unix_secs(1_700_000_001).unwrap())
        );
    }

    #[test]
    fn object_defaults_missing_content_type() {
        let object: ObjectMetadata = serde_json::from_str("{}").unwrap();
        assert_eq!(object.content_type, DEFAULT_CONTENT_TYPE);
        assert_eq!(Metadata::from(object), Metadata::default());
    }

    #[test]
    fn object_rejects_invalid_values() {
        let cases = [
            r#"{"content_encoding":"brotli"}"#,
            r#"{"expiration":"garbage"}"#,
            r#"{"time_created":"not-a-timestamp"}"#,
            r#"{"time_expires":1700000000}"#,
            r#"{"size":-1}"#,
            r#"{"size":"12"}"#,
            // not an object, including positional fields
            r#"[]"#,
            r#"["text/plain"]"#,
            r#"null"#,
            r#""content_type""#,
        ];

        for json in cases {
            assert!(
                serde_json::from_str::<ObjectMetadata>(json).is_err(),
                "unexpectedly accepted {json}"
            );
        }
    }
}

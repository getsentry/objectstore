//! Contains a remote implementation using HTTP to interact with objectstore.

use std::io::Read;

use jsonwebtoken::{EncodingKey, Header};
use reqwest::Body;
use tokio_util::io::ReaderStream;

use crate::workload::{InternalId, Payload};

/// A remote implementation using HTTP to interact with objectstore.
#[derive(Debug)]
pub struct HttpRemote {
    /// The URL at which the objectstore is reachable.
    pub remote: String,
    /// The JWT secret used to sign the authorization tokens.
    pub jwt_secret: String,
    /// The HTTP client used to make requests to the objectstore.
    pub client: reqwest::Client,
}

impl HttpRemote {
    /// Creates a new `HttpRemote` instance with the given remote URL and a default client.
    ///
    /// The JWT secret is empty and can be changed with `with_secret`.
    pub fn new(remote: String) -> Self {
        Self {
            remote,
            jwt_secret: String::new(),
            client: reqwest::Client::new(),
        }
    }

    /// Sets the JWT secret to use for authorization.
    pub fn with_secret(mut self, jwt_secret: impl Into<String>) -> Self {
        self.jwt_secret = jwt_secret.into();
        self
    }

    fn make_authorization(&self, permission: &str) -> String {
        let claims = serde_json::json!({
            "exp": jsonwebtoken::get_current_timestamp() + 30,
            "usecase": "attachments",
            "scope": {
                "organization": 12345,
            },
            "permissions": [permission],
        });

        let header = Header::default();
        let key = EncodingKey::from_secret(self.jwt_secret.as_bytes());

        jsonwebtoken::encode(&header, &claims, &key).unwrap()
    }

    pub(crate) async fn write(&self, id: InternalId, payload: Payload) -> String {
        let stream = ReaderStream::new(payload);

        let key = id.to_string();
        let put_url = format!("{}/{key}", self.remote);
        let _response = self
            .client
            .put(put_url)
            .header("Authorization", self.make_authorization("write"))
            .body(Body::wrap_stream(stream))
            .send()
            .await
            .unwrap();

        key
    }

    pub(crate) async fn read(&self, key: &str, mut payload: Payload) {
        let get_url = format!("{}/{key}", self.remote);
        let file_contents = self
            .client
            .get(get_url)
            .header("Authorization", self.make_authorization("read"))
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        let mut expected_payload = Vec::new();
        payload.read_to_end(&mut expected_payload).unwrap();

        if file_contents != expected_payload {
            eprintln!("contents of `{key}` do not match expectation");
        }
    }

    pub(crate) async fn delete(&self, key: String) {
        let delete_url = format!("{}/{key}", self.remote);
        self.client
            .delete(delete_url)
            .header("Authorization", self.make_authorization("write"))
            .send()
            .await
            .unwrap();
    }
}

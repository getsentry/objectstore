use std::io::Read;

use reqwest::Body;
use serde::Deserialize;
use tokio_util::io::ReaderStream;

use crate::workload::Payload;

pub struct HttpRemote {
    pub remote: String,
    pub prefix: String,
    pub client: reqwest::Client,
}

#[derive(Debug, Deserialize)]
struct PutBlobResponse {
    key: String,
}

impl HttpRemote {
    pub async fn write(&self, payload: Payload) -> String {
        let stream = ReaderStream::new(payload);

        let put_url = format!("{}/{}", self.remote, self.prefix);
        let response = self
            .client
            .put(put_url)
            .body(Body::wrap_stream(stream))
            .send()
            .await
            .unwrap();
        let PutBlobResponse { key } = response.json().await.unwrap();

        key
    }

    pub async fn read(&self, id: &str, mut payload: Payload) {
        let get_url = format!("{}/{id}", self.remote);
        let file_contents = self
            .client
            .get(get_url)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        let mut expected_payload = Vec::new();
        payload.read_to_end(&mut expected_payload).unwrap();

        if file_contents != expected_payload {
            eprintln!("contents of `{id}` do not match expectation");
        }
    }

    pub async fn delete(&self, id: String) {
        let delete_url = format!("{}/{id}", self.remote);
        self.client.delete(delete_url).send().await.unwrap();
    }
}

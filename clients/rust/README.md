# Objectstore Client

The client is used to interface with the [Objectstore](https://getsentry.github.io/objectstore/) backend. It handles
responsibilities like transparent compression, and making sure that uploads and
downloads are done as efficiently as possible.

## Quick Start

```rust
use objectstore_client::{Client, Usecase, Result};

async fn example() -> Result<()> {
    let client = Client::new("http://localhost:8888/")?;
    let session = Usecase::new("attachments")
        .for_project(42, 1337)
        .session(&client)?;

    // Upload
    let response = session.put("Hello, world!").send().await?;

    // Download
    let object = session.get(&response.key).send().await?.expect("object to exist");
    let payload = object.payload().await?;

    // Delete
    session.delete(&response.key).send().await?;

    Ok(())
}
```

## Core Concepts

### Usecases and Scopes

A [`Usecase`] represents a server-side namespace with its own configuration defaults.
Within a Usecase, [`Scope`]s provide further isolation — typically keyed by organization
and project IDs. A [`Session`] ties a Client to a specific Usecase + Scope for operations.

Scope components form a hierarchical path, so their order matters:
`org=42;project=1337` and `project=1337;org=42` are different scopes. The convenience
method [`Usecase::for_project`] pushes `org` then `project` in the recommended order.

```rust,ignore
// Scope with org and project (recommended first components)
let session = Usecase::new("attachments")
    .for_project(42, 1337)
    .session(&client)?;

// Additional components are appended after org/project
let session = Usecase::new("attachments")
    .for_project(42, 1337)
    .push("app_slug", "email_app")
    .session(&client)?;
```

### Expiration

Objects can expire automatically using Time To Live (from creation) or Time To Idle
(from last access). Defaults are set at the Usecase level and can be overridden per-upload.
Without an expiration policy, objects use `Manual` expiration (no auto-deletion).

**We strongly recommend setting an expiration policy on every Usecase** to prevent
unbounded storage growth. Choose `TimeToIdle` for cache-like data that should stay
alive while actively used, or `TimeToLive` for data with a fixed retention period.

```rust,ignore
use std::time::Duration;
use objectstore_client::ExpirationPolicy;

// Set default expiration on the Usecase
Usecase::new("attachments")
    .with_expiration_policy(ExpirationPolicy::TimeToIdle(Duration::from_secs(30 * 86400)));

// Override per-upload
session.put("payload")
    .expiration_policy(ExpirationPolicy::TimeToLive(Duration::from_secs(3600)))
    .send().await?;
```

### Origin Tracking

We encourage setting the `origin` on every upload to track where the payload was
originally obtained from (e.g., the IP address of the Sentry SDK or CLI). This is
optional but helps with auditing and debugging.

```rust,ignore
session.put("payload").origin("203.0.113.42").send().await?;
```

### Compression

Uploads are compressed with Zstd by default, and downloads are transparently decompressed.
See [`Compression`] for available options. Compression can be overridden per-upload:

```rust,ignore
use objectstore_client::Compression;

// upload as-is and record no encoding:
session.put(video_data)
    .compress(None)
    .send().await?;

// upload as-is, but record the encoding so that downloads still decompress:
session.put(zstd_data)
    .precompressed(Compression::Zstd)
    .send().await?;
```

To receive a compressed payload without decompressing it — for example, when forwarding
to a system that accepts zstd natively — use `accept_encoding` on the get request:

```rust,ignore
use objectstore_client::Compression;

// Returns zstd-compressed bytes; metadata.compression is preserved.
let response = session.get(key)
    .accept_encoding([Compression::Zstd])
    .send().await?
    .expect("object to exist");

assert_eq!(response.metadata.compression, Some(Compression::Zstd));
let compressed_bytes = response.payload().await?;
```

### Custom Metadata

Arbitrary key-value pairs can be attached to objects and retrieved on download.

```rust,ignore
session.put("payload")
    .append_metadata("source", "upload-service")
    .send().await?;
```

### Resumable Upload API

> **Feature flag required:** Enable `resumable-upload-api` to use this API.
>
> ```toml
> objectstore-client = { version = "...", features = ["resumable-upload-api"] }
> ```

The resumable upload API allows you to upload an object across multiple requests.
It's suitable for use for particularly large objects, where restarting an upload
from scratch would be expensive.
It's recommended to always try to upload the whole object in a single request if
possible, as that's always the more efficient approach.
If the request fails midway, it will be possible to resume it from the persisted offset.

**Note:** This feature flag exposes a low-level API that maps directly to the server API and
requires appropriate manual handling of different states and error scenarios.
Therefore, this API should only be used for advanced use cases that demand it.
In a future release of `objectstore-client`, the resumable uploads API will be used
internally for eligible `put` calls without the need for this feature flag or direct
interaction with this API.

**Important:** resumable uploads do not automatically compress chunk contents. The `compression`
setting only records how the object is encoded; the caller must compress the payload accordingly.
The object length and all offsets refer to the bytes after compression.

```rust,no_run
#[cfg(feature = "resumable-upload-api")]
mod example {
    use bytes::Bytes;
    use objectstore_client::{Error, ResumableUploadError, Result, Session, UploadProgress};

    async fn upload_large_object(session: &Session, object: Bytes) -> Result<()> {
        const KEY: &str = "my-large-object";

        let upload = match session
            .create_upload(object.len() as u64)
            .key(KEY)
            .content_type("application/octet-stream")
            .compression(None)
            .send()
            .await
        {
            Ok(upload) => upload,
            Err(Error::ResumableUpload(ResumableUploadError::Declined)) => {
                // Objectstore refused the upload creation request for this object.
                // Fall back to a normal PUT.
                session
                    .put(object)
                    .key(KEY)
                    .content_type("application/octet-stream")
                    .compress(None)
                    .send()
                    .await?;
                return Ok(());
            }
            // Something else went wrong. Handle the error and retry if appropriate.
            Err(_error) => todo!(),
        };

        let mut offset = 0;
        loop {
            // Send everything after the authoritative offset.
            // The first request therefore attempts to upload the whole object in one request.
            let result = upload
                .put_chunk(offset, object.slice(offset as usize..))
                .send()
                .await;

            offset = match result {
                Ok(UploadProgress::Complete) => return Ok(()),

                Err(Error::ResumableUpload(error @ ResumableUploadError::Gone))
                | Err(Error::ResumableUpload(
                    error @ ResumableUploadError::NotFound,
                )) => {
                    // The upload session doesn't exist (anymore).
                    // The whole upload must be retried.
                    return Err(error.into());
                }

                Ok(UploadProgress::Incomplete { offset: next })
                | Err(Error::ResumableUpload(
                    ResumableUploadError::OffsetMismatch { offset: next },
                )) => next,

                // A network error happened, or an unexpected HTTP error status was returned.
                Err(Error::Reqwest(_)) => {
                    // You may wish to retry this a bounded amount of times.
                    match upload.progress().send().await? {
                        UploadProgress::Complete => return Ok(()),
                        UploadProgress::Incomplete { offset: next } => next,
                    }
                }
                // Something else went wrong. Handle the error and retry if appropriate.
                Err(_) => todo!(),
            };
        }
    }
}
```

Use `upload.key()` and `upload.token()` to resume after a process restart:

```rust,ignore
let upload = session.resume_upload(saved_key, saved_token);

let progress = upload.progress().send().await?;
```

or cancel the upload:
```rust,ignore
upload.cancel().send().await?
```

### Multipart Upload API

> **Feature flag required:** Enable the `multipart` Cargo feature to use this API.
> It is not included in the default feature set.
>
> ```toml
> objectstore-client = { version = "...", features = ["multipart"] }
> ```

For large objects, use multipart uploads to upload parts concurrently with bounded
parallelism.

**Important:** unlike single-object uploads, multipart uploads do **not** auto-compress.
`compression` on the initiate request behaves like `precompressed` above: it only records the
algorithm, and the caller must pre-compress each part accordingly.

```rust,ignore
use futures_util::StreamExt as _;
use futures_util::stream;
use objectstore_client::Compression;

let upload = session
    .initiate_multipart_upload()
    .key("my-large-object")
    .compression(Compression::Zstd)
    .send()
    .await?;

let parts: Vec<(Vec<u8>, u32)> = vec![
    (zstd::encode_all(&part1_data[..], 0)?, 1),
    (zstd::encode_all(&part2_data[..], 0)?, 2),
];

let results: Vec<_> = stream::iter(
    parts
        .into_iter()
        .map(|(data, part_number)| upload.put(data, part_number, None)),
)
.buffer_unordered(8)
.collect()
.await;

let mut done = Vec::new();
let mut errors = Vec::new();
for result in results {
    match result {
        Ok(part) => done.push(part),
        Err(e) => errors.push(e),
    }
}

if !errors.is_empty() {
    // reupload failed parts...
}

let key = upload.complete(done).await?;
// or
upload.abort().await?;
```

You can also resume an in-progress multipart upload, e.g. after a process restart.

```rust,ignore
use futures_util::{StreamExt as _, TryStreamExt as _};
use futures_util::stream;
use objectstore_client::CompletePart;

let upload = session.resume_multipart_upload("my-large-object", saved_upload_id)?;

let existing = upload.list_parts().await?;
let total_parts = 10;
let uploaded: Vec<u32> = existing.iter().map(|p| p.part_number.get()).collect();
let missing: Vec<u32> = (1..=total_parts)
    .filter(|n| !uploaded.contains(n))
    .collect();

let mut done: Vec<_> = stream::iter(
    missing
        .into_iter()
        .map(|part_number| upload.put(get_part_data(part_number), part_number, None)),
)
.buffer_unordered(8)
.try_collect()
.await?;

done.extend(existing.into_iter().map(CompletePart::from));

let key = upload.complete(done).await?;
```

### Many API

The Many API allows you to enqueue multiple requests that the client can execute using Objectstore's batch endpoint, minimizing network overhead.

`send()` returns a stream of the results of each operation. Results are **not** guaranteed to be in the order they were originally enqueued in.

```rust
use futures_util::StreamExt as _;
use objectstore_client::{Client, Usecase, OperationResult, Result};

async fn example_batch() -> Result<()> {
    let client = Client::new("http://localhost:8888/")?;
    let session = Usecase::new("attachments")
        .for_project(42, 1337)
        .session(&client)?;

    let mut results = session
        .many()
        .push(session.put("file1 contents").key("file1"))
        .push(session.put("file2 contents").key("file2"))
        .push(session.put("file3 contents").key("file3"))
        .send()
        .await;

    while let Some(result) = results.next().await {
        match result {
            OperationResult::Put(_key, Ok(_response)) => { /* ... */ }
            OperationResult::Get(_key, Ok(_object)) => { /* ... */ }
            OperationResult::Delete(_key, Ok(_response)) => { /* ... */ }
            OperationResult::Head(_key, Ok(_metadata)) => { /* ... */ }
            OperationResult::Put(_key, Err(_e))
            | OperationResult::Get(_key, Err(_e))
            | OperationResult::Delete(_key, Err(_e))
            | OperationResult::Head(_key, Err(_e)) => { /* handle per-op error */ }
            OperationResult::Error(_e) => { /* unattributable error */ }
        }
    }

    Ok(())
}
```

If you don't need to inspect individual operation results and just want to fail if any error occurs,
use `error_for_failures` which drains the stream and returns all errors at once:

```rust,ignore
session
    .many()
    .push(session.put("file1 contents").key("file1"))
    .push(session.put("file2 contents").key("file2"))
    .push(session.put("file3 contents").key("file3"))
    .send()
    .await
    .error_for_failures()
    .await
    .map_err(|errors| { /* Iterator<Item = objectstore_client::Error> */ })?;
```

### Authentication

If your Objectstore instance enforces authorization, you must configure authentication
via [`ClientBuilder::token`]. It accepts either:

- A **[`TokenGenerator`]** — for internal services that have access to an EdDSA keypair.
  The generator signs a fresh JWT for each request, scoped to the specific usecase
  and scope being accessed.
- A **`String` / `&str`** — a pre-signed JWT, used as-is for every request.
  Use this for external services that receive a token from another source.
- An `Option` of any of the above — useful for chained builder calls.

```rust,ignore
use objectstore_client::{Client, SecretKey, TokenGenerator, Usecase};

// Option 1: Internal service with a keypair
let client = Client::builder("http://localhost:8888/")
    .token(
        TokenGenerator::new(SecretKey {
            secret_key: "<private key>".into(),
            kid: "my-service".into(),
        })?
    )
    .build()?;

// Option 2: External service with a pre-signed JWT
// Use TokenGenerator::create_token() to obtain a static token from an authority
let scope = Usecase::new("my_app").for_project(42, 1337);
let generator = TokenGenerator::new(SecretKey {
    secret_key: "<private key>".into(),
    kid: "my-service".into(),
})?;
let token = generator.create_token(&scope).sign()?;

// Then pass the token directly to a client builder.
let client = Client::builder("http://localhost:8888/")
    .token(token)
    .build()?;
```

To create a token with narrower permissions or a different expiry, use the
methods on the token request returned from `create_token`.

## Configuration

In production, store the [`Client`] and [`Usecase`] in a `static` and reuse them.
The following shows all available builder options with their defaults:

```rust
use std::time::Duration;
use std::sync::LazyLock;
use objectstore_client::{Client, Usecase, Result};

static CLIENT: LazyLock<Client> = LazyLock::new(|| {
    Client::builder("http://localhost:8888/")
        // .propagate_traces(true) // default: false
        // .timeout(Duration::from_secs(5)) // default: no read timeout (connect: 100ms)
        // .configure_reqwest(|builder| { ... }) // customize the reqwest::ClientBuilder
        // .token(token_generator) // see Authentication section
        .build()
        .expect("Objectstore client to build successfully")
});

static ATTACHMENTS: LazyLock<Usecase> = LazyLock::new(|| {
    Usecase::new("attachments")
});

async fn example() -> Result<()> {
    let session = CLIENT.session(ATTACHMENTS.for_project(42, 1337))?;
    let response = session.put("Hello, world!").send().await?;

    Ok(())
}
```

See [`ClientBuilder`] for all available options, including authentication via
[`TokenGenerator`].

See the [API docs](https://getsentry.github.io/objectstore/rust/objectstore_client/) for full reference documentation.

## License

Like Sentry, Objectstore is licensed under the FSL. See the `LICENSE.md` file
and [this blog post](https://blog.sentry.io/introducing-the-functional-source-license-freedom-without-free-riding/)
for more information.

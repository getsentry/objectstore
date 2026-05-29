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
`org=42/project=1337` and `project=1337/org=42` are different scopes. The convenience
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

Uploads are compressed with Zstd by default. Downloads are transparently decompressed.
You can override compression per-upload for pre-compressed or uncompressible data.
See [`Compression`] for available options.

```rust,ignore
use objectstore_client::Compression;

session.put(already_compressed_data)
    .compression(None) // disable compression
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

### Multipart Upload API

For large objects, use multipart uploads to upload parts concurrently with bounded
parallelism.

**Important:** unlike single-object uploads, multipart uploads do **not** auto-compress.
The caller must pre-compress each part according to the compression set as part of the metadata
when initiating the upload.

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
    .map_err(|errors| { /* errors: Vec<Error> */ })?;
```

### Authentication

If your Objectstore instance enforces authorization, you must configure authentication
via [`ClientBuilder::token`]. It accepts either:

- A **[`TokenGenerator`]** — for internal services that have access to an EdDSA keypair.
  The generator signs a fresh JWT for each request, scoped to the specific usecase
  and scope being accessed.
- A **`String` / `&str`** — a pre-signed JWT, used as-is for every request.
  Use this for external services that receive a token from another source.

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
// Use TokenGenerator::sign() to obtain a static token from an internal
// service, then pass it to the external consumer:
let scope = Usecase::new("my_app").for_project(42, 1337);
let token = TokenGenerator::new(SecretKey {
    secret_key: "<private key>".into(),
    kid: "my-service".into(),
})?.sign(&scope)?;

let client = Client::builder("http://localhost:8888/")
    .token(token)
    .build()?;
```

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

# Objectstore Client

The client is used to interface with the Objectstore backend. It handles
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

### Custom Metadata

Arbitrary key-value pairs can be attached to objects and retrieved on download.

```rust,ignore
session.put("payload")
    .append_metadata("source", "upload-service")
    .send().await?;
```

### Many API

The Many API allows you to enqueue multiple requests that the client can execute using Objectstore's batch endpoint, minimizing network overhead.

```rust
use objectstore_client::{Client, Usecase, OperationResult, Result};

async fn example_batch() -> Result<()> {
    let client = Client::new("http://localhost:8888/")?;
    let session = Usecase::new("attachments")
        .for_project(42, 1337)
        .session(&client)?;

    let results: Vec<_> = session
        .many()
        .push(session.put("file1 contents").key("file1"))
        .push(session.put("file2 contents").key("file2"))
        .push(session.put("file3 contents").key("file3"))
        .send()
        .await?
        .into_iter()
        .collect();

    for result in results {
        match result {
            // ...
            _ => {},
        }
    }

    Ok(())
}
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
        // .token_generator(token_generator) // for authorized Objectstore instances
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

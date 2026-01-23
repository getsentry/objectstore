# Objectstore Client

The client is used to interface with the Objectstore backend. It handles
responsibilities like transparent compression, and making sure that uploads and
downloads are done as efficiently as possible.

## Usage

Here's a basic example that shows how to use this client:

```rust
use objectstore_client::{Client, Usecase, Result};

async fn example_basic() -> Result<()> {
    let client = Client::new("http://localhost:8888/")?;
    let session = Usecase::new("attachments")
        .for_project(42, 1337)
        .session(&client)?;

    let response = session.put("Hello, world!")
        .send()
        .await
        .expect("put to succeed");

    let object = session
        .get(&response.key)
        .send()
        .await?
        .expect("object to exist");
    assert_eq!(object.payload().await?, "hello world");

    session
        .delete(&response.key)
        .send()
        .await
        .expect("delete to succeed");

    Ok(())
}
```

In practice, you would most likely want to store the `Client` and `Usecase`
in something like a `static` and reuse them, like so:

```rust
use std::time::Duration;
use std::sync::LazyLock;
use objectstore_client::{Client, Compression, Usecase, Result};

static OBJECTSTORE_CLIENT: LazyLock<Client> = LazyLock::new(|| {
    Client::builder("http://localhost:8888/")
        // Optionally, propagate tracing headers to use distributed tracing in Sentry
        .propagate_traces(true)
        // Customize the `reqwest::ClientBuilder`
        .configure_reqwest(|builder| {
            builder.pool_idle_timeout(Duration::from_secs(90))
                   .pool_max_idle_per_host(10)
        })
        .build()
        .expect("Objectstore client to build successfully")
});

static ATTACHMENTS: LazyLock<Usecase> = LazyLock::new(|| {
    Usecase::new("attachments")
});

async fn example() -> Result<()> {
    let session = OBJECTSTORE_CLIENT
        .session(ATTACHMENTS.for_project(42, 1337))?;

    let response = session.put("Hello, world!").send().await?;

    let object = session
        .get(&response.key)
        .send()
        .await?
        .expect("object to exist");
    assert_eq!(object.payload().await?, "hello world");

    session
        .delete(&response.key)
        .send()
        .await
        .expect("deletion to succeed");

    Ok(())
}
```

### Many API

The Many API allows you to enqueue multiple requests that the client can decide to execute as a single batch request to minimize network overhead.

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
        .collect();

    for result in results {
        match result {
            OperationResult::Put(key, Ok(_)) => {}
            OperationResult::Put(key, Err(e)) => return Err(e),
            _ => unreachable!(),
        }
    }

    Ok(())
}
```

See the [API docs](https://getsentry.github.io/objectstore/rust/objectstore_client/) for more in-depth documentation.

## License

Like Sentry, Objectstore is licensed under the FSL. See the `LICENSE.md` file
and [this blog post](https://blog.sentry.io/introducing-the-functional-source-license-freedom-without-free-riding/)
for more information.

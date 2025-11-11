# Objectstore Client

The client is used to interface with the objectstore backend. It handles
responsibilities like transparent compression, and making sure that uploads and
downloads are done as efficiently as possible.

## Usage

```rust
use objectstore_client::{Client, Usecase};

let client = Client::builder("http://localhost:8888/").build()?;

let usecase = Usecase::new("attachments").for_project(42, 1337).session(&client)?;

let response = session.put("hello world").send().await?;

let object = session.get(&response.key).send().await?.expect("object to exist");
assert_eq!(object.payload().await?, "hello world");
```

See the [API docs](https://getsentry.github.io/objectstore/rust/objectstore_client/) for more in-depth documentation.

## License

Like Sentry, Objectstore is licensed under the FSL. See the `LICENSE.md` file
and [this blog post](https://blog.sentry.io/introducing-the-functional-source-license-freedom-without-free-riding/)
for more information.

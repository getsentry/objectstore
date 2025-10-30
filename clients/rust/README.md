# Objectstore Client

The client is used to interface with the objectstore backend. It handles
responsibilities like transparent compression, and making sure that uploads and
downloads are done as efficiently as possible.

## Usage

```rust
use objectstore_client::ClientBuilder;

let client = ClientBuilder::new("http://localhost:8888/", "my-usecase")?
    .for_organization(42);

let id = client.put("hello world").send().await?;
let object = client.get(&id.key).send().await?.expect("object to exist");
assert_eq!(object.payload().await?, "hello world");
```

## License

Like Sentry, Objectstore is licensed under the FSL. See the `LICENSE.md` file
and [this blog post](https://blog.sentry.io/introducing-the-functional-source-license-freedom-without-free-riding/)
for more information.

# Objectstore

Objectstore is a data storage platform for blobs, files, and other unstructured
data at Sentry. It comprises a service with an RPC interface that internally
manages multiple backends, client libraries for easy integration, and a set of
utilities to manage stored data.

<img width="531" height="716" alt="overview" src="https://github.com/user-attachments/assets/e9a4df55-591c-495f-b2a6-60d76d49958e" />

## Internals

The platform is split into the following core components:

- `objectstore-client`: The Rust client library SDK, which exposes
  high-performance blob storage access. A Python library is planned but not yet
  available.
- `objectstore-server`: An `HTTP` server that exposes blob storage and calls
  functionality from the internal services. This crate creates the `objectstore`
  binary.
- `objectstore-service`: The core object storage logic.

Additionally, it contains a number of utilities:

- `stresstest`: A stresstest binary that can run various workloads against
  storage backends.

## Building

Ensure `protoc` and the latest stable Rust toolchain are installed on your
machine. A release build can be created with:

```sh
cargo build --release --locked
```

The server binary will be located at `target/release/objectstore`.

### Docker Container

To build the Docker container, first build the release binary, then build the
container:

```sh
docker build -f Dockerfile target/release/
```

The last argument must be the path to the directory that contains the
`objectstore` binary.

### Cross-building for AMD64 from ARM64

We have utilities to cross-build an AMD64 container from an ARM64 host, such as
a macOS machine running on Apple Silicon. To do this, run:

```sh
scripts/build-cross.sh
```

This script will take care of the entire cross-compilation build process inside
an ARM-based Docker container and use the project's `target/` directory for
caching and build output. The build results in two artifacts:

- The binary at `target/x86_64-unknown-linux-gnu/release/objectstore`
- The docker container, tagged with `objectstore:latest`

## Development

### Environment Setup

Before starting development, ensure you have development tools and environment
configured:

```bash
# Option 1: Use direnv (recommended)
direnv allow

# Option 2: Manually source the environment
source .envrc
```

This will create and activate the `.venv` virtual environment with the correct
Python version, install development tools, and export environment variables for
development.

### Devservices

Our test suite also tests backends that require development services to connect
to. We recommend to use [`devservices`] and start objectstore in "full" mode.
Note that this does not start objectstore itself, but only its dependencies:

[`devservices`]: https://github.com/getsentry/devservices

```sh
devservices up objectstore --mode=full
```

Devservices continue to run in the background until explicitly stopped. If you
prefer to start containers manually, please check `devservices/config.yml` for
the required images and configuration, such as port mapping.

For **Google BigTable**, you additionally need to provision table into the test
instance. Run `scripts/setup-bigtable.sh` to do that. Use this configuration to
connect (recommended as high-volume backend):

```yaml
high_volume_storage:
  type: bigtable
  endpoint: localhost:8086
  project_id: testing
  instance_name: objectstore
  table_name: objectstore
```

For **Google Cloud Storage** (GCS), a test bucket is already configured in the
dev container. Use this configuration to connect (recommended as long-term
backend):

```yaml
long_term_storage:
  type: gcs
  endpoint: http://localhost:8087
  bucket: test-bucket
```

### Editor Setup

We recommend using **Visual Studio Code** with the recommended extensions. The
project includes VSCode configuration that will:

- Automatically format code on save
- Automatically organize imports on save
- Show linting errors inline

When you open this project in VSCode, it will prompt you to install the
recommended extensions if they're not already installed.

### Code Quality

We recommend using Rust Analyzer and clippy, which will apply formatting and
warn about lints during development. If you use the recommended VSCode setup,
this is done automatically for you.

To run linters manually, use:

```sh
# Check and fix formatting
cargo fmt

# Lint all features
cargo clippy --workspace --all-features
```

### Development Server

You can run a development build of the server with `cargo run`. Objectstore can
be configured through a configuration file or environment variables. Note that
if both are given, environment variables override the config file. For this
reason, we do **not** provide default configuration variables in `.envrc`.

```sh
# Option 1: Configuration file
cargo run -- -c objectstore-server/config/local.example.yaml

# Option 2: Environment variables
export FSS_HIGH_VOLUME_STORAGE__TYPE=filesystem
export FSS_HIGH_VOLUME_STORAGE__PATH=data
export FSS_LONG_TERM_STORAGE__TYPE=filesystem
export FSS_LONG_TERM_STORAGE__PATH=data
cargo run
```

You can copy and save additional config files into next to the examples in
`objectstore-server/config`. All other files are ignored by git.

### Tests

To run tests:

```sh
cargo test --workspace --all-features
```

## Utilities

Run the stresstest binary against the running server with:

```sh
cargo run --release -p stresstest -- -c stresstest/config/example.yaml
```

Similar to the objectstore server, you can find example configuration files in
the `config` subfolder. Copy and save your own files there, as they will not be
tracked by git.

## License

Like Sentry, Objectstore is licensed under the FSL. See the `LICENSE.md` file
and [this blog post](https://blog.sentry.io/introducing-the-functional-source-license-freedom-without-free-riding/)
for more information.

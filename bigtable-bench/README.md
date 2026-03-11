# bigtable-bench

A load testing tool for the Bigtable storage backend. Bypasses the HTTP layer
and writes objects directly via `BigTableBackend`, printing live latency
percentiles every second.

## Usage

Always run in release mode:

```
cargo run --release -p bigtable-bench -- [OPTIONS]
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `-c` | `10` | Concurrent put requests |
| `-s` | `50KB` | Object size (e.g. `4096`, `1MB`) |
| `-p` | `1` | Bigtable gRPC connection pool size |
| `-a` | — | Emulator address (omit for real GCP) |
| `--project` | `testing` | GCP project ID |
| `--instance` | `objectstore` | Bigtable instance name |
| `--table` | `objectstore` | Bigtable table name |

### Local emulator

Start the Bigtable emulator via devservices, then point at it with `-a`:

```
devservices up --mode=full
./target/release/bigtable-bench -a localhost:8090
```

### Real GCP Bigtable

Authenticate with `gcloud` and pass your project:

```
gcloud auth login
./target/release/bigtable-bench --project <project>
```

Adjust `--instance` and `--table` if your instance or table name differs from
the default (`objectstore`).

## Sandbox / interactive testing

The production Docker image uses `gcr.io/distroless/cc-debian12:nonroot`, which
has no shell. For interactive testing inside a container, change the base image
to `gcr.io/distroless/cc-debian12:debug-nonroot`, which includes a busybox
shell and allows you to `exec` into the container and run the binary manually.

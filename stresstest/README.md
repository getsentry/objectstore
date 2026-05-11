# Objectstore Stresstest

A load generator for stress-testing Objectstore servers.

## Usage

```bash
cargo run -p stresstest -- -c config/example.yaml
```

## Configuration

The stresstest is configured via a YAML file. See `config/example.yaml` for a
full example.

### Top-level options

| Field      | Required | Default | Description                                             |
|------------|----------|---------|---------------------------------------------------------|
| `remote`   | yes      | —       | URL of the Objectstore server (e.g. `http://localhost:18888`) |
| `duration` | yes      | —       | How long to run the test (e.g. `5s`, `10m`, `1h`)       |
| `workloads`| yes      | —       | List of workload definitions (see below)                |
| `cleanup`  | no       | `false` | Delete all created objects after the test finishes      |

### Workload options

Each entry in `workloads` configures an independent, concurrent workload:

| Field           | Required | Default    | Description                                                        |
|-----------------|----------|------------|--------------------------------------------------------------------|
| `name`          | yes      | —          | Identifier for this workload (used as the Objectstore usecase)     |
| `concurrency`   | no       | CPU count  | Maximum number of concurrent operations                            |
| `organizations` | no       | `1`        | Number of organizations to distribute load across                  |
| `mode`          | no       | `weighted` | Scheduling mode: `weighted`, `throughput`, or `batch`              |
| `file_sizes`    | yes      | —          | File size distribution (see below)                                 |
| `actions`       | no       | 97/2/1     | Operation distribution (see below)                                 |
| `multipart`     | no       | —          | Multipart upload configuration (see below)                         |

### File sizes

Controls the LogNormal distribution used to generate object sizes:

| Field | Required | Description                              |
|-------|----------|------------------------------------------|
| `p50` | yes      | Median file size (e.g. `50 KiB`)         |
| `p99` | yes      | 99th percentile file size (e.g. `200 KiB`) |
| `max` | no       | Hard cap on generated file sizes         |

### Actions

Controls the mix of operations. Interpretation depends on the mode:

| Field     | Default | Description                        |
|-----------|---------|------------------------------------|
| `writes`  | `97`    | Weight (or ops/s) for write operations  |
| `reads`   | `2`     | Weight (or ops/s) for read operations   |
| `deletes` | `1`     | Weight (or ops/s) for delete operations |

- In **weighted** mode: values are relative weights for random selection.
- In **throughput** mode: values are target operations per second.
- In **batch** mode: `writes` is the number of puts per batch request; `reads`
  and `deletes` must be `0`.

### Multipart

Enables multipart uploads for `actions.writes` ops of objects larger than 1 KiB.
Cannot be used in `batch` mode, but can be combined with `throughput` or `weighted`.

| Field         | Required | Description                                              |
|---------------|----------|----------------------------------------------------------|
| `concurrency` | yes      | Max concurrent part uploads per object                   |
| `part_size`   | yes      | Size of each part (min `5 MiB`); last part may be smaller |

```yaml
multipart:
  concurrency: 4
  part_size: 10 MiB
```

## Modes

### Weighted (default)

Runs at maximum speed with fixed concurrency. Each operation is randomly
selected based on the `actions` weights. Good for saturating the server.

### Throughput

Maintains a fixed operations-per-second rate derived from `actions` values.
Useful for simulating steady-state production traffic.

### Batch

Sends multiple writes in a single HTTP request using the batch ("many") API.
The `actions.writes` value determines how many puts per batch request.
Note that reads and deletes are currently not supported in this mode.

# Objectstore Stresstest

A load generator for stress-testing Objectstore servers.

## Usage

```bash
cargo run -p stresstest -- -c config/example.yaml
```

## Configuration

The stresstest is configured via a YAML file. See `config/example.yaml` for a
full example, and `config/production.example.yaml` for workloads that replay
the object sizes of the production usecases.

### Top-level options

| Field      | Required | Default | Description                                             |
|------------|----------|---------|---------------------------------------------------------|
| `remote`   | yes      | —       | URL of the Objectstore server (e.g. `http://localhost:18888`) |
| `duration` | yes      | —       | How long to run the test (e.g. `5s`, `10m`, `1h`)       |
| `workloads`| yes      | —       | List of workload definitions (see below)                |
| `cleanup`  | no       | `false` | Delete all created objects after the test finishes      |
| `ttl`      | no       | `1h`    | Object expiration. `never`/`none`/`manual` or a duration |

### Workload options

Each entry in `workloads` configures an independent, concurrent workload:

| Field           | Required | Default    | Description                                                        |
|-----------------|----------|------------|--------------------------------------------------------------------|
| `name`          | yes      | —          | Identifier for this workload (used as the Objectstore usecase)     |
| `concurrency`   | no       | CPU count  | Maximum number of concurrent operations                            |
| `organizations` | no       | `1`        | Number of organizations to distribute load across                  |
| `mode`          | no       | `fixed-concurrency` | Scheduling mode: `fixed-concurrency`, `fixed-rate`, or `batch` |
| `file_sizes`    | yes      | —          | File size distribution (see below)                                 |
| `actions`       | no       | 97/2/1     | Operation distribution (see below)                                 |

### File sizes

Controls the sizes of the objects that a workload writes. A workload sets either
percentiles or size buckets, not both.

#### Percentiles

Sizes are drawn from a LogNormal distribution fitted to `p50` and `p99`. Sizes
above `p99` are a guess, so the average object size can be several times off
compared to a real usecase.

| Field | Required | Description                              |
|-------|----------|------------------------------------------|
| `p50` | yes      | Median file size (e.g. `50 KiB`)         |
| `p99` | yes      | 99th percentile file size (e.g. `200 KiB`) |
| `min` | no       | Minimum file size, below p50. The distribution is fitted above this floor. |
| `max` | no       | Hard cap on generated file sizes         |

#### Size buckets

Sizes are drawn from a histogram, which replays the object sizes that a usecase
has in production. See `config/production.example.yaml`.

| Field     | Required | Description                              |
|-----------|----------|------------------------------------------|
| `buckets` | yes      | Sizes, each with the number of objects at or below that size |

```yaml
file_sizes:
  buckets:
    1 KiB: 2500    # 2500 objects are at most 1 KiB
    2 KiB: 6000    # 3500 more are between 1 and 2 KiB
    4 KiB: 9900
    8 KiB: 10000   # no object is larger
```

- Counts are cumulative, and only their ratio to the last count matters.
- Make each size twice the previous one. A size is picked log-uniformly within
  its bucket, so wider buckets are less accurate.
- The first bucket starts at half its size.

With buckets from one week of production, the average generated size was within
about 10% of the real average for every usecase.

#### Getting size buckets from Datadog

The `objectstore.put.size` metric has the size of every stored object, tagged by
`usecase`. This query, summed over a time range, returns the count for the
`64 KiB` (65536 bytes) line of each usecase:

```
count(v: v<=65536):objectstore.put.size{sentry_region:us,environment:production} by {usecase}.as_count()
```

Run it for every power of two from 64 B to 8 GiB. An agent with the
[Datadog MCP server](https://docs.datadoghq.com/bits_ai/mcp_server/) can do this
without an API key: its `execute_code` tool runs the script below, which returns
`workloads` entries scaled to one million objects. Round the counts before you
commit them to this repository, so that production data stays private.

```javascript
import { client, v2 } from "@datadog/datadog-api-client";

const scope = "{sentry_region:us,environment:production}";
const days = 7;
const sizes = Array.from({ length: 28 }, (_, i) => 2 ** (i + 6)); // 64 B to 8 GiB

const api = new v2.MetricsApi(client.createConfiguration());
const to = Date.parse(dd.time.hoursAgo(1));
const from = to - days * 24 * 3600 * 1000;
const metric = `objectstore.put.size${scope} by {usecase}.as_count()`;

// Count all objects, and the objects at or below each size.
const counts = {};
for (let i = 0; i < sizes.length; i += 7) {
  const queries = [
    { name: "total", query: `count:${metric}` },
    ...sizes.slice(i, i + 7).map((size) => ({ name: `${size}`, query: `count(v: v<=${size}):${metric}` })),
  ].map((query) => ({ ...query, dataSource: "metrics", aggregator: "sum" }));
  const response = await api.queryScalarData({
    body: { data: { type: "scalar_request", attributes: { from, to, queries } } },
  });
  const columns = response.data.attributes.columns;
  const usecases = columns.find((column) => column.type === "group")?.values;
  if (!usecases) throw new Error(`no data for ${scope} in the last ${days} days`);
  for (const column of columns.filter((column) => column.type !== "group")) {
    usecases.forEach(([usecase], row) => {
      (counts[usecase] ??= {})[column.name] = column.values[row] ?? 0;
    });
  }
}

const unit = (size) =>
  size >= 2 ** 30 ? `${size / 2 ** 30} GiB`
  : size >= 2 ** 20 ? `${size / 2 ** 20} MiB`
  : size >= 2 ** 10 ? `${size / 2 ** 10} KiB`
  : `${size} B`;

// Scale to objects per million, then drop empty leading and repeated trailing buckets.
const workloads = Object.keys(counts).sort().map((usecase) => {
  const perMillion = (count) => Math.round((count / counts[usecase].total) * 1e6);
  const scaled = sizes.map((size) => [size, perMillion(counts[usecase][size] ?? 0)]);
  const [largest, last] = scaled.at(-1);
  const lines = scaled
    .filter(([, count], i) => count > 0 && (i === 0 || scaled[i - 1][1] < last))
    .map(([size, count]) => `        ${unit(size)}: ${count}`);
  if (last < 1e6) {
    lines.unshift(`        # WARNING: ${1e6 - last} per million objects are above ${unit(largest)}, add larger sizes`);
  }
  return `  - name: ${usecase}\n    file_sizes:\n      buckets:\n${lines.join("\n")}`;
});
return `workloads:\n${workloads.join("\n")}`;
```

### Actions

Controls the mix of operations. Interpretation depends on the mode:

| Field     | Default | Description                        |
|-----------|---------|------------------------------------|
| `writes`  | `97`    | Weight (or ops/s) for write operations  |
| `reads`   | `2`     | Weight (or ops/s) for read operations   |
| `deletes` | `1`     | Weight (or ops/s) for delete operations |

- In **fixed-concurrency** mode: values are relative weights for random selection.
- In **fixed-rate** mode: values are target operations per second.
- In **batch** mode: `writes` is the number of puts per batch request; `reads`
  and `deletes` must be `0`.

## Modes

### Fixed concurrency (default)

Runs at maximum speed with fixed concurrency. Each operation is randomly
selected based on the `actions` weights. Good for saturating the server.

### Fixed rate

Targets an operations-per-second rate derived from `actions` values, with a
maximum of 100 concurrent operations per workload. The `concurrency` setting
does not apply to this mode. If operations slow down enough to reach the cap,
the workload can fall below the target rate.

### Batch

Sends multiple writes in a single HTTP request using the batch ("many") API.
The `actions.writes` value determines how many puts per batch request.
Note that reads and deletes are currently not supported in this mode.

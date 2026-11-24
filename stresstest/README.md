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

Controls the sizes of the objects that a workload writes. There are two ways
to describe them, and a workload uses one or the other:

- [Percentiles](#percentiles): a rough shape from a median and a 99th
  percentile. Use this for hand-written workloads.
- [Size buckets](#size-buckets): a histogram measured in production. Use this
  to replay the real object sizes of a usecase.

#### Percentiles

Sizes are drawn from a LogNormal distribution fitted to `p50` and `p99`:

| Field | Required | Description                              |
|-------|----------|------------------------------------------|
| `p50` | yes      | Median file size (e.g. `50 KiB`)         |
| `p99` | yes      | 99th percentile file size (e.g. `200 KiB`) |
| `min` | no       | Minimum file size, below p50. The distribution is fitted above this floor. |
| `max` | no       | Hard cap on generated file sizes         |

This matches the two given percentiles, but everything above `p99` is a guess.
Large objects dominate the bytes written, so the average object size can be
several times off compared to a real usecase. Use size buckets when that matters.

#### Size buckets

`buckets` is a histogram of object sizes. Each line is a size and the number of
objects **at or below** that size:

```yaml
file_sizes:
  buckets:
    1 KiB: 2500    # 2500 objects are at most 1 KiB
    2 KiB: 6000    # 6000 are at most 2 KiB, so 3500 are between 1 and 2 KiB
    4 KiB: 9900    # 3900 are between 2 and 4 KiB
    8 KiB: 10000   # 100 are between 4 and 8 KiB, and no object is larger
```

The rules are:

- Counts are cumulative: each one includes all objects of the lines above it,
  so counts never decrease from one line to the next.
- Only the ratio to the last count matters. Raw counts, counts per million and
  percentages (ending at `100`) all describe the same distribution.
- Sizes must be in increasing order. Use a size that is twice the previous one
  on every line. The histogram does not record where objects fall within a
  bucket, so wider buckets make the generated sizes less accurate.
- The first bucket starts at half its size. In the example above, the first
  2500 objects are between 512 B and 1 KiB.
- A bucket that adds no objects generates no objects. Lines that repeat the
  previous count are harmless, and so are leading lines with a count of `0`.

To generate a size, the stresstest picks a bucket with a probability
proportional to the objects it adds, then picks a size within the bucket so that
every doubling of size is equally likely (log-uniform).

`buckets` cannot be combined with `min`, `p50`, `p99` or `max`.

#### Getting size buckets from Datadog

Objectstore reports the size of every stored object in the
`objectstore.put.size` distribution metric, tagged by `usecase`. Datadog can
count the objects at or below a given size, which is the number that `buckets`
takes. For a size of 64 KiB (65536 bytes), the query is:

```
count(v: v<=65536):objectstore.put.size{sentry_region:us,environment:production} by {usecase}.as_count()
```

Summed over the time range, this returns the count for the `64 KiB` line of each
usecase. Getting all buckets means running this query once per size:

1. Pick the scope (region and environment) and a time range. A week covers the
   daily and weekly cycles.
2. Run the query for every power of two from `64` (64 B) to `8589934592`
   (8 GiB), replacing `65536` with the size in bytes. Sum each result over the
   whole time range.
3. Write the results under `buckets`, one line per size. Leading lines with a
   count of `0` and trailing lines that repeat the final count can be left out.
4. Before committing the numbers to this repository, scale them to a fixed total
   such as one million objects and round them, so that production data stays
   private. The numbers in `config/production.example.yaml` are rounded this way.

The quickest way to do all of this is to ask a coding agent that has the
[Datadog MCP server](https://docs.datadoghq.com/bits_ai/mcp_server/) to run the
script below with its `execute_code` tool. It needs no API key. It runs the
queries for all usecases and returns `workloads` entries that are scaled to one
million objects and ready to paste into a config. Change `scope` and `days` as
needed.

```javascript
import { client, v2 } from "@datadog/datadog-api-client";

const scope = "{sentry_region:us,environment:production}";
const days = 7;

const api = new v2.MetricsApi(client.createConfiguration());
const to = Date.parse(dd.time.hoursAgo(1));
const from = to - days * 24 * 3600 * 1000;

// Count the objects at or below every power of two from 64 B to 8 GiB.
const sizes = Array.from({ length: 28 }, (_, i) => 2 ** (i + 6));
const counts = {};
for (let i = 0; i < sizes.length; i += 7) {
  const queries = sizes.slice(i, i + 7).map((size) => ({
    name: `${size}`,
    dataSource: "metrics",
    aggregator: "sum",
    query: `count(v: v<=${size}):objectstore.put.size${scope} by {usecase}.as_count()`,
  }));
  const response = await api.queryScalarData({
    body: { data: { type: "scalar_request", attributes: { from, to, queries } } },
  });
  const columns = response.data.attributes.columns;
  const usecases = columns.find((column) => column.type === "group").values;
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
  const total = counts[usecase][sizes.at(-1)];
  const scaled = sizes.map((size) => [size, Math.round(((counts[usecase][size] ?? 0) / total) * 1e6)]);
  const buckets = scaled.filter(([, count], i) => count > 0 && (i === 0 || scaled[i - 1][1] < 1e6));
  const lines = buckets.map(([size, count]) => `        ${unit(size)}: ${count}`);
  return `  - name: ${usecase}\n    file_sizes:\n      buckets:\n${lines.join("\n")}`;
});
const window = `${new Date(from).toISOString()} to ${new Date(to).toISOString()}`;
return `# objectstore.put.size${scope}, ${window}\nworkloads:\n${workloads.join("\n")}`;
```

Without an agent, run the query in the Datadog UI with a widget that reduces a
metric query to one value per group over the selected time range, such as a
table in a notebook, and sum the values over time. Threshold queries like
`count(v: v<=...)` need percentiles to be enabled for the metric, which is the
case for `objectstore.put.size`.

Measured against one week of production data, sizes generated from these
buckets had an average within about 10% of the real average for every usecase,
and a p50 and p90 within 15%. Higher percentiles can be off by more, up to the
2x width of a bucket, when the largest objects of a usecase cluster in one part
of a bucket.

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

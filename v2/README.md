# goetl v2 — columnar pipeline proof of concept

A proof of concept for rebuilding goetl's core as an Arrow-backed columnar
pipeline, with optional in-process SQL via DuckDB.

This is a spike, not a library. It exists to answer four questions with numbers
rather than estimates. All measurements below are reproducible from this
directory.

## Results

Intel Xeon @ 2.80GHz, 4 cores, Go 1.25.13, Arrow v18.7.0, duckdb-go v2.10505.0.
`b.N` is the **row** count everywhere, so `ns/op` reads as **ns per row**.

### End-to-end, head to head

The number that matters. `../benchcmp` imports both versions into one module
and runs them in one test binary: same machine, same toolchain, same process.
The job is identical — generate rows, apply a region-dependent markup, write
CSV to a discarding writer — and `TestSameOutput` asserts both pipelines
produce the same values, so the comparison is not measuring different work.

| Configuration | ns/row | B/row | allocs/row |
|---|---:|---:|---:|
| v1, 1 row per payload | 7,510 | 2,269 | 56 |
| v1, 4096 rows per payload (v1's best case) | 2,232 | 1,262 | 22 |
| **v2, 4096 rows per batch** | **307** | **174** | **3** |

**7.3x faster than v1 at its best, 24x at one row per payload**, with 7x fewer
allocations. Three runs each, spread under 3%.

The per-payload row matters because many v1 sources are inherently
per-record — `IoReader` line-by-line, `FileReader`, `S3Reader`. Getting v1's
best case requires the source to batch.

### Why this is 7x and not the 78x below

The micro-benchmarks isolate *framework overhead*, and there v2 really is ~78x
better. But an end-to-end job also does work neither version can avoid, and
that work now dominates: of v2's 307 ns/row, roughly 245 ns is CSV encoding.
Once the framework stops being the bottleneck, the format does.

So **78x is the honest number for what the runtime costs, and 7x is the honest
number for what a pipeline costs.** Quote the second one. Jobs whose sink is
cheaper than CSV, or that fan out to several stages, will land higher.

### Component micro-benchmarks

| Stage | v1 (ns/row) | v2 (ns/row) | Speedup |
|---|---:|---:|---:|
| Runtime overhead (source → passthrough → sink) | 4,835 | **62.1** | **78x** |
| Row-wise transform (Tier 2) | — | 82.1 | — |
| Transform + CSV encode to a sink | — | 304.5 | — |
| DuckDB SQL aggregation in-pipeline | — | 241.8 | — |

Allocations per row: **0** for the first two. v1 did 14 allocs/payload for the
equivalent passthrough.

The v1 runtime-overhead figure comes from a scratch harness built with a
different Go toolchain, so treat it as indicative; the head-to-head table above
has no such confound and is the one to rely on.

### The tier gap is smaller than expected

Row-wise access costs **1.3x** columnar, not the 5-10x I estimated before
measuring. Both are within noise of the bare runtime, which means at these
volumes the dominant cost is the runtime itself, not the access pattern. The
API should still steer people toward `ColumnTransform`, but `RowTransform` is
not the cliff I predicted — that changes how hard the ergonomics have to fight
against it.

## Reproduce

```bash
go test ./...                                        # core, pure Go
go test -race ./...                                  # clean
go test ./ -run=XXX -bench=. -benchtime=2000000x     # core benchmarks

go test -tags duckdb_arrow ./...                     # + DuckDB (cgo)
go test -tags duckdb_arrow ./... -run=XXX -bench=. -benchtime=2000000x
```

## What the four questions turned out to be

**1. Is Go → DuckDB actually zero-copy?** Yes, confirmed.
`TestZeroCopyHandoff` registers an Arrow view, mutates the Go-owned buffer
*after* registration and *before* querying, and DuckDB returns the mutated
value (1049, not 60). DuckDB reads Go memory directly through the Arrow C Data
Interface.

**2. Is DuckDB → Go zero-copy?** No, and it cannot be. Result records alias
memory owned by the result stream, invalidated when the stream advances or is
released. A pipeline hands batches downstream through a channel, so the reader
is gone by the time they are consumed. `duckdbx/copy.go` deep-copies results
into Go-owned memory. That copy is on the smaller side of an aggregation, so it
is cheap in practice, but the asymmetry is real and permanent.

**3. Can the core stay pure Go?** Yes. The root package has no cgo and
cross-compiles normally. DuckDB lives in `duckdbx`, behind both a separate
import and the `duckdb_arrow` build tag. Pipelines that do not need SQL never
link C.

**4. Does the columnar bet pay off?** Yes — 78x on runtime overhead, with zero
allocations per row.

## The blocker: Arrow views are unsound for any schema containing strings

This is the significant negative finding, it is worse than a crash, and it is
**already reported upstream and still open**: [duckdb/duckdb-go#24](https://github.com/duckdb/duckdb-go/issues/24),
migrated from `marcboeker/go-duckdb#513`.

Note also that `marcboeker/go-duckdb` was **archived in October 2025** and moved
to `github.com/duckdb/duckdb-go`. This module tracks the new path.

Measured on both duckdb-go v2.4.3 and v2.10505.0 (DuckDB 1.5.5):

| Case | v2.4.3 | v2.10505.0 |
|---|---|---|
| Numeric-only view, numeric query | correct, zero-copy | correct, zero-copy |
| View **contains** a VARCHAR; query never references it | **corrupt** | **corrupt** |
| Query reads a VARCHAR from the view | SIGSEGV | **corrupt strings** |
| `GROUP BY` a VARCHAR from the view | SIGSEGV | **corrupt strings** |
| String literal not sourced from a view (`SELECT 'x'`) | fine | fine |

**Upgrading makes it more dangerous, not less.** The latest release removes the
segfault but returns silently corrupt data in its place — group keys come back
as binary garbage (`map[<garbage>:6]` instead of `map[east:6 west:4]`), and
`SELECT SUM(amount)` over `(id, region, year, amount)` returns a denormal
(~1.4e-322) instead of 32 with no error at all. A loud crash is strictly safer
than a wrong number.

One detail here is not in the upstream issue: **the failure is triggered by the
mere presence of a VARCHAR column in the view schema**, not by aggregation.
Upstream #24 attributes it to aggregate queries, and its reproducer happens to
carry a string column without identifying it as the cause. The identical
pipeline over a numeric-only source is correct
(`TestNumericOnlyViewIsCorrect`), which isolates it. That is worth adding to
the issue.

### Is there a reliable path? Yes, and it costs 1.8x

The defect is query-shape and schema dependent, and it is not confined to the
Arrow result path:

| Path | Result |
|---|---|
| Arrow view → Arrow results, numeric-only schema | correct |
| Arrow view → Arrow results, varchar in schema | corrupt |
| Arrow view → `database/sql`, `GROUP BY` | correct |
| Arrow view → `database/sql`, **direct aggregation** | **corrupt** |
| **Materialize to a table first, then anything** | **correct** |

Direct aggregation straight off an Arrow view is wrong on *both* result paths,
which is exactly what upstream #24 reports. Materializing the view into a table
on the same connection is correct in every shape tested.

`BenchmarkMaterializePerBatch` measures that route end to end:

| Route | ns/row | Sound? |
|---|---:|---|
| Query the Arrow view directly | 247.7 | only for numeric-only schemas |
| Materialize, then query | 455.5 | yes |

So the safe route costs **1.8x** the unsafe one — and is still ~24x faster than
v1's framework overhead alone. The materialization copy is the real cost of the
bug, not correctness.

**This does not invalidate the architecture.** The Arrow core is independent of
DuckDB (62 ns/row, pure Go, no cgo). What weakens is the specific claim of
"zero serialization anywhere": until #24 is fixed, the SQL boundary costs one
copy of the input. Zero-copy registration is confirmed working and becomes an
optimization to switch on later, not a foundation to build on now.

For production the `Appender` API is likely a better ingest route than
`CREATE TABLE AS SELECT` — it is go-duckdb's native bulk path and far more
exercised than the Arrow C Data Interface binding. Not benchmarked here.

## Connector port: what the migration actually costs

`SQLWriter` (`sqlwriter.go`) ports v1's Postgres and MySQL writers to the
columnar core, to size the wider connector migration with a real example rather
than an estimate.

| | v1 | v2 |
|---|---:|---:|
| Postgres writer | 78 + 127 | — |
| MySQL writer | 73 + 129 | — |
| Shared `sortedColumns` | ~20 | 0 (schema is known) |
| **Total** | **~427** | **322** |

Both dialects now share one writer with a `Dialect` interface holding only the
flavor-specific fragments, where v1 had two near-duplicate implementations
across four files.

**What carried over unchanged**: the SQL shapes. `INSERT ... ON CONFLICT (t) DO
UPDATE SET c=EXCLUDED.c` and `INSERT ... ON DUPLICATE KEY UPDATE c=VALUES(c)`,
the batch-splitting behavior, and the default of updating every column on
conflict. That is the accumulated production knowledge, and it transfers
verbatim — which is the argument for porting connectors rather than rewriting
them.

**What disappeared**: v1 unioned the keys of every object in a payload, sorted
them, and nil-filled rows missing a key, because a `[]map[string]interface{}`
has no schema. An Arrow batch does, so that entire pass is gone.

**What got fixed on the way**:

- v1 built the INSERT by repeated concatenation in nested loops, quadratic in
  batch size. Verified linear here: 500 rows 31.3µs, 5000 rows 332.2µs — 10.6x
  for 10x the rows, at 2 allocations per statement.
- v1 interpolated column names unquoted, so a column named `order` or `select`
  produced invalid SQL. Identifiers are now quoted per dialect, with embedded
  quotes doubled (`TestIdentifierQuotingHandlesReservedWords`).
- v1 called `db.Prepare` for every batch. Statements are now cached by row
  count, so a uniform stream prepares twice: once for the full shape, once for
  the final partial batch.

**Effort**: roughly half a day including tests, for two dialects. Extrapolating
across v1's 28 processors is not linear — the cloud connectors (S3, SFTP, FTP,
BigQuery, Redshift) are I/O glue that barely touches the data model and should
port faster, while `RedshiftWriter`'s S3-manifest logic is the outlier.

**Not a v1-vs-v2 throughput comparison.** `BenchmarkSQLWriter` reports ~11.8µs
per row, but that is dominated by DuckDB executing single-row-per-tuple INSERTs,
not by this code; DuckDB wants its `Appender` API for bulk load. v1's writers
were never benchmarked against a database either, so there is no baseline to
compare against. The generation and marshalling path is what was measured here.

## Design notes

**Batch** wraps `arrow.Record`. `Column[T]` returns a Go slice aliasing the
Arrow buffer — no copy, no allocation, writes mutate in place.

**Processor** is `Process(ctx, *Batch, Emit) error` plus `Flush`. Errors return
and cancel the pipeline through `errgroup`; there is no kill channel.

Every v1 defect found in the analysis is structurally absent here, and each has
a test: `TestFlushExactlyOnce` (v1 called `Finish` twice on stage 1),
`TestErrorPropagates` and `TestContextCancelStops` (v1's `killChan` was
fire-and-continue over a single-receive unbuffered channel),
`TestNoPerBatchRetention` (v1 leaked ~314 bytes per payload, unbounded — this
asserts a checked Arrow allocator drains to zero after 200k rows).

## What this does not cover

Branching/merging DAGs, the connector port, spill-to-disk, backpressure metrics,
and schema evolution across batches. Those are deliberate omissions — the spike
targets the four questions above.

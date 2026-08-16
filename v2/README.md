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
| v1, 1 row per payload | 6,971 | 2,269 | 56 |
| v1, 4096 rows per payload (v1's best case) | 1,883 | 1,265 | 22 |
| **v2, 4096 rows per batch** | **258** | **110** | **3** |

**7.3x faster than v1 at its best, 27x at one row per payload.** Three runs
each, spread under 3%.

The per-payload row matters because many v1 sources are inherently
per-record — `IoReader` line-by-line, `FileReader`, `S3Reader`. Getting v1's
best case requires the source to batch.

### Why this is 7x and not the 78x below

The micro-benchmarks isolate *framework overhead*, and there v2 really is ~78x
better. But an end-to-end job also does work neither version can avoid, and
that work now dominates. Of v2's 258 ns/row, roughly **200 ns is CSV encoding**
and only 58 ns is the runtime plus the transform.

So **78x is the honest number for what the runtime costs, and 7x is the honest
number for what a CSV-writing pipeline costs.** Quote the second one.

**The runtime is no longer the bottleneck — the output format is, by roughly
3:1.** Further work on the pipeline core buys almost nothing. The next real
gain comes from a sink that does not convert floats to decimal text at all.

### Component micro-benchmarks

| Stage | v1 (ns/row) | v2 (ns/row) | Speedup |
|---|---:|---:|---:|
| Runtime overhead (source → passthrough → sink) | 4,835 | **62.1** | **78x** |
| Row-wise transform (Tier 2) | — | 82.1 | — |
| Transform + CSV encode to a sink | — | 259.3 | — |
| DuckDB SQL aggregation in-pipeline | — | 241.8 | — |

Allocations per row: **0** for the runtime and transform stages; the CSV sink
adds 3, all of them strings that `encoding/csv` requires.

### The CSV encoder: a deliberate 1.6x given up

Encoding is delegated to `encoding/csv`. A hand-rolled encoder that appended
bytes directly was built, measured, and then **removed on purpose**.

| Encoder | pipeline ns/row | allocs/row | vs v1 best |
|---|---:|---:|---:|
| hand-rolled direct append | 165 | 0 | 13x |
| **`encoding/csv` (current)** | **258** | **3** | **7.3x** |

The hand-rolled version was genuinely 1.6x faster end to end and allocation
free, and it was verified byte-for-byte against the standard library by a fuzz
target that ran 3,049,395 cases without a mismatch. It was still the wrong
thing to keep: it meant owning an RFC 4180 implementation whose failure mode is
silently malformed output, in a library whose entire value proposition is that
the data arrives correct. A fuzz oracle reduces that risk but does not remove
the obligation to run it, understand it, and keep it aligned forever.

`encoding/csv` cannot be made allocation-free from the outside: `Write` takes
`[]string`, so every numeric cell must be materialized as a string. That is a
property of the API, not something a third-party CSV library fixes — they share
the same shape and the same `strconv` underneath.

**The right place for that 1.6x is a different sink, not a better CSV writer.**
Measured per value:

| Operation | ns | allocs |
|---|---:|---:|
| `strconv.FormatFloat` shortest, allocating | 91.1 | 1 |
| `strconv.AppendFloat` shortest, into buffer | 68.1 | 0 |
| `strconv.AppendFloat` fixed `'f',2` | 138.5 | 0 |
| `strconv.AppendInt` | 14.4 | 0 |
| `encoding/csv` framing, 4 fields | 65.8/row | 0 |
| direct append framing, 4 fields | 14.2/row | 0 |

Framing was only 66 ns/row. The rest is float-to-decimal conversion, which is
irreducible for any text format — note that fixed precision measures *slower*
than shortest-round-trip, so trading digits for speed does not work either.

Parquet or Arrow IPC store a float64 as 8 raw bytes with no conversion at all.
That removes the entire ~200 ns/row rather than shaving 1.6x off it, and the
encoder is maintained upstream by `parquet-go` or Arrow itself. Keep
`encoding/csv` for interchange; reach for a columnar sink when throughput is
the point. `csvprofile_test.go` retains the measurements above as the evidence
for that direction.

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

## Custom processors

The extension point is unchanged in spirit from v1: implement an interface,
write ordinary Go. Nothing built in is privileged — `SQLWriter`, `CSVWriter`
and `duckdbx.Transform` are just implementations of the same two interfaces.

```go
type Source interface {
    Read(ctx context.Context, emit Emit) error
}

type Processor interface {
    Process(ctx context.Context, b *Batch, emit Emit) error
    Flush(ctx context.Context, emit Emit) error
}
```

`example_custom_test.go` lives in an external test package, so it exercises the
exported API only, and covers the four shapes that matter:

| Shape | Example |
|---|---|
| Stateful aggregation across batches, emitting from `Flush` | `RegionTotals` |
| Filtering rows | `ExampleNewFilter` |
| Adding a column | `Enricher` |
| External I/O enrichment — the case SQL cannot do | `Enricher` |
| A custom source | `CountSource` |

Compared with v1 the interface gained `context` and an `error` return, and lost
the kill channel. `Flush` replaces `Finish` and is guaranteed to run exactly
once per stage. Embedding `goetl.NopFlush` covers stateless processors.

### The friendly path

The columnar API demands you know each column's name and type at every access,
handle an error per lookup, and use Arrow builders to change a schema. v1 let
you write `d.Parse(&rows)` and work in ordinary Go structs, which is a large
part of why it was pleasant. `NewStructTransform` restores that:

```go
markup := goetl.NewStructTransform("markup", func(rows []Sale) ([]Sale, error) {
    var out []Sale
    for _, r := range rows {
        if r.Year < 2024 {
            continue // filtering is just not appending
        }
        r.Amount *= 1.08
        out = append(out, r)
    }
    return out, nil
})
```

No column names at call sites, no per-access errors, no builders, and
filtering falls out for free. Columns bind to fields by `goetl:"..."` tag or
field name.

**At identical ergonomics, v2 is 6.4x faster than v1.** Both sides of this
comparison decode into a `[]struct`, mutate, and re-encode — the only
difference is the runtime and the wire format:

| Configuration | ns/row | allocs/row |
|---|---:|---:|
| v1, struct API (JSON payloads) | 2,202 | 22 |
| **v2, struct API (Arrow batches)** | **346** | **3** |
| v2, columnar API | 319 | 3 |

The striking part is the last two rows. In isolation the struct path costs
2.9x the columnar path (205 vs 70 ns/row), but **in a full CSV-writing
pipeline it costs 6%** — because the sink dominates everything upstream.

So the friendly path is not a compromise for most pipelines. Write structs,
get v1's developer experience and 6.4x its throughput; drop to `Column[T]`
only where a profile says a stage matters.

Remaining gaps, honestly: struct binding is reflection-based so column names
are still unchecked strings; nullable fields are unsupported (no pointer
fields); the type coverage is int64/float64/string/bool only; and sorting,
joining and grouping have no helpers yet.

**Two more helpers exist because of ergonomics, not performance.** Arrow arrays are
fixed length, so dropping rows means rebuilding a batch — written by hand that
is a ~45-line type switch over every column type in every filtering processor.
`goetl.Filter` and `goetl.AppendColumn` absorb that, so a filter is just its
predicate:

```go
recent := goetl.NewFilter("recent", func(b *goetl.Batch) ([]bool, error) {
    year, err := goetl.Column[int64](b, "year")
    if err != nil {
        return nil, err
    }
    keep := make([]bool, len(year))
    for i, y := range year {
        keep[i] = y >= 2024
    }
    return keep, nil
})
```

`Filter` retains and returns the input unchanged when nothing is dropped, so a
predicate that matches everything costs no copy. `TestFilterNoLeak` runs it over
100k rows under a checked Arrow allocator and asserts the allocator drains to
zero.

This is the area that still needs the most design work before the API is worth
publishing: sorting, joining, grouping and column projection would each
otherwise push the same boilerplate onto users.

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

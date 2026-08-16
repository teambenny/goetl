# goetl v2 — columnar pipeline proof of concept

A proof of concept for rebuilding goetl's core as an Arrow-backed columnar
pipeline, with optional in-process SQL via DuckDB.

This is a spike, not a library. It exists to answer four questions with numbers
rather than estimates. All measurements below are reproducible from this
directory.

## Results

Intel Xeon @ 2.80GHz, 4 cores, Go 1.25.13, Arrow v18.7.0, duckdb-go v2.10505.0.

`b.N` is the **row** count in every benchmark, so `ns/op` reads as
**nanoseconds per row** and is directly comparable to the v1 baseline.

| Stage | v1 (ns/row) | v2 (ns/row) | Speedup |
|---|---:|---:|---:|
| Runtime overhead (source → passthrough → sink) | 4,835 | **62.1** | **78x** |
| Realistic transform + runtime | 10,947 | **62.2** | **176x** |
| Row-wise transform (Tier 2) | — | 82.1 | — |
| Transform + CSV encode to a sink | — | 304.5 | — |
| DuckDB SQL aggregation in-pipeline | — | 241.8 | — |

Allocations per row: **0** for the first three. v1 did 14 allocs/payload for the
equivalent passthrough and 40 for the JSON transform.

v1 numbers come from the harness in the analysis for this branch: a 3-stage
pipeline over `etldata.JSON`, measured on the same machine.

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

**This does not invalidate the architecture** — the Arrow core is independent of
DuckDB, and question 1 confirms the C Data Interface itself works. But shipping
DuckDB as the SQL layer is blocked until #24 is fixed, worked around
(dictionary-encode strings to int32 across the boundary), or replaced (ADBC, or
DuckDB's non-Arrow API via `database/sql`, which is correct but not zero-copy).

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

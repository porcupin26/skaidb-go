# Batches, and living without transactions

skaidb has no multi-statement transactions. `db.Begin()` returns
`skaidb: transactions are not supported`; every statement is atomic on its
own and commits when the server acknowledges it at the requested
consistency.

## Bulk writes: `ExecBatch`

`database/sql` has no batch call, and `ExecContext` per row costs one
network round trip per row. `skaidb.ExecBatch` runs one statement over many
parameter rows in a single `OP_EXECUTE_BATCH` request and returns the total
affected-row count:

```go
rows := make([][]any, 0, len(events))
for _, e := range events {
	rows = append(rows, []any{e.ID, e.At, e.Payload})
}
n, err := skaidb.ExecBatch(ctx, db, "INSERT INTO events (id, ts, payload) VALUES (?, ?, ?)", rows)
if err != nil { return err }
log.Printf("%d rows written", n)
```

1,000 rows are two requests: one `OP_PREPARE` (cached per connection after
the first time) and one `OP_EXECUTE_BATCH`.

On a connection you have pinned, `skaidb.ExecBatchConn(ctx, conn, query,
rows)` does the same. Both wrap the `database/sql` escape hatch, which you
can use directly: the driver's `driver.Conn` implements
`skaidb.BatchExecer`.

```go
conn, err := db.Conn(ctx)
if err != nil { return err }
defer conn.Close()
var n int64
err = conn.Raw(func(driverConn any) error {
	bc := driverConn.(skaidb.BatchExecer)
	var err error
	n, err = bc.ExecBatch(ctx, "INSERT INTO t (id, v) VALUES (?, ?)", [][]any{{1, "a"}, {2, "b"}})
	return err
})
```

What to know:

- **Binding.** Each row binds like the arguments of `ExecContext`:
  scalars, `time.Time`, `[]byte`, `sql.Null*` and any `driver.Valuer`,
  pointers, named types, slices and maps (arrays and documents). Every row
  must carry the statement's parameter count; a mismatch fails before
  anything is sent (`skaidb: batch row 3: statement expects 2 parameters,
  got 1`).
- **Context.** Deadline, cancellation and `skaidb.WithConsistency` apply to
  the whole batch.
- **Size.** One request is at most one wire frame (64 MiB). A larger batch
  goes out in consecutive chunks, each one round trip, in row order. A
  single row larger than a frame is an error.
- **Failure.** Rows run in order, each auto-committed. The first failing
  row ends the batch with the server's error, which names the row within
  its request; the rows before it stay applied. When the batch was
  chunked, the returned count covers the chunks that applied and the error
  adds `(row numbers count from batch row N; the N rows before it
  applied)`.
- **Fallbacks.** A statement the server will not prepare (DDL, `CALL`) runs
  text-bound, one request per row; a server older than the batch opcode
  (< 0.87.0) gets one `OP_EXECUTE` per row. Same results, more round trips.
- **Retries.** `ExecBatch` (the `*sql.DB` form) retries on a fresh
  connection only when the request never reached the wire, as
  `database/sql` does for its own calls. Once a request was sent, a lost
  connection is reported, never retried.

## A loop of prepared statements

When each row needs its own result, or its own error handling, run one
prepared statement many times on one connection:

```go
conn, err := db.Conn(ctx)
if err != nil { return err }
defer conn.Close()

stmt, err := conn.PrepareContext(ctx, "INSERT INTO events (id, ts, payload) VALUES (?, ?, ?)")
if err != nil { return err }
defer stmt.Close()

for _, e := range events {
	if _, err := stmt.ExecContext(ctx, e.ID, e.At, e.Payload); err != nil {
		return err // events before this one are committed; this one is not
	}
}
```

Pinning a `*sql.Conn` keeps the prepared id warm and the statements in
order on one socket; it is optional — `db.Exec` in a loop also works and
lets the pool spread the writes.

Multi-row `VALUES` is one statement with N×k parameters:

```go
db.ExecContext(ctx, "INSERT INTO t (id, v) VALUES (?, ?), (?, ?), (?, ?)", 1, "a", 2, "b", 3, "c")
```

For throughput, run several loops concurrently on several connections
(`db.SetMaxOpenConns`). Every node accepts writes, so the pool's spread
across seeds is the parallelism.

## Failure in the middle

A batch that fails part-way leaves the earlier statements committed. Make
the writes idempotent (skaidb writes are upserts by primary key, so
re-running a batch of `INSERT`s converges) and re-run from the start or
from the failed item.

One error deserves care: `skaidb: connection lost mid-statement (statement
may or may not have applied)`. The request reached the wire but the reply
did not come back. For an idempotent upsert, retry. For an
`UPDATE … SET n = n + 1`, read first.

## Generated values: `RETURNING`

```go
var id int64
err := db.QueryRowContext(ctx, "INSERT INTO t (name) VALUES (?) RETURNING id", name).Scan(&id)
```

`Result.LastInsertId` always errors; the row is the only channel.

## Upserts and conditional writes

```sql
INSERT INTO t (id, v) VALUES (?, ?) ON CONFLICT DO NOTHING       -- keep existing; affected 0
INSERT INTO t (id, v) VALUES (?, ?) ON CONFLICT DO UPDATE SET v = ?
UPDATE t SET v = ? WHERE id = ? AND v = ?                         -- affected 0 if the guard failed
```

Each is a single statement and therefore atomic on the row. Check
`RowsAffected` to learn which branch happened. See the
[SQL syntax reference](https://skaidb.org/docs/query_syntax.html).

## Reading many rows back

The batch's mirror image — reading a large table — is the
[streaming path](streaming.md), not a plain `SELECT`.

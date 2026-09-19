# Batches, and living without transactions

skaidb has no multi-statement transactions. `db.Begin()` returns
`skaidb: transactions are not supported`; every statement is atomic on its
own and commits when the server acknowledges it at the requested
consistency.

## Bulk writes

The efficient shape is one prepared statement, executed many times, on one
connection:

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

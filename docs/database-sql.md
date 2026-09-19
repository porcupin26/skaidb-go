# Using `database/sql` with skaidb

The driver registers as `"skaidb"`. Everything below is the standard
library API; this chapter is about what each call does against skaidb.

## `*sql.DB`

The pool. Create one per process (or per cluster) and share it. Its options
behave as usual:

```go
db.SetMaxOpenConns(32)              // upper bound on sockets to the cluster
db.SetMaxIdleConns(8)
db.SetConnMaxLifetime(30 * time.Minute) // recycle: rebalances across seeds over time
```

Recycling connections periodically (`SetConnMaxLifetime`) is worth doing on
a cluster: every new dial shuffles the seed list, so the pool drifts back to
an even spread after a node was down.

## `ExecContext`

For statements that do not return rows: DDL, `INSERT`, `UPDATE`, `DELETE`,
`USE`, `CREATE STREAM`…

```go
res, err := db.ExecContext(ctx, "UPDATE users SET age = ? WHERE id = ?", 37, 1)
n, _ := res.RowsAffected() // rows the server reports as affected
```

`RowsAffected` is the server's count for mutations and `0` for DDL.
`LastInsertId` always errors: skaidb does not carry a generated id on the
wire. Use `RETURNING` with `QueryRow` instead.

A row-producing statement passed to `Exec` succeeds and reports `0`
affected; the rows are discarded.

## `QueryContext` and `QueryRowContext`

```go
var name string
err := db.QueryRowContext(ctx, "SELECT name FROM users WHERE id = ?", 1).Scan(&name)
if errors.Is(err, sql.ErrNoRows) { /* no such user */ }
```

`QueryRow` is the idiomatic single-row read and the way to get a
`RETURNING` value back from a write.

`Query` returns `*sql.Rows`. Two things the driver relies on you doing:

1. `defer rows.Close()` — always, and non-negotiably on the
   [streaming](streaming.md) path.
2. check `rows.Err()` after the loop: a server error that ends a stream
   part-way arrives there, not from `Query`.

A non-row statement passed to `Query` returns an empty result set, which is
how `CALL` of a procedure with no `EMIT` behaves.

### Multiple result sets

A procedure that `EMIT`s several sets is read with `NextResultSet`:

```go
rows, err := db.QueryContext(ctx, "CALL monthly_report(?)", 9)
defer rows.Close()
for {
	cols, _ := rows.Columns()
	for rows.Next() { … }
	if err := rows.Err(); err != nil { return err }
	if !rows.NextResultSet() { break }
}
```

## `PrepareContext` and `*sql.Stmt`

A `*sql.Stmt` is prepared on the server, on whichever pooled connection runs
it, and executed with typed parameters — the same path `db.Exec` with
arguments takes. See [prepared-statements.md](prepared-statements.md).

## `Conn`

`db.Conn(ctx)` pins one physical connection. Use it when a sequence must
stay on one socket: a batch on one prepared statement, or a `USE` followed by
statements that depend on it (prefer the DSN path for the session database —
it is applied to every pooled connection, `USE` through `Exec` is not).

Two goroutines must not share a `*sql.Conn` concurrently while one is
iterating a streamed result; the second statement fails with a "busy
streaming" error rather than corrupting the socket.

## `Begin` / `BeginTx`

Return `skaidb: transactions are not supported`. Every statement
auto-commits. See [batch.md](batch.md) for the patterns that replace
transactions.

## `Ping`

Checks out a connection, dialling one if the pool is empty. There is no
lighter server round-trip; the dial itself (TCP, TLS, SCRAM, `USE`) is the
meaningful health check.

## Consistency per statement

`database/sql` has no slot for it, so the driver reads it from the context:

```go
rows, err := db.QueryContext(skaidb.WithConsistency(ctx, "one"), "SELECT …")
```

The DSN sets the default for the whole pool. See [dsn.md](dsn.md#consistency).

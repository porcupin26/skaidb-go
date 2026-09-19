# Streaming large result sets

## Why

A plain `SELECT` is materialised on the coordinating node and returned in
one frame. The server bounds that with scan budgets (rows examined and
bytes; see the [server docs](https://skaidb.org/docs/)) and **errors**
rather than truncates when a query exceeds them. Reading a big table needs
the streaming request: the rows come back as a header, N chunks and an end
marker, and `rows.Next` pulls chunks as you go, so the client holds one at a
time.

On the server, a streamed *simple scan* — `SELECT cols FROM t [WHERE
simple predicate]` — is paged on the primary key, each page its own
statement with a fresh budget, so table size stops mattering. Statements
the pager declines (`LIMIT`/`OFFSET`, `DISTINCT`, `JOIN`, `GROUP BY`,
`HAVING`, set operations, window functions, `NEAREST`, predicates with
functions, parameters, aggregates or subqueries) are still delivered in
chunks but run as one materialised statement under the usual budgets.
Adding a `LIMIT` "to help" disqualifies the pager. Name your columns: a
bare `SELECT *` on a cluster needs consistency `one` for the paged lane,
because the wildcard's column discovery reads the local shard.

## How

```go
ctx, cancel := context.WithTimeout(ctx, time.Hour) // covers the whole stream
defer cancel()

rows, err := db.QueryContext(skaidb.WithStreaming(ctx), "SELECT id, name, tags FROM users")
if err != nil {
	return err
}
defer rows.Close() // mandatory — see below

for rows.Next() {
	var id int64
	var name, tags string
	if err := rows.Scan(&id, &name, &tags); err != nil {
		return err
	}
	// …
}
return rows.Err() // a server error part-way arrives here
```

`WithStreaming` is a context decorator because `database/sql` has no other
slot for it; it applies to that one `QueryContext` call.

- **No parameters.** The streaming request carries SQL text; a call with
  arguments silently takes the buffered path.
- **The rows own the connection** until `Close`. The pool pins the
  connection to the rows; a second statement on the same `*sql.Conn` from
  another goroutine fails with `skaidb: connection is busy streaming a
  result set — close those rows before running another statement`.
- **The context must outlive the loop.** Cancelling it ends the stream and
  retires the connection; the deadline applies to the last chunk as much as
  to the first.
- **Older servers** that do not know the streaming opcode answer a
  buffered result; the code above still works.
- A procedure that `EMIT`s answers with its (buffered) result sets on this
  path too.

## The abandon/drain rule

Leaving a stream before its end — `break`, `return`, a failed `Scan` — is
allowed, but the unread chunks are still queued on the socket. If the pool
handed that connection to the next caller, its first statement would read a
leftover chunk as its own reply. The [protocol
§3.4](https://skaidb.org/docs/PROTOCOL.html) says the client must **drain
the rest or stop using the connection**, and the driver does that in
`rows.Close`:

- it reads and discards the remaining frames while that is cheap — at most
  64 frames or 8 MiB, waiting at most 2 s for a peer that has gone quiet —
  and the connection goes back to the pool healthy;
- past any of those bounds it marks the connection broken instead; the pool
  closes it before the next checkout and dials a fresh one.

Neither outcome is an error, and `Close` returns `nil` in both. What the
driver cannot do is run `Close` for you:

> `database/sql` closes a `Rows` on your behalf only when the iteration runs
> to exhaustion (`Next` returned false) or when the context is cancelled.
> **It does not close on a bare `break` or an early `return`, and there is
> no finalizer.** A streamed `*sql.Rows` that is dropped unclosed keeps its
> connection checked out of the pool, mid-stream, until the process exits.

So `defer rows.Close()` is the contract on this path, not a courtesy.
Everything the driver does to keep the pool safe hangs off it.

## Deciding between streaming and buffering

| Use | Path |
|---|---|
| point reads, small filtered reads, aggregates, anything with `LIMIT` | plain `QueryContext` |
| exports, full-table scans, ETL, anything that might exceed a budget | `WithStreaming` |
| a parameterised big read | filter server-side by other means (a stream, a view, an interpolated trusted value), or page it yourself with a keyset (`WHERE id > ? ORDER BY id LIMIT n`) on the plain path |

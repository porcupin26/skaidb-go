# Context: deadlines and cancellation

Every `…Context` method — `ExecContext`, `QueryContext`, `QueryRowContext`,
`PrepareContext`, `Stmt.ExecContext`, `Conn`, `PingContext` — honours its
context. The non-context variants use `context.Background()`.

## Deadlines

A context deadline becomes the socket deadline for the statement: the
write of the request and the read of the reply both fail once it passes.
The driver reports it as `context.DeadlineExceeded`:

```go
ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
defer cancel()
err := db.QueryRowContext(ctx, "SELECT …").Scan(&v)
if errors.Is(err, context.DeadlineExceeded) { … }
```

On a streamed result the deadline covers the whole stream, since the
statement lasts until `rows.Close`.

## Cancellation

Cancelling the context while a statement is in flight unblocks the socket
at once; the call returns `context.Canceled`.

## What happens to the connection

An interrupted statement leaves the connection mid-frame — the reply may
be half read, or still on its way — so it cannot be reused. The driver
marks it broken; `database/sql` asks the driver whether a connection is
valid before every reuse, gets "no", closes it, and dials a fresh one (which
re-walks the seed list). You never see a poisoned connection, at the cost
of one dial per cancelled statement.

## What happens to the statement

Unknown. The server may have executed it (a write applied, a `CREATE
STREAM` created) and only the reply was abandoned. A cancelled write is
therefore like a lost reply: retry it only if it is idempotent.

Whether the server abandons work for a client that went away depends on
the server's configuration (its disconnect-cancellation setting is opt-in);
do not rely on cancelling a context to relieve a busy server of a long
scan. Bound the scan in the statement instead.

## Dial timeout

The TCP connect to each seed has a fixed 10 s timeout, and the seeds are
tried in turn. `database/sql` opens connections without a context: a
deadline on the first statement bounds how long *that call* waits, but a
dial already in progress continues in the background and, if it succeeds,
the connection joins the pool for the next caller.

## Streaming and `Close`

`rows.Close` on an abandoned stream may read from the socket to drain it
(bounded by 2 s of silence; see [streaming.md](streaming.md)). That read is
not covered by the statement's context — the drain's own timer bounds it —
so `Close` never blocks indefinitely even with a live context.

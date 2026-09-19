# skaidb Go driver

[![CI](https://github.com/porcupin26/skaidb-go/actions/workflows/ci.yml/badge.svg)](https://github.com/porcupin26/skaidb-go/actions/workflows/ci.yml)

A standard [`database/sql`](https://pkg.go.dev/database/sql) driver for
[skaidb](https://skaidb.org). Import it for its side effect and use the
stdlib API you already know: `sql.Open`, `db.Query`, `db.Exec`, `rows.Scan`.
Pure standard library, no third-party dependencies, Go 1.21+.

```go
import (
    "database/sql"
    _ "github.com/porcupin26/skaidb-go"
)

db, err := sql.Open("skaidb", "skaidb://skaidb:secret@localhost:7000/app?consistency=quorum")
```

> **Documentation lives in this repository.** The driver is licensed under
> the [SSPL-1.0](LICENSE), which pkg.go.dev does not recognise as
> redistributable, so it shows no API documentation for this module. Every
> public symbol is documented below and in [`docs/`](docs/), and the source
> carries the same doc comments (`go doc github.com/porcupin26/skaidb-go`
> works locally once the module is in your cache).

**Contents**

1. [Install](#install)
2. [Quick start](#quick-start)
3. [DSN reference](#dsn-reference) — seeds and failover, database, consistency, TLS modes
4. [Public API](#public-api) — every exported symbol
5. [Parameter binding and type mapping](#parameter-binding-and-type-mapping)
6. [Prepared statements](#prepared-statements)
7. [Batches and transactions](#batches-and-transactions)
8. [Streaming large results](#streaming-large-results) — and the abandon/drain rule
9. [Streams: `Subscribe`](#streams-subscribe)
10. [Context: deadlines and cancellation](#context-deadlines-and-cancellation)
11. [Connection pooling](#connection-pooling)
12. [Multiple result sets](#multiple-result-sets)
13. [Errors](#errors)
14. [Versioning and compatibility](#versioning-and-compatibility)
15. [Development](#development)

Further reading: [server documentation](https://skaidb.org/docs/) ·
[wire protocol](https://skaidb.org/docs/PROTOCOL.html) ·
[SQL syntax](https://skaidb.org/docs/query_syntax.html) ·
[`docs/`](docs/) in this repo.

## Install

```sh
go get github.com/porcupin26/skaidb-go@latest
```

### Moving from `skaidb.org/drivers/go`

This driver used to ship inside the skaidb server repository as
`skaidb.org/drivers/go`, versioned in lock-step with the server. That import
path is **frozen at v0.290.x** — it keeps resolving for existing builds but
receives no further releases. Switch your imports:

```diff
-	_ "skaidb.org/drivers/go"
+	_ "github.com/porcupin26/skaidb-go"
```

then `go get github.com/porcupin26/skaidb-go@latest && go mod tidy`. The
registered driver name (`"skaidb"`), the DSN and the API are unchanged; the
[changelog](CHANGELOG.md) lists the DSN fixes and the version reporting
change.

### Vendoring / offline

The module is a single package with no dependencies. `go mod vendor` works
as usual; or point a `replace` at a checkout:

```
require github.com/porcupin26/skaidb-go v1.0.1
replace github.com/porcupin26/skaidb-go => ../skaidb-go
```

## Quick start

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/porcupin26/skaidb-go"
)

func main() {
	db, err := sql.Open("skaidb", "skaidb://skaidb:secret@node1:7000,node2:7000,node3:7000/app")
	if err != nil {
		log.Fatal(err) // only a malformed DSN fails here; dialling is lazy
	}
	defer db.Close()

	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		log.Fatal(err) // first real connection: dial, TLS, SCRAM auth, USE app
	}

	// skaidb is schema-less: declare the primary key, columns appear on write.
	if _, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS users (PRIMARY KEY (id))"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO users (id, name, age) VALUES (?, ?, ?)", 1, "Ada", 36); err != nil {
		log.Fatal(err)
	}

	rows, err := db.QueryContext(ctx, "SELECT id, name, age FROM users WHERE age > ?", 30)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, age int
		var name string
		if err := rows.Scan(&id, &name, &age); err != nil {
			log.Fatal(err)
		}
		fmt.Println(id, name, age)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
```

Runnable programs live in [`examples/`](examples/): `basic`, `prepared`
(typed parameters, `RETURNING`, per-statement consistency, batch loop),
`streaming`, `subscribe`. Run one with
`go run ./examples/basic "skaidb://user:pass@host:7000/app"`.

## DSN reference

```
skaidb://[user[:password]@]host[:port][,host2[:port]...][/database][?option=value&...]
```

| Part | Meaning | Default |
|---|---|---|
| `user`, `password` | SCRAM-SHA-256 credentials. Percent-encode reserved characters (`@` → `%40`, `/` → `%2F`, `:` → `%3A`, `?` → `%3F`, `#` → `%23`). Omit both for a server with authentication disabled. | `anonymous`, empty |
| `host[:port]`, comma-separated | The **seed list**. Any mix of ports; IPv6 literals in brackets (`[::1]:7000`, `[fe80::1]`). | port `7000` |
| `/database` | Session database: `USE "<database>"` runs on every new pooled connection. | server default |
| `consistency` | `one`, `quorum` or `all` — the level for every statement on the connection unless overridden per statement with [`WithConsistency`](#withconsistency). | `quorum` |
| `tls` | `true`/`1` enables TLS with system-root certificate verification. Implied by `tls_ca` or `tls_insecure`. | `false` |
| `tls_ca` | Path to a PEM bundle; the server certificate must chain to it. Enables TLS. | — |
| `tls_insecure` | `true`/`1` encrypts but verifies **nothing**. Development against a self-signed node only. Enables TLS. | `false` |
| `tls_server_name` | SNI and the name checked against the certificate's SANs. skaidb's own certificates carry `DNS:skaidb`, so the default is right for them and is usually *not* the address you dialled. | `skaidb` |

Unknown options are ignored; a malformed value (`consistency=eventual`,
`tls=maybe`, a non-numeric port) makes `sql.Open` itself return the error —
the driver implements `driver.DriverContext`, so the DSN is parsed when the
`*sql.DB` is created, before anything is dialled.

### Seeds and failover

skaidb is leaderless: every node serves every statement, so a seed list is
just "somewhere to land", not a primary to discover. Each time
`database/sql` opens a connection the driver **shuffles the seeds** and
dials them in turn until one connects *and authenticates* (a node that
accepts TCP but is unhealthy does not swallow the attempt). Shuffling spreads
a pool across the cluster; the seed walk is also how failover works: when a
connection breaks (`IsValid` fails, see [pooling](#connection-pooling)) the
pool discards it and the replacement dial may land on another node. If no
seed answers, the error is
`skaidb: no reachable endpoint in [h1:7000 h2:7000]: <last error>`.

### TLS modes

| DSN | Encryption | Server identity checked against |
|---|---|---|
| (none) | plaintext | — |
| `?tls=true` | yes | the system root store, name `tls_server_name` |
| `?tls_ca=/etc/skaidb/ca.pem` | yes | that CA bundle, name `tls_server_name` |
| `?tls_insecure=true` | yes | nothing (MITM-able) |

A server configured with `client_tls = required` refuses plaintext, so one
of the three TLS forms is mandatory there. The handshake completes before
any protocol byte is written; a failure surfaces as
`skaidb: TLS handshake failed: …` from the connect, never mid-statement.

### Consistency

`one`, `quorum`, `all` select how many replicas must acknowledge a write or
agree on a read. The DSN level applies to the whole pool;
[`WithConsistency`](#withconsistency) overrides it for one statement. DDL
always runs at quorum server-side regardless of the level requested.

More: [docs/dsn.md](docs/dsn.md).

## Public API

The package registers itself as `"skaidb"` with `database/sql` in `init`.
Beyond the stdlib surface it exports five symbols.

### `Version`

```go
func Version() string
```

The driver's own version as the server sees it in the Hello frame (the
`client_version` column of the server's `drivers` table): the module version
this program was built against, without the `v` — `"1.0.0"` for `v1.0.0`, a
pseudo-version for an untagged commit — or `"devel"` when the driver is the
main module (its own tests) or the build carries no module metadata. It is
derived from `runtime/debug.ReadBuildInfo()` at start-up, so it can never
lag the tag.

### `WithStreaming`

```go
func WithStreaming(ctx context.Context) context.Context
```

Marks `ctx` so that `QueryContext` **streams** the result instead of
buffering it: the client holds one chunk at a time rather than the whole
set. Use it for exports and large scans — and read
[the rules](#streaming-large-results) first, because the rows own the
connection until closed.

```go
rows, err := db.QueryContext(skaidb.WithStreaming(ctx), "SELECT id, name FROM events")
if err != nil { return err }
defer rows.Close() // mandatory
```

Parameters are **not** supported on this path (the streaming opcode carries
SQL text only); a query with arguments takes the buffered path silently.
Falls back to a buffered query against a server too old for the opcode.

### `WithConsistency`

```go
func WithConsistency(ctx context.Context, level string) context.Context
```

Overrides the connection's consistency for the statements run with this
context. `level` is `"one"`, `"quorum"` or `"all"`, any case. An
unrecognised level makes the statement **fail** rather than silently run at
the connection default.

```go
var n int
err := db.QueryRowContext(skaidb.WithConsistency(ctx, "one"),
    "SELECT count(*) FROM users").Scan(&n)
```

### `Subscribe` and `Event`

```go
func Subscribe(ctx context.Context, db *sql.DB, stream, after string, fn func(Event) error) error

type Event struct {
    ID  string    // log position: keep the last one to resume
    Op  string    // "put" (matches now), "exit" (left the predicate) or "delete"
    Key string    // the row's primary key (JSON when composite)
    Ts  time.Time // when the change was captured (UTC)
    Doc string    // the row document, as JSON text
}
```

Delivers a stream's events (`CREATE STREAM <name> ON <table>`) to `fn` as
they arrive, blocking until `ctx` is cancelled (returns `ctx.Err()`) or `fn`
returns an error (returned as is). `after` is the `Event.ID` to resume from;
`""` starts at the beginning of the retained log. The `old` column of a
`pre_image = true` stream is not carried by `Event`; query `_stream_<name>`
directly for it. See [Streams](#streams-subscribe).

### `database/sql` interfaces implemented

For the curious, and for anyone using `driver` types directly:

| Interface | Notes |
|---|---|
| `driver.Driver`, `driver.DriverContext` | `OpenConnector(dsn)` parses the DSN at `sql.Open` (a malformed DSN fails there, nothing is dialled); `Open(dsn)` parses and dials, for code that holds the driver value directly |
| `driver.Connector` | one seed-list dial per pooled connection; `db.Driver()` returns the registered driver |
| `driver.Conn` | `Prepare`, `Close`; `Begin` returns an error (no transactions) |
| `driver.Validator` | retires connections broken mid-statement before the pool reuses them |
| `driver.NamedValueChecker` | accepts any Go value so slices and maps can reach the typed path |
| `driver.QueryerContext`, `driver.ExecerContext` | the main statement path |
| `driver.Stmt`, `driver.StmtQueryContext`, `driver.StmtExecContext` | `*sql.Stmt` goes through the same server-prepared, typed path as `db.Exec` with arguments |
| `driver.Rows`, `driver.RowsNextResultSet` | multi-set replies from procedures that `EMIT` |
| `driver.Result` | `RowsAffected` only; `LastInsertId` returns an error (use `RETURNING`) |

`driver.Pinger` is not implemented: `db.Ping` opens or validates a
connection, which exercises dial, TLS, authentication and `USE`, and that is
the useful check.

## Parameter binding and type mapping

Placeholders are `?`, positional, counted outside string literals (a `?`
inside `'…'` is text). The number of arguments must equal the number of
placeholders.

**How a statement with arguments is sent.** The driver first asks the server
to *prepare* the text and executes it with **typed** parameters (tag +
payload per value; see the [wire protocol §3.3](https://skaidb.org/docs/PROTOCOL.html)).
The prepared id is cached per pooled connection by statement text (up to
240 entries), so a loop over one statement prepares once per connection. If
the server declines to prepare the statement (DDL, session statements such
as `USE`), the driver falls back to **client-side binding**: each argument
is rendered as a safely quoted SQL literal and the text is sent as one
query. Statements without arguments are sent as text directly.

### Go → skaidb (arguments)

| Go value | Typed path (prepared) | Client-side fallback |
|---|---|---|
| `nil` | Null | `NULL` |
| `bool` | Bool | `TRUE`/`FALSE` |
| any integer type (`int`…`int64`, `uint`…`uint64` up to `MaxInt64`, named integer types) | Int | decimal literal |
| `float32`, `float64` | Float (NaN/±Inf refused) | `strconv 'g'` literal (NaN/±Inf refused) |
| `string` | String | `'…'` with `'` doubled |
| `[]byte`, named byte slices (`json.RawMessage`) | Bytes | hex string literal |
| `time.Time` | Timestamp (Unix milliseconds, UTC) | integer milliseconds |
| a pointer | Null when nil, else the pointee | same |
| a `driver.Valuer` (`sql.NullString`, `sql.NullInt64`, your own types) | whatever `Value()` returns, by these rules | same |
| any slice or array (except bytes) | Array (elements encoded recursively) | **error** |
| any map with string keys | Document (keys sorted) | **error** |
| anything else | error `skaidb: cannot bind value of type T` | same |

Scalars go through `database/sql`'s default converter first, which is what
makes the integer widths, pointers and `Valuer`s uniform; composites are
passed straight to the typed encoder. Arrays and documents have no SQL
literal form, so they can only travel on the typed path — which is the
normal path for every DML statement. A `[]byte` bound to a prepared
statement is a Bytes value; client-side it becomes a hex string, so prefer
prepared DML when the distinction matters.

### skaidb → Go (`rows.Scan` targets)

| skaidb | `driver.Value` delivered | Scan into |
|---|---|---|
| Null | `nil` | `sql.NullString` etc., or a pointer |
| Bool | `bool` | `*bool` |
| Int | `int64` | `*int`, `*int64`, `*uint`… |
| Float | `float64` | `*float64` |
| Decimal | `string` (exact, e.g. `"12.34"`) | `*string`, then parse |
| String | `string` | `*string` |
| Bytes | `[]byte` (a fresh copy) | `*[]byte` |
| Uuid | `string` (canonical `8-4-4-4-12`) | `*string` |
| Timestamp | `time.Time` (UTC, millisecond precision) | `*time.Time` |
| Array | JSON `string` | `*string`, then `json.Unmarshal` |
| Document | JSON `string` | `*string`, then `json.Unmarshal` |

`database/sql`'s usual conversions apply on top (an `int64` scans into
`*string`, a `string` into `*int` when it parses, and so on). Document key
order is not preserved in the JSON text (it passes through a Go map).

More: [docs/types.md](docs/types.md).

## Prepared statements

`db.Prepare` / `db.PrepareContext` returns a `*sql.Stmt` that runs through
the same server-prepared, typed path as `db.Exec(query, args...)`:

```go
ins, err := db.PrepareContext(ctx, "INSERT INTO orders (id, tags, meta) VALUES (?, ?, ?)")
if err != nil { return err }
defer ins.Close()
for _, o := range orders {
    if _, err := ins.ExecContext(ctx, o.ID, o.Tags, map[string]any{"total": o.Total}); err != nil {
        return err
    }
}
```

Because a prepared id only means something on the connection that created
it, `database/sql` re-prepares transparently when a `*sql.Stmt` runs on a
different pooled connection; the per-connection cache makes that a one-off
per connection. There is no functional difference between a `*sql.Stmt` and
calling `db.Exec` with the same text and arguments repeatedly — pick
whichever reads better.

The parameter count is checked against the server's view of the statement:
`skaidb: statement expects 3 parameters, got 2`.

More: [docs/prepared-statements.md](docs/prepared-statements.md).

## Batches and transactions

skaidb is non-transactional: every statement auto-commits, and `db.Begin()`
/ `db.BeginTx()` return `skaidb: transactions are not supported`. Do not
write code that expects rollback.

A **batch** is therefore a loop over one prepared statement on one
connection. To keep it on one connection (one prepare, one socket, in-order
delivery) take a `*sql.Conn`:

```go
conn, err := db.Conn(ctx)
if err != nil { return err }
defer conn.Close()
stmt, err := conn.PrepareContext(ctx, "INSERT INTO t (id, v) VALUES (?, ?)")
if err != nil { return err }
defer stmt.Close()
for i, v := range values {
    if _, err := stmt.ExecContext(ctx, i, v); err != nil { return err }
}
```

For throughput, run several such loops on several connections
(`db.SetMaxOpenConns`) — the server is leaderless and every node accepts
writes. Multi-row `INSERT … VALUES (…), (…)` is a single statement and works
as one prepared statement with N×k parameters. A generated id comes back via
`RETURNING`, not `LastInsertId`:

```go
var id int64
err := db.QueryRowContext(ctx, "INSERT INTO t (name) VALUES (?) RETURNING id", name).Scan(&id)
```

Atomicity is per statement: an upsert with `INSERT … ON CONFLICT DO
NOTHING | DO UPDATE SET …` and a single-row `UPDATE` are atomic on the row
(see the [SQL syntax reference](https://skaidb.org/docs/query_syntax.html));
there is no client-side transaction spanning statements.

More: [docs/batch.md](docs/batch.md).

## Streaming large results

A plain `SELECT` is materialised on the coordinator and bounded by the
server's scan budgets (rows examined and bytes), which **error** rather than
truncate. To read a big table, stream it: `WithStreaming(ctx)` sends the
streaming opcode and the rows arrive as chunks that `rows.Next` pulls on
demand, so the client holds one chunk at a time and the server pages a
simple scan in per-page statements, each with a fresh budget.

```go
rows, err := db.QueryContext(skaidb.WithStreaming(ctx), "SELECT id, payload FROM events")
if err != nil { return err }
defer rows.Close() // ← mandatory, see below
for rows.Next() {
    var id int64
    var payload string
    if err := rows.Scan(&id, &payload); err != nil { return err }
    // …
}
return rows.Err()
```

Rules of the streaming path:

- **No parameters.** The streaming opcode carries SQL text; a call with
  arguments silently takes the buffered path. Interpolate values yourself
  only from trusted input, or filter server-side by other means.
- **The rows own the connection** until `Close`. `database/sql` pins the
  `*sql.Conn` for the rows' lifetime, and if two goroutines share one
  `*sql.Conn` the second statement fails with `skaidb: connection is busy
  streaming a result set — close those rows before running another
  statement` rather than interleaving on the socket.
- **The context must outlive the iteration.** Its deadline covers the whole
  stream; cancelling it interrupts the stream and retires the connection.
- **Server-side paging applies to simple scans.** On a cluster the server's
  keyset lane pages a bare `SELECT cols FROM t [WHERE simple predicate]` on
  its primary key; `LIMIT`/`OFFSET`, `DISTINCT`, `JOIN`, `GROUP BY`, set
  operations and predicates with functions or subqueries make it fall back
  to a single materialised statement (still delivered in chunks, but subject
  to the budgets). Name your columns: a lone `SELECT *` on a cluster needs
  `WithConsistency(ctx, "one")` for the paged lane, because the wildcard's
  column discovery reads the local shard.
- **Procedures that `EMIT`** answer in one ordinary multi-set frame even on
  the streaming opcode; they work, but are not streamed.

### The abandon/drain rule — always `defer rows.Close()`

If you stop reading a stream before its end — `break`, an early `return`, a
`Scan` error — the unread chunk frames are still queued on the socket. The
[wire protocol §3.4](https://skaidb.org/docs/PROTOCOL.html) requires the
client to either **drain** them or **stop using the connection**; otherwise
the next statement on that pooled connection would decode a leftover chunk
as its own reply (`skaidb: unknown response tag 6`, if it fails at all).

`rows.Close` is where the driver does this: it drains the remainder when that
is cheap (up to 64 frames / 8 MiB, waiting at most 2 s for a quiet peer) and
otherwise marks the connection broken so the pool closes it instead of
reusing it. Both outcomes are correct and neither is reported as an error.

The catch: **`database/sql` calls `Rows.Close` for you only when the
iteration runs to exhaustion or the context is cancelled — not on a bare
`break`, and there is no finalizer.** A streamed `*sql.Rows` you drop
without closing keeps its connection checked out, mid-stream, until the
process exits. `defer rows.Close()` is not a nicety on this path; it is the
contract.

More: [docs/streaming.md](docs/streaming.md).

## Streams: `Subscribe`

A skaidb stream captures every change to a table into a log table
`_stream_<name>` in the same database:

```sql
CREATE STREAM big_orders ON orders WHEN (total > 1000) WITH (retention = '24h');
```

`Subscribe` is a dependency-free follower of that log. It pages with a
keyset cursor (`WHERE id > ? ORDER BY id LIMIT 500`), calls `fn` for each
event in order, and polls every 500 ms when caught up:

```go
err := skaidb.Subscribe(ctx, db, "big_orders", lastSeenID, func(ev skaidb.Event) error {
    if ev.Op != "put" { // "exit" (no longer matches) or "delete"
        return forget(ev.Key)
    }
    var order Order
    if err := json.Unmarshal([]byte(ev.Doc), &order); err != nil { return err }
    if err := index(order); err != nil { return err } // returning an error stops Subscribe
    lastSeenID = ev.ID // persist this to resume after a restart
    return nil
})
```

Delivery is at-least-once from the caller's point of view: an event whose
`fn` failed (or one delivered just before a crash, if `lastSeenID` was not
persisted) is delivered again on resume. Make `fn` idempotent or persist the
id in the same step as the effect.

For push delivery instead of polling, subscribe to the MQTT topic
`$stream/<db>/<name>` on the server's broker with any MQTT client; the
events are identical. See the
[streams documentation](https://skaidb.org/docs/streams.html).

More: [docs/streams.md](docs/streams.md).

## Context: deadlines and cancellation

Every `…Context` method honours its context:

- A **deadline** becomes the socket deadline for the statement. On a
  streamed result it covers the whole stream.
- **Cancellation** forces a blocked read or write to return at once. The
  statement is interrupted mid-frame, so the connection is marked broken and
  discarded by the pool; the *statement's* fate on the server is unknown (a
  write may or may not have applied).
- The returned error satisfies `errors.Is(err, context.DeadlineExceeded)` /
  `context.Canceled`, as with any other driver.

```go
ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
defer cancel()
if err := db.QueryRowContext(ctx, "SELECT …").Scan(&v); errors.Is(err, context.DeadlineExceeded) {
    // the server did not answer in time; the pool has already dropped the socket
}
```

There is no per-connection timeout setting: `sql.Open` is lazy, the dial has
a fixed 10 s TCP connect timeout, and everything else is context-driven.

More: [docs/context.md](docs/context.md).

## Connection pooling

`*sql.DB` is the pool and is safe for concurrent use; the standard knobs
apply (`SetMaxOpenConns`, `SetMaxIdleConns`, `SetConnMaxLifetime`,
`SetConnMaxIdleTime`). What is per-connection in this driver:

- the session database (`USE` from the DSN path, run on every dial);
- the prepared-statement cache (by text, up to 240 entries);
- the default consistency (from the DSN; `WithConsistency` is per
  statement).

Each pooled connection is a single socket that serialises one request /
one response; concurrency comes from the pool, not from pipelining. A
connection broken by a transport error, a cancelled statement or an
undrainable abandoned stream fails `IsValid` and is closed by the pool
before the next checkout; the replacement dial re-walks the seed list. When
the request had **not** reached the wire, the driver returns
`driver.ErrBadConn` and `database/sql` retries the statement on a fresh
connection transparently; when it had, the error is surfaced (see
[Errors](#errors)) because a retry could double-apply a write.

## Multiple result sets

A procedure whose body `EMIT`s answers with several result sets. They are
served through the stdlib API:

```go
rows, err := db.QueryContext(ctx, "CALL report(?)", 2026)
if err != nil { return err }
defer rows.Close()
for {
    cols, _ := rows.Columns()
    for rows.Next() { /* scan cols */ }
    if err := rows.Err(); err != nil { return err }
    if !rows.NextResultSet() { break }
}
```

A statement that produces no rows (DDL, a mutation) run through `Query`
returns an empty result set; a row-producing statement run through `Exec`
returns `RowsAffected() == 0`.

## Errors

All driver errors are `*fmt` errors whose text starts with `skaidb:`; there
is no error type hierarchy to switch on. The messages are stable enough to
match on, and the context errors and `driver.ErrBadConn` are the standard
sentinels.

| Situation | Error |
|---|---|
| malformed DSN | `skaidb: bad DSN: …`, `skaidb: DSN scheme must be skaidb://`, `skaidb: DSN has no host`, `skaidb: bad seed "…"`, `skaidb: bad consistency "…"`, `skaidb: bad tls …` — from `sql.Open` |
| no seed reachable | `skaidb: no reachable endpoint in [seeds]: <last error>` |
| TLS | `skaidb: TLS handshake failed: …`, `skaidb: cannot read tls_ca "…"`, `skaidb: no certificates found in tls_ca "…"` |
| authentication | `skaidb: authentication denied: <reason>`, `skaidb: server signature mismatch (mutual auth failed)` |
| the server rejected the statement (syntax, missing table, constraint, permission, budget…) | `skaidb: <server message>` |
| parameter problems | `skaidb: statement expects N parameters, got M`, `skaidb: more placeholders than parameters`, `skaidb: more parameters than placeholders`, `skaidb: cannot bind value of type T`, `skaidb: cannot bind NaN/Infinity`, `skaidb: document keys must be strings` |
| bad consistency in `WithConsistency` | `skaidb: bad consistency "…"` |
| transactions | `skaidb: transactions are not supported` |
| `LastInsertId` | `skaidb: no LastInsertId — use INSERT … RETURNING id with QueryRow` |
| second statement during a stream on the same `*sql.Conn` | `skaidb: connection is busy streaming a result set — close those rows before running another statement` |
| connection lost before the request was written | `driver.ErrBadConn` (retried by `database/sql` on another connection; never visible to you unless every retry fails) |
| connection lost after the request was written | `skaidb: connection lost mid-statement (statement may or may not have applied): <io error>` — the write's fate is unknown; the driver does not retry |
| deadline / cancellation | `context.DeadlineExceeded` / `context.Canceled` (via `errors.Is`) |
| corrupt or unexpected frame | `skaidb: truncated server message`, `skaidb: unknown response tag N`, `skaidb: unexpected frame in stream` — the connection is retired |

Errors ending a **stream** mid-way arrive from `rows.Next()` → `rows.Err()`;
the rows already delivered stay valid.

More: [docs/errors.md](docs/errors.md).

## Versioning and compatibility

- **Driver:** semantic versioning, Go module style (`v1.x.y`). The 1.x line
  keeps the exported API and the DSN grammar backward compatible. Go 1.21 or
  newer; CI runs 1.21, 1.22, 1.23 and the latest stable.
- **Server:** the driver speaks the
  [skaidb wire protocol](https://skaidb.org/docs/PROTOCOL.html) on port
  7000. It negotiates nothing at connect time; instead each optional feature
  degrades gracefully on an older server: prepared statements need server
  ≥ 0.17.0 (older servers get client-side binding), streaming needs the
  streaming opcode (older servers answer a buffered result), the Hello
  self-identification (server ≥ 0.203.0) is best-effort and ignored where
  unknown. Multiple result sets and `RETURNING` are server features the
  driver merely passes through. Developed against skaidb 0.290.
- The version the driver reports in Hello is [`Version()`](#version), always
  equal to the module version in your `go.mod`.

## Development

```sh
go vet ./... && go test -race ./...
```

The tests need no server: they drive the connection against scripted
loopback peers. `examples/` builds with `go build ./...` and runs against a
live server. Pull requests run the same on the CI matrix.

License: [SSPL-1.0](LICENSE).

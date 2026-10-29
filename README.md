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
3. [DSN reference](#dsn-reference) — seeds and failover, database, consistency, TLS modes, certificate login
4. [Public API](#public-api) — every exported symbol
5. [Parameter binding and type mapping](#parameter-binding-and-type-mapping)
6. [Prepared statements](#prepared-statements)
7. [Batches and transactions](#batches-and-transactions) — `ExecBatch`: many rows, one round trip
8. [Streaming large results](#streaming-large-results) — and the abandon/drain rule
9. [Streams: `Subscribe`](#streams-subscribe)
10. [Context: deadlines and cancellation](#context-deadlines-and-cancellation)
11. [Connection pooling](#connection-pooling) — balancing across members, health ejection, discovery
12. [Multiple result sets](#multiple-result-sets)
13. [Errors](#errors)
14. [Versioning and compatibility](#versioning-and-compatibility)
15. [Conformance](#conformance)
16. [Development](#development)
17. [Releasing](#releasing)

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
require github.com/porcupin26/skaidb-go v1.3.0
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
(typed parameters, `RETURNING`, per-statement consistency, a statement loop, `ExecBatch`),
`streaming`, `subscribe`. Run one with
`go run ./examples/basic "skaidb://user:pass@host:7000/app"`.

## DSN reference

```
skaidb://[user[:password]@]host[:port][,host2[:port]...][/database][?option=value&...]
```

| Part | Meaning | Default |
|---|---|---|
| `user`, `password` | SCRAM-SHA-256 credentials. Percent-encode reserved characters (`@` → `%40`, `/` → `%2F`, `:` → `%3A`, `?` → `%3F`, `#` → `%23`). Omit both for a server with authentication disabled. With `auth_mechanism=certificate` the user is optional (it must equal the certificate's Common Name) and there is no password. | `anonymous`, empty |
| `host[:port]`, comma-separated | The **seed list**. Any mix of ports; IPv6 literals in brackets (`[::1]:7000`, `[fe80::1]`). | port `7000` |
| `/database` | Session database: `USE "<database>"` runs on every new pooled connection. | server default |
| `consistency` | `one`, `quorum` or `all` — the level for every statement on the connection unless overridden per statement with [`WithConsistency`](#withconsistency). | `quorum` |
| `tls` | `true`/`1` enables TLS with system-root certificate verification. Implied by `tls_ca`, `tls_insecure` or `tls_client_cert`. | `false` |
| `tls_ca` | Path to a PEM bundle; the server certificate must chain to it. Enables TLS. | — |
| `tls_insecure` | `true`/`1` encrypts but verifies **nothing**. Development against a self-signed node only. Enables TLS. | `false` |
| `tls_server_name` | SNI and the name checked against the certificate's SANs. skaidb's own certificates carry `DNS:skaidb`, so the default is right for them and is usually *not* the address you dialled. | `skaidb` |
| `tls_client_cert`, `tls_client_key` | Paths to a PEM client certificate and its private key, presented in the TLS handshake. Always given together. Enables TLS. | — |
| `auth_mechanism` | `scram` (password, SCRAM-SHA-256) or `certificate` (the TLS client certificate is the credential; needs `tls_client_cert`/`tls_client_key`). See [certificate login](#certificate-login). | `scram` |
| `load_balancing` | The member each new pooled connection goes to: `random`, `round_robin` or `least_latency`. See [balancing](#balancing-across-members). | `random` |
| `discover` | `true`/`1` reads the members from `SHOW ENDPOINTS` (server ≥ 0.327): joiners added, leavers dropped, `resyncing`/`unreachable` members skipped. | `false` |
| `refresh_interval` | How often discovery re-reads the members (a Go duration: `30s`, `2m`); every 5 s instead while one is not ready. | `60s` |
| `eject_errors` | Consecutive transport failures that eject a member. | `3` |
| `eject_latency_factor`, `eject_latency_floor` | A member whose round-trip average is above factor × the other members' median **and** more than floor above it is ejected. Factor `0` disables. | `3`, `50ms` |
| `eject_backoff`, `eject_backoff_max` | How long an ejection lasts, doubling per repeat up to the max. | `5s`, `60s` |
| `idle_order` | Only `lifo`, which is what `database/sql` does; `fifo` is refused (see [balancing](#balancing-across-members)). | `lifo` |
| `read_your_writes` | Only `0`; a window is refused — use a `*sql.Conn` (see [balancing](#balancing-across-members)). | `0` |

Unknown options are ignored; a malformed value (`consistency=eventual`,
`tls=maybe`, a non-numeric port) makes `sql.Open` itself return the error —
the driver implements `driver.DriverContext`, so the DSN is parsed when the
`*sql.DB` is created, before anything is dialled.

### Seeds and failover

skaidb is leaderless: every node serves every statement, so a seed list is
just "somewhere to land", not a primary to discover. Each time
`database/sql` opens a connection the driver dials the member the
[balancing policy](#balancing-across-members) picks — by default the seeds
**in random order** — and walks on to the others until one connects *and
authenticates* (a node that accepts TCP but is unhealthy does not swallow
the attempt). That spreads a pool across the cluster, and the walk is also
how failover works: when a connection breaks (`IsValid` fails, see
[pooling](#connection-pooling)) the pool discards it and the replacement
dial may land on another node; a node that keeps failing is ejected and
tried last. If no member answers, the error is
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

### Certificate login

With `auth_mechanism=certificate` the connection authenticates with its TLS
client certificate instead of a password (the protocol's EXTERNAL
mechanism). The server must have `auth.x509_enabled` and trust the
certificate's CA (`auth.x509_ca_file`); the certificate's subject **Common
Name is the username**.

```go
db, err := sql.Open("skaidb", "skaidb://db1,db2,db3/app"+
	"?auth_mechanism=certificate"+
	"&tls_ca=/etc/skaidb/ca.pem"+
	"&tls_client_cert=/etc/skaidb/ada.crt&tls_client_key=/etc/skaidb/ada.key")
```

A user in the DSN (`skaidb://ada@…`) is optional and must equal the CN; a
password is ignored. The server's 32-byte signature in the outcome is not
checked on this path — TLS already authenticated the server, so pair it with
`tls_ca` (or the system roots), not `tls_insecure`. A client certificate
without `auth_mechanism=certificate` is simply presented in the handshake
and the login stays SCRAM. A denied certificate fails the connect with
`skaidb: authentication denied: <reason>`.

### Consistency

`one`, `quorum`, `all` select how many replicas must acknowledge a write or
agree on a read. The DSN level applies to the whole pool;
[`WithConsistency`](#withconsistency) overrides it for one statement. DDL
always runs at quorum server-side regardless of the level requested.

More: [docs/dsn.md](docs/dsn.md).

## Public API

The package registers itself as `"skaidb"` with `database/sql` in `init`.
Beyond the stdlib surface it exports `Version`, `WithStreaming`,
`WithConsistency`, `Subscribe`/`Event`, the batch API (`ExecBatch`,
`ExecBatchConn`, `BatchExecer`) and the balancing connector
(`NewConnector`, `Connector`, `Option` and its constructors, `Policy`,
`MemberInfo`).

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

Arguments bind `?` placeholders exactly as on any other query — typed,
arrays and documents included — and travel in the streamed request itself
(`OP_QUERY_STREAM_PARAMS`, server ≥ 0.330):

```go
rows, err := db.QueryContext(skaidb.WithStreaming(ctx),
    "SELECT id, ts, body FROM events WHERE ts >= ? AND ts < ?", from, to)
```

Without arguments the request is the original streaming opcode, which every
server knows. Against a server too old for the request a call needs (the
streaming opcode, or the parameterised form before 0.330) the call falls
back to a buffered query: it still works, but is not streamed.

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

### `ExecBatch`, `ExecBatchConn` and `BatchExecer`

```go
func ExecBatch(ctx context.Context, db *sql.DB, query string, rows [][]any) (int64, error)
func ExecBatchConn(ctx context.Context, c *sql.Conn, query string, rows [][]any) (int64, error)

type BatchExecer interface {
	ExecBatch(ctx context.Context, query string, rows [][]any) (int64, error)
}
```

Run one statement over many parameter rows in **one round trip** and get the
total affected-row count; see [batches](#batches-and-transactions).
`BatchExecer` is implemented by the driver's `driver.Conn`, reachable through
`(*sql.Conn).Raw`; the two functions wrap that.

### `NewConnector`, `Connector` and `Option`

```go
func NewConnector(dsn string, opts ...Option) (*Connector, error)

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error)
func (c *Connector) Driver() driver.Driver
func (c *Connector) Members() []MemberInfo
func (c *Connector) Close() error

type Option func(*balance) // opaque: built by the functions below
func LoadBalancing(p Policy) Option
func Discover(on bool) Option
func RefreshInterval(d time.Duration) Option
func EjectErrors(n int) Option
func EjectLatencyFactor(f float64) Option
func EjectLatencyFloor(d time.Duration) Option
func EjectBackoff(d time.Duration) Option
func EjectBackoffMax(d time.Duration) Option

type Policy string
const (
	Random       Policy = "random"
	RoundRobin   Policy = "round_robin"
	LeastLatency Policy = "least_latency"
)

type MemberInfo struct {
	Endpoint string        // what the connector dials, host:port
	Name     string        // the member's own endpoint from SHOW ENDPOINTS; "" until discovered
	State    string        // ready, ejected, resyncing, unreachable or unlisted
	Latency  time.Duration // round-trip average; 0 until measured
	Open     int           // connections open to it
	Seed     bool          // configured in the DSN
}
```

`*Connector` is the `driver.Connector` behind every `*sql.DB` of this driver:
`sql.Open` builds one from the DSN, `NewConnector` builds one with options
applied on top of the DSN (they override the query parameters of the same
name) and keeps it in your hands for `Members()`. Hand it to `sql.OpenDB`;
`db.Close()` closes it, which stops its background goroutine. Use one
connector per `*sql.DB`. An invalid option fails `NewConnector` with the
same error the DSN form fails `sql.Open` with. See
[balancing](#balancing-across-members).

### `database/sql` interfaces implemented

For the curious, and for anyone using `driver` types directly:

| Interface | Notes |
|---|---|
| `driver.Driver`, `driver.DriverContext` | `OpenConnector(dsn)` parses the DSN at `sql.Open` (a malformed DSN fails there, nothing is dialled); `Open(dsn)` parses and dials, for code that holds the driver value directly |
| `driver.Connector`, `io.Closer` | `*Connector`: one dial per pooled connection, to the member the balancing policy picks; `db.Driver()` returns the registered driver; `db.Close()` closes it |
| `driver.Conn` | `Prepare`, `Close`; `Begin` returns an error (no transactions). Also implements this package's `BatchExecer` (via `(*sql.Conn).Raw`) |
| `driver.Validator` | retires connections broken mid-statement, and connections to a member that left the rotation, before the pool reuses them |
| `driver.SessionResetter` | turns down an idle connection whose member left the rotation while it sat in the pool, so `database/sql` closes it and uses another |
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

A **batch** runs one statement over many parameter rows. `database/sql`
has no batch call — `ExecContext` per row costs one network round trip each
— so the driver adds one. `ExecBatch` sends every row in a single
`OP_EXECUTE_BATCH` request:

```go
rows := make([][]any, 0, len(events))
for _, e := range events {
    rows = append(rows, []any{e.ID, e.At, e.Payload})
}
n, err := skaidb.ExecBatch(ctx, db, "INSERT INTO events (id, ts, payload) VALUES (?, ?, ?)", rows)
// n is the total affected-row count
```

On a connection you already hold (`db.Conn`), use `skaidb.ExecBatchConn(ctx,
conn, query, rows)`, or the `database/sql` escape hatch it wraps:

```go
err := conn.Raw(func(driverConn any) error {
    bc := driverConn.(skaidb.BatchExecer)
    n, err := bc.ExecBatch(ctx, "INSERT INTO t (id, v) VALUES (?, ?)", rows)
    _ = n
    return err
})
```

- Rows bind exactly like `ExecContext` arguments (scalars, `time.Time`,
  `[]byte`, `sql.Null*` and other `driver.Valuer`s, pointers, slices and
  maps as arrays and documents). Every row must have the statement's
  parameter count; a mismatch fails before anything is sent.
- The context's deadline, cancellation and [`WithConsistency`](#withconsistency)
  apply to the whole batch.
- A request is at most one wire frame (64 MiB). A larger batch is split
  into consecutive chunks, one round trip each; a single row over the limit
  is an error.
- Rows run in order, each auto-committed. The first failing row stops the
  batch with the server's error, which names the row; the rows before it
  stay applied. In a multi-chunk batch the returned count covers the chunks
  that applied, and the error says from which batch row the server's row
  numbers count.
- A statement the server will not prepare (DDL, `CALL`), or a server
  without the batch opcode, falls back to one statement per row.

The per-row loop still works, and is the shape when each row needs its own
result. To keep it on one connection (one prepare, one socket, in-order
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
demand, so the client holds one chunk at a time. The server executes a
plain `SELECT` page by page, and no scan budget applies to a streamed page
(server ≥ 0.330).

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

- **Arguments bind as on any query.** `?` placeholders take typed
  arguments — arrays and documents included — carried by the streaming
  request itself (`OP_QUERY_STREAM_PARAMS`, server ≥ 0.330), so a bounded
  export needs no values written into its SQL:
  `db.QueryContext(skaidb.WithStreaming(ctx), "SELECT id, ts FROM events WHERE ts >= ?", since)`.
  An older server answers a call with arguments with a buffered result.
- **The rows own the connection** until `Close`. `database/sql` pins the
  `*sql.Conn` for the rows' lifetime, and if two goroutines share one
  `*sql.Conn` the second statement fails with `skaidb: connection is busy
  streaming a result set — close those rows before running another
  statement` rather than interleaving on the socket.
- **The context must outlive the iteration.** Its deadline covers the whole
  stream; cancelling it interrupts the stream and retires the connection.
- **Server-side paging applies to plain selects.** A `SELECT` of bare
  columns (or a lone `*`) from one table, filtered by any function of the
  row, unordered or in storage-key order, is paged, and a bound argument
  pages like the literal it stands for. Joins, `GROUP BY` and aggregates,
  `DISTINCT`, set operations, `LIMIT`/`OFFSET`, other orderings, searches
  and subqueries are materialised first — still delivered in chunks, but
  subject to the budgets, and a budget error says why the statement could
  not page. `EXPLAIN` reports which path a statement takes; the server's
  rules are in [QUERY_SYNTAX, "Streamed queries"](https://skaidb.org/docs/query_syntax.html).
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
before the next checkout; the replacement dial goes through the
[balancing](#balancing-across-members) connector. When
the request had **not** reached the wire, the driver returns
`driver.ErrBadConn` and `database/sql` retries the statement on a fresh
connection transparently; when it had, the error is surfaced (see
[Errors](#errors)) because a retry could double-apply a write.

### Balancing across members

The connector spreads the pool's connections across the cluster's members by
the rules every skaidb driver shares
([PROTOCOL.md §6.1](https://skaidb.org/docs/PROTOCOL.html#61-balancing-a-pool-across-members)).
`database/sql` owns the pool — it keeps the idle connections and picks which
one serves a statement — so the balancing acts where a driver can: on the
member **each new connection** is dialled to, and on which connections may
stay in the pool.

```go
c, err := skaidb.NewConnector("skaidb://app:pw@db1,db2,db3/app",
	skaidb.LoadBalancing(skaidb.LeastLatency), // or skaidb.RoundRobin
	skaidb.Discover(true),                     // learn the members from the cluster
)
if err != nil {
	return err
}
db := sql.OpenDB(c)
defer db.Close()
db.SetConnMaxLifetime(10 * time.Minute) // keep re-dialling, so the spread follows the cluster
```

The same through the DSN alone:
`sql.Open("skaidb", "skaidb://app:pw@db1,db2,db3/app?load_balancing=least_latency&discover=true")`.

| Option (DSN / Go) | Default | What it does |
|---|---|---|
| `load_balancing` / `LoadBalancing` | `random` | `random`: a new connection dials the usable members in random order and stays on the one it reached. `round_robin`: new connections go to the usable members in turn, in endpoint order. `least_latency`: of two usable members picked at random, the one with the lower round-trip average × (open connections + 1); a member not measured in the last 2 s scores 0, so it is tried and a quiet pool keeps re-measuring every member. |
| `discover` / `Discover` | off | Reads the members with `SHOW ENDPOINTS` (server ≥ 0.327; no privilege needed) on the first connection to each member not yet named, and every `refresh_interval` after (every 5 s while a member is `resyncing` or `unreachable`). New members are added, members that left are dropped (a seed is kept, marked `unlisted`), and only `ready` members get new connections; a member back to `ready` starts with a fresh average. A seed given as a host name is matched to the endpoint the server reports. A standalone server (no rows) or an older one (an error) leaves the seeds as they are, and the connector stops asking. |
| `refresh_interval` / `RefreshInterval` | 60 s | How often discovery re-reads the members. |
| `eject_errors` / `EjectErrors` | 3 | Consecutive transport failures that eject a member: a failed dial, an I/O error or a deadline mid-statement, a capacity refusal. Statement errors, refused logins, a cancelled context and this side's problems (an unreadable TLS file, a session database the server refuses) do not count. |
| `eject_latency_factor`, `eject_latency_floor` / `EjectLatencyFactor`, `EjectLatencyFloor` | 3, 50 ms | With at least 10 round trips on it and on another usable member, a member whose average is above factor × the other usable members' median **and** more than floor above it is ejected. Factor 0 disables. |
| `eject_backoff`, `eject_backoff_max` / `EjectBackoff`, `EjectBackoffMax` | 5 s, 60 s | How long an ejection lasts, doubled per repeat up to the max; a member healthy for the max after a re-admission starts over. When it is up, a background goroutine dials the member alone and runs one statement: success re-admits it with a fresh average, failure ejects it again. **The last usable member is never ejected.** |

The round-trip average is per member: each statement's time from sending
the request to the first response frame moves it 5 % of the way, and no
sample counts for more than 10 × the larger of the average and the floor —
one slow statement barely moves it, a member that stays slow is ejected
within a few dozen statements.

**What happens to existing connections.** A connection whose member is
ejected, reported not ready, or dropped from the listing fails `IsValid`
when it is returned and is turned down by `ResetSession` when the pool
offers it again, so `database/sql` closes it and dials a replacement through
the connector. A connection checked out at that moment (a `*sql.Conn`, an
open `Rows`) keeps working until it is returned. When no member is usable
at all, connections are kept: closing them would only re-dial the same
members.

**Which connection serves a statement is `database/sql`'s choice.** It
hands out the most recently returned idle connection first; there is no
`fifo` idle order (the DSN refuses `idle_order=fifo`). To keep every member
in use and keep a server-side idle timeout from finding a connection, let
connections rotate: `SetConnMaxLifetime` re-dials each connection through
the policy after a while, `SetConnMaxIdleTime` closes the ones a quiet pool
does not need.

**Read-your-writes.** The member that coordinated a write reads it back at
any consistency, `ONE` included; another member may not have applied it yet.
`database/sql` gives no say over which pooled connection the next statement
gets, so the driver has no `read_your_writes` window (the DSN refuses one).
Run the write and the reads that must see it on one `*sql.Conn` — it stays
on the member that took the write:

```go
conn, err := db.Conn(ctx)
if err != nil {
	return err
}
defer conn.Close()
if _, err := conn.ExecContext(ctx, "INSERT INTO notes (id, body) VALUES (?, ?)", id, body); err != nil {
	return err
}
// Same connection, same member: sees the insert even at consistency ONE.
err = conn.QueryRowContext(skaidb.WithConsistency(ctx, "one"),
	"SELECT body FROM notes WHERE id = ?", id).Scan(&body)
```

**Watching it.** `c.Members()` reports each member as the connector sees it:

```go
for _, m := range c.Members() {
	fmt.Printf("%s %-11s %v open=%d seed=%v name=%s\n", m.Endpoint, m.State, m.Latency, m.Open, m.Seed, m.Name)
}
```

`State` is `ready`, `ejected`, `resyncing`, `unreachable` or `unlisted`.
`Open` counts the member's connections, idle or in use (`db.Stats()` has the
pool-wide split). Keep the `*Connector` from `NewConnector` to call it;
`database/sql` does not hand back the connector of a `*sql.DB` opened with
`sql.Open`.

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
| TLS | `skaidb: TLS handshake failed: …`, `skaidb: cannot read tls_ca "…"`, `skaidb: no certificates found in tls_ca "…"`, `skaidb: cannot load tls_client_cert/tls_client_key: …` |
| certificate options | `skaidb: tls_client_cert and tls_client_key go together`, `skaidb: auth_mechanism=certificate needs tls_client_cert and tls_client_key`, `skaidb: bad auth_mechanism "…"` — from `sql.Open` |
| balancing options | `skaidb: bad load_balancing "…"`, `skaidb: bad discover "…"`, `skaidb: bad refresh_interval "…"` (and the other durations), `skaidb: eject_errors must be at least 1, got N`, `skaidb: idle_order=fifo is not available: …`, `skaidb: read_your_writes is not available: …` — from `sql.Open` or `NewConnector` |
| `Connect` after the connector was closed | `skaidb: connector is closed` |
| authentication | `skaidb: authentication denied: <reason>`, `skaidb: server signature mismatch (mutual auth failed)` |
| the server rejected the statement (syntax, missing table, constraint, permission, budget…) | `skaidb: <server message>` |
| batches | `skaidb: batch row N: statement expects K parameters, got M`, `skaidb: batch row N is B bytes, over the L-byte frame limit`, `skaidb: <server message> (row numbers count from batch row N; the N rows before it applied)` |
| parameter problems | `skaidb: statement expects N parameters, got M`, `skaidb: more placeholders than parameters`, `skaidb: more parameters than placeholders`, `skaidb: cannot bind value of type T`, `skaidb: cannot bind NaN/Infinity`, `skaidb: document keys must be strings`, `skaidb: N parameters, over the protocol's 65535` |
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
  streaming opcode and streaming with arguments server ≥ 0.330 (older
  servers answer a buffered result), the Hello
  self-identification (server ≥ 0.203.0) is best-effort and ignored where
  unknown, `ExecBatch` needs server ≥ 0.87.0 (older servers get one
  statement per row), certificate login needs a server with the
  EXTERNAL mechanism and `auth.x509_enabled`, and member discovery needs
  `SHOW ENDPOINTS` (server ≥ 0.327; older and standalone servers keep the
  seed list). Multiple result sets and `RETURNING` are server features the
  driver merely passes through. Developed against skaidb 0.290.
- The version the driver reports in Hello is [`Version()`](#version), always
  equal to the module version in your `go.mod`.

## Conformance

The driver runs skaidb's shared wire-protocol conformance suite:
[`conformance/vectors.json`](conformance/vectors.json), generated from the
server's reference encoders and published at
<https://skaidb.org/conformance/vectors.json> (contract:
[`conformance/README.md`](conformance/README.md)). `conformance_test.go`

- decodes every value vector (and encodes it where Go can express the
  value as a parameter), and checks the driver's SCRAM computations;
- runs the auth outcomes (ok, a server signature that does not verify,
  denied) and every case through `database/sql` and `ExecBatchConn` against
  a fake server that verifies the client proof with its own crypto and
  replays the reference responses, matching the driver's requests byte for
  byte.

Skipped, each printed as a `conformance: SKIP` line
(`go test -v -run TestConformance .`):

- **encoding** of the Decimal and Uuid vectors — the driver surfaces both
  as strings and a string binds as String (decoding is checked);
- the **affected count** of `stream_non_row_result` — `WithStreaming` is
  `database/sql`'s `Query`, and `Rows` carry no affected count; the case
  still checks the request bytes and that the statement succeeded without
  rows.

A Document decodes to a `map[string]any`, so its wire key order is not
compared. CI fails when the vendored vectors differ from the published
copy.

## Development

```sh
go vet ./... && go test -race ./...
```

The tests need no server: they drive the connection against scripted
loopback peers. `examples/` builds with `go build ./...` and runs against a
live server. Pull requests run the same on the CI matrix.

## Releasing

A release is a tag. Add a `## [X.Y.Z] — YYYY-MM-DD` entry to
[CHANGELOG.md](CHANGELOG.md), commit, then

```sh
git tag -a vX.Y.Z -m "vX.Y.Z" && git push origin main vX.Y.Z
```

The [Release workflow](.github/workflows/release.yml) runs on the tag: it
re-runs gofmt, vet and the tests, creates the GitHub Release with the
changelog entry as its notes, warms proxy.golang.org and sum.golang.org so
`go get github.com/porcupin26/skaidb-go@vX.Y.Z` resolves at once, and checks
that the published module reports `X.Y.Z` from `Version()`. There is no
version constant to bump and no registry credential: a Go module is
published by its tag. Never move or delete a pushed tag — the proxy and the
checksum database keep it forever.

Do not ask the proxy for a version before its tag is pushed — no
`go get …@vX.Y.Z`, no `curl` of `@v/vX.Y.Z.info`. proxy.golang.org caches the
miss, and until that entry expires everyone (the workflow included) gets
`unknown revision vX.Y.Z`. The workflow retries for about 35 minutes; past
that, re-run it after the entry has expired rather than re-tagging.

License: [SSPL-1.0](LICENSE).

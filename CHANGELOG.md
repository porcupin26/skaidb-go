# Changelog

All notable changes to the skaidb Go driver. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/) and Go module conventions.

## [1.4.2] — 2026-11-16

### Fixed
- **A connection the server closed while it sat idle cost a statement.**
  After a member restart, a failover or an idle timeout, the first
  statement on each idle connection could fail with `skaidb: connection
  lost mid-statement (statement may or may not have applied)`: across a
  real network the write to a dead socket succeeds and only the read of
  the answer fails, when it is too late to send the statement elsewhere.
  The driver now looks at the socket before it writes a request and before
  the pool hands a connection out — one non-blocking read, no round trip,
  under TLS too. A connection found closed or reset is dropped with
  nothing sent: the statement runs on another connection, `db.Conn` gives
  one that works, and `Transaction` opens on one. On a `*sql.Conn` you
  already hold, such a statement returns `driver.ErrBadConn` — nothing was
  sent — and inside a transaction that is a lost transaction, as before.
  A connection found closed this way does not count against its member.
  The look is made on Linux, macOS, the BSDs and Solaris.
- **A `USE` sent through the `*sql.DB` when the DSN names no database**
  stayed on the one pooled connection that ran it: statements ran in that
  database or in `default` depending on the connection they got, and in
  `default` on all of them after a restart. The pool's database is now
  `default` when the DSN names none, and a connection is taken back to it
  (`USE "default"`) before its next borrower gets it, as it already was
  taken back to the DSN's database.
- **A borrower's `SET CONSISTENCY` and `SET SCAN BUDGET` reached the next
  borrower.** Sent on a `*sql.Conn` or through the `*sql.DB`, a session
  setting stayed on its pooled connection: the next borrower's scan was
  refused for a budget it never set, or its statements ran at a
  consistency it did not ask for. The driver now notes a setting the
  server accepted and clears it (`SET SCAN BUDGET DEFAULT`,
  `SET CONSISTENCY DEFAULT`) before the connection's next borrower; a
  connection it cannot clear — a server before 0.349 has no
  `SET CONSISTENCY DEFAULT` — is closed instead. A borrower that changed
  nothing costs no round trip.
- **A transaction left open reached the next borrower.** A `BEGIN` sent
  through the `*sql.DB`, or a `*sql.Conn` closed between its `BEGIN` and
  its `COMMIT`, left the transaction open on a pooled connection, and the
  pool's next statements ran inside it: they reported success, no other
  session saw them, and they were never committed. The driver now notes a
  transaction a statement opened until its `COMMIT` or `ROLLBACK` is
  answered; a connection that comes back with one open is closed, which
  ends the transaction on the server.
- **A transaction written by hand, when its connection was lost.** On a
  `*sql.Conn` whose connection the driver had retired inside the
  transaction, `ROLLBACK` failed with `skaidb: connection is retired: …`
  and so did `COMMIT`, which does not say whether anything was committed.
  `ROLLBACK` now succeeds without being sent — the server dropped the
  transaction with the connection — and `COMMIT` fails with
  `skaidb: transaction not committed: …`, the error `Transaction` returns
  for it; a `COMMIT` that finds the connection gone returns the same
  instead of a bare `driver: bad connection`. Lost under the `COMMIT`
  itself, the error still says that the outcome is unknown.
- **A `USE` written with other whitespace than the driver knew** — a
  no-break space pasted in with the statement, any Unicode space, a
  vertical tab, a form feed, all of which the server accepts — moved the
  session unnoticed, and the connection went to its next borrower in that
  database. The driver reads a `USE` as the server's lexer does.
- **An answer of a kind the request cannot have left the connection in
  the pool.** After `skaidb: unexpected prepare response`, `skaidb: unknown
  response tag N` or `skaidb: unexpected response to stream request`, the
  next statement on that connection could read what followed as its own
  answer. The connection is now retired, as the documentation said.
- **A value the driver could not decode was read as `NULL`.** A value of
  a type this driver version does not know — from a newer server — or one
  cut short came back as `NULL`, `0` or `[null]` with no error. The
  statement now fails with `skaidb: unknown value tag N` or
  `skaidb: truncated server message`.

### Upgrade notes
- **Session state no longer carries over from one borrower of a pooled
  connection to the next.** On a pool without a database in the DSN, a
  `USE` sent through the `*sql.DB` used to stick to one connection; it is
  now over when the statement returns, like `SET CONSISTENCY` and
  `SET SCAN BUDGET` sent that way. Choose the database in the DSN, or send
  `USE` and the settings on a `*sql.Conn`, where they hold for as long as
  you keep it.
- **`BEGIN`, statements and `COMMIT` each sent through the `*sql.DB` are
  not a transaction** — they never were on a pool of more than one
  connection. The `COMMIT` now fails with the server's `no transaction in
  progress` on a pool of one too. Use `skaidb.Transaction`, or one
  `*sql.Conn`.
- A statement on a `*sql.Conn` whose connection was closed while idle
  returns `driver.ErrBadConn` where it could return `skaidb: connection
  lost mid-statement …`.

## [1.4.1] — 2026-11-16

### Fixed
- **A context cancelled after its statement had returned could break the
  connection.** `defer cancel()` does exactly that. The goroutine that
  watches a statement's context was not waited for, and one that ran late
  marked the healthy connection broken and left a dead deadline on its
  socket: the next statement on a `*sql.Conn` failed with `driver: bad
  connection`, and a pooled connection was thrown away — possibly after
  `database/sql` had already handed it to the next caller. A statement now
  returns only once its watcher is gone.
- **A connection the driver gives up on closes its socket at once, and
  takes no further statement.** After a transport failure, a statement cut
  short by its deadline or a cancelled context, or an unexpected frame,
  the socket stayed open until `database/sql` closed the connection — for
  a `*sql.Conn`, whenever its owner did — so the server kept the session
  and any transaction it had open. Worse, a `*sql.Conn` kept sending on
  it: after a statement cut short by its deadline, the next statement read
  that statement's late reply as its own, and `skaidb.Transaction` could
  report a commit whose answer it never read. Statements on such a
  connection now fail with `skaidb: connection is retired: …` and nothing
  is written.
- **`Transaction` after a server restart.** The first `Transaction` on
  each connection left dead in the pool failed with `driver: bad
  connection`, where `db.Exec` in the same state succeeds. Its opening
  statement now moves to another connection when the one it took is dead,
  as `database/sql` does for its own statements; nothing after the opening
  statement is ever moved or retried. `ExecBatch` finds a live connection
  the same way, however many idle connections the restart left dead (it
  gave up after three).
- **A statement with arguments after a server restart.** A transport
  failure while the statement was being prepared came back as the raw
  socket error (`write: broken pipe`, or a bare `EOF`) and the dead
  connection went back to the pool, so every statement with arguments
  whose text that connection had not prepared yet kept failing on it. The
  statement is now run on another connection, and the dead one dropped.
- **A transaction whose connection was lost.** When the function passed to
  `Transaction` carried on past the error and returned `nil`,
  `Transaction` returned `sql: connection is already closed`. It now
  returns `skaidb: transaction not committed: …` (matches
  `driver.ErrBadConn`): `COMMIT` was not sent and nothing was committed. A
  connection lost under the `COMMIT` itself still reports that the outcome
  is unknown.
- **A transaction whose function left a streamed result open.** Neither
  `COMMIT` nor `ROLLBACK` can be sent on a connection that is streaming,
  so the transaction stayed open on the server; `Transaction` then waited
  for the rows to be closed, and once they were, the connection went back
  to the pool with the transaction still open — the pool's next statements
  ran inside it and were never committed. The connection is now closed,
  which ends the transaction on the server, and the function's error (or
  the "busy streaming" one) is returned at once. `Transaction` no longer
  waits for rows its function left open, buffered ones included: the
  connection returns to the pool when they are closed.
- **A statement on a dead connection while rows were open on it never
  returned.** In the loop that reads rows from a `*sql.Conn` and runs a
  statement per row, a write that failed returned `driver.ErrBadConn`,
  `database/sql` closed the `*sql.Conn`, and that close waited for the
  rows the same goroutine was reading. Such a statement now fails with
  `skaidb: connection lost before the statement was sent: …`.
- **A borrower's `USE` followed its connection back into the pool.** With
  a database in the DSN, a `USE` sent on a `*sql.Conn` or through the
  `*sql.DB` left that one pooled connection in another database, and
  whoever got it next ran there — against a table of the same name,
  silently. The DSN's database is now entered again before the connection
  is handed to its next borrower; a connection that cannot be taken back
  is closed. So with a database in the DSN, a `USE` lasts for the
  `*sql.Conn` it was sent on and no longer. A `USE` is recognised with a
  `--` comment behind or before it too (`USE other -- for the report`),
  as the server accepts it. A DSN that names no database is unchanged: a
  `USE` stays on the one connection it was sent on, and is gone when that
  connection is replaced — name the database in the DSN.
- **A prepare the server refused for lack of room was answered by sending
  the statement as text.** On a connection holding the server's limit of
  256 prepared statements, the statement ran with its arguments written
  into it as literals, which types them differently (a `time.Time` stored
  as an integer, a `[]byte` as the text of its hex digits). The driver's
  own cache stays below the limit, so this takes statements prepared past
  it; the refusal is now returned as the statement's error.

## [1.4.0] — 2026-11-15

### Added
- **A retrying transaction helper**: `skaidb.Transaction(ctx, db,
  func(conn *sql.Conn) error { … })` runs the function between `BEGIN` and
  `COMMIT` on one pooled connection it holds for the whole transaction, and
  `skaidb.TransactionConn` does the same on a `*sql.Conn` of yours. When
  the server refuses the `COMMIT` with a transaction conflict — a
  standalone server ≥ 0.346.0 does when another writer changed a row the
  transaction writes after it began — the function runs again from the
  opening statement, at once and on the same connection: three runs in all
  by default (`TxAttempts`). `TxBegin` sets the opening statement
  (`BEGIN ATOMIC …` on a cluster). A refused opening statement ends it
  before the function runs; an error from the function sends `ROLLBACK`
  and comes back as it is, whatever the server answers to the `ROLLBACK`
  (a panic gets its `ROLLBACK` too); any other `COMMIT` error is returned
  with no retry and no `ROLLBACK`; `TxAttempts` below 1 is an error before
  anything is sent.
- **`ErrTransactionConflict`**: a sentinel `errors.Is` matches for a server
  error whose message starts with `transaction conflict:`, on every
  statement path — text, prepared, streamed, batch. The error's text stays
  `skaidb: <server message>`, and it is a statement error: the connection
  stays in the pool and nothing counts against its member.

## [1.3.1] — 2026-10-31

### Fixed
- One slow round trip at the start can no longer get a healthy member
  ejected for latency. A member's first round trip set its average
  outright, so a statement that stalled in the client — a CPU-bound
  application starting up, the goroutine waiting for the answer not
  scheduled — put seconds into it; ten round trips later the member was
  compared with the others, and the average had barely decayed: ejected
  while it answered in milliseconds. The fastest of a member's first three
  round trips now seeds its average (a round trip is only ever
  lengthened), and later ones move it as before. Until it is seeded the
  member counts as not measured: `least_latency` tries it, and `Members()`
  reports its `Latency` as 0.

### Added
- The connector logs each ejection and re-admission as a warning through
  the default `log/slog` logger, worded as the other skaidb drivers log them
  (`skaidb pool: ejecting db3:7000 (3 consecutive connection failures)`,
  `skaidb pool: re-admitted db3:7000`); ejections were silent. A latency
  ejection names the member's average, the median it was compared with and
  the slowest of its last 10 round trips (`latency 516.3 ms vs median
  0.2 ms; slowest of the last 10 round trips 900.0 ms`), which tells a slow
  member from one slow statement. The log is written after the connector
  releases its lock, so a handler may itself use the pool.

## [1.3.0] — 2026-10-29

### Added
- **Parameterised streaming**: `db.QueryContext(skaidb.WithStreaming(ctx),
  sql, args...)` binds `?` placeholders exactly as a buffered query does —
  typed, arrays and documents included — and sends them with the streamed
  request over `OP_QUERY_STREAM_PARAMS` (server ≥ 0.330), so a
  range-bounded export no longer needs its values written into the SQL; a
  `*sql.Stmt` run with a streaming context streams the same way. Without
  arguments the request is the original `OP_QUERY_STREAM`, byte for byte.
  The conformance harness passes `call.params` to the streaming call (the
  new `stream_params` case).

### Changed
- A `WithStreaming` query with arguments streams instead of silently
  running buffered, so it follows the streaming rules: the rows own the
  connection until `rows.Close`, and the context must outlive the
  iteration. Against a server older than 0.330 (which answers the new
  opcode with an `unknown opcode` error) the call still runs buffered, as a
  prepared statement, on the same connection; a statement kind the server
  does not bind (DDL, session statements) still takes the client-side text
  path.

### Fixed
- A `CALL` of a procedure that `EMIT`s, run with `WithStreaming`, failed
  with `skaidb: unexpected response to stream request`; its result sets now
  arrive through `NextResultSet`, as on the buffered path.

## [1.2.0] — 2026-10-25

### Added
- **Pool load balancing across members** (the rules every skaidb driver
  shares, PROTOCOL.md §6.1), in the connector `database/sql` dials through:
  `load_balancing=round_robin` / `least_latency` (power of two choices on
  the round-trip average × open connections; `random` stays the default)
  for the member each new connection goes to; health ejection (consecutive
  transport failures, or a latency average far above the other members')
  with a background probe that re-admits the member; and `discover=true`
  (members from `SHOW ENDPOINTS`, server ≥ 0.327: joiners added, leavers
  dropped, resyncing/unreachable members skipped). Tuned with
  `refresh_interval`, `eject_errors`, `eject_latency_factor`,
  `eject_latency_floor`, `eject_backoff` and `eject_backoff_max`.
- `NewConnector(dsn, opts...)` with the same settings as Go options
  (`LoadBalancing`, `Discover`, `RefreshInterval`, `EjectErrors`,
  `EjectLatencyFactor`, `EjectLatencyFloor`, `EjectBackoff`,
  `EjectBackoffMax`) for `sql.OpenDB`, and `(*Connector).Members()`, which
  reports each member's state, latency and open connections. The exported
  `*Connector` is also what `sql.Open` builds; `db.Close()` closes it.

### Changed
- A connection whose member is ejected, reported not ready or dropped from
  the listing is retired: it fails `IsValid` when returned to the pool, and
  the connection now implements `driver.SessionResetter`, which turns it
  down when the pool offers it again, so `database/sql` dials a replacement.
- The DSN refuses the two shared options `database/sql` leaves no room for,
  rather than ignoring them: `idle_order=fifo` (`database/sql` hands out
  the most recently returned connection first) and a non-zero
  `read_your_writes` (run a write and its reads on one `*sql.Conn`).

## [1.1.0] — 2026-09-26

### Added
- One-round-trip batches: `ExecBatch(ctx, db, query, rows)` and
  `ExecBatchConn(ctx, conn, query, rows)` run one statement over many
  parameter rows with `OP_EXECUTE_BATCH` and return the total affected
  count. The driver's `driver.Conn` implements the new `BatchExecer`
  interface, reachable through `(*sql.Conn).Raw`. Batches larger than a wire
  frame (64 MiB) go out in chunks, one round trip each; unpreparable
  statements and servers older than the opcode fall back to one statement
  per row.
- Certificate login (the protocol's EXTERNAL mechanism): DSN options
  `tls_client_cert` / `tls_client_key` present a client certificate in the
  TLS handshake, and `auth_mechanism=certificate` authenticates with it (the
  certificate's Common Name is the username; a DSN without a user sends an
  empty one).
- The shared skaidb wire-protocol conformance suite
  (`conformance/vectors.json`, `conformance_test.go`): value and SCRAM
  vectors, the auth outcomes, and every case through `database/sql` against
  a fake server replaying the server's reference bytes. CI also fails when
  the vendored vectors differ from <https://skaidb.org/conformance/vectors.json>.

### Fixed
- A statement prepared after the per-connection cache was full (240
  statement texts) was never closed on the server. A long-lived connection
  running many distinct statements reached the server's cap of 256 open
  statements, after which every new statement silently fell back to
  client-side text binding — and arrays and documents, which only bind on
  the typed path, failed. Such a statement is now closed (`OP_CLOSE`) after
  its execution.
- A successful SCRAM outcome shorter than its 32-byte signature is reported
  as a handshake decode error instead of being checked against zeros.

## [1.0.3] — 2026-09-20

### Fixed
- Release automation: the workflow warms proxy.golang.org with a real
  download (`go mod download` by tag, after resolving the tagged commit) and
  waits out a cached miss instead of failing on the first 404; `go list -m`
  prints a canonical version without contacting the proxy, so the 1.0.2 run
  warmed nothing and then tripped over a miss cached before the tag existed.
  No driver code changed.

## [1.0.2] — 2026-09-20

### Changed
- Release automation: published from GitHub Actions. Pushing a `vX.Y.Z` tag
  now runs the checks, creates the GitHub Release from this changelog, warms
  the Go module proxy and the checksum database, and verifies that the
  published module reports the tag as `Version()`
  (`.github/workflows/release.yml`). No driver code changed.

## [1.0.1] — 2026-09-19

### Fixed
- A malformed DSN (`consistency=eventual`, `tls=maybe`, a non-numeric port,
  a bad scheme, no host) now fails `sql.Open` itself, as documented. The
  driver only implemented `driver.Driver`, so `database/sql` never parsed
  the DSN until the first connection and the error surfaced on the first
  statement or `Ping` instead. The driver now implements
  `driver.DriverContext`: `OpenConnector` parses eagerly and hands
  `database/sql` a `driver.Connector` that dials the seed list per pooled
  connection. Well-formed DSNs still open lazily; `db.Driver()` is
  unchanged; `Open(dsn)` on the driver value keeps its parse-and-dial
  behaviour.

### Added
- Unit tests for `OpenConnector` (every malformed-DSN case through
  `sql.Open`, the parsed configuration it keeps) and `Connector.Connect`
  (a scripted handshake, Hello and `USE` over a loopback server; a
  cancelled context; unreachable seeds).

## [1.0.0] — 2026-09-19

The first release from its own repository. The driver previously shipped
inside the skaidb monorepo as `skaidb.org/drivers/go`, versioned in lock-step
with the server (last published there: v0.290.x). That import path stays
served but is frozen; switch imports to `github.com/porcupin26/skaidb-go`.

### Added
- `Version()` — the driver's own version as reported to the server in the
  Hello frame. Derived from the Go build metadata (`runtime/debug`), so it
  always equals the module version a program was built against and can
  never go stale.
- Examples under `examples/`: `basic`, `prepared` (typed parameters,
  RETURNING, per-statement consistency, a batch loop), `streaming` and
  `subscribe`.
- Full documentation in `README.md` and `docs/` (pkg.go.dev does not render
  documentation for SSPL-licensed modules).
- Unit tests for DSN parsing, client-side binding, the typed value codec,
  the Hello frame and version resolution; CI runs `go vet` and `go test
  -race` on Go 1.21, 1.22, 1.23 and the latest stable.

### Fixed
- A DSN seed list whose last host names no port while an earlier one does
  (`skaidb://h1:7001,h2/db`) was rejected as "invalid port". The authority
  is now split by the driver, so any mix of ports works, and a bracketed
  IPv6 seed without a port (`[::1]`) gets the default port instead of being
  dialled as `[::1]` alone.
- Seed ports are validated (numeric, 1–65535) at `sql.Open` time instead of
  failing on the first dial.

### Changed
- Module path is `github.com/porcupin26/skaidb-go` (was
  `skaidb.org/drivers/go`).
- The Hello frame reports the real module version instead of a fixed
  `0.1.0`.
- `example/` moved to `examples/basic/`.

### Carried over (unchanged behaviour)
- Standard `database/sql` driver, pure standard library, `?` placeholders.
- Server-side prepared statements with typed parameters (arrays and
  documents included), falling back to client-side binding for statements
  the server will not prepare.
- Seed-list dialling with shuffle and failover, TLS (`tls`, `tls_ca`,
  `tls_insecure`, `tls_server_name`), SCRAM-SHA-256 with mutual auth,
  per-connection `USE` from the DSN path.
- `WithStreaming` for chunked result delivery with a bounded drain on an
  abandoned stream; `WithConsistency` per-statement override; `Subscribe`
  for stream logs; multiple result sets from procedures that `EMIT`;
  context deadlines and cancellation mapped to `context.DeadlineExceeded`
  and `context.Canceled`.

# Errors

Driver errors are plain `error` values whose message starts with
`skaidb:`; there is no exported error type. Match on the sentinels where
they apply: the standard ones (`context.DeadlineExceeded`,
`context.Canceled`, `sql.ErrNoRows`) and this package's
`skaidb.ErrTransactionConflict`. Match on message text otherwise — the
messages below are stable within the 1.x line.

## From `sql.Open` (DSN validation)

| Message | Cause |
|---|---|
| `skaidb: DSN scheme must be skaidb://` | wrong scheme |
| `skaidb: bad DSN: …` | the URL parser rejected the path/query/credentials |
| `skaidb: DSN has no host` | empty seed list |
| `skaidb: bad seed "…"` | non-numeric or out-of-range port, unclosed IPv6 bracket, empty host |
| `skaidb: bad consistency "…"` | not `one`/`quorum`/`all` |
| `skaidb: bad tls "…"`, `skaidb: bad tls_insecure "…"` | not a boolean |
| `skaidb: tls_client_cert and tls_client_key go together` | only one of the two given |
| `skaidb: auth_mechanism=certificate needs tls_client_cert and tls_client_key` | certificate login without a certificate |
| `skaidb: bad auth_mechanism "…"` | not `scram` or `certificate` |
| `skaidb: bad load_balancing "…" (use random, round_robin or least_latency)` | unknown policy (also from `NewConnector` with `LoadBalancing`) |
| `skaidb: bad discover "…"` | not a boolean |
| `skaidb: bad refresh_interval "…" (a duration such as 5s or 50ms)` | not a Go duration; likewise `eject_latency_floor`, `eject_backoff`, `eject_backoff_max`, `read_your_writes` |
| `skaidb: bad eject_errors "…"`, `skaidb: bad eject_latency_factor "…"` | not a number |
| `skaidb: eject_errors must be at least 1, got N`, `skaidb: eject_latency_factor must be a finite number >= 0 …`, `skaidb: refresh_interval must be positive …`, `skaidb: eject_backoff and eject_backoff_max must be positive …`, `skaidb: eject_latency_floor must not be negative …` | out of range (also from `NewConnector` options) |
| `skaidb: idle_order=fifo is not available: …` | `database/sql` owns the idle order (lifo); `skaidb: bad idle_order "…"` for any other value |
| `skaidb: read_your_writes is not available: …` | a non-zero window; use one `*sql.Conn` for the write and its reads |

## From the first statement on a connection (dial)

| Message | Cause |
|---|---|
| `skaidb: no reachable endpoint in [members]: <last error>` | every member tried failed; the wrapped error is the last one's |
| `skaidb: connector is closed` | `Connect` on a `*Connector` after `Close` (`db.Close()` closes it) |
| `skaidb: connect failed: …` | TCP (wrapped in the above) |
| `skaidb: TLS handshake failed: …` | certificate not trusted, name mismatch (`tls_server_name`), plaintext server, or `client_tls = required` on a plaintext dial |
| `skaidb: cannot read tls_ca "…"`, `skaidb: no certificates found in tls_ca "…"` | the CA file |
| `skaidb: cannot load tls_client_cert/tls_client_key: …` | the client certificate or key file is unreadable, not PEM, or the two do not match |
| `skaidb: authentication denied: <reason>` | wrong user or password, user disabled; with certificate login, a certificate the server does not trust or a DSN user that differs from its CN |
| `skaidb: server signature mismatch (mutual auth failed)` | the peer knows the password's derived key but not the password — or is not the server you think |
| `skaidb: bad handshake challenge` / `outcome`, `skaidb: handshake decode: …` | not a skaidb server on that port |
| an error from `USE "<database>"` | the DSN database does not exist or is not granted |

## From statements

| Message | Cause |
|---|---|
| `skaidb: <server message>` | the server rejected or failed the statement: syntax, unknown table or column, constraint violation, permission, scan budget exceeded, read/write quorum unavailable… The text is the server's |
| `skaidb: statement expects N parameters, got M` | argument count vs. the server's parse |
| `skaidb: more placeholders than parameters`, `skaidb: more parameters than placeholders` | count mismatch on the client-side text path |
| `skaidb: cannot bind value of type T` | an unsupported argument type (struct, chan…), or a composite on the client-side path |
| `skaidb: cannot bind NaN/Infinity` | non-finite float |
| `skaidb: document keys must be strings` | a map with non-string keys |
| `skaidb: N parameters, over the protocol's 65535` | more arguments than the wire's 16-bit count carries; nothing was sent |
| `sql: converting argument $N type: …` | `database/sql`'s default converter refused a scalar (e.g. `uint64` above `MaxInt64`) |
| `skaidb: batch row N: statement expects K parameters, got M` | an `ExecBatch` row of the wrong width; nothing was sent |
| `skaidb: batch row N is B bytes, over the L-byte frame limit` | one `ExecBatch` row larger than a wire frame |
| `skaidb: <server message> (row numbers count from batch row N; the N rows before it applied)` | a chunked `ExecBatch` failed in a later chunk |
| `skaidb: bad consistency "…"` | `WithConsistency` with an unknown level; the statement did not run |
| `skaidb: transaction conflict: …` | the server refused a `COMMIT` because another writer changed a row the transaction writes after it began. Matches `skaidb.ErrTransactionConflict` with `errors.Is`; only the `transaction conflict:` start of the server's text is fixed. Nothing was committed, the transaction is over and the connection stays usable: run the transaction again from its opening statement (`skaidb.Transaction` does) |
| `skaidb: transaction attempts must be at least 1, got N` | `Transaction`/`TransactionConn` with `TxAttempts` below 1; nothing was sent |
| `skaidb: transaction not committed: its connection was lost, or retired after a statement failed part-way, before COMMIT was sent: driver: bad connection` | `Transaction`/`TransactionConn`: the connection went inside the transaction and the function returned `nil` all the same, or the `COMMIT` could not be written. Matches `driver.ErrBadConn` with `errors.Is`. The server dropped the transaction with its session: nothing was committed, and running it again is safe |
| `skaidb: transactions are not supported` | `Begin`/`BeginTx`: `*sql.Tx` is not wired — use `skaidb.Transaction` |
| `skaidb: no LastInsertId — use INSERT … RETURNING id with QueryRow` | `Result.LastInsertId` |
| `skaidb: connection is busy streaming a result set — close those rows before running another statement` | a second statement on a `*sql.Conn` whose streamed rows are still open |
| `skaidb: connection is busy streaming a result set — close those rows before running another statement (the transaction's function left them open: its connection was closed to end the transaction, and nothing was committed)` | `Transaction`/`TransactionConn` whose function returned `nil` with a streamed result still open; when the function returned an error, that error is returned instead and the transaction ends the same way |
| `skaidb: connection closed` | a statement on a `*sql.Conn` after `Close` |
| `skaidb: connection is retired: an earlier statement on it failed or was cancelled part-way and its socket is closed — nothing was sent` | a statement on a `*sql.Conn` whose connection the driver has retired (see below); close the `*sql.Conn` and take another |

## Transport failures and retries

| Situation | What you get | Retry? |
|---|---|---|
| the socket failed **before** the request was written | nothing: the driver returns `driver.ErrBadConn`, `database/sql` discards the connection and re-runs the statement on a fresh one (up to its internal limit), transparently | already done |
| the socket failed **after** the request was written | `skaidb: connection lost mid-statement (statement may or may not have applied): <io error>` | only if idempotent |
| the context expired / was cancelled | `context.DeadlineExceeded` / `context.Canceled` (via `errors.Is`); the connection is retired | only if idempotent |
| a corrupt or unexpected frame | `skaidb: truncated server message`, `skaidb: unknown response tag N`, `skaidb: unknown value tag N`, `skaidb: unexpected frame in stream`; the connection is retired | investigate; this indicates a protocol mismatch |

"Retired" means the driver closes the connection's socket there and then
— the server ends the session, and with it a transaction the session had
open — and reports the connection invalid, so `database/sql` drops it
before the next checkout; the replacement dial walks the members, which is
where failover to another node happens. The pool never hands a retired
connection out. A `*sql.Conn` you hold keeps pointing at it: every further
statement on it fails with `skaidb: connection is retired: …` and nothing
is written, so no statement can read the reply the failed one left behind.
A
transport failure (or an expired deadline, not a cancellation) also counts
against the member; enough in a row eject it for a while (see
[dsn.md](dsn.md#load-balancing)).

A statement error — a transaction conflict included — is none of these: the
connection stays in the pool and nothing counts against the member.
`skaidb.Transaction` runs a transaction again only after a conflict at
`COMMIT`; it never retries a transport failure, and after one it sends
nothing more of the transaction on any connection. Its opening statement
alone moves to another connection when the pooled one it took is dead.

## Errors during a stream

A server error after the header (a budget exceeded on a later page, a
node lost mid-scan) arrives from `rows.Next()` returning false and
`rows.Err()` carrying `skaidb: <message>`. Rows already scanned are valid.
The connection is at a frame boundary after a server error and stays
usable; after a transport error it is retired.

## `sql.ErrNoRows`

`QueryRow(...).Scan` returns `sql.ErrNoRows` when the statement produced no
row — including a `RETURNING` on an `INSERT … ON CONFLICT DO NOTHING` that
kept the existing row.

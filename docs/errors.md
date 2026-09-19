# Errors

Driver errors are plain `error` values whose message starts with
`skaidb:`; there is no exported error type. Match on the standard sentinels
where they apply (`context.DeadlineExceeded`, `context.Canceled`,
`sql.ErrNoRows`) and on message text otherwise — the messages below are
stable within the 1.x line.

## From `sql.Open` (DSN validation)

| Message | Cause |
|---|---|
| `skaidb: DSN scheme must be skaidb://` | wrong scheme |
| `skaidb: bad DSN: …` | the URL parser rejected the path/query/credentials |
| `skaidb: DSN has no host` | empty seed list |
| `skaidb: bad seed "…"` | non-numeric or out-of-range port, unclosed IPv6 bracket, empty host |
| `skaidb: bad consistency "…"` | not `one`/`quorum`/`all` |
| `skaidb: bad tls "…"`, `skaidb: bad tls_insecure "…"` | not a boolean |

## From the first statement on a connection (dial)

| Message | Cause |
|---|---|
| `skaidb: no reachable endpoint in [seeds]: <last error>` | every seed failed; the wrapped error is the last seed's |
| `skaidb: connect failed: …` | TCP (wrapped in the above) |
| `skaidb: TLS handshake failed: …` | certificate not trusted, name mismatch (`tls_server_name`), plaintext server, or `client_tls = required` on a plaintext dial |
| `skaidb: cannot read tls_ca "…"`, `skaidb: no certificates found in tls_ca "…"` | the CA file |
| `skaidb: authentication denied: <reason>` | wrong user or password, user disabled |
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
| `sql: converting argument $N type: …` | `database/sql`'s default converter refused a scalar (e.g. `uint64` above `MaxInt64`) |
| `skaidb: bad consistency "…"` | `WithConsistency` with an unknown level; the statement did not run |
| `skaidb: transactions are not supported` | `Begin`/`BeginTx` |
| `skaidb: no LastInsertId — use INSERT … RETURNING id with QueryRow` | `Result.LastInsertId` |
| `skaidb: connection is busy streaming a result set — close those rows before running another statement` | a second statement on a `*sql.Conn` whose streamed rows are still open |
| `skaidb: connection closed` | a statement on a `*sql.Conn` after `Close` |

## Transport failures and retries

| Situation | What you get | Retry? |
|---|---|---|
| the socket failed **before** the request was written | nothing: the driver returns `driver.ErrBadConn`, `database/sql` discards the connection and re-runs the statement on a fresh one (up to its internal limit), transparently | already done |
| the socket failed **after** the request was written | `skaidb: connection lost mid-statement (statement may or may not have applied): <io error>` | only if idempotent |
| the context expired / was cancelled | `context.DeadlineExceeded` / `context.Canceled` (via `errors.Is`); the connection is retired | only if idempotent |
| a corrupt or unexpected frame | `skaidb: truncated server message`, `skaidb: unknown response tag N`, `skaidb: unknown value tag N`, `skaidb: unexpected frame in stream`; the connection is retired | investigate; this indicates a protocol mismatch |

"Retired" means the driver reports the connection invalid and
`database/sql` closes it before the next checkout; the replacement dial
walks the seed list, which is where failover to another node happens.

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

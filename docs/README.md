# skaidb Go driver — documentation

The [top-level README](../README.md) is the complete reference. These
chapters go deeper on one topic each.

| Chapter | What it covers |
|---|---|
| [getting-started.md](getting-started.md) | install, first connection, the schema-less model, where things can go wrong |
| [database-sql.md](database-sql.md) | how the stdlib API maps onto skaidb: `Exec`, `Query`, `QueryRow`, `Stmt`, `Conn`, `Rows`, `Result` |
| [dsn.md](dsn.md) | the DSN grammar, seeds and failover, the session database, consistency, the TLS modes |
| [prepared-statements.md](prepared-statements.md) | server-side preparation, the per-connection cache, typed parameters, the client-side fallback |
| [batch.md](batch.md) | bulk writes without transactions, `RETURNING`, upserts, throughput patterns |
| [streaming.md](streaming.md) | `WithStreaming`, server-side paging, the abandon/drain rule |
| [streams.md](streams.md) | `Subscribe` over `CREATE STREAM` logs, resuming, idempotency, MQTT push |
| [context.md](context.md) | deadlines, cancellation, what happens to the connection and the statement |
| [types.md](types.md) | the full type mapping in both directions, decimals, timestamps, JSON composites |
| [errors.md](errors.md) | every error the driver produces, retry semantics, `driver.ErrBadConn` |

Server-side documentation: <https://skaidb.org/docs/>. Wire protocol:
<https://skaidb.org/docs/PROTOCOL.html>.

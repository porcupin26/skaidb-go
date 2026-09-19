# The DSN

```
skaidb://[user[:password]@]host[:port][,host2[:port]...][/database][?option=value&...]
```

`sql.Open("skaidb", dsn)` parses it eagerly and returns an error for any
malformed part; nothing is dialled until the first statement.

## Credentials

`user:password@` are SCRAM-SHA-256 credentials. The driver proves the
password without sending it and verifies the server's signature back
(mutual authentication), so a wrong server cannot harvest the password nor
impersonate a right one. Percent-encode reserved characters in either
field: `p@ss/word` is `p%40ss%2Fword`. Omit both for a server with
authentication disabled; the user is then `anonymous`.

## Seeds

The host part is a comma-separated list of `host[:port]`; a missing port is
`7000`. IPv6 literals are bracketed: `[::1]`, `[fe80::1]:7001`. Any mix is
fine: `node1,node2:7001,[::1]`.

skaidb is leaderless, so a seed is only "somewhere to land". For each new
pooled connection the driver:

1. shuffles the list,
2. dials the seeds in that order with a 10 s TCP timeout each,
3. returns the first one that connects **and** authenticates (a node that
   accepts TCP but fails the handshake is skipped),
4. otherwise fails with `skaidb: no reachable endpoint in [...]: <last error>`.

Failover follows from that. When a connection breaks, `database/sql` drops
it (the driver reports it invalid) and the next dial walks the seeds again.
A `SetConnMaxLifetime` on the pool makes the spread across nodes recover on
its own after an outage.

List several nodes in production. A single DNS name that resolves to several
addresses does **not** get the same treatment: `net.Dial` picks one address.

## Session database

A path selects the database: `skaidb://host/app` runs `USE "app"` on every
new connection, including the ones the pool opens later. Without it,
unqualified table names resolve in the server's default database; you can
also qualify names (`app.users`) in the SQL.

## Consistency

`?consistency=one|quorum|all` (default `quorum`) is the level for every
statement on every connection of this pool. Override per statement with
`skaidb.WithConsistency(ctx, level)`.

- `one` — a single replica answers. Fastest, may read a value a replica has
  not yet received.
- `quorum` — a majority of replicas. Reads see every acknowledged quorum
  write.
- `all` — every replica. Fails while any replica of the row is down.

DDL runs at quorum on the server regardless of the requested level.

## TLS

Any of `tls`, `tls_ca` or `tls_insecure` switches the connection to TLS;
the handshake completes before the first protocol byte.

| Option | Effect |
|---|---|
| `tls=true` | TLS, server certificate verified against the system root store |
| `tls_ca=/path/ca.pem` | TLS, verified against that PEM bundle (the usual choice: skaidb clusters run their own CA) |
| `tls_insecure=true` | TLS, **no verification** — a man in the middle can present any certificate. Local development against a self-signed node only |
| `tls_server_name=name` | the SNI sent and the name matched against the certificate's SANs. Default `skaidb`, which is the SAN skaidb's own certificates carry. Set it only if your certificates name something else |

`tls_server_name` defaulting to `skaidb` rather than the dialled host is
deliberate: cluster nodes share one certificate whose SAN is not any
particular address.

A server with `client_tls = required` closes plaintext connections at
once; the symptom without a TLS option is a connect or handshake error on
every seed.

## Validation

These fail `sql.Open`:

- a scheme other than `skaidb://`;
- no host;
- a port that is not a number in 1–65535, an unclosed IPv6 bracket;
- `consistency` outside `one|quorum|all`;
- `tls` / `tls_insecure` outside `true|false|1|0`.

Unknown options are ignored, so a typo in an option name is not caught.

## Examples

```
skaidb://localhost
skaidb://app:s3cret@10.0.0.1,10.0.0.2,10.0.0.3/orders?consistency=quorum
skaidb://app:s3cret@db.internal:7000/orders?tls_ca=/etc/skaidb/ca.pem
skaidb://dev:dev@[::1]/scratch?tls_insecure=true&consistency=one
```

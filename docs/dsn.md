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

1. orders the members: the one the [load-balancing](#load-balancing)
   policy picks first (with the default `random` policy, none), the other
   usable members in random order, then the members that are not usable
   (resyncing or unreachable before ejected);
2. dials them in that order with a 10 s TCP timeout each;
3. returns the first one that connects **and** authenticates (a node that
   accepts TCP but fails the handshake is skipped);
4. otherwise fails with `skaidb: no reachable endpoint in [...]: <last error>`.

Failover follows from that. When a connection breaks, `database/sql` drops
it (the driver reports it invalid) and the next dial walks the members
again; a member that keeps failing is ejected for a while and tried last.
A `SetConnMaxLifetime` on the pool makes the spread across nodes recover on
its own after an outage.

List several nodes in production. A single DNS name that resolves to several
addresses does **not** get the same treatment: `net.Dial` picks one address.

## Load balancing

The pool is spread across the cluster's members by the rules every skaidb
driver shares
([PROTOCOL.md §6.1](https://skaidb.org/docs/PROTOCOL.html#61-balancing-a-pool-across-members)).
`database/sql` decides which open connection serves a statement; these
options decide which member each **new** connection goes to and when a
connection is retired. The same settings exist as Go options on
`skaidb.NewConnector` (README, "Balancing across members"), which override
the DSN.

| Option | Default | Effect |
|---|---|---|
| `load_balancing` | `random` | `random`: the usable members in random order. `round_robin`: the next usable member in endpoint order. `least_latency`: of two usable members picked at random, the one with the lower round-trip average × (open connections + 1); one not measured in the last 2 s scores 0 |
| `discover` | `false` | `true`/`1`: read the members with `SHOW ENDPOINTS` (server ≥ 0.327) on the first connection to each member not yet named and every `refresh_interval` after. Joiners are added, leavers dropped (seeds are kept, as `unlisted`), only `ready` members get new connections. No rows (standalone) or an error (older server): the seeds stay and the driver stops asking |
| `refresh_interval` | `60s` | discovery's period; every 5 s while a member is `resyncing` or `unreachable` |
| `eject_errors` | `3` | consecutive transport failures (failed dial, I/O error or deadline mid-statement, capacity refusal) that eject a member; statement errors and refused logins do not count |
| `eject_latency_factor` | `3` | eject a member whose average exceeds this × the other members' median (after 10 round trips on it and on another member); `0` disables |
| `eject_latency_floor` | `50ms` | … and is also more than this above the median |
| `eject_backoff` | `5s` | how long an ejection lasts; doubled per repeat. A background probe re-admits the member afterwards. The last usable member is never ejected |
| `eject_backoff_max` | `60s` | the cap on the doubling; a member healthy this long after re-admission starts over |
| `idle_order` | `lifo` | the order `database/sql` uses; `fifo` is refused because the driver cannot provide it — use `SetConnMaxIdleTime` / `SetConnMaxLifetime` to keep connections rotating |
| `read_your_writes` | `0` | a window is refused: `database/sql` picks the connection per statement. Run a write and the reads that must see it on one `*sql.Conn` from `db.Conn(ctx)`; it stays on the member that took the write |

Durations are Go durations (`500ms`, `30s`, `2m`); a bare number is
refused.

```
skaidb://app:s3cret@db1,db2,db3/orders?load_balancing=least_latency&discover=true
```

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

Any of `tls`, `tls_ca`, `tls_insecure` or `tls_client_cert` switches the connection to TLS;
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

## Certificate login

`auth_mechanism=certificate` authenticates with a TLS client certificate
instead of a password (the wire protocol's EXTERNAL mechanism):

| Option | Effect |
|---|---|
| `tls_client_cert=/path/client.crt` | PEM client certificate presented in the TLS handshake. Enables TLS |
| `tls_client_key=/path/client.key` | its PEM private key; always given with `tls_client_cert` |
| `auth_mechanism=certificate` | log in as the certificate's subject Common Name. `scram` (the default) logs in with the DSN password |

```
skaidb://db1,db2/app?auth_mechanism=certificate&tls_ca=/etc/skaidb/ca.pem&tls_client_cert=/etc/skaidb/ada.crt&tls_client_key=/etc/skaidb/ada.key
```

The server needs `auth.x509_enabled` and a CA for client certificates
(`auth.x509_ca_file`). The certificate's **Common Name is the username**. A
user in the DSN is optional and must equal it; without one the driver sends
an empty username and the server takes the CN. No password is exchanged,
and the server's signature in the outcome is not checked — TLS already
authenticated the server, so verify it with `tls_ca` or the system roots
rather than `tls_insecure`.

A client certificate without `auth_mechanism=certificate` is presented in
the TLS handshake (for a server that asks for one) and the login stays
SCRAM.

## Validation

These fail `sql.Open`:

- a scheme other than `skaidb://`;
- no host;
- a port that is not a number in 1–65535, an unclosed IPv6 bracket;
- `consistency` outside `one|quorum|all`;
- `tls` / `tls_insecure` outside `true|false|1|0`;
- `tls_client_cert` without `tls_client_key` or the reverse;
- `auth_mechanism` outside `scram|password|certificate|external|x509`, or
  `certificate` without a client certificate;
- `load_balancing` outside `random|round_robin|least_latency`, `discover`
  not a boolean, a duration option that is not a non-negative Go duration
  (`refresh_interval`, `eject_backoff` and `eject_backoff_max` must be
  positive), `eject_errors` below 1, a negative `eject_latency_factor`;
- `idle_order=fifo` or a non-zero `read_your_writes` (see
  [load balancing](#load-balancing)).

Unknown options are ignored, so a typo in an option name is not caught.

## Examples

```
skaidb://localhost
skaidb://app:s3cret@10.0.0.1,10.0.0.2,10.0.0.3/orders?consistency=quorum
skaidb://app:s3cret@db.internal:7000/orders?tls_ca=/etc/skaidb/ca.pem
skaidb://dev:dev@[::1]/scratch?tls_insecure=true&consistency=one
```

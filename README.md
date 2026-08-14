# skaidb — Go driver

A standard [`database/sql`](https://pkg.go.dev/database/sql) driver. Import it
for its side effect and use the stdlib API you already know — `sql.Open`,
`db.Query`, `db.Exec`, `rows.Scan`. **No third-party dependencies.**

## Install

```sh
go get skaidb.org/drivers/go
```

Offline, or to pin a vendored copy, unpack the driver tarball and point your
`go.mod` at it instead — no network, no proxy:

```
require skaidb.org/drivers/go v0.0.0
replace skaidb.org/drivers/go => ./skaidb-driver-go    # path to the unpacked driver
```

## Use

```go
import (
    "database/sql"
    _ "skaidb.org/drivers/go"
)

db, err := sql.Open("skaidb", "skaidb://skaidb:secret@localhost:7000/?consistency=quorum")
if err != nil { log.Fatal(err) }
defer db.Close()

db.Exec("CREATE TABLE users (PRIMARY KEY (id))")
db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", 1, "Ada")

rows, _ := db.Query("SELECT id, name FROM users WHERE id = ?", 1)
defer rows.Close()
for rows.Next() {
    var id int
    var name string
    rows.Scan(&id, &name)
    fmt.Println(id, name)
}
```

### DSN

```
skaidb://[user[:password]@]host[:port][,host2[:port]...][/database]?consistency=quorum&tls_ca=/path/ca.crt
```

- Several comma-separated hosts form a **seed list**: dialled in shuffled
  order until one connects and authenticates, and re-walked whenever
  `database/sql` opens a replacement connection, which is how failover works.
- A trailing `/name` selects the session database (`USE`) on every dial.
- `port` defaults to `7000`.
- `consistency` is `one`, `quorum` (default), or `all`.
- Omit `user`/`password` for a server with auth disabled.
- `tls=true` enables TLS; it is implied by either option below. A server with
  `client_tls = required` refuses plaintext, so one of these is mandatory there.
- `tls_ca=<path>` verifies the server certificate against a PEM CA bundle.
- `tls_insecure=true` encrypts without verifying anything — development only.
- `tls_server_name=<name>` sets SNI; it must match a SAN on the server
  certificate and defaults to `skaidb`, which is what skaidb's own certs carry
  (so it usually is *not* the address you dialled).

### Notes

- **Placeholders** use `?` (the `database/sql` convention). Args are quoted
  safely client-side.
- skaidb is **non-transactional**: `db.Begin()` returns an error. Each statement
  auto-commits. Use `db.Query`/`db.Exec` directly.
- A `*sql.DB` is a connection pool and is safe for concurrent use — the usual
  Go pattern. Each pooled connection serializes its own request/response.

### Types (`rows.Scan` targets)

| skaidb     | Go (driver value)        | Scan into            |
|------------|--------------------------|----------------------|
| Null       | `nil`                    | `*sql.NullX`, pointer|
| Bool       | `bool`                   | `*bool`              |
| Int        | `int64`                  | `*int`, `*int64`     |
| Float      | `float64`                | `*float64`           |
| Decimal    | `string` (exact)         | `*string`            |
| String     | `string`                 | `*string`            |
| Bytes      | `[]byte`                 | `*[]byte`            |
| Uuid       | `string` (canonical)     | `*string`            |
| Timestamp  | `time.Time` (UTC)        | `*time.Time`         |
| Array      | JSON `string`            | `*string`            |
| Document   | JSON `string`            | `*string`            |

## Run the example

```sh
cd example
go run . "skaidb://skaidb:secret@192.168.7.117:7000/?consistency=quorum"
```

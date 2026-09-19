# Getting started

## Requirements

- Go 1.21 or newer.
- A reachable skaidb node (port 7000 by default). Any node of a cluster
  will do — skaidb is leaderless. For a local server see the
  [install guide](https://skaidb.org/docs/install.html) or
  [Docker](https://skaidb.org/docs/docker.html).

## Install

```sh
go get github.com/porcupin26/skaidb-go@latest
```

The module has no dependencies beyond the Go standard library.

If you are coming from `skaidb.org/drivers/go`: that path is frozen at
v0.290.x. Change the import string, `go get` the new module and run
`go mod tidy`; nothing else changes.

## First connection

```go
package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	_ "github.com/porcupin26/skaidb-go" // registers the "skaidb" driver
)

func main() {
	db, err := sql.Open("skaidb", "skaidb://skaidb:secret@localhost:7000/app")
	if err != nil {
		log.Fatal(err) // a malformed DSN; nothing has been dialled yet
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		log.Fatal(err) // dial, TLS, authentication or USE failed — see errors.md
	}
	log.Println("connected")
}
```

`sql.Open` only parses the DSN. The first statement (or `Ping`) dials a
seed, completes the TLS handshake if configured, authenticates with
SCRAM-SHA-256, announces the driver version, and runs `USE "app"`.

## The data model in one minute

skaidb is schema-less SQL. A table declares its primary key; columns come
into existence when a row carrying them is written, and every row may carry
different columns.

```go
db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS users (PRIMARY KEY (id))`)
db.ExecContext(ctx, `INSERT INTO users (id, name, tags) VALUES (?, ?, ?)`, 1, "Ada", []string{"admin"})
db.ExecContext(ctx, `INSERT INTO users (id, name, age) VALUES (?, ?, ?)`, 2, "Linus", 54)

rows, err := db.QueryContext(ctx, `SELECT id, name, age FROM users ORDER BY id`)
// row 1 has age NULL; row 2 has no tags. Scan NULLs into sql.NullInt64 or *int.
```

Composite values — arrays and documents — are bound from Go slices and maps
and come back as JSON text. See [types.md](types.md).

## Where to go next

- The DSN in full, including TLS and multiple seeds: [dsn.md](dsn.md).
- Reading a table that is too big to buffer: [streaming.md](streaming.md).
- Bulk loading: [batch.md](batch.md).
- Reacting to changes: [streams.md](streams.md).
- Runnable programs: [`../examples/`](../examples/).

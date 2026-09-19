// Prepared statements, typed parameters (arrays and documents), per-statement
// consistency, RETURNING, and a batch loop:
//
//	go run ./examples/prepared "skaidb://user:pass@host:7000/app"
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	skaidb "github.com/porcupin26/skaidb-go"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: prepared <dsn>")
	}
	db, err := sql.Open("skaidb", os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	must(db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS orders (PRIMARY KEY (id))"))

	// A *sql.Stmt is prepared on the SERVER (cached per pooled connection)
	// and executed with typed parameters. Slices and maps travel as skaidb
	// arrays and documents — there is no SQL literal for them, so this is the
	// only way they reach the wire.
	ins, err := db.PrepareContext(ctx,
		"INSERT INTO orders (id, customer, tags, meta) VALUES (?, ?, ?, ?)")
	if err != nil {
		log.Fatal(err)
	}
	defer ins.Close()
	for i := 1; i <= 100; i++ { // a "batch": one prepared statement, many executions
		must(ins.ExecContext(ctx, i, fmt.Sprintf("cust-%d", i%7),
			[]string{"web", "promo"}, map[string]any{"amount": i * 10, "rush": i%3 == 0}))
	}

	// RETURNING instead of LastInsertId (which is not carried on the wire).
	var id int64
	if err := db.QueryRowContext(ctx,
		"INSERT INTO orders (id, customer) VALUES (?, ?) RETURNING id", 101, "cust-x").Scan(&id); err != nil {
		log.Fatal(err)
	}
	fmt.Println("inserted", id)

	// A per-statement consistency override: this read is served by ONE
	// replica instead of the connection's default quorum.
	var n int
	if err := db.QueryRowContext(skaidb.WithConsistency(ctx, "one"),
		"SELECT count(*) FROM orders WHERE customer = ?", "cust-3").Scan(&n); err != nil {
		log.Fatal(err)
	}
	fmt.Println("cust-3 orders:", n)

	// Composite columns come back as JSON text.
	var tags, meta string
	if err := db.QueryRowContext(ctx, "SELECT tags, meta FROM orders WHERE id = ?", 3).Scan(&tags, &meta); err != nil {
		log.Fatal(err)
	}
	fmt.Println(tags, meta)

	must(db.ExecContext(ctx, "DROP TABLE orders"))
}

func must(_ sql.Result, err error) {
	if err != nil {
		log.Fatal(err)
	}
}

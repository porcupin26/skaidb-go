// Streaming a large result set without holding it all in memory:
//
//	go run ./examples/streaming "skaidb://user:pass@host:7000/app" "SELECT id, name FROM people"
//
// skaidb.WithStreaming(ctx) makes QueryContext pull the rows chunk by chunk
// instead of buffering the whole set. The one rule: ALWAYS defer rows.Close().
// database/sql does not close a Rows on a bare `break`, and Close is where an
// abandoned stream is drained (or the connection retired) so the pool never
// hands a half-read socket to the next caller.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	skaidb "github.com/porcupin26/skaidb-go"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatal("usage: streaming <dsn> <select statement>")
	}
	db, err := sql.Open("skaidb", os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// A deadline covers the WHOLE stream, not just the first chunk.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// The streaming path carries SQL text only: no `?` parameters here.
	rows, err := db.QueryContext(skaidb.WithStreaming(ctx), os.Args[2])
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close() // mandatory — see the package doc

	cols, _ := rows.Columns()
	fmt.Println(cols)
	n := 0
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			log.Fatal(err)
		}
		n++
		if n <= 10 {
			fmt.Println(vals...)
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%d rows\n", n)
}

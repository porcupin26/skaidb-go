// Following a stream (CREATE STREAM ... ON <table>) from Go:
//
//	go run ./examples/subscribe "skaidb://user:pass@host:7000/app" big_orders [after-id]
//
// skaidb.Subscribe pages the stream's log (_stream_<name>) with a keyset
// cursor and calls fn per event. Keep the last Event.ID somewhere durable and
// pass it as `after` on the next start to resume exactly where you stopped.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"

	skaidb "github.com/porcupin26/skaidb-go"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatal("usage: subscribe <dsn> <stream> [after-id]")
	}
	after := ""
	if len(os.Args) > 3 {
		after = os.Args[3]
	}
	db, err := sql.Open("skaidb", os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	err = skaidb.Subscribe(ctx, db, os.Args[2], after, func(ev skaidb.Event) error {
		fmt.Printf("%s %-6s key=%s ts=%s doc=%s\n", ev.ID, ev.Op, ev.Key, ev.Ts.Format("15:04:05.000"), ev.Doc)
		return nil // return an error to stop; Subscribe returns it
	})
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

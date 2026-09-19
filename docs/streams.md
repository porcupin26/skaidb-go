# Change streams: `Subscribe`

A skaidb **stream** is a standing filter over a table's writes. Every
committed change that matches is captured into a log table,
`_stream_<name>`, kept for a retention window, and published live over
MQTT. Full server documentation: <https://skaidb.org/docs/streams.html>.

```sql
CREATE STREAM big_orders ON orders
  WHEN (total > 1000 AND status != 'draft')
  WITH (start = 'now', retention = '24h');
```

The log's columns: `id` (position, the resume cursor), `op` (`put`,
`exit`, `delete`), `k` (primary key), `ts`, `doc` (the row as JSON), and
`old` with `pre_image = true`.

## Following a stream from Go

```go
err := skaidb.Subscribe(ctx, db, "big_orders", checkpoint.Load(), func(ev skaidb.Event) error {
	switch ev.Op {
	case "put":
		var o Order
		if err := json.Unmarshal([]byte(ev.Doc), &o); err != nil {
			return err
		}
		if err := index.Upsert(o); err != nil {
			return err
		}
	case "exit", "delete":
		if err := index.Remove(ev.Key); err != nil {
			return err
		}
	}
	return checkpoint.Store(ev.ID)
})
```

```go
type Event struct {
	ID  string    // log position; pass the last one as `after` to resume
	Op  string    // "put" (matches now), "exit" (matched before, no longer does), "delete"
	Key string    // the row's primary key (JSON when composite)
	Ts  time.Time // when the change was written (UTC)
	Doc string    // the row as JSON text (for a delete: as it last was)
}
```

`Subscribe` pages the log with a keyset cursor (`WHERE id > ? ORDER BY id
LIMIT 500`), calls `fn` for each event in order, and when caught up polls
every 500 ms. It returns when `ctx` is done (`ctx.Err()`), when `fn`
returns an error (that error), or when a query fails.

## Resuming

`after` is an `Event.ID`. `""` starts from the oldest retained event; to
resume after a restart, persist the last id you handled and pass it back.
Events are delivered in id order, once each per `Subscribe` call.

Delivery is **at-least-once** across restarts: if the process dies after
`fn` succeeded but before the id was persisted, that event is delivered
again. Either persist the id in the same step as the effect, or make `fn`
idempotent (an upsert by `Key` is).

An `after` older than the retention window silently starts at the oldest
retained event; the gap is not reported. Choose the retention with the
consumer's longest expected downtime in mind.

## What `Subscribe` does not do

- It does not read the `old` column (`pre_image = true`). Query
  `_stream_<name>` yourself for it; it is an ordinary table.
- It does not fan out: one call, one consumer. Run several consumers with
  their own checkpoints.
- It does not push. For push delivery subscribe to `$stream/<db>/<name>` on
  the server's MQTT broker ([docs](https://skaidb.org/docs/mqtt.html)) with
  any MQTT client; the payloads are the same JSON events.

## Consistency

The polling reads run at the connection's consistency (or a
`WithConsistency` override on `ctx`). At `one` a replica that lags may
briefly show a gap that fills in on the next poll; since the cursor only
advances past events actually seen, a lagging replica can make the follower
skip nothing but can delay it. `quorum` (the default) avoids the question.

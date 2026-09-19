# Prepared statements and parameter binding

Placeholders are `?`, positional. A `?` inside a string literal is text.
The number of arguments must match the number of placeholders.

## Two paths

**Typed (server-prepared).** Whenever a statement has arguments the driver
sends the text to the server to prepare, gets back an id and the parameter
count, and executes with each argument encoded as a typed wire value (tag
and payload; the encoding is the same one result rows use — see the
[protocol, §3.3 and §4](https://skaidb.org/docs/PROTOCOL.html)). No SQL
text is rebuilt on the client, so quoting is never a concern, and arrays and
documents — which have no literal form — bind naturally.

**Client-side (text).** If the server declines to prepare the statement
(DDL, `USE`, other session statements), the driver renders each argument as
a safely quoted literal into the text and sends it as a plain query.
Scalars only; a slice or a map here is an error. Statements without
arguments always go as text.

You do not choose between the two; every DML statement with arguments takes
the typed path against any server that supports preparation (≥ 0.17.0),
and older servers get the text path for everything.

## The cache

Prepared ids are only valid on the connection that created them, so the
cache is per pooled connection, keyed by statement text, up to 240
entries. Running one statement in a loop prepares it once per connection it
lands on. There is nothing to close or invalidate: a connection's cache
goes away with it.

Consequence: a `*sql.Stmt` and `db.Exec(text, args...)` in a loop cost the
same. Use whichever reads better.

## `*sql.Stmt`

```go
stmt, err := db.PrepareContext(ctx, "UPDATE users SET tags = ? WHERE id = ?")
if err != nil { return err }
defer stmt.Close()
for id, tags := range updates {
	if _, err := stmt.ExecContext(ctx, tags, id); err != nil { return err }
}
```

`database/sql` may run a `*sql.Stmt` on any pooled connection and
re-prepares as needed. `Prepare` itself does not contact the server; the
first `Exec`/`Query` does. Argument count mismatches are reported by the
server's view: `skaidb: statement expects 2 parameters, got 1`.

## Argument conversion

Before an argument reaches the encoder:

- a `driver.Valuer` (`sql.NullString`, `sql.NullTime`, your own types) is
  replaced by what `Value()` returns;
- slices (other than byte slices), arrays and maps — also through pointers
  — are accepted as they are, to become Array / Document;
- everything else goes through `database/sql`'s default converter: integer
  and float widths are normalised, pointers dereferenced (`nil` → NULL),
  named string/bool types unwrapped.

So `uint8(7)`, `myID(42)`, `&name`, `sql.NullInt64{}` and `[]string{…}` all
bind. `uint64` above `math.MaxInt64` is refused, as are NaN and ±Inf.

The full mapping is in [types.md](types.md).

## What cannot be parameterised

Identifiers (table and column names), keywords, and the SQL text of the
[streaming path](streaming.md) (`WithStreaming` sends text only; arguments
switch it back to a buffered query). Build those into the statement string
from trusted values.

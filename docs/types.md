# Type mapping

skaidb's value types on the wire are Null, Bool, Int (64-bit), Float
(64-bit), Decimal (128-bit mantissa + scale), String, Bytes, Uuid,
Timestamp (milliseconds), Array and Document. Full encoding: [protocol
§4](https://skaidb.org/docs/PROTOCOL.html).

## Results: skaidb → Go

`rows.Scan` receives these `driver.Value`s; `database/sql`'s usual
conversions apply on top of them (an `int64` into `*string`, a `string`
into `*int` when it parses, anything into `*any`).

| skaidb | delivered as | typical Scan target |
|---|---|---|
| Null | `nil` | `sql.NullString` / `sql.NullInt64` / …, or a pointer (`*int` set to nil) |
| Bool | `bool` | `*bool` |
| Int | `int64` | `*int64`, `*int`, `*uint` (no overflow check beyond `database/sql`'s) |
| Float | `float64` | `*float64` |
| Decimal | `string`, exact, e.g. `"-12.340"` | `*string`; parse with `math/big` (`new(big.Rat).SetString`) or `shopspring/decimal` |
| String | `string` | `*string` |
| Bytes | `[]byte` (a copy, safe to retain) | `*[]byte` |
| Uuid | `string`, canonical lower-case `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` | `*string` |
| Timestamp | `time.Time` in UTC, millisecond precision | `*time.Time` |
| Array | JSON text (`string`) | `*string`, then `json.Unmarshal` into `[]any` or a typed slice |
| Document | JSON text (`string`) | `*string`, then `json.Unmarshal` into `map[string]any` or a struct |

Notes:

- Timestamps are always UTC. Convert with `t.In(loc)` if you need a zone.
- Decimals are never converted to `float64` by the driver; the text is
  exact.
- Document key order is not preserved (the value passes through a Go map);
  nested arrays keep their order.
- A Document or Array *inside* a Document/Array is nested JSON, as expected.

## Arguments: Go → skaidb

Arguments go through `database/sql`'s default converter for scalars and
straight to the typed encoder for composites (see
[prepared-statements.md](prepared-statements.md#argument-conversion)).

| Go | typed path (prepared) | client-side fallback |
|---|---|---|
| `nil`, a nil pointer, an invalid `sql.Null*` | Null | `NULL` |
| `bool` and named bool types | Bool | `TRUE` / `FALSE` |
| `int`, `int8`…`int64`, `uint`…`uint64` (≤ `MaxInt64`), named integer types | Int | integer literal |
| `float32`, `float64` (finite) | Float | `strconv.FormatFloat(f, 'g', -1, 64)` |
| `string` and named string types | String | `'…'`, single quotes doubled |
| `[]byte`, named byte slices (`json.RawMessage`) | Bytes | hex-encoded string literal |
| `time.Time` | Timestamp — `t.UnixMilli()`, sub-millisecond precision dropped | integer milliseconds |
| a non-nil pointer | the pointee, by these rules | same |
| a `driver.Valuer` | what `Value()` returns, by these rules | same |
| any slice or array except bytes (`[]string`, `[]int`, `[]any`, `[3]float64`, nested) | Array, elements recursively | **error** |
| any map with string keys (`map[string]any`, `map[string]int`…) | Document, keys sorted | **error** |
| a struct, a channel, a func, a map with non-string keys, NaN, ±Inf, `uint64 > MaxInt64` | **error** | **error** |

To bind a struct as a document, convert it first:
`json.Marshal` → `json.Unmarshal` into `map[string]any`, or build the map.
A `[]byte` you *mean* as a document is a Bytes value, not a Document; a
JSON string you mean as a document is a String. Only a map binds as
Document.

Inside an Array or Document the same rules apply per element, so
`[]any{1, "two", []any{3.0}, map[string]any{"k": nil}}` is a legal nested
value, and so are `[]uint16`, `[]sql.NullString`, `[]*int`,
`map[string]int8` and named element types — the encoder normalises
elements the way `database/sql` normalises top-level scalars.

## Decimal input

There is no Go-side Decimal input type. Bind a decimal column from a
`string` (`"12.34"`) or a `float64`; the server converts according to the
column and statement.

## Uuid input

Bind a canonical UUID `string`; the server's UUID functions and comparisons
accept it.

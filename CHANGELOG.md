# Changelog

All notable changes to the skaidb Go driver. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/) and Go module conventions.

## [1.1.0] — 2026-09-26

### Added
- One-round-trip batches: `ExecBatch(ctx, db, query, rows)` and
  `ExecBatchConn(ctx, conn, query, rows)` run one statement over many
  parameter rows with `OP_EXECUTE_BATCH` and return the total affected
  count. The driver's `driver.Conn` implements the new `BatchExecer`
  interface, reachable through `(*sql.Conn).Raw`. Batches larger than a wire
  frame (64 MiB) go out in chunks, one round trip each; unpreparable
  statements and servers older than the opcode fall back to one statement
  per row.
- Certificate login (the protocol's EXTERNAL mechanism): DSN options
  `tls_client_cert` / `tls_client_key` present a client certificate in the
  TLS handshake, and `auth_mechanism=certificate` authenticates with it (the
  certificate's Common Name is the username; a DSN without a user sends an
  empty one).
- The shared skaidb wire-protocol conformance suite
  (`conformance/vectors.json`, `conformance_test.go`): value and SCRAM
  vectors, the auth outcomes, and every case through `database/sql` against
  a fake server replaying the server's reference bytes. CI also fails when
  the vendored vectors differ from <https://skaidb.org/conformance/vectors.json>.

### Fixed
- A statement prepared after the per-connection cache was full (240
  statement texts) was never closed on the server. A long-lived connection
  running many distinct statements reached the server's cap of 256 open
  statements, after which every new statement silently fell back to
  client-side text binding — and arrays and documents, which only bind on
  the typed path, failed. Such a statement is now closed (`OP_CLOSE`) after
  its execution.
- A successful SCRAM outcome shorter than its 32-byte signature is reported
  as a handshake decode error instead of being checked against zeros.

## [1.0.3] — 2026-09-20

### Fixed
- Release automation: the workflow warms proxy.golang.org with a real
  download (`go mod download` by tag, after resolving the tagged commit) and
  waits out a cached miss instead of failing on the first 404; `go list -m`
  prints a canonical version without contacting the proxy, so the 1.0.2 run
  warmed nothing and then tripped over a miss cached before the tag existed.
  No driver code changed.

## [1.0.2] — 2026-09-20

### Changed
- Release automation: published from GitHub Actions. Pushing a `vX.Y.Z` tag
  now runs the checks, creates the GitHub Release from this changelog, warms
  the Go module proxy and the checksum database, and verifies that the
  published module reports the tag as `Version()`
  (`.github/workflows/release.yml`). No driver code changed.

## [1.0.1] — 2026-09-19

### Fixed
- A malformed DSN (`consistency=eventual`, `tls=maybe`, a non-numeric port,
  a bad scheme, no host) now fails `sql.Open` itself, as documented. The
  driver only implemented `driver.Driver`, so `database/sql` never parsed
  the DSN until the first connection and the error surfaced on the first
  statement or `Ping` instead. The driver now implements
  `driver.DriverContext`: `OpenConnector` parses eagerly and hands
  `database/sql` a `driver.Connector` that dials the seed list per pooled
  connection. Well-formed DSNs still open lazily; `db.Driver()` is
  unchanged; `Open(dsn)` on the driver value keeps its parse-and-dial
  behaviour.

### Added
- Unit tests for `OpenConnector` (every malformed-DSN case through
  `sql.Open`, the parsed configuration it keeps) and `Connector.Connect`
  (a scripted handshake, Hello and `USE` over a loopback server; a
  cancelled context; unreachable seeds).

## [1.0.0] — 2026-09-19

The first release from its own repository. The driver previously shipped
inside the skaidb monorepo as `skaidb.org/drivers/go`, versioned in lock-step
with the server (last published there: v0.290.x). That import path stays
served but is frozen; switch imports to `github.com/porcupin26/skaidb-go`.

### Added
- `Version()` — the driver's own version as reported to the server in the
  Hello frame. Derived from the Go build metadata (`runtime/debug`), so it
  always equals the module version a program was built against and can
  never go stale.
- Examples under `examples/`: `basic`, `prepared` (typed parameters,
  RETURNING, per-statement consistency, a batch loop), `streaming` and
  `subscribe`.
- Full documentation in `README.md` and `docs/` (pkg.go.dev does not render
  documentation for SSPL-licensed modules).
- Unit tests for DSN parsing, client-side binding, the typed value codec,
  the Hello frame and version resolution; CI runs `go vet` and `go test
  -race` on Go 1.21, 1.22, 1.23 and the latest stable.

### Fixed
- A DSN seed list whose last host names no port while an earlier one does
  (`skaidb://h1:7001,h2/db`) was rejected as "invalid port". The authority
  is now split by the driver, so any mix of ports works, and a bracketed
  IPv6 seed without a port (`[::1]`) gets the default port instead of being
  dialled as `[::1]` alone.
- Seed ports are validated (numeric, 1–65535) at `sql.Open` time instead of
  failing on the first dial.

### Changed
- Module path is `github.com/porcupin26/skaidb-go` (was
  `skaidb.org/drivers/go`).
- The Hello frame reports the real module version instead of a fixed
  `0.1.0`.
- `example/` moved to `examples/basic/`.

### Carried over (unchanged behaviour)
- Standard `database/sql` driver, pure standard library, `?` placeholders.
- Server-side prepared statements with typed parameters (arrays and
  documents included), falling back to client-side binding for statements
  the server will not prepare.
- Seed-list dialling with shuffle and failover, TLS (`tls`, `tls_ca`,
  `tls_insecure`, `tls_server_name`), SCRAM-SHA-256 with mutual auth,
  per-connection `USE` from the DSN path.
- `WithStreaming` for chunked result delivery with a bounded drain on an
  abandoned stream; `WithConsistency` per-statement override; `Subscribe`
  for stream logs; multiple result sets from procedures that `EMIT`;
  context deadlines and cancellation mapped to `context.DeadlineExceeded`
  and `context.Canceled`.

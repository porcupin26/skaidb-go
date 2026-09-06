// Package skaidb is a database/sql driver for skaidb.
//
// Import it for its side effect (it registers the "skaidb" driver) and use the
// standard database/sql API — there is nothing new to learn:
//
//	import (
//		"database/sql"
//		_ "skaidb.org/drivers/go"
//	)
//
//	db, _ := sql.Open("skaidb", "skaidb://user:pass@localhost:7000/?consistency=quorum")
//	rows, _ := db.Query("SELECT id, name FROM users WHERE id = ?", 1)
//
// Placeholders use "?" (the database/sql norm). Pure standard library: no
// third-party dependencies.
package skaidb

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"math/rand"
	"net"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func init() { sql.Register("skaidb", &drv{}) }

const (
	consistencyOne    = 0
	consistencyQuorum = 1
	consistencyAll    = 2
)

// Reported in the server's drivers table via Hello.
const driverVersion = "0.1.0"

var nonceCounter uint64

// ---- driver registration --------------------------------------------------

type drv struct{}

func (d *drv) Open(dsn string) (driver.Conn, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return dial(cfg)
}

type config struct {
	addrs       []string
	user        string
	password    string
	consistency byte
	database    string
	tls         bool
	tlsCA       string
	tlsInsecure bool
	tlsName     string
}

func parseDSN(dsn string) (config, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return config{}, fmt.Errorf("skaidb: bad DSN: %w", err)
	}
	if u.Scheme != "skaidb" {
		return config{}, fmt.Errorf("skaidb: DSN scheme must be skaidb://")
	}
	cfg := config{user: "anonymous", consistency: consistencyQuorum}
	if u.User != nil {
		cfg.user = u.User.Username()
		if p, ok := u.User.Password(); ok {
			cfg.password = p
		}
	}
	// Seeds: skaidb://user:pass@host1:7000,host2:7000,host3/db . skaidb is
	// leaderless, so any node serves — a seed list is just "somewhere to
	// land", with no primary to discover.
	for _, h := range strings.Split(u.Host, ",") {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		if !strings.Contains(h, ":") {
			h += ":7000"
		}
		cfg.addrs = append(cfg.addrs, h)
	}
	if len(cfg.addrs) == 0 {
		return config{}, fmt.Errorf("skaidb: DSN has no host")
	}
	// Session database from the URL path: skaidb://host:7000/app
	cfg.database = strings.TrimPrefix(u.Path, "/")
	switch strings.ToLower(u.Query().Get("consistency")) {
	case "one":
		cfg.consistency = consistencyOne
	case "all":
		cfg.consistency = consistencyAll
	case "", "quorum":
		cfg.consistency = consistencyQuorum
	default:
		return config{}, fmt.Errorf("skaidb: bad consistency %q", u.Query().Get("consistency"))
	}
	// TLS. A server with client_tls = required refuses plaintext outright, so
	// without these a cluster is simply unreachable. Any of the three knobs
	// turns TLS on: tls=true, a CA file to verify against, or tls_insecure.
	q := u.Query()
	cfg.tlsCA = q.Get("tls_ca")
	switch strings.ToLower(q.Get("tls_insecure")) {
	case "", "false", "0":
	case "true", "1":
		cfg.tlsInsecure = true
	default:
		return config{}, fmt.Errorf("skaidb: bad tls_insecure %q", q.Get("tls_insecure"))
	}
	switch strings.ToLower(q.Get("tls")) {
	case "", "false", "0":
	case "true", "1":
		cfg.tls = true
	default:
		return config{}, fmt.Errorf("skaidb: bad tls %q", q.Get("tls"))
	}
	cfg.tls = cfg.tls || cfg.tlsCA != "" || cfg.tlsInsecure
	// SNI must match a SAN on the server certificate, which is usually not
	// the address you dialled — skaidb's own certs carry DNS:skaidb.
	cfg.tlsName = q.Get("tls_server_name")
	if cfg.tlsName == "" {
		cfg.tlsName = "skaidb"
	}
	return cfg, nil
}

// tlsWrap upgrades an established TCP connection to TLS and completes the
// handshake before any protocol byte is written, so a failure here is
// reported as a connect error rather than a mid-handshake protocol error.
func tlsWrap(nc net.Conn, cfg config) (net.Conn, error) {
	tcfg := &tls.Config{ServerName: cfg.tlsName}
	if cfg.tlsInsecure {
		// Encrypts, but authenticates nothing: a man in the middle can
		// present any certificate. For development against a self-signed
		// node only — pass tls_ca in anything that matters.
		tcfg.InsecureSkipVerify = true
	} else if cfg.tlsCA != "" {
		pem, err := os.ReadFile(cfg.tlsCA)
		if err != nil {
			return nil, fmt.Errorf("skaidb: cannot read tls_ca %q: %w", cfg.tlsCA, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("skaidb: no certificates found in tls_ca %q", cfg.tlsCA)
		}
		tcfg.RootCAs = pool
	}
	tc := tls.Client(nc, tcfg)
	if err := tc.Handshake(); err != nil {
		return nil, fmt.Errorf("skaidb: TLS handshake failed: %w", err)
	}
	return tc, nil
}

// ---- connection ------------------------------------------------------------

type preparedStmt struct {
	id      uint32
	nparams int
}

type conn struct {
	nc          net.Conn
	consistency byte
	closed      bool
	// Written by the context watcher goroutine (see applyContext) while the
	// owning goroutine may be reading it, so it is atomic.
	broken   atomic.Bool
	prepared map[string]preparedStmt
	// Consistency for the statement in flight: the connection default unless
	// a context override is set for this call (see WithConsistency).
	// database/sql runs one statement at a time per connection, so a plain
	// field is enough.
	stmtLevel byte
}

// IsValid implements driver.Validator: database/sql asks before handing a
// pooled connection out again, so a socket broken by a transport error is
// discarded instead of failing the next caller. The replacement is dialled
// through Open, which walks the seed list — that is where failover happens.
func (c *conn) IsValid() bool { return !c.closed && !c.broken.Load() }

// transportErr classifies a mid-statement I/O failure.
//
// `sent` says whether the request reached the wire. If it did NOT, the server
// cannot have executed anything, so returning driver.ErrBadConn is safe and
// database/sql transparently retries the statement on a fresh connection —
// which re-dials and may land on a different node. If it DID, the statement's
// fate is unknown: it may have been applied and only the reply lost. Retrying
// then could double-apply a non-idempotent write (an UPDATE ... SET n = n + 1
// is not an upsert), so the error is surfaced to the caller instead. The
// connection is marked broken either way.
func (c *conn) transportErr(err error, sent bool) error {
	c.broken.Store(true)
	if !sent {
		return driver.ErrBadConn
	}
	return fmt.Errorf("skaidb: connection lost mid-statement (statement may or may not have applied): %w", err)
}

// dial tries the seeds until one connects AND authenticates, so a node that
// accepts TCP but is unhealthy does not swallow the attempt. The order is
// shuffled per dial: database/sql opens connections on demand, so shuffling
// spreads a pool across the cluster instead of piling it on the first seed.
func dial(cfg config) (*conn, error) {
	order := make([]string, len(cfg.addrs))
	copy(order, cfg.addrs)
	rand.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	var lastErr error
	for _, addr := range order {
		c, err := dialOne(cfg, addr)
		if err == nil {
			return c, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("skaidb: no reachable endpoint in %v: %w", cfg.addrs, lastErr)
}

func dialOne(cfg config, addr string) (*conn, error) {
	nc, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("skaidb: connect failed: %w", err)
	}
	if tcp, ok := nc.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	if cfg.tls {
		tc, err := tlsWrap(nc, cfg)
		if err != nil {
			nc.Close()
			return nil, err
		}
		nc = tc
	}
	c := &conn{nc: nc, consistency: cfg.consistency, stmtLevel: cfg.consistency, prepared: map[string]preparedStmt{}}
	if err := c.handshake(cfg.user, cfg.password); err != nil {
		nc.Close()
		return nil, err
	}
	c.sendHello()
	// Session database from the DSN path (skaidb://host:port/app). USE is
	// per-connection session state, so it must run on every dial — including
	// the ones database/sql makes to grow the pool, which is exactly why this
	// belongs here and not in the caller.
	if cfg.database != "" {
		if _, err := c.exec(`USE "` + strings.ReplaceAll(cfg.database, `"`, `""`) + `"`); err != nil {
			nc.Close()
			return nil, err
		}
	}
	return c, nil
}

// sendHello self-identifies best-effort: it fills the server's `drivers`
// table client_name/client_version columns. An old server answers the
// unknown opcode with an error frame, which is ignored — identity is
// telemetry, never load-bearing.
func (c *conn) sendHello() {
	name := []byte("go")
	ver := []byte(driverVersion)
	req := make([]byte, 0, 1+4+len(name)+4+len(ver))
	req = append(req, 8)
	var l [4]byte
	binary.LittleEndian.PutUint32(l[:], uint32(len(name)))
	req = append(req, l[:]...)
	req = append(req, name...)
	binary.LittleEndian.PutUint32(l[:], uint32(len(ver)))
	req = append(req, l[:]...)
	req = append(req, ver...)
	if err := c.writeFrame(req); err != nil {
		return
	}
	_, _ = c.readFrame()
}

func (c *conn) writeFrame(payload []byte) error {
	var head [4]byte
	binary.BigEndian.PutUint32(head[:], uint32(len(payload)))
	if _, err := c.nc.Write(head[:]); err != nil {
		return err
	}
	_, err := c.nc.Write(payload)
	return err
}

func (c *conn) readFrame() ([]byte, error) {
	var head [4]byte
	if _, err := io.ReadFull(c.nc, head[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(head[:])
	buf := make([]byte, n)
	if _, err := io.ReadFull(c.nc, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (c *conn) handshake(user, password string) error {
	clientNonce := fmt.Sprintf("go%d.%d", time.Now().UnixNano()&0xffff, atomic.AddUint64(&nonceCounter, 1))

	start := []byte{10}
	start = appendStr(start, user)
	start = appendStr(start, clientNonce)
	if err := c.writeFrame(start); err != nil {
		return err
	}

	r := &reader{buf: mustFrame(c.readFrame())}
	if r.err == nil && r.u8() != 11 {
		return fmt.Errorf("skaidb: bad handshake challenge")
	}
	salt := r.blob()
	iterations := r.u32()
	serverNonce := r.text()
	if r.err != nil {
		return fmt.Errorf("skaidb: handshake decode: %w", r.err)
	}

	authMessage := []byte(strings.Join(
		[]string{user, clientNonce, serverNonce, hex.EncodeToString(salt), strconv.FormatUint(uint64(iterations), 10)},
		"\x00"))
	salted := pbkdf2SHA256([]byte(password), salt, int(iterations), 32)
	clientKey := hmacSHA256(salted, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	clientSig := hmacSHA256(storedKey[:], authMessage)
	proof := make([]byte, 32)
	for i := range proof {
		proof[i] = clientKey[i] ^ clientSig[i]
	}
	if err := c.writeFrame(append([]byte{12}, proof...)); err != nil {
		return err
	}

	r = &reader{buf: mustFrame(c.readFrame())}
	if r.err != nil {
		return r.err
	}
	if r.u8() != 13 {
		return fmt.Errorf("skaidb: bad handshake outcome")
	}
	if r.u8() == 1 {
		serverSig := r.take(32)
		if password != "" {
			serverKey := hmacSHA256(salted, []byte("Server Key"))
			expected := hmacSHA256(serverKey, authMessage)
			if subtle.ConstantTimeCompare(serverSig, expected) != 1 {
				return fmt.Errorf("skaidb: server signature mismatch (mutual auth failed)")
			}
		}
		return nil
	}
	return fmt.Errorf("skaidb: authentication denied: %s", r.text())
}

// ---- database/sql/driver interfaces ---------------------------------------

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{c: c, query: query, n: strings.Count(stripStrings(query), "?")}, nil
}

func (c *conn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.nc.Close()
}

// skaidb is non-transactional; expose that honestly.
func (c *conn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("skaidb: transactions are not supported")
}

// CheckNamedValue accepts ANY Go value. Without it database/sql pre-converts
// arguments to its own small driver.Value set and rejects slices and maps
// outright, so an array or a document could never reach the wire.
func (c *conn) CheckNamedValue(nv *driver.NamedValue) error { return nil }

// streamKey marks a context as requesting streamed delivery.
type streamKey struct{}

// WithStreaming marks ctx so QueryContext streams the result instead of
// buffering it: the client holds one chunk rather than the whole set. Use it
// for exports and large scans.
//
//	rows, err := db.QueryContext(skaidb.WithStreaming(ctx), "SELECT ...")
//
// Parameters are not supported on this path — the streaming opcode carries
// SQL text, so bind values yourself or use a parameterless statement. Falls
// back to a buffered query against a server too old to know the opcode.
func WithStreaming(ctx context.Context) context.Context {
	return context.WithValue(ctx, streamKey{}, true)
}

// Subscribe delivers a stream's events to fn as they arrive, blocking until
// ctx is cancelled or fn returns an error.
//
// A dependency-free helper over the stream's log: it pages the log with the
// keyset cursor and calls fn per event. The id is the position — keep the
// last one and pass it as `after` to resume exactly where you stopped,
// across restarts. Composites (k, doc) arrive as JSON text, like any other
// document column in this driver.
//
// This polls; for push delivery subscribe to $stream/<db>/<name> with any
// MQTT client instead. The events are identical.
//
//	err := skaidb.Subscribe(ctx, db, "big_orders", "", func(ev skaidb.Event) error {
//		return handle(ev)
//	})
func Subscribe(ctx context.Context, db *sql.DB, stream, after string, fn func(Event) error) error {
	log := "_stream_" + stream
	cur := after
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var (
			rows *sql.Rows
			err  error
		)
		if cur == "" {
			rows, err = db.QueryContext(ctx,
				"SELECT id, op, k, ts, doc FROM "+log+" ORDER BY id LIMIT 500")
		} else {
			rows, err = db.QueryContext(ctx,
				"SELECT id, op, k, ts, doc FROM "+log+" WHERE id > ? ORDER BY id LIMIT 500", cur)
		}
		if err != nil {
			return err
		}
		n := 0
		for rows.Next() {
			var ev Event
			if err := rows.Scan(&ev.ID, &ev.Op, &ev.Key, &ev.Ts, &ev.Doc); err != nil {
				rows.Close()
				return err
			}
			cur = ev.ID
			n++
			if err := fn(ev); err != nil {
				rows.Close()
				return err
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if n == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
}

// Event is one change captured by a stream.
type Event struct {
	// ID is the log position: keep the last one to resume.
	ID  string
	Op  string // "put" or "delete"
	Key string // the row's primary key (JSON when composite)
	Ts  time.Time
	Doc string // the row document, as JSON text
}

// consistencyKey carries a per-statement consistency override on the context.
type consistencyKey struct{}

// WithConsistency overrides the connection's consistency for the statements
// run with this context — the per-statement control database/sql has no
// first-class shape for. Accepts "one", "quorum" or "all" (any case).
//
//	rows, err := db.QueryContext(skaidb.WithConsistency(ctx, "one"), "SELECT ...")
//
// An unrecognised level makes the statement fail rather than silently
// running at the connection default: a read that quietly used the wrong
// level is worse than one that errors.
func WithConsistency(ctx context.Context, level string) context.Context {
	return context.WithValue(ctx, consistencyKey{}, level)
}

// consistencyFor resolves the level for one statement: the context override
// if present, otherwise the connection's own.
func (c *conn) consistencyFor(ctx context.Context) (byte, error) {
	if ctx == nil {
		return c.consistency, nil
	}
	v, ok := ctx.Value(consistencyKey{}).(string)
	if !ok {
		return c.consistency, nil
	}
	switch strings.ToLower(v) {
	case "one":
		return consistencyOne, nil
	case "quorum":
		return consistencyQuorum, nil
	case "all":
		return consistencyAll, nil
	default:
		return 0, fmt.Errorf("skaidb: bad consistency %q", v)
	}
}

// applyContext wires ctx into this connection for one statement. A deadline
// becomes a socket deadline; cancellation forces any blocked read/write to
// return at once by setting a deadline in the past.
//
// An interrupted statement leaves the stream mid-frame, so the connection
// cannot safely be reused: it is marked broken and database/sql discards it
// (IsValid). The returned finish func clears the deadline, stops the watcher,
// and reports ctx.Err() in place of the resulting i/o timeout, so callers can
// test errors.Is(err, context.DeadlineExceeded) / context.Canceled as they
// would with any other driver.
func (c *conn) applyContext(ctx context.Context) func(error) error {
	if ctx == nil {
		return func(err error) error { return err }
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = c.nc.SetDeadline(dl)
	}
	if ctx.Done() == nil { // context.Background(): nothing can cancel it
		return func(err error) error {
			_ = c.nc.SetDeadline(time.Time{})
			return err
		}
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			c.broken.Store(true)
			_ = c.nc.SetDeadline(time.Now())
		case <-done:
		}
	}()
	var once sync.Once
	return func(err error) error {
		once.Do(func() {
			close(done)
			_ = c.nc.SetDeadline(time.Time{})
		})
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return err
	}
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	lvl, err := c.consistencyFor(ctx)
	if err != nil {
		return nil, err
	}
	c.stmtLevel = lvl
	defer func() { c.stmtLevel = c.consistency }()
	finish := c.applyContext(ctx)
	if ctx != nil && ctx.Value(streamKey{}) != nil && len(args) == 0 {
		rs, err := c.streamQuery(query)
		if err == nil {
			// A stream stays open while the caller iterates, so the deadline
			// and the cancel watcher must outlive this call — Close ends them.
			if r, ok := rs.(*rows); ok {
				r.release = finish
			}
			return rs, nil
		}
		if !errors.Is(err, errNoStreaming) {
			return nil, finish(err)
		}
		// fall through to the buffered path
	}
	if len(args) > 0 {
		r, err := c.preparedRoundtrip(query, args)
		if err == nil {
			rs, err := c.readRows(r)
			if err != nil {
				return nil, finish(err)
			}
			return rs, finish(nil)
		}
		if !errors.Is(err, errUnpreparable) {
			return nil, finish(err)
		}
	}
	sqlText, err := bind(query, args)
	if err != nil {
		return nil, finish(err)
	}
	rs, err := c.query(sqlText)
	if err != nil {
		return nil, finish(err)
	}
	return rs, finish(nil)
}

// preparedRoundtrip prepares (or reuses) the statement and executes it with
// typed parameters. Returns errUnpreparable for statement kinds the server
// declines, so the caller can fall back to client-side text binding.
func (c *conn) preparedRoundtrip(query string, args []driver.NamedValue) (*reader, error) {
	id, nparams, err := c.prepareServer(query)
	if err != nil {
		return nil, err
	}
	if nparams != len(args) {
		return nil, fmt.Errorf("skaidb: statement expects %d parameters, got %d", nparams, len(args))
	}
	return c.sendPrepared(id, args)
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	lvl, err := c.consistencyFor(ctx)
	if err != nil {
		return nil, err
	}
	c.stmtLevel = lvl
	defer func() { c.stmtLevel = c.consistency }()
	finish := c.applyContext(ctx)
	if len(args) > 0 {
		r, err := c.preparedRoundtrip(query, args)
		if err == nil {
			res, err := c.readResult(r)
			if err != nil {
				return nil, finish(err)
			}
			return res, finish(nil)
		}
		if !errors.Is(err, errUnpreparable) {
			return nil, finish(err)
		}
	}
	sqlText, err := bind(query, args)
	if err != nil {
		return nil, finish(err)
	}
	res, err := c.exec(sqlText)
	if err != nil {
		return nil, finish(err)
	}
	return res, finish(nil)
}

// encodeValue writes v as a TYPED skaidb value (tag + payload) — the inverse
// of decodeValue. This is what server-side prepared binding buys: arrays and
// documents have no SQL literal form, so they can only travel as typed
// values, never as interpolated text.
func encodeValue(out []byte, v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return append(out, 0), nil
	case bool:
		b := byte(0)
		if x {
			b = 1
		}
		return append(out, 1, b), nil
	case int:
		return binary.LittleEndian.AppendUint64(append(out, 2), uint64(int64(x))), nil
	case int32:
		return binary.LittleEndian.AppendUint64(append(out, 2), uint64(int64(x))), nil
	case int64:
		return binary.LittleEndian.AppendUint64(append(out, 2), uint64(x)), nil
	case float32:
		return encodeValue(out, float64(x))
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil, fmt.Errorf("skaidb: cannot bind NaN/Infinity")
		}
		return binary.LittleEndian.AppendUint64(append(out, 3), math.Float64bits(x)), nil
	case string:
		out = binary.LittleEndian.AppendUint32(append(out, 5), uint32(len(x)))
		return append(out, x...), nil
	case []byte:
		out = binary.LittleEndian.AppendUint32(append(out, 6), uint32(len(x)))
		return append(out, x...), nil
	case time.Time:
		return binary.LittleEndian.AppendUint64(append(out, 8), uint64(x.UnixMilli())), nil
	case []any:
		out = binary.LittleEndian.AppendUint32(append(out, 9), uint32(len(x)))
		var err error
		for _, item := range x {
			if out, err = encodeValue(out, item); err != nil {
				return nil, err
			}
		}
		return out, nil
	case map[string]any:
		out = binary.LittleEndian.AppendUint32(append(out, 10), uint32(len(x)))
		// Sorted so the same map always produces the same bytes.
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var err error
		for _, k := range keys {
			out = binary.LittleEndian.AppendUint32(out, uint32(len(k)))
			out = append(out, k...)
			if out, err = encodeValue(out, x[k]); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	// Fall back through reflection for named slice/map types.
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		items := make([]any, rv.Len())
		for i := range items {
			items[i] = rv.Index(i).Interface()
		}
		return encodeValue(out, items)
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return nil, fmt.Errorf("skaidb: document keys must be strings")
		}
		m := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			m[iter.Key().String()] = iter.Value().Interface()
		}
		return encodeValue(out, m)
	}
	return nil, fmt.Errorf("skaidb: cannot bind value of type %T", v)
}

// errUnpreparable marks a statement kind the server declines to prepare (DDL,
// session statements). The caller then falls back to client-side text binding.
var errUnpreparable = fmt.Errorf("skaidb: statement cannot be prepared")

// prepareServer prepares sqlText on the SERVER, returning (id, paramCount).
// Cached per connection: a prepared id is only meaningful on the connection
// that created it.
func (c *conn) prepareServer(sqlText string) (uint32, int, error) {
	if hit, ok := c.prepared[sqlText]; ok {
		return hit.id, hit.nparams, nil
	}
	if c.closed {
		return 0, 0, fmt.Errorf("skaidb: connection closed")
	}
	body := []byte(sqlText)
	req := append([]byte{2}, make([]byte, 0, 4+len(body))...)
	req = binary.LittleEndian.AppendUint32(req, uint32(len(body)))
	req = append(req, body...)
	if err := c.writeFrame(req); err != nil {
		return 0, 0, err
	}
	frame, err := c.readFrame()
	if err != nil {
		return 0, 0, err
	}
	r := &reader{buf: frame}
	switch r.u8() {
	case 4: // Prepared
		id := r.u32()
		n := int(r.u16())
		if r.err != nil {
			return 0, 0, r.err
		}
		if len(c.prepared) < 240 {
			c.prepared[sqlText] = preparedStmt{id: id, nparams: n}
		}
		return id, n, nil
	case 3: // Error
		return 0, 0, fmt.Errorf("%w: %s", errUnpreparable, r.text())
	}
	return 0, 0, fmt.Errorf("skaidb: unexpected prepare response")
}

// sendPrepared executes a prepared statement with TYPED parameters.
func (c *conn) sendPrepared(id uint32, args []driver.NamedValue) (*reader, error) {
	if c.closed {
		return nil, fmt.Errorf("skaidb: connection closed")
	}
	req := []byte{3, c.stmtLevel}
	req = binary.LittleEndian.AppendUint32(req, id)
	req = binary.LittleEndian.AppendUint16(req, uint16(len(args)))
	for _, a := range args {
		vb, err := encodeValue(nil, a.Value)
		if err != nil {
			return nil, err
		}
		req = binary.LittleEndian.AppendUint32(req, uint32(len(vb)))
		req = append(req, vb...)
	}
	if err := c.writeFrame(req); err != nil {
		return nil, c.transportErr(err, false)
	}
	frame, err := c.readFrame()
	if err != nil {
		return nil, c.transportErr(err, true)
	}
	return &reader{buf: frame}, nil
}

func (c *conn) sendQuery(sqlText string) (*reader, error) {
	if c.closed {
		return nil, fmt.Errorf("skaidb: connection closed")
	}
	body := []byte(sqlText)
	req := make([]byte, 0, 6+len(body))
	req = append(req, 1, c.stmtLevel)
	req = binary.LittleEndian.AppendUint32(req, uint32(len(body)))
	req = append(req, body...)
	if err := c.writeFrame(req); err != nil {
		return nil, c.transportErr(err, false)
	}
	frame, err := c.readFrame()
	if err != nil {
		return nil, c.transportErr(err, true)
	}
	return &reader{buf: frame}, nil
}

func (c *conn) query(sqlText string) (driver.Rows, error) {
	r, err := c.sendQuery(sqlText)
	if err != nil {
		return nil, err
	}
	return c.readRows(r)
}

// readRowsBody parses a Rows frame whose tag has already been consumed.
func (c *conn) readRowsBody(r *reader) (driver.Rows, error) {
	ncols := int(r.u32())
	cols := make([]string, ncols)
	for i := range cols {
		cols[i] = r.text()
	}
	nrows := int(r.u32())
	data := make([][]driver.Value, nrows)
	for i := 0; i < nrows; i++ {
		ncells := int(r.u32())
		row := make([]driver.Value, ncells)
		for j := 0; j < ncells; j++ {
			row[j] = decodeValue(&reader{buf: r.blob()})
		}
		data[i] = row
	}
	if r.err != nil {
		return nil, r.err
	}
	return &rows{cols: cols, data: data}, nil
}

func (c *conn) readRows(r *reader) (driver.Rows, error) {
	switch tag := r.u8(); tag {
	case 0: // Rows
		ncols := int(r.u32())
		cols := make([]string, ncols)
		for i := range cols {
			cols[i] = r.text()
		}
		nrows := int(r.u32())
		data := make([][]driver.Value, nrows)
		for i := 0; i < nrows; i++ {
			ncells := int(r.u32())
			row := make([]driver.Value, ncells)
			for j := 0; j < ncells; j++ {
				row[j] = decodeValue(&reader{buf: r.blob()})
			}
			data[i] = row
		}
		if r.err != nil {
			return nil, r.err
		}
		return &rows{cols: cols, data: data}, nil
	case 8: // ResultSets: a CALL whose body EMITted — first set current
		n := int(r.u32())
		sets := make([]resultSet, 0, n)
		for s := 0; s < n; s++ {
			ncols := int(r.u32())
			cols := make([]string, ncols)
			for i := range cols {
				cols[i] = r.text()
			}
			nrows := int(r.u32())
			data := make([][]driver.Value, nrows)
			for i := 0; i < nrows; i++ {
				ncells := int(r.u32())
				row := make([]driver.Value, ncells)
				for j := 0; j < ncells; j++ {
					row[j] = decodeValue(&reader{buf: r.blob()})
				}
				data[i] = row
			}
			sets = append(sets, resultSet{cols: cols, data: data})
		}
		if r.err != nil {
			return nil, r.err
		}
		if len(sets) == 0 {
			return &rows{cols: []string{}, data: nil}, nil
		}
		return &rows{cols: sets[0].cols, data: sets[0].data, more: sets[1:]}, nil
	case 1: // Mutation: a SELECT-less result; surface an empty row set
		return &rows{cols: []string{}, data: nil}, nil
	case 2: // Ddl
		return &rows{cols: []string{}, data: nil}, nil
	case 3:
		return nil, fmt.Errorf("skaidb: %s", r.text())
	default:
		return nil, fmt.Errorf("skaidb: unknown response tag %d", tag)
	}
}

func (c *conn) exec(sqlText string) (driver.Result, error) {
	r, err := c.sendQuery(sqlText)
	if err != nil {
		return nil, err
	}
	return c.readResult(r)
}

func (c *conn) readResult(r *reader) (driver.Result, error) {
	switch tag := r.u8(); tag {
	case 0: // Rows returned to Exec — discard, report 0 affected
		return result{affected: 0}, nil
	case 1:
		return result{affected: int64(r.u64())}, nil
	case 2:
		return result{affected: 0}, nil
	case 3:
		return nil, fmt.Errorf("skaidb: %s", r.text())
	default:
		return nil, fmt.Errorf("skaidb: unknown response tag %d", tag)
	}
}

// ---- stmt / rows / result --------------------------------------------------

type stmt struct {
	c     *conn
	query string
	n     int
}

func (s *stmt) Close() error  { return nil }
func (s *stmt) NumInput() int { return s.n }
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	sqlText, err := bind(s.query, named(args))
	if err != nil {
		return nil, err
	}
	return s.c.query(sqlText)
}
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	sqlText, err := bind(s.query, named(args))
	if err != nil {
		return nil, err
	}
	return s.c.exec(sqlText)
}

type rows struct {
	cols []string
	data [][]driver.Value
	pos  int
	// Further result sets of a multi-set reply (a CALL whose body EMITs),
	// served through database/sql's NextResultSet.
	more []resultSet
	// Streaming state (nil c = fully materialised, the classic path).
	c    *conn
	done bool
	// Ends the statement's context watcher/deadline; set only on the
	// streaming path, where the statement outlives QueryContext.
	release func(error) error
}

// streamRows issues OP_QUERY_STREAM and returns rows that pull chunks from
// the wire as database/sql asks for them, so the client holds one chunk
// rather than the whole result. database/sql's Rows.Next is already
// pull-based, so this is a better fit than materialising and walking a
// slice — which is what the non-streaming path does.
//
// Falls back to the buffered path when the server does not know the opcode
// (older servers answer Error) or when the statement is not row-producing.
func (c *conn) streamQuery(sqlText string) (driver.Rows, error) {
	if c.closed {
		return nil, fmt.Errorf("skaidb: connection closed")
	}
	body := []byte(sqlText)
	req := make([]byte, 0, 6+len(body))
	req = append(req, 5, c.stmtLevel) // OP_QUERY_STREAM
	req = binary.LittleEndian.AppendUint32(req, uint32(len(body)))
	req = append(req, body...)
	if err := c.writeFrame(req); err != nil {
		return nil, c.transportErr(err, false)
	}
	frame, err := c.readFrame()
	if err != nil {
		return nil, c.transportErr(err, true)
	}
	r := &reader{buf: frame}
	switch r.u8() {
	case 5: // RowsHeader — a real stream follows
		ncols := int(r.u32())
		cols := make([]string, ncols)
		for i := range cols {
			cols[i] = r.text()
		}
		if r.err != nil {
			return nil, r.err
		}
		return &rows{cols: cols, c: c}, nil
	case 0: // Rows — server chose to answer in one frame
		return c.readRowsBody(r)
	case 1, 2: // Mutation / Ddl through a streaming call
		return &rows{cols: []string{}, done: true}, nil
	case 3:
		msg := r.text()
		if strings.Contains(msg, "unknown opcode") {
			return nil, errNoStreaming
		}
		return nil, fmt.Errorf("skaidb: %s", msg)
	}
	return nil, fmt.Errorf("skaidb: unexpected response to stream request")
}

// errNoStreaming marks a server too old to know OP_QUERY_STREAM.
var errNoStreaming = fmt.Errorf("skaidb: server does not support streaming")

// fill pulls the next chunk. io.EOF once RowsEnd arrives.
func (r *rows) fill() error {
	for {
		frame, err := r.c.readFrame()
		if err != nil {
			r.done = true
			return r.c.transportErr(err, true)
		}
		rd := &reader{buf: frame}
		switch rd.u8() {
		case 6: // RowsChunk
			n := int(rd.u32())
			if n == 0 {
				continue // empty chunk: keep reading
			}
			data := make([][]driver.Value, n)
			for i := 0; i < n; i++ {
				ncells := int(rd.u32())
				row := make([]driver.Value, ncells)
				for j := 0; j < ncells; j++ {
					row[j] = decodeValue(&reader{buf: rd.blob()})
				}
				data[i] = row
			}
			if rd.err != nil {
				r.done = true
				return rd.err
			}
			r.data, r.pos = data, 0
			return nil
		case 7: // RowsEnd
			r.done = true
			return io.EOF
		case 3: // Error mid-stream: rows already delivered stay valid
			r.done = true
			return fmt.Errorf("skaidb: %s", rd.text())
		default:
			r.done = true
			return fmt.Errorf("skaidb: unexpected frame in stream")
		}
	}
}

// One decoded result set of a multi-set reply.
type resultSet struct {
	cols []string
	data [][]driver.Value
}

// HasNextResultSet reports whether a further result set follows
// (driver.RowsNextResultSet).
func (r *rows) HasNextResultSet() bool { return len(r.more) > 0 }

// NextResultSet advances to the next result set of a multi-set reply.
func (r *rows) NextResultSet() error {
	if len(r.more) == 0 {
		return io.EOF
	}
	next := r.more[0]
	r.more = r.more[1:]
	r.cols, r.data, r.pos = next.cols, next.data, 0
	return nil
}

func (r *rows) Columns() []string { return r.cols }
func (r *rows) Close() error {
	if r.release != nil {
		r.release(nil)
		r.release = nil
	}
	return nil
}
func (r *rows) Next(dest []driver.Value) error {
	if r.pos >= len(r.data) {
		if r.c == nil || r.done {
			return io.EOF
		}
		if err := r.fill(); err != nil {
			return err
		}
	}
	copy(dest, r.data[r.pos])
	r.pos++
	return nil
}

type result struct{ affected int64 }

// LastInsertId is not carried on the wire: a generated id comes back as a
// row — `db.QueryRow("INSERT INTO t (name) VALUES (?) RETURNING id", n).Scan(&id)`.
func (r result) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("skaidb: no LastInsertId — use INSERT … RETURNING id with QueryRow")
}
func (r result) RowsAffected() (int64, error) { return r.affected, nil }

// ---- value decoding (§4) ---------------------------------------------------

// decodeValue returns a database/sql-compatible driver.Value. Composite values
// (Array, Document) are surfaced as a JSON string so they can be Scanned into a
// string or []byte.
func decodeValue(r *reader) driver.Value {
	v := decodeNative(r)
	switch v.(type) {
	case []interface{}, map[string]interface{}:
		return jsonString(v)
	default:
		return v
	}
}

// decodeNative decodes into a plain Go value, recursing into composites.
func decodeNative(r *reader) interface{} {
	switch tag := r.u8(); tag {
	case 0:
		return nil
	case 1:
		return r.u8() != 0
	case 2:
		return int64(r.u64())
	case 3:
		return math.Float64frombits(r.u64())
	case 4: // Decimal -> string
		mant := r.take(16)
		scale := r.u32()
		return decimalString(mant, scale)
	case 5:
		return r.text()
	case 6:
		return append([]byte(nil), r.blob()...)
	case 7: // Uuid -> canonical string
		b := r.take(16)
		return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
	case 8: // Timestamp(ms) -> time.Time
		return time.UnixMilli(int64(r.u64())).UTC()
	case 9: // Array
		n := int(r.u32())
		arr := make([]interface{}, n)
		for i := range arr {
			arr[i] = decodeNative(r)
		}
		return arr
	case 10: // Document (insertion order not preserved in a Go map)
		n := int(r.u32())
		m := make(map[string]interface{}, n)
		for i := 0; i < n; i++ {
			k := r.text()
			m[k] = decodeNative(r)
		}
		return m
	default:
		r.err = fmt.Errorf("skaidb: unknown value tag %d", tag)
		return nil
	}
}

type reader struct {
	buf []byte
	pos int
	err error
}

func (r *reader) take(n int) []byte {
	if r.err != nil {
		return make([]byte, n)
	}
	if r.pos+n > len(r.buf) {
		r.err = fmt.Errorf("skaidb: truncated server message")
		return make([]byte, n)
	}
	b := r.buf[r.pos : r.pos+n]
	r.pos += n
	return b
}
func (r *reader) u8() byte    { return r.take(1)[0] }
func (r *reader) u16() uint16 { return binary.LittleEndian.Uint16(r.take(2)) }
func (r *reader) u32() uint32 { return binary.LittleEndian.Uint32(r.take(4)) }
func (r *reader) u64() uint64 { return binary.LittleEndian.Uint64(r.take(8)) }
func (r *reader) blob() []byte {
	n := int(r.u32())
	return r.take(n)
}
func (r *reader) text() string { return string(r.blob()) }

// ---- parameter binding (§5) ------------------------------------------------

func named(args []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(args))
	for i, a := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: a}
	}
	return out
}

func bind(query string, args []driver.NamedValue) (string, error) {
	if len(args) == 0 {
		return query, nil
	}
	var b strings.Builder
	inStr := false
	idx := 0
	for i := 0; i < len(query); i++ {
		ch := query[i]
		if inStr {
			b.WriteByte(ch)
			if ch == '\'' {
				if i+1 < len(query) && query[i+1] == '\'' {
					b.WriteByte('\'')
					i++
				} else {
					inStr = false
				}
			}
			continue
		}
		if ch == '\'' {
			inStr = true
			b.WriteByte(ch)
			continue
		}
		if ch == '?' {
			if idx >= len(args) {
				return "", fmt.Errorf("skaidb: more placeholders than parameters")
			}
			s, err := quote(args[idx].Value)
			if err != nil {
				return "", err
			}
			b.WriteString(s)
			idx++
			continue
		}
		b.WriteByte(ch)
	}
	if idx != len(args) {
		return "", fmt.Errorf("skaidb: more parameters than placeholders")
	}
	return b.String(), nil
}

func quote(v driver.Value) (string, error) {
	switch x := v.(type) {
	case nil:
		return "NULL", nil
	case bool:
		if x {
			return "TRUE", nil
		}
		return "FALSE", nil
	case int64:
		return strconv.FormatInt(x, 10), nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return "", fmt.Errorf("skaidb: cannot bind NaN/Infinity")
		}
		return strconv.FormatFloat(x, 'g', -1, 64), nil
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'", nil
	case []byte:
		return "'" + hex.EncodeToString(x) + "'", nil
	case time.Time:
		return strconv.FormatInt(x.UnixMilli(), 10), nil
	default:
		return "", fmt.Errorf("skaidb: cannot bind value of type %T", v)
	}
}

func stripStrings(s string) string {
	var b strings.Builder
	inStr := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inStr {
			if ch == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
					continue
				}
				inStr = false
			}
			continue
		}
		if ch == '\'' {
			inStr = true
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

// ---- small crypto + helpers ------------------------------------------------

func hmacSHA256(key, msg []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(msg)
	return m.Sum(nil)
}

// pbkdf2SHA256 implements PBKDF2-HMAC-SHA256 (kept local to avoid x/crypto).
func pbkdf2SHA256(password, salt []byte, iter, keyLen int) []byte {
	hashLen := sha256.Size
	numBlocks := (keyLen + hashLen - 1) / hashLen
	var out []byte
	for block := 1; block <= numBlocks; block++ {
		var idx [4]byte
		binary.BigEndian.PutUint32(idx[:], uint32(block))
		u := hmacSHA256(password, append(append([]byte(nil), salt...), idx[:]...))
		t := append([]byte(nil), u...)
		for i := 1; i < iter; i++ {
			u = hmacSHA256(password, u)
			for j := range t {
				t[j] ^= u[j]
			}
		}
		out = append(out, t...)
	}
	return out[:keyLen]
}

func appendStr(b []byte, s string) []byte {
	b = binary.LittleEndian.AppendUint32(b, uint32(len(s)))
	return append(b, s...)
}

func mustFrame(b []byte, err error) []byte {
	if err != nil {
		return nil
	}
	return b
}

func decimalString(mant []byte, scale uint32) string {
	// little-endian signed 16-byte integer -> decimal string with scale
	v := bigIntFromLE(mant)
	s := v.String()
	if scale == 0 {
		return s
	}
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	for uint32(len(s)) <= scale {
		s = "0" + s
	}
	point := uint32(len(s)) - scale
	res := s[:point] + "." + s[point:]
	if neg {
		res = "-" + res
	}
	return res
}

// bigIntFromLE interprets b as a little-endian two's-complement integer.
func bigIntFromLE(b []byte) *big.Int {
	le := make([]byte, len(b))
	for i := range b {
		le[i] = b[len(b)-1-i] // reverse to big-endian
	}
	v := new(big.Int).SetBytes(le)
	if len(b) > 0 && b[len(b)-1]&0x80 != 0 { // negative
		v.Sub(v, new(big.Int).Lsh(big.NewInt(1), uint(8*len(b))))
	}
	return v
}

func jsonString(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "null"
	}
	return string(b)
}

package skaidb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

// The driver implements driver.DriverContext, so sql.Open goes through
// OpenConnector and a malformed DSN fails at sql.Open — the behaviour the
// README promises — instead of surfacing on the first statement.
func TestSQLOpenRejectsMalformedDSN(t *testing.T) {
	if _, ok := driver.Driver(&drv{}).(driver.DriverContext); !ok {
		t.Fatal("drv must implement driver.DriverContext")
	}
	for _, tc := range []struct{ dsn, want string }{
		{"skaidb://localhost/?consistency=eventual", `skaidb: bad consistency "eventual"`},
		{"skaidb://localhost/?tls=maybe", `skaidb: bad tls "maybe"`},
		{"skaidb://localhost/?tls_insecure=yes", `skaidb: bad tls_insecure "yes"`},
		{"skaidb://h1:notaport", `skaidb: bad seed`},
		{"skaidb://h1:70000", `skaidb: bad seed`},
		{"skaidb://[::1", `skaidb: bad seed`},
		{"skaidb://", `skaidb: DSN has no host`},
		{"mysql://localhost", `skaidb: DSN scheme must be skaidb://`},
	} {
		db, err := sql.Open("skaidb", tc.dsn)
		if err == nil {
			db.Close()
			t.Errorf("sql.Open(%q) must fail", tc.dsn)
			continue
		}
		if !strings.HasPrefix(err.Error(), tc.want) {
			t.Errorf("sql.Open(%q) = %q, want prefix %q", tc.dsn, err, tc.want)
		}
		// The same error straight from the connector factory.
		if _, cerr := (&drv{}).OpenConnector(tc.dsn); cerr == nil || cerr.Error() != err.Error() {
			t.Errorf("OpenConnector(%q) = %v, sql.Open said %v", tc.dsn, cerr, err)
		}
	}
}

// A well-formed DSN still opens lazily: sql.Open succeeds without dialling,
// even when nothing listens at the seed, and db.Driver() is this driver.
func TestSQLOpenIsLazyForWellFormedDSN(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("no loopback listener available: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close() // now guaranteed closed: any dial would be refused
	db, err := sql.Open("skaidb", "skaidb://ada:pw@"+addr+"/app?consistency=one&tls=false")
	if err != nil {
		t.Fatalf("sql.Open on a well-formed DSN must not fail: %v", err)
	}
	defer db.Close()
	if _, ok := db.Driver().(*drv); !ok {
		t.Errorf("db.Driver() = %T, want *drv", db.Driver())
	}
	// Only now is anything dialled, and it fails as the connect error.
	err = db.Ping()
	if err == nil || !strings.HasPrefix(err.Error(), "skaidb: no reachable endpoint in ["+addr+"]: skaidb: connect failed:") {
		t.Errorf("Ping against a closed port = %v", err)
	}
}

// OpenConnector keeps the parsed DSN: the connector dials what was parsed,
// including the seed list, the consistency level and the session database.
func TestOpenConnectorKeepsParsedConfig(t *testing.T) {
	c, err := (&drv{}).OpenConnector("skaidb://ada:s%40cret@h1,h2:7001/app?consistency=all")
	if err != nil {
		t.Fatal(err)
	}
	cn, ok := c.(*connector)
	if !ok {
		t.Fatalf("OpenConnector returned %T", c)
	}
	if got := strings.Join(cn.cfg.addrs, ","); got != "h1:7000,h2:7001" {
		t.Errorf("addrs = %q", got)
	}
	if cn.cfg.user != "ada" || cn.cfg.password != "s@cret" || cn.cfg.database != "app" || cn.cfg.consistency != consistencyAll {
		t.Errorf("cfg = %+v", cn.cfg)
	}
	if _, ok := cn.Driver().(*drv); !ok {
		t.Errorf("Driver() = %T", cn.Driver())
	}
}

// Connect does the real dial: SCRAM handshake, Hello, USE from the DSN
// path — the connection it returns is usable through database/sql.
func TestConnectorConnectDialsAndAuthenticates(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("no loopback listener available: %v", err)
	}
	defer ln.Close()
	seen := make(chan string, 8)
	go func() {
		s, err := ln.Accept()
		if err != nil {
			return
		}
		defer s.Close()
		// 1. handshake start: opcode 10, user, client nonce
		req, err := readReq(s)
		if err != nil || len(req) == 0 || req[0] != 10 {
			seen <- "bad start"
			return
		}
		r := &reader{buf: req[1:]}
		seen <- "start user=" + r.text()
		nonce := r.text()
		ch := []byte{11}
		ch = binary.LittleEndian.AppendUint32(ch, 4)
		ch = append(ch, 's', 'a', 'l', 't')
		ch = binary.LittleEndian.AppendUint32(ch, 1)
		ch = appendStr(ch, nonce+"srv")
		s.Write(serverFrame(ch))
		// 2. proof: opcode 12 + 32 bytes; accept with a 32-byte signature
		// (unchecked by the client for an empty password)
		if req, err = readReq(s); err != nil || len(req) != 33 || req[0] != 12 {
			seen <- "bad proof"
			return
		}
		seen <- "proof"
		s.Write(serverFrame(append([]byte{13, 1}, make([]byte, 32)...)))
		// 3. Hello: opcode 8; answer like a server that knows it (Ddl)
		if req, err = readReq(s); err != nil || len(req) == 0 || req[0] != 8 {
			seen <- "bad hello"
			return
		}
		seen <- "hello"
		s.Write(ddlFrame())
		// 4. USE "app" from the DSN path: opcode 1, level, sql
		if req, err = readReq(s); err != nil || len(req) < 6 || req[0] != 1 {
			seen <- "bad use"
			return
		}
		seen <- "sql " + string(req[6:])
		s.Write(ddlFrame())
		// 5. the test's own statement through *sql.DB
		if req, err = readReq(s); err != nil || len(req) < 6 || req[0] != 1 {
			seen <- "bad stmt"
			return
		}
		seen <- "sql " + string(req[6:])
		s.Write(mutationFrame(3))
	}()

	db, err := sql.Open("skaidb", "skaidb://ada@"+ln.Addr().String()+"/app?consistency=one")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := db.ExecContext(ctx, "DELETE FROM t")
	if err != nil {
		t.Fatalf("statement over a connector-dialled connection: %v", err)
	}
	if n, _ := res.RowsAffected(); n != 3 {
		t.Errorf("RowsAffected = %d, want 3", n)
	}
	want := []string{"start user=ada", "proof", "hello", `sql USE "app"`, "sql DELETE FROM t"}
	for _, w := range want {
		select {
		case got := <-seen:
			if got != w {
				t.Fatalf("server saw %q, want %q", got, w)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("server never saw %q", w)
		}
	}
}

// A context that is already cancelled when the pool asks for a connection
// is honoured without dialling.
func TestConnectorConnectHonoursCancelledContext(t *testing.T) {
	c, err := (&drv{}).OpenConnector("skaidb://192.0.2.1:7000") // TEST-NET, never dialled
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if _, err := c.Connect(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Connect on a cancelled ctx = %v, want context.Canceled", err)
	}
	if time.Since(start) > time.Second {
		t.Errorf("Connect dialled despite the cancelled context (%v)", time.Since(start))
	}
}

// Connect on a seed nothing listens at fails with the seed-list error, and
// database/sql reports it from the statement, not from sql.Open.
func TestConnectorConnectReportsUnreachableSeeds(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("no loopback listener available: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	c, err := (&drv{}).OpenConnector("skaidb://" + addr)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Connect(context.Background())
	if err == nil || !strings.HasPrefix(err.Error(), "skaidb: no reachable endpoint in ["+addr+"]: skaidb: connect failed:") {
		t.Fatalf("Connect = %v", err)
	}
}

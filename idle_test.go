package skaidb

// A connection the server closed while it sat idle. Across a real network
// the write to such a socket succeeds — the reset it provokes has not come
// back yet — and only the read fails, after the request has left; these
// tests get that on loopback from a server that shuts down its sending side
// alone (lossy.halfClose). The driver looks at the socket before it writes.

import (
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	debit    = "UPDATE accounts SET balance = balance - 10 WHERE id = 1"
	debitArg = "UPDATE accounts SET balance = balance - ? WHERE id = 1"
	balance1 = "SELECT balance FROM accounts WHERE id = 1"
)

// statementKinds are a write, a read, and a write with arguments — which
// runs under a statement id the connection already holds, so nothing but the
// statement itself is sent.
var statementKinds = []struct {
	name, sql string
	run       func(ctx context.Context, q querier) error
}{
	{"a write", debit, func(ctx context.Context, q querier) error {
		_, err := q.ExecContext(ctx, debit)
		return err
	}},
	{"a read", balance1, func(ctx context.Context, q querier) error {
		rows, err := q.QueryContext(ctx, balance1)
		if err != nil {
			return err
		}
		return rows.Close()
	}},
	{"a write with arguments", debitArg, func(ctx context.Context, q querier) error {
		_, err := q.ExecContext(ctx, debitArg, 10)
		return err
	}},
}

// querier is a *sql.DB or a *sql.Conn.
type querier interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// closings are the two ways an idle connection is found closed: end-of-file
// with the socket still writable, and a reset.
var closings = []struct {
	name  string
	close func(l *lossy, n int)
}{
	{"closed", (*lossy).halfClose},
	{"reset", (*lossy).reset},
}

// The server closed the pool's connection while it sat idle — a restart, a
// failover, an idle timeout. The next statement costs nothing: the driver
// sees the closed socket before it writes, the statement runs on another
// connection, and no error reaches the caller. Nothing of the statement was
// sent on the dead one — a write the server never saw is safe to send again,
// one it may have seen is not.
func TestAConnectionClosedWhileIdleCostsNoStatement(t *testing.T) {
	ctx := context.Background()
	for _, how := range closings {
		for _, kind := range statementKinds {
			t.Run(how.name+", "+kind.name, func(t *testing.T) {
				l, db := lossyServer(t, nil)
				if err := kind.run(ctx, db); err != nil {
					t.Fatalf("the first statement: %v", err)
				}
				how.close(l, 0)
				if err := kind.run(ctx, db); err != nil {
					t.Fatalf("the statement after the connection was closed while idle: %v", err)
				}
				l.wantReceived([]string{kind.sql}, []string{kind.sql})
				if n := l.lateBytes(0); n != 0 {
					t.Errorf("%d bytes were written to the closed connection", n)
				}
			})
		}
	}
}

// A checkout gives a working connection: the ones the server closed while
// they sat idle are dropped by the pool, not handed to a *sql.Conn — which
// database/sql never moves to another connection — however many there are.
func TestACheckoutNeverHandsOutAConnectionClosedWhileIdle(t *testing.T) {
	ctx := context.Background()
	for _, how := range closings {
		t.Run(how.name, func(t *testing.T) {
			const dead = 4
			l, db := lossyServer(t, nil)
			idle(t, db, dead)
			for i := 0; i < dead; i++ {
				how.close(l, i)
			}
			for i := 0; i < dead; i++ { // more checkouts than database/sql retries one
				conn, err := db.Conn(ctx)
				if err != nil {
					t.Fatalf("checkout %d: %v", i, err)
				}
				if _, err := conn.ExecContext(ctx, debit); err != nil {
					t.Fatalf("a statement on checkout %d: %v", i, err)
				}
				conn.Close()
			}
			for i := 0; i < dead; i++ {
				if got := l.received(i); len(got) != 1 {
					t.Errorf("closed connection %d received %q after it was warmed", i, got[1:])
				}
				if n := l.lateBytes(i); n != 0 {
					t.Errorf("%d bytes were written to closed connection %d", n, i)
				}
			}
		})
	}
}

// A *sql.Conn whose connection the server closed while its owner held it
// idle: the statement fails — database/sql puts no other connection under a
// *sql.Conn — but with driver.ErrBadConn, which says that nothing was sent,
// not with a loss "mid-statement" that leaves the caller wondering whether
// a write was applied. And nothing was sent.
func TestAHeldConnClosedWhileIdleSendsNothing(t *testing.T) {
	ctx := context.Background()
	for _, how := range closings {
		for _, kind := range statementKinds {
			t.Run(how.name+", "+kind.name, func(t *testing.T) {
				l, db := lossyServer(t, nil)
				conn, err := db.Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				if err := kind.run(ctx, conn); err != nil {
					t.Fatalf("the first statement: %v", err)
				}
				how.close(l, 0)
				if err := kind.run(ctx, conn); !errors.Is(err, driver.ErrBadConn) {
					t.Fatalf("err = %v, want driver.ErrBadConn: nothing was sent", err)
				}
				l.wantReceived([]string{kind.sql})
				if n := l.lateBytes(0); n != 0 {
					t.Errorf("%d bytes were written to the closed connection", n)
				}
			})
		}
	}
}

// Rule 7 is untouched: a transaction whose connection the server closed
// between two of its statements is lost. The statement that finds the socket
// closed is not sent — on the dead connection or on another — the function's
// later statements fail too, and Transaction reports that nothing was
// committed.
func TestATransactionWhoseConnectionIsClosedWhileIdleIsLost(t *testing.T) {
	ctx := context.Background()
	for _, how := range closings {
		t.Run(how.name, func(t *testing.T) {
			l, db := lossyServer(t, nil)
			var e2, e3 error
			err := Transaction(ctx, db, carriesOn(ctx, func() { how.close(l, 0) }, &e2, &e3))
			if !errors.Is(e2, driver.ErrBadConn) {
				t.Errorf("the statement that found the connection closed: err = %v, want driver.ErrBadConn", e2)
			}
			if e3 == nil {
				t.Error("the statement after the loss succeeded")
			}
			if !errors.Is(err, driver.ErrBadConn) || !strings.Contains(err.Error(), "before COMMIT was sent") {
				t.Errorf("the transaction: err = %v, want the lost connection reported, COMMIT not sent", err)
			}
			l.wantReceived([]string{"BEGIN", updateSQL})
			if n := l.lateBytes(0); n != 0 {
				t.Errorf("%d bytes were written to the closed connection", n)
			}
		})
	}
}

// The opening statement is not inside anything yet: with the pool's
// connection closed while idle, the transaction opens — and runs — on
// another one.
func TestATransactionOpensElsewhereWhenThePooledConnectionWasClosedWhileIdle(t *testing.T) {
	ctx := context.Background()
	for _, how := range closings {
		t.Run(how.name, func(t *testing.T) {
			l, db := lossyServer(t, nil)
			if _, err := db.ExecContext(ctx, debit); err != nil {
				t.Fatal(err)
			}
			how.close(l, 0)
			runs := 0
			if err := Transaction(ctx, db, update(ctx, &runs)); err != nil {
				t.Fatalf("Transaction: %v", err)
			}
			l.wantReceived([]string{debit}, []string{"BEGIN", updateSQL, "COMMIT"})
			if n := l.lateBytes(0); n != 0 {
				t.Errorf("%d bytes were written to the closed connection", n)
			}
		})
	}
}

// A connection found closed while idle says that the connection ended — an
// idle timeout ends every one of them — not that its member is unwell.
// Nothing counts against the member: with eject_errors=1, one closed socket
// would otherwise take a healthy member out of rotation.
func TestAConnectionClosedWhileIdleDoesNotCountAgainstItsMember(t *testing.T) {
	ms := fakeMembers(t, "a", "b")
	c, db := lbDB(t, endpointsOf(ms), "eject_errors=1")
	first := who(t, db)
	for _, m := range ms {
		m.hangUp()
	}
	who(t, db)
	for _, m := range ms {
		if got := stateOf(c, m.endpoint); got != "ready" {
			t.Errorf("member %s is %s after the connection to %s was closed while idle", m.name, got, first)
		}
	}
	conns := 0
	for _, m := range ms {
		conns += len(m.received())
	}
	if conns != 2 {
		t.Errorf("%d connections were dialled, want the closed one and its replacement", conns)
	}
}

// A connection that is not idle is not looked at. While a streamed result is
// open the server is sending — chunks wait unread on the socket, which is
// what a closed connection looks like too — and the rows keep arriving; once
// they are read, the connection takes the next statement.
func TestAStreamInFlightIsNotTakenForAClosedConnection(t *testing.T) {
	l, db := streamServer(t)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rows, err := conn.QueryContext(WithStreaming(ctx), scan)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // every chunk has arrived, none is read
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil || n == 0 {
		t.Fatalf("the stream: %d rows, err = %v", n, err)
	}
	rows.Close()
	if _, err := conn.ExecContext(ctx, debit); err != nil {
		t.Fatalf("the statement after the stream: %v", err)
	}
	l.wantReceived([]string{scan, debit})
}

// tlsMember is a fake server behind TLS: certificate login, then every
// statement answered with one affected row. It keeps the TCP socket under
// each connection, and what each connection was sent.
type tlsMember struct {
	ln   net.Listener
	addr string

	mu    sync.Mutex
	socks []net.Conn
	got   [][]string
}

func newTLSMember(t *testing.T, pki *testPKI) *tlsMember {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("no loopback listener available: %v", err)
	}
	m := &tlsMember{ln: ln, addr: ln.Addr().String()}
	cfg := &tls.Config{Certificates: []tls.Certificate{pki.server},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pki.caPool}
	var wg sync.WaitGroup
	go func() {
		for {
			raw, err := ln.Accept()
			if err != nil {
				return
			}
			m.mu.Lock()
			n := len(m.socks)
			m.socks = append(m.socks, raw)
			m.got = append(m.got, nil)
			m.mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer raw.Close()
				s := tls.Server(raw, cfg)
				if _, err := readReq(s); err != nil { // AuthStart (EXTERNAL)
					return
				}
				s.Write(serverFrame(append([]byte{13, 1}, make([]byte, 32)...)))
				for {
					req, err := readReq(s)
					if err != nil {
						return
					}
					if req[0] != 1 {
						s.Write(ddlFrame())
						continue
					}
					m.mu.Lock()
					m.got[n] = append(m.got[n], string(req[6:]))
					m.mu.Unlock()
					s.Write(mutationFrame(1))
				}
			}()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		m.mu.Lock()
		for _, s := range m.socks {
			s.Close()
		}
		m.mu.Unlock()
		wg.Wait()
	})
	return m
}

func (m *tlsMember) received() [][]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([][]string, len(m.got))
	for i, c := range m.got {
		out[i] = append([]string(nil), c...)
	}
	return out
}

// Under TLS the driver looks at the socket, not at the decrypted stream. A
// healthy connection that sat idle is used again — nothing TLS itself left
// on the socket is taken for the server's goodbye — and one the server
// closed meanwhile (its process gone: no TLS alert, the socket just ends)
// costs no statement.
func TestATLSConnectionClosedWhileIdleCostsNoStatement(t *testing.T) {
	pki := newTestPKI(t, "ada")
	m := newTLSMember(t, pki)
	db, err := sql.Open("skaidb", "skaidb://"+m.addr+"/?auth_mechanism=certificate"+
		"&tls_ca="+pki.caFile+"&tls_client_cert="+pki.clientCert+"&tls_client_key="+pki.clientKey)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := db.ExecContext(ctx, debit); err != nil {
			t.Fatalf("statement %d on the idle connection: %v", i, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := m.received(); len(got) != 1 || len(got[0]) != 3 {
		t.Fatalf("the member received %q, want the three statements on one connection", got)
	}
	m.mu.Lock()
	raw := m.socks[0]
	m.mu.Unlock()
	if err := raw.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // the end-of-file has to arrive
	if _, err := db.ExecContext(ctx, debit); err != nil {
		t.Fatalf("the statement after the connection was closed while idle: %v", err)
	}
	if got := m.received(); len(got) != 2 || len(got[0]) != 3 || len(got[1]) != 1 {
		t.Fatalf("the member received %q, want the fourth statement alone on a second connection", got)
	}
}

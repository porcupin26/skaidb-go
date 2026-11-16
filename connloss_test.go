package skaidb

// Connection loss as database/sql and the transaction helper see it, against
// a fake server that serves every connection the pool dials and can lose
// them one by one.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// hangup, returned by a lossy reply, closes the server's end of the
// connection instead of answering.
var hangup = []byte{0xff}

// lossy is a fake server behind a *sql.DB: each Connect dials it afresh, and
// it answers every statement with what reply returns for the connection's
// number (in dial order) and the SQL text — nil is a Mutation of one row,
// silent no answer, hangup a closed socket, anything else the bytes to
// write (one frame, or several for a stream). It records what each
// connection received. reply runs on the connection's own goroutine.
type lossy struct {
	t     *testing.T
	ln    net.Listener
	reply func(conn int, sql string) []byte

	mu       sync.Mutex
	accepted chan struct{}
	clients  []*conn         // the driver's end of each connection
	got      [][]string      // statements received, per connection
	gone     []chan struct{} // closed once a connection's server end is done
}

func lossyServer(t *testing.T, reply func(conn int, sql string) []byte) (*lossy, *sql.DB) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("no loopback listener available: %v", err)
	}
	if reply == nil {
		reply = func(int, string) []byte { return nil }
	}
	l := &lossy{t: t, ln: ln, reply: reply, accepted: make(chan struct{}, 64)}
	go l.accept()
	db := sql.OpenDB(l)
	t.Cleanup(func() {
		db.Close()
		ln.Close()
		l.mu.Lock()
		for _, c := range l.clients {
			c.nc.Close()
		}
		gone := append([]chan struct{}(nil), l.gone...)
		l.mu.Unlock()
		for _, g := range gone {
			<-g
		}
	})
	return l, db
}

func (l *lossy) accept() {
	for {
		s, err := l.ln.Accept()
		if err != nil {
			return
		}
		l.mu.Lock()
		n := len(l.got)
		l.got = append(l.got, nil)
		gone := make(chan struct{})
		l.gone = append(l.gone, gone)
		l.mu.Unlock()
		l.accepted <- struct{}{}
		go func() {
			defer close(gone)
			defer s.Close()
			l.serve(n, s)
		}()
	}
}

func (l *lossy) serve(n int, s net.Conn) {
	var prepared []string // by id - 1
	for {
		req, err := readReq(s)
		if err != nil {
			return
		}
		var text string
		switch req[0] {
		case 1, 5: // OP_QUERY, OP_QUERY_STREAM
			text = string(req[6:])
		case 2: // OP_PREPARE
			prepared = append(prepared, string(req[5:]))
			f := preparedFrame(uint32(len(prepared)), uint16(strings.Count(string(req[5:]), "?")))
			if _, err := s.Write(f); err != nil {
				return
			}
			continue
		case 3: // OP_EXECUTE
			text = prepared[binary.LittleEndian.Uint32(req[2:])-1]
		default:
			if _, err := s.Write(errorFrame("unexpected request")); err != nil {
				return
			}
			continue
		}
		l.mu.Lock()
		l.got[n] = append(l.got[n], text)
		l.mu.Unlock()
		f := l.reply(n, text)
		switch {
		case f == nil:
			f = mutationFrame(1)
		case len(f) == 0: // silent
			continue
		case len(f) == 1 && f[0] == hangup[0]:
			return
		}
		if _, err := s.Write(f); err != nil {
			return
		}
	}
}

// mortal is a client socket that can be made to die: from then on every
// write fails before a byte leaves, as on a socket whose peer is long gone.
type mortal struct {
	net.Conn
	left atomic.Int64 // writes it still carries; negative: no limit
}

func (m *mortal) Write(p []byte) (int, error) {
	if m.left.Load() == 0 {
		return 0, &net.OpError{Op: "write", Net: "tcp", Err: errors.New("broken pipe")}
	}
	m.left.Add(-1)
	return m.Conn.Write(p)
}

// kill makes connection n dead, as an idle connection is once its server
// has restarted: nobody knows until the next write.
func (l *lossy) kill(n int) { l.client(n).nc.(*mortal).left.Store(0) }

// killAfter lets connection n send that many more requests, then kills it.
func (l *lossy) killAfter(n, requests int) {
	l.client(n).nc.(*mortal).left.Store(2 * int64(requests)) // a request is two writes
}

// Connect implements driver.Connector: a new connection to the fake server,
// past the handshake.
func (l *lossy) Connect(context.Context) (driver.Conn, error) {
	nc, err := net.Dial("tcp", l.ln.Addr().String())
	if err != nil {
		return nil, err
	}
	<-l.accepted
	m := &mortal{Conn: nc}
	m.left.Store(-1)
	c := &conn{nc: m, consistency: consistencyQuorum, stmtLevel: consistencyQuorum,
		prepared: map[string]preparedStmt{}}
	l.mu.Lock()
	l.clients = append(l.clients, c)
	l.mu.Unlock()
	return c, nil
}

func (l *lossy) Driver() driver.Driver { return &drv{} }

// dials is how many connections the pool has opened.
func (l *lossy) dials() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.clients)
}

// client is the driver's end of connection n.
func (l *lossy) client(n int) *conn {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.clients[n]
}

// received is what connection n was sent, in order.
func (l *lossy) received(n int) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.got[n]...)
}

// wantReceived checks, connection by connection, what the server was sent.
func (l *lossy) wantReceived(want ...[]string) {
	l.t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.got) != len(want) {
		l.t.Fatalf("%d connections reached the server (%q), want %d", len(l.got), l.got, len(want))
	}
	for i := range want {
		if strings.Join(l.got[i], "\n") != strings.Join(want[i], "\n") {
			l.t.Errorf("connection %d received\n  %q\nwant\n  %q", i, l.got[i], want[i])
		}
	}
}

// goneSoon reports whether the server saw connection n closed, waiting a
// little for the close to travel.
func (l *lossy) goneSoon(n int) bool {
	l.mu.Lock()
	gone := l.gone[n]
	l.mu.Unlock()
	select {
	case <-gone:
		return true
	case <-time.After(2 * time.Second):
		return false
	}
}

// socketClosed reports whether the driver has closed c's socket (or its end
// of a net.Pipe).
func socketClosed(c *conn) bool {
	err := c.nc.SetReadDeadline(time.Time{})
	return errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe)
}

// ---- a retired connection ---------------------------------------------------

// A statement cut short — here by its deadline, inside a transaction —
// retires the connection, and its socket closes there and then: the server
// drops the session and the open transaction with it, instead of keeping
// both (and the rows the transaction claimed) until the *sql.Conn is closed.
//
// Nothing more is sent on it either. The reply to the statement that was
// given up on is still on its way: a statement written behind it would read
// that reply as its own, and a COMMIT behind that one the statement's — the
// helper would report a commit the server refused.
func TestAStatementCutShortInATransactionClosesTheSocketAtOnce(t *testing.T) {
	const slow = "UPDATE accounts SET balance = 0 WHERE id = 2"
	const next = "INSERT INTO ledger (id) VALUES (7)"
	l, db := lossyServer(t, func(_ int, sql string) []byte {
		switch sql {
		case slow:
			time.Sleep(150 * time.Millisecond) // answered after the client gave up
		case "COMMIT":
			return errorFrame("write quorum unavailable")
		}
		return nil
	})
	ctx := context.Background()
	var cut, after error
	closedAtOnce := false
	err := Transaction(ctx, db, func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, updateSQL); err != nil {
			return err
		}
		short, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
		_, cut = conn.ExecContext(short, slow)
		cancel()
		closedAtOnce = socketClosed(l.client(0))
		_, after = conn.ExecContext(ctx, next)
		return nil
	})
	if !errors.Is(cut, context.DeadlineExceeded) {
		t.Fatalf("the statement cut short: err = %v, want context.DeadlineExceeded", cut)
	}
	if !closedAtOnce {
		t.Error("the socket was still open after the statement on it was cut short")
	}
	if !errors.Is(after, errRetired) {
		t.Errorf("the statement after it: err = %v, want the retired-connection error", after)
	}
	if err == nil {
		t.Error("Transaction reported a commit; the server refused the COMMIT, and its answer was never read")
	}
	if !l.goneSoon(0) {
		t.Error("the server never saw the connection close")
	}
	l.wantReceived([]string{"BEGIN", updateSQL, slow})
}

// Every way a connection is retired closes its socket, and a statement on
// it afterwards is refused without a byte written.
func TestARetiredConnectionClosesItsSocket(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		answer func(req []byte) [][]byte // nil: the server hangs up
		break_ func(t *testing.T, c *conn)
	}{
		{"the peer hangs up mid-statement", nil, func(t *testing.T, c *conn) {
			if _, err := c.ExecContext(ctx, "DELETE FROM t", nil); err == nil || !strings.Contains(err.Error(), "connection lost mid-statement") {
				t.Fatalf("err = %v, want the mid-statement loss", err)
			}
		}},
		{"a cancelled context", func([]byte) [][]byte { return nil }, func(t *testing.T, c *conn) {
			cctx, cancel := context.WithCancel(ctx)
			time.AfterFunc(20*time.Millisecond, cancel)
			if _, err := c.ExecContext(cctx, "DELETE FROM t", nil); !errors.Is(err, context.Canceled) {
				t.Fatalf("err = %v, want context.Canceled", err)
			}
		}},
		{"a frame that does not belong in a stream", func([]byte) [][]byte {
			return [][]byte{headerFrame("id"), serverFrame([]byte{9})}
		}, func(t *testing.T, c *conn) {
			rs, err := c.streamQuery("SELECT id FROM t", nil)
			if err != nil {
				t.Fatalf("streamQuery: %v", err)
			}
			if err := rs.Next(make([]driver.Value, 1)); err == nil {
				t.Fatal("the unexpected frame was taken for a row")
			}
			rs.Close()
		}},
		{"a stream too long to drain", func([]byte) [][]byte {
			frames := [][]byte{headerFrame("id")}
			for i := 0; i < drainMaxFrames*3; i++ {
				frames = append(frames, chunkFrame(int64(i)))
			}
			return append(frames, endFrame())
		}, func(t *testing.T, c *conn) {
			rs, err := c.streamQuery("SELECT id FROM big", nil)
			if err != nil {
				t.Fatalf("streamQuery: %v", err)
			}
			if err := rs.Next(make([]driver.Value, 1)); err != nil {
				t.Fatalf("first row: %v", err)
			}
			rs.Close()
		}},
		{"an answer that is not a batch's", func(req []byte) [][]byte {
			if req[0] == 2 {
				return [][]byte{preparedFrame(9, 2)}
			}
			return [][]byte{serverFrame([]byte{9})}
		}, func(t *testing.T, c *conn) {
			if _, err := c.ExecBatch(ctx, "INSERT INTO t (id, name) VALUES (?, ?)", intRows(2)); err == nil {
				t.Fatal("the unexpected answer was taken for the batch's")
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var c *conn
			var rec *recorder
			if tc.answer == nil {
				rec = &recorder{}
				c = fakeServer(t, func(s net.Conn) {
					if req, err := readReq(s); err == nil {
						rec.mu.Lock()
						rec.reqs = append(rec.reqs, req)
						rec.mu.Unlock()
					}
				})
			} else {
				c, rec = recordingServer(t, tc.answer)
			}
			tc.break_(t, c)
			if c.IsValid() {
				t.Fatal("the connection was not retired")
			}
			if !socketClosed(c) {
				t.Error("the retired connection's socket is still open")
			}
			sent := len(rec.all())
			// Bounded: a statement that did go out would wait for an answer.
			bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			if _, err := c.ExecContext(bounded, "DELETE FROM after", nil); !errors.Is(err, errRetired) {
				t.Errorf("a statement on the retired connection: err = %v, want the retired-connection error", err)
			}
			if _, err := c.ExecContext(bounded, "DELETE FROM after WHERE id = ?", []driver.NamedValue{{Ordinal: 1, Value: int64(1)}}); !errors.Is(err, errRetired) {
				t.Errorf("a statement with arguments on the retired connection: err = %v, want the retired-connection error", err)
			}
			if n := len(rec.all()); n != sent {
				t.Errorf("%d requests were written to the retired connection", n-sent)
			}
			if err := c.Close(); err != nil {
				t.Errorf("Close of a retired connection: %v", err)
			}
		})
	}
}

// ---- the opening statement on a dead pooled connection ----------------------

// idle leaves n connections idle in db's pool, each having run a statement.
func idle(t *testing.T, db *sql.DB, n int) {
	t.Helper()
	db.SetMaxIdleConns(n)
	ctx := context.Background()
	conns := make([]*sql.Conn, n)
	for i := range conns {
		c, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.ExecContext(ctx, "DELETE FROM warm"); err != nil {
			t.Fatal(err)
		}
		conns[i] = c
	}
	for _, c := range conns {
		c.Close()
	}
	if got := db.Stats().Idle; got != n {
		t.Fatalf("%d connections idle, want %d", got, n)
	}
}

// The server restarted while the pool's connection sat idle: the socket is
// dead and nobody knows. database/sql runs an ordinary statement on another
// connection when the first write fails; a pinned *sql.Conn gets no such
// retry, so the opening statement would come back as "driver: bad
// connection". Nothing is open yet — the transaction opens on a fresh
// connection and runs there.
func TestTransactionOpensOnAnotherConnectionWhenThePooledOneIsDead(t *testing.T) {
	l, db := lossyServer(t, nil)
	ctx := context.Background()
	runs := 0
	if err := Transaction(ctx, db, update(ctx, &runs)); err != nil {
		t.Fatalf("the first Transaction: %v", err)
	}
	l.kill(0)
	if err := Transaction(ctx, db, update(ctx, &runs)); err != nil {
		t.Fatalf("Transaction on a pool whose idle connection is dead: %v", err)
	}
	if runs != 2 {
		t.Errorf("the function ran %d times, want once per transaction", runs)
	}
	l.wantReceived(
		[]string{"BEGIN", updateSQL, "COMMIT"},
		[]string{"BEGIN", updateSQL, "COMMIT"})
	if s := db.Stats(); s.OpenConnections != 1 || s.InUse != 0 {
		t.Errorf("pool: %d open, %d in use; want the new connection, idle", s.OpenConnections, s.InUse)
	}
}

// However many idle connections died with the server: database/sql ends an
// ordinary statement's retries on a new connection, so it succeeds whatever
// the pool holds, and the opening statement does the same.
func TestTransactionOpensWhateverTheNumberOfDeadConnections(t *testing.T) {
	const dead = 5
	ctx := context.Background()
	for name, run := range map[string]func(db *sql.DB) error{
		"an ordinary statement": func(db *sql.DB) error {
			_, err := db.ExecContext(ctx, updateSQL)
			return err
		},
		"Transaction": func(db *sql.DB) error {
			runs := 0
			return Transaction(ctx, db, update(ctx, &runs))
		},
	} {
		t.Run(name, func(t *testing.T) {
			l, db := lossyServer(t, nil)
			idle(t, db, dead)
			for i := 0; i < dead; i++ {
				l.kill(i)
			}
			if err := run(db); err != nil {
				t.Fatalf("with %d dead connections in the pool: %v", dead, err)
			}
			if got := l.received(l.dials() - 1); got[len(got)-1] != updateSQL && got[len(got)-1] != "COMMIT" {
				t.Errorf("the last connection dialled received %q, want the statement", got)
			}
		})
	}
}

// deadOnArrival is a lossy server's connector whose connections are dead
// from the start.
type deadOnArrival struct{ *lossy }

func (d deadOnArrival) Connect(ctx context.Context) (driver.Conn, error) {
	c, err := d.lossy.Connect(ctx)
	if err == nil {
		c.(*conn).nc.(*mortal).left.Store(0)
	}
	return c, err
}

// The search for a live connection ends. When every connection is dead on
// arrival, the opening statement gives up after as many as database/sql
// dials for an ordinary statement, with the same error.
func TestTransactionGivesUpWhenEveryConnectionIsDead(t *testing.T) {
	ctx := context.Background()
	tries := map[string]int{}
	for name, run := range map[string]func(db *sql.DB) error{
		"an ordinary statement": func(db *sql.DB) error {
			_, err := db.ExecContext(ctx, updateSQL)
			return err
		},
		"Transaction": func(db *sql.DB) error {
			runs := 0
			return Transaction(ctx, db, update(ctx, &runs))
		},
	} {
		l, _ := lossyServer(t, nil)
		db := sql.OpenDB(deadOnArrival{l})
		defer db.Close()
		if err := run(db); !errors.Is(err, driver.ErrBadConn) {
			t.Fatalf("%s: err = %v, want driver.ErrBadConn", name, err)
		}
		tries[name] = l.dials()
	}
	if tries["Transaction"] != tries["an ordinary statement"] {
		t.Errorf("Transaction dialled %d connections, an ordinary statement %d", tries["Transaction"], tries["an ordinary statement"])
	}
}

// A run after a conflict opens on the connection the refused COMMIT came
// back on — unless that one died in between, and then on another: the
// refused COMMIT ended the transaction, so nothing is open here either.
func TestTransactionOpensElsewhereWhenItsConnectionDiesBetweenRuns(t *testing.T) {
	l, db := lossyServer(t, func(conn int, sql string) []byte {
		if conn == 0 && sql == "COMMIT" {
			return errorFrame(conflictMsg)
		}
		return nil
	})
	ctx := context.Background()
	runs := 0
	err := Transaction(ctx, db, func(conn *sql.Conn) error {
		if runs == 0 {
			l.killAfter(0, 2) // this run's statement and its COMMIT, no more
		}
		return update(ctx, &runs)(conn)
	})
	if err != nil {
		t.Fatalf("Transaction: %v", err)
	}
	if runs != 2 {
		t.Errorf("the function ran %d times, want 2", runs)
	}
	l.wantReceived(
		[]string{"BEGIN", updateSQL, "COMMIT"},
		[]string{"BEGIN", updateSQL, "COMMIT"})
}

// Only the opening statement moves. Once it has been answered the
// transaction is on its connection for good: a statement of the function
// that finds the connection dead is not sent on another one — it would
// commit there on its own — and the transaction is not started over.
func TestTransactionStaysOnItsConnectionOnceItIsOpen(t *testing.T) {
	l, db := lossyServer(t, nil)
	ctx := context.Background()
	runs := 0
	err := Transaction(ctx, db, func(conn *sql.Conn) error {
		l.kill(0)
		return update(ctx, &runs)(conn)
	})
	if !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("err = %v, want the statement's driver.ErrBadConn", err)
	}
	if runs != 1 {
		t.Errorf("the function ran %d times, want 1", runs)
	}
	l.wantReceived([]string{"BEGIN"})
	// The pool is as it is after any lost connection: the next statement
	// dials.
	if _, err := db.ExecContext(ctx, "DELETE FROM audit"); err != nil {
		t.Fatalf("the pool after the lost transaction: %v", err)
	}
	l.wantReceived([]string{"BEGIN"}, []string{"DELETE FROM audit"})
}

// TransactionConn has the caller's connection and no other: on a dead one
// its opening statement fails as any statement on that *sql.Conn does.
func TestTransactionConnDoesNotLeaveTheCallersConnection(t *testing.T) {
	l, db := lossyServer(t, nil)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `USE "app"`); err != nil {
		t.Fatal(err)
	}
	l.kill(0)
	runs := 0
	err = TransactionConn(ctx, conn, update(ctx, &runs))
	if !errors.Is(err, driver.ErrBadConn) || runs != 0 {
		t.Fatalf("err = %v after %d runs; want driver.ErrBadConn and no run", err, runs)
	}
	l.wantReceived([]string{`USE "app"`})
}

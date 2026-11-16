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

	// The connection whose next OP_PREPARE is answered with a closed socket;
	// -1 for none.
	dropPrepare atomic.Int64
	// Executes that named a statement id the connection never issued.
	unknownIDs atomic.Int64

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
	l.dropPrepare.Store(-1)
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
			if l.dropPrepare.CompareAndSwap(int64(n), -1) {
				return
			}
			prepared = append(prepared, string(req[5:]))
			f := preparedFrame(uint32(len(prepared)), uint16(strings.Count(string(req[5:]), "?")))
			if _, err := s.Write(f); err != nil {
				return
			}
			continue
		case 3, 7: // OP_EXECUTE, OP_EXECUTE_BATCH
			id := binary.LittleEndian.Uint32(req[2:])
			if id == 0 || int(id) > len(prepared) {
				l.unknownIDs.Add(1)
				if _, err := s.Write(errorFrame("unknown prepared statement id")); err != nil {
					return
				}
				continue
			}
			text = prepared[id-1]
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
		// An answer of a kind the request cannot have: whatever it answers,
		// or begins, it is not this statement's, and the next statement on
		// the connection would read what follows it as its own.
		{"an answer that is not a prepare's", func([]byte) [][]byte {
			return [][]byte{serverFrame([]byte{9})}
		}, func(t *testing.T, c *conn) {
			_, err := c.ExecContext(ctx, "INSERT INTO t (id) VALUES (?)", []driver.NamedValue{{Ordinal: 1, Value: int64(1)}})
			if err == nil || !strings.Contains(err.Error(), "unexpected prepare response") {
				t.Fatalf("err = %v, want the unexpected prepare response", err)
			}
		}},
		{"an answer that is not a statement's", func([]byte) [][]byte {
			return [][]byte{serverFrame([]byte{9})}
		}, func(t *testing.T, c *conn) {
			if _, err := c.ExecContext(ctx, "DELETE FROM t", nil); err == nil || !strings.Contains(err.Error(), "unknown response tag 9") {
				t.Fatalf("err = %v, want the unknown response tag", err)
			}
		}},
		{"an answer that is not a query's", func([]byte) [][]byte {
			return [][]byte{serverFrame([]byte{9})}
		}, func(t *testing.T, c *conn) {
			if _, err := c.QueryContext(ctx, "SELECT id FROM t", nil); err == nil || !strings.Contains(err.Error(), "unknown response tag 9") {
				t.Fatalf("err = %v, want the unknown response tag", err)
			}
		}},
		{"an answer that does not open a stream", func([]byte) [][]byte {
			return [][]byte{serverFrame([]byte{9})}
		}, func(t *testing.T, c *conn) {
			if _, err := c.streamQuery("SELECT id FROM t", nil); err == nil || !strings.Contains(err.Error(), "unexpected response to stream request") {
				t.Fatalf("err = %v, want the unexpected response", err)
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

// What an answer of no known kind costs when the connection stays in the
// pool: the frame behind it is still on the socket, and the pool's next
// statement reads it as its own answer — here a count of rows affected that
// belongs to nothing it ran. The connection is dropped instead, and the next
// statement gets its answer on another.
func TestAnAnswerOfNoKnownKindLeavesNothingForTheNextStatement(t *testing.T) {
	const odd = "DELETE FROM t WHERE id = 1"
	l, db := lossyServer(t, func(_ int, sql string) []byte {
		if sql == odd {
			return append(serverFrame([]byte{9}), mutationFrame(7)...)
		}
		return nil
	})
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, odd); err == nil || !strings.Contains(err.Error(), "unknown response tag 9") {
		t.Fatalf("err = %v, want the unknown response tag", err)
	}
	res, err := db.ExecContext(ctx, updateSQL)
	if err != nil {
		t.Fatalf("the next statement: %v", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Errorf("the next statement affected %d rows: it read the frame left behind the unknown answer", n)
	}
	l.wantReceived([]string{odd}, []string{updateSQL})
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

// ---- a connection lost inside a transaction ---------------------------------

const (
	second = "UPDATE accounts SET balance = 110 WHERE id = 2"
	third  = "INSERT INTO ledger (id) VALUES (7)"
)

// carriesOn is a transaction function that does not stop at an error: its
// second statement fails with the connection, it runs a third all the same,
// and returns nil.
func carriesOn(ctx context.Context, lose func(), e2, e3 *error) func(*sql.Conn) error {
	return func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, updateSQL); err != nil {
			return err
		}
		lose()
		_, *e2 = conn.ExecContext(ctx, second)
		_, *e3 = conn.ExecContext(ctx, third)
		return nil
	}
}

// No statement of a transaction leaves its session (PROTOCOL.md §6 rule 7).
// The connection is lost under the function's second statement; the function
// carries on and returns nil. Its third statement must fail as well — sent
// on another connection it would commit on its own — and so must the
// transaction, with a transport error that says what is known: COMMIT was
// not sent, so nothing was committed. Nothing reaches the server on a
// second connection, and the pool is usable afterwards as after any loss.
func TestATransactionWhoseConnectionIsLostSendsNothingElsewhere(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		// lose makes connection 0 fail under the second statement.
		reply func(conn int, sql string) []byte
		lose  func(l *lossy)
		// what connection 0 received of the transaction
		want []string
		e2   func(error) bool
	}{
		{
			name: "the reply never comes",
			reply: func(conn int, sql string) []byte {
				if conn == 0 && sql == second {
					return hangup
				}
				return nil
			},
			lose: func(*lossy) {},
			want: []string{"BEGIN", updateSQL, second},
			e2: func(err error) bool {
				return err != nil && strings.Contains(err.Error(), "connection lost mid-statement")
			},
		},
		{
			name:  "the request cannot be written",
			lose:  func(l *lossy) { l.kill(0) },
			want:  []string{"BEGIN", updateSQL},
			e2:    func(err error) bool { return errors.Is(err, driver.ErrBadConn) },
			reply: nil,
		},
	} {
		for _, pinned := range []bool{false, true} {
			name := tc.name + ", Transaction"
			if pinned {
				name = tc.name + ", TransactionConn"
			}
			t.Run(name, func(t *testing.T) {
				l, db := lossyServer(t, tc.reply)
				var e2, e3, err error
				fn := carriesOn(ctx, func() { tc.lose(l) }, &e2, &e3)
				var held *sql.Conn
				if pinned {
					held, err = db.Conn(ctx)
					if err != nil {
						t.Fatal(err)
					}
					defer held.Close()
					err = TransactionConn(ctx, held, fn)
				} else {
					err = Transaction(ctx, db, fn)
				}
				if !tc.e2(e2) {
					t.Errorf("the statement the connection was lost under: err = %v, want the transport error", e2)
				}
				if e3 == nil {
					t.Error("the statement after the loss succeeded")
				}
				if !errors.Is(err, driver.ErrBadConn) || !strings.Contains(err.Error(), "before COMMIT was sent") {
					t.Errorf("the transaction: err = %v, want the lost connection reported, COMMIT not sent", err)
				}
				l.wantReceived(tc.want)
				if pinned {
					// The caller's *sql.Conn is as dead as after any loss, and
					// still sends nothing.
					if _, err := held.ExecContext(ctx, third); err == nil {
						t.Error("a statement on the caller's lost connection succeeded")
					}
					l.wantReceived(tc.want)
					held.Close()
				}
				// The pool dials for the next statement, as after any loss.
				if _, err := db.ExecContext(ctx, "DELETE FROM audit"); err != nil {
					t.Fatalf("an ordinary statement afterwards: %v", err)
				}
				l.wantReceived(tc.want, []string{"DELETE FROM audit"})
			})
		}
	}
}

// When the loss is under the COMMIT itself the transaction's fate is not
// known, and the error says so: it does not claim that nothing was
// committed, and it is not one a caller would retry on.
func TestATransactionLostUnderItsCommitDoesNotClaimARollback(t *testing.T) {
	l, db := lossyServer(t, func(_ int, sql string) []byte {
		if sql == "COMMIT" {
			return hangup
		}
		return nil
	})
	ctx := context.Background()
	runs := 0
	err := Transaction(ctx, db, update(ctx, &runs))
	if err == nil || !strings.Contains(err.Error(), "statement may or may not have applied") {
		t.Fatalf("err = %v, want the mid-statement loss of the COMMIT", err)
	}
	if errors.Is(err, driver.ErrBadConn) || strings.Contains(err.Error(), "nothing was committed") {
		t.Errorf("err = %v: the COMMIT was sent, its outcome is unknown", err)
	}
	if runs != 1 {
		t.Errorf("the function ran %d times, want 1", runs)
	}
	l.wantReceived([]string{"BEGIN", updateSQL, "COMMIT"})
}

// A COMMIT that could not be written is the other case: it never left, so
// the transaction was not committed, and the error is the same as for a
// connection the function lost.
func TestATransactionWhoseCommitCannotBeWrittenWasNotCommitted(t *testing.T) {
	l, db := lossyServer(t, nil)
	ctx := context.Background()
	runs := 0
	err := Transaction(ctx, db, func(conn *sql.Conn) error {
		l.killAfter(0, 1) // the function's statement, and no more
		return update(ctx, &runs)(conn)
	})
	if !errors.Is(err, driver.ErrBadConn) || !strings.Contains(err.Error(), "before COMMIT was sent") {
		t.Fatalf("err = %v, want the lost connection reported, COMMIT not sent", err)
	}
	if runs != 1 {
		t.Errorf("the function ran %d times, want 1", runs)
	}
	l.wantReceived([]string{"BEGIN", updateSQL})
}

// ---- a stream the transaction's function left open --------------------------

const scan = "SELECT id FROM big"

// leavesAStream is a transaction function that opens a streamed result,
// reads one row of it and returns result without closing it. The rows go to
// left, for the test to close when it chooses.
func leavesAStream(ctx context.Context, left chan<- *sql.Rows, result func() error) func(*sql.Conn) error {
	return func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, updateSQL); err != nil {
			return err
		}
		rows, err := conn.QueryContext(WithStreaming(ctx), scan)
		if err != nil {
			return err
		}
		rows.Next()
		left <- rows
		return result()
	}
}

// streamServer answers scan with a short stream — all of it sent at once, so
// what the function does not read is there to be drained — and everything
// else with a success.
func streamServer(t *testing.T) (*lossy, *sql.DB) {
	stream := append(append(headerFrame("id"), chunkFrame(1, 2)...), endFrame()...)
	return lossyServer(t, func(_ int, sql string) []byte {
		if sql == scan {
			return stream
		}
		return nil
	})
}

// A connection that is streaming a result takes no other statement, so
// neither COMMIT nor ROLLBACK can be sent while the function's stream is
// open. Left at that, the transaction stays open on the server, on a live
// socket: once the rows are closed the connection is back in the pool, and
// whatever runs on it next runs inside the abandoned transaction, never to
// be committed. So the connection is closed instead — the server ends the
// transaction with the session — and the caller is told: the function's own
// error, or the busy-streaming one. Transaction does not wait for the rows
// to be closed either; the connection leaves the pool when they are.
func TestATransactionThatLeavesAStreamOpenIsNotJoinable(t *testing.T) {
	ctx := context.Background()
	failed := errors.New("insufficient funds")
	for _, tc := range []struct {
		name   string
		result func() error
		want   error
	}{
		{"the function returns nil", func() error { return nil }, errStreamBusy},
		{"the function returns an error", func() error { return failed }, failed},
		{"the function panics", func() error { panic(failed) }, failed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l, db := streamServer(t)
			left := make(chan *sql.Rows, 1)
			done := make(chan error, 1)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						done <- r.(error)
					}
				}()
				done <- Transaction(ctx, db, leavesAStream(ctx, left, tc.result))
			}()
			rows := <-left
			var err error
			select {
			case err = <-done:
			case <-time.After(2 * time.Second):
				t.Error("Transaction is waiting for the function's rows to be closed")
				rows.Close()
				err = <-done
			}
			if !errors.Is(err, tc.want) {
				t.Errorf("err = %v, want %v", err, tc.want)
			}
			if !l.goneSoon(0) {
				t.Error("the connection is still open on the server, and its transaction with it")
			}
			// The pool's next statement is not the abandoned transaction's.
			if _, err := db.ExecContext(ctx, "DELETE FROM audit"); err != nil {
				t.Fatalf("an ordinary statement afterwards: %v", err)
			}
			l.wantReceived([]string{"BEGIN", updateSQL, scan}, []string{"DELETE FROM audit"})
			// Closing the rows is all it takes for the pool to drop the
			// transaction's connection.
			rows.Close()
			waitFor(t, "the pool to drop the transaction's connection", func() bool {
				return db.Stats().OpenConnections == 1
			})
		})
	}
}

// The same on a connection the caller holds: it is retired, so that what the
// caller sends on it once the rows are closed is refused rather than run
// inside the transaction it believes is over.
func TestATransactionConnThatLeavesAStreamOpenRetiresTheConnection(t *testing.T) {
	l, db := streamServer(t)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	left := make(chan *sql.Rows, 1)
	err = TransactionConn(ctx, conn, leavesAStream(ctx, left, func() error { return nil }))
	if !errors.Is(err, errStreamBusy) {
		t.Errorf("err = %v, want the busy-streaming error", err)
	}
	if !l.goneSoon(0) {
		t.Error("the connection is still open on the server, and its transaction with it")
	}
	(<-left).Close()
	if _, err := conn.ExecContext(ctx, "DELETE FROM audit"); !errors.Is(err, errRetired) {
		t.Errorf("a statement on the connection afterwards: err = %v, want the retired-connection error", err)
	}
	l.wantReceived([]string{"BEGIN", updateSQL, scan})
	if err := conn.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// ---- a statement with arguments on a dead pooled connection -----------------

// A statement with arguments is prepared on its connection first. When that
// round trip fails, nothing has run — a PREPARE executes nothing — so the
// statement is database/sql's to run on another connection, like one whose
// own request could not be written, and the dead connection leaves the
// pool. Reported as the raw socket error instead, the statement failed and
// the connection went back to the pool for the next statement to fail on.
func TestAStatementWithArgumentsMovesOffADeadPooledConnection(t *testing.T) {
	const insert = "INSERT INTO t (id) VALUES (?)"
	ctx := context.Background()
	for name, lose := range map[string]func(l *lossy){
		"the prepare cannot be written": func(l *lossy) { l.kill(0) },
		"the prepare's reply is lost":   func(l *lossy) { l.dropPrepare.Store(0) },
	} {
		for kind, run := range map[string]func(db *sql.DB) error{
			"Exec": func(db *sql.DB) error {
				_, err := db.ExecContext(ctx, insert, 1)
				return err
			},
			"Query": func(db *sql.DB) error {
				rows, err := db.QueryContext(ctx, insert, 1)
				if err == nil {
					rows.Close()
				}
				return err
			},
			"ExecBatch": func(db *sql.DB) error {
				_, err := ExecBatch(ctx, db, insert, [][]any{{1}, {2}})
				return err
			},
		} {
			t.Run(name+", "+kind, func(t *testing.T) {
				l, db := lossyServer(t, nil)
				if _, err := db.ExecContext(ctx, "DELETE FROM warm"); err != nil {
					t.Fatal(err)
				}
				lose(l)
				if err := run(db); err != nil {
					t.Fatalf("on a pool whose idle connection is dead: %v", err)
				}
				l.wantReceived([]string{"DELETE FROM warm"}, []string{insert})
				if c := l.client(0); c.IsValid() || !socketClosed(c) {
					t.Error("the dead connection was not retired")
				}
				if s := db.Stats(); s.OpenConnections != 1 || s.InUse != 0 {
					t.Errorf("pool: %d open, %d in use; want the new connection alone, idle", s.OpenConnections, s.InUse)
				}
			})
		}
	}
}

// ExecBatch takes its own connection from the pool, as Transaction does,
// and finds a live one the same way: whatever the number of idle
// connections that died with the server, and no further than database/sql
// goes when every connection is dead on arrival.
func TestExecBatchRunsWhateverTheNumberOfDeadConnections(t *testing.T) {
	const insert = "INSERT INTO t (id) VALUES (?)"
	const dead = 5
	ctx := context.Background()
	l, db := lossyServer(t, nil)
	idle(t, db, dead)
	for i := 0; i < dead; i++ {
		l.kill(i)
	}
	n, err := ExecBatch(ctx, db, insert, [][]any{{1}, {2}})
	if err != nil || n != 1 {
		t.Fatalf("with %d dead connections in the pool: n = %d, err = %v", dead, n, err)
	}
	if got := l.received(l.dials() - 1); len(got) != 1 || got[0] != insert {
		t.Errorf("the last connection dialled received %q, want the batch", got)
	}

	l, _ = lossyServer(t, nil)
	db = sql.OpenDB(deadOnArrival{l})
	defer db.Close()
	if _, err := ExecBatch(ctx, db, insert, [][]any{{1}, {2}}); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("with every connection dead on arrival: err = %v, want driver.ErrBadConn", err)
	}
	if got := l.dials(); got != 3 {
		t.Errorf("%d connections dialled, want database/sql's 3", got)
	}
}

// ---- a result set still open on the connection ------------------------------

const list = "SELECT id FROM accounts"

// listServer answers list with two buffered rows and everything else with a
// success.
func listServer(t *testing.T) (*lossy, *sql.DB) {
	rows := serverFrame(fakeRowsFrame([]string{"id"}, [][]any{{"a"}, {"b"}}))
	return lossyServer(t, func(_ int, sql string) []byte {
		if sql == list {
			return rows
		}
		return nil
	})
}

// within runs f and reports whether it returned in time, with its error.
func within(d time.Duration, f func() error) (error, bool) {
	done := make(chan error, 1)
	go func() { done <- f() }()
	select {
	case err := <-done:
		return err, true
	case <-time.After(d):
		return nil, false
	}
}

// A statement on a *sql.Conn that finds the connection dead, while a result
// set read earlier on that connection is still open — the loop that reads
// rows and writes one statement per row. driver.ErrBadConn there makes
// database/sql close the *sql.Conn on the spot, and that close waits for
// the open rows: the statement never returns, the rows are never closed.
// Nothing would retry it either — only a pinned connection runs a statement
// with a result set open — so it is reported as the lost connection it is.
func TestAStatementOnADeadConnectionReturnsWhileRowsAreOpen(t *testing.T) {
	ctx := context.Background()
	for name, run := range map[string]func(db *sql.DB, fn func(*sql.Conn) error) error{
		"on a *sql.Conn": func(db *sql.DB, fn func(*sql.Conn) error) error {
			conn, err := db.Conn(ctx)
			if err != nil {
				return err
			}
			defer conn.Close()
			return fn(conn)
		},
		"in a Transaction": func(db *sql.DB, fn func(*sql.Conn) error) error {
			return Transaction(ctx, db, fn)
		},
	} {
		t.Run(name, func(t *testing.T) {
			l, db := listServer(t)
			open := make(chan *sql.Rows, 1)
			seen := 0
			err, returned := within(2*time.Second, func() error {
				return run(db, func(conn *sql.Conn) error {
					rows, err := conn.QueryContext(ctx, list)
					if err != nil {
						return err
					}
					defer rows.Close()
					open <- rows
					for rows.Next() {
						seen++
						l.kill(0)
						if _, err := conn.ExecContext(ctx, updateSQL); err != nil {
							return err
						}
					}
					return rows.Err()
				})
			})
			if !returned {
				t.Error("the statement on the dead connection is waiting for the rows to be closed, in the goroutine that reads them")
				(<-open).Close() // let it go
				return
			}
			if err == nil || !strings.Contains(err.Error(), "connection lost before the statement was sent") || errors.Is(err, driver.ErrBadConn) {
				t.Errorf("err = %v, want the lost connection, and not driver.ErrBadConn", err)
			}
			if seen != 1 {
				t.Errorf("%d rows read before the statement failed, want 1", seen)
			}
			if c := l.client(0); c.IsValid() || !socketClosed(c) {
				t.Error("the dead connection was not retired")
			}
			// Without rows open the same failure is database/sql's to retry:
			// the pool's next statement runs on a new connection.
			if _, err := db.ExecContext(ctx, "DELETE FROM audit"); err != nil {
				t.Fatalf("an ordinary statement afterwards: %v", err)
			}
		})
	}
}

// A buffered result the transaction's function left open does not stand in
// the way of the COMMIT, but (*sql.Conn).Close waits for it: Transaction
// would sit there, its transaction long committed, until somebody closed
// the rows. It returns when the transaction is over; the connection goes
// back to the pool when the rows are closed.
func TestTransactionDoesNotWaitForRowsItsFunctionLeftOpen(t *testing.T) {
	l, db := listServer(t)
	ctx := context.Background()
	left := make(chan *sql.Rows, 1)
	err, returned := within(2*time.Second, func() error {
		return Transaction(ctx, db, func(conn *sql.Conn) error {
			rows, err := conn.QueryContext(ctx, list)
			if err != nil {
				return err
			}
			rows.Next()
			left <- rows
			_, err = conn.ExecContext(ctx, updateSQL)
			return err
		})
	})
	rows := <-left
	if !returned {
		t.Error("Transaction is waiting for the function's rows to be closed")
	} else if err != nil {
		t.Errorf("Transaction: %v", err)
	}
	l.wantReceived([]string{"BEGIN", list, updateSQL, "COMMIT"})
	rows.Close()
	waitFor(t, "the connection to be back in the pool", func() bool {
		s := db.Stats()
		return s.OpenConnections == 1 && s.InUse == 0
	})
	// Healthy, and with no transaction open: the pool's next statement
	// takes it.
	if _, err := db.ExecContext(ctx, "DELETE FROM audit"); err != nil {
		t.Fatal(err)
	}
	l.wantReceived([]string{"BEGIN", list, updateSQL, "COMMIT", "DELETE FROM audit"})
}

// A COMMIT that cannot be written is the same case whether or not the
// function left rows open: it never left, the transaction was not
// committed, and Transaction says so with the one error for it.
func TestACommitThatCannotBeWrittenWithRowsOpenWasNotCommitted(t *testing.T) {
	l, db := listServer(t)
	ctx := context.Background()
	left := make(chan *sql.Rows, 1)
	err, returned := within(2*time.Second, func() error {
		return Transaction(ctx, db, func(conn *sql.Conn) error {
			rows, err := conn.QueryContext(ctx, list)
			if err != nil {
				return err
			}
			rows.Next()
			left <- rows
			l.kill(0)
			return nil
		})
	})
	defer (<-left).Close()
	if !returned {
		t.Fatal("Transaction did not return")
	}
	if !errors.Is(err, driver.ErrBadConn) || !strings.Contains(err.Error(), "before COMMIT was sent") {
		t.Fatalf("err = %v, want the lost connection reported, COMMIT not sent", err)
	}
	l.wantReceived([]string{"BEGIN", list})
}

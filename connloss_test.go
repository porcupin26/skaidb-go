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

// Connect implements driver.Connector: a new connection to the fake server,
// past the handshake.
func (l *lossy) Connect(context.Context) (driver.Conn, error) {
	nc, err := net.Dial("tcp", l.ln.Addr().String())
	if err != nil {
		return nil, err
	}
	<-l.accepted
	c := &conn{nc: nc, consistency: consistencyQuorum, stmtLevel: consistencyQuorum,
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

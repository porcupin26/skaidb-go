package skaidb

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// A context override picks the level for one statement; anything else keeps
// the connection's. An unrecognised level must ERROR rather than silently
// falling back — a read that quietly used the wrong consistency is worse
// than one that fails.
func TestConsistencyForResolvesPerStatement(t *testing.T) {
	c := &conn{consistency: consistencyQuorum}
	for _, tc := range []struct {
		name string
		ctx  context.Context
		want byte
	}{
		{"nil context", nil, consistencyQuorum},
		{"no override", context.Background(), consistencyQuorum},
		{"one", WithConsistency(context.Background(), "one"), consistencyOne},
		{"all", WithConsistency(context.Background(), "all"), consistencyAll},
		{"quorum", WithConsistency(context.Background(), "quorum"), consistencyQuorum},
		{"case-insensitive", WithConsistency(context.Background(), "ONE"), consistencyOne},
	} {
		got, err := c.consistencyFor(tc.ctx)
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", tc.name, err)
		}
		if got != tc.want {
			t.Errorf("%s: got %d, want %d", tc.name, got, tc.want)
		}
	}
	if _, err := c.consistencyFor(WithConsistency(context.Background(), "eventual")); err == nil {
		t.Fatal("an unknown consistency level must be rejected, not ignored")
	}
}

// Cancelling the context must unblock a read that would otherwise hang,
// report ctx.Err() rather than a raw i/o timeout, and retire the connection:
// the statement was interrupted mid-frame, so the stream can no longer be
// trusted and database/sql must not hand it to the next caller.
func TestApplyContextCancelUnblocksAndRetiresConn(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	c := &conn{nc: client, consistency: consistencyQuorum}

	ctx, cancel := context.WithCancel(context.Background())
	finish := c.applyContext(ctx)
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	// Nothing is ever written to the pipe, so this blocks until the watcher
	// forces a deadline.
	buf := make([]byte, 1)
	_, readErr := client.Read(buf)
	if readErr == nil {
		t.Fatal("expected the blocked read to fail once the context was cancelled")
	}
	if err := finish(readErr); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if c.IsValid() {
		t.Fatal("an interrupted connection must not be reused: IsValid should be false")
	}
}

// A deadline that has already passed behaves the same way and surfaces as
// context.DeadlineExceeded.
func TestApplyContextDeadlineSurfacesAsDeadlineExceeded(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	c := &conn{nc: client, consistency: consistencyQuorum}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	finish := c.applyContext(ctx)

	buf := make([]byte, 1)
	_, readErr := client.Read(buf)
	if readErr == nil {
		t.Fatal("expected the blocked read to time out")
	}
	if err := finish(readErr); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want context.DeadlineExceeded, got %v", err)
	}
}

// Without a context there is nothing to watch and no deadline to clear, and
// the connection stays usable.
func TestApplyContextBackgroundLeavesConnUsable(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	c := &conn{nc: client, consistency: consistencyQuorum}

	finish := c.applyContext(context.Background())
	if err := finish(nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !c.IsValid() {
		t.Fatal("an uncancelled statement must leave the connection usable")
	}
}

// ---- streamed result sets --------------------------------------------------
//
// These use a loopback socket rather than net.Pipe: the bug under test is
// about frames sitting UNREAD in the client's receive buffer, and an
// unbuffered pipe cannot hold any, so the server would just block instead.

// fakeServer accepts one connection, runs script against it, and returns the
// client end wired into a conn. script must not call t.Fatal (wrong goroutine).
func fakeServer(t *testing.T, script func(s net.Conn)) *conn {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("no loopback listener available: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s, err := ln.Accept()
		if err != nil {
			return
		}
		defer s.Close()
		script(s)
	}()
	nc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		ln.Close()
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		nc.Close()
		ln.Close()
		<-done
	})
	return &conn{nc: nc, consistency: consistencyQuorum, stmtLevel: consistencyQuorum,
		prepared: map[string]preparedStmt{}}
}

func serverFrame(payload []byte) []byte {
	out := make([]byte, 4, 4+len(payload))
	binary.BigEndian.PutUint32(out, uint32(len(payload)))
	return append(out, payload...)
}

// readReq reads one length-prefixed client request.
func readReq(s net.Conn) ([]byte, error) {
	var head [4]byte
	if _, err := io.ReadFull(s, head[:]); err != nil {
		return nil, err
	}
	buf := make([]byte, binary.BigEndian.Uint32(head[:]))
	_, err := io.ReadFull(s, buf)
	return buf, err
}

func headerFrame(cols ...string) []byte {
	p := []byte{5} // RowsHeader
	p = binary.LittleEndian.AppendUint32(p, uint32(len(cols)))
	for _, c := range cols {
		p = appendStr(p, c)
	}
	return serverFrame(p)
}

// chunkFrame builds a RowsChunk of single-column integer rows.
func chunkFrame(vals ...int64) []byte {
	p := []byte{6}
	p = binary.LittleEndian.AppendUint32(p, uint32(len(vals)))
	for _, v := range vals {
		cell := binary.LittleEndian.AppendUint64([]byte{2}, uint64(v)) // Int
		p = binary.LittleEndian.AppendUint32(p, 1)                     // one cell
		p = binary.LittleEndian.AppendUint32(p, uint32(len(cell)))
		p = append(p, cell...)
	}
	return serverFrame(p)
}

func endFrame() []byte { return serverFrame([]byte{7}) } // RowsEnd
func ddlFrame() []byte { return serverFrame([]byte{2}) } // Ddl
func mutationFrame(n uint64) []byte { // Mutation
	return serverFrame(binary.LittleEndian.AppendUint64([]byte{1}, n))
}

// Breaking out of a stream early leaves RowsChunk/RowsEnd frames queued on the
// socket. They must be drained by Close, or the next statement on the pooled
// connection decodes one of them as its own reply ("unknown response tag 6").
func TestAbandonedStreamIsDrainedSoConnStaysUsable(t *testing.T) {
	c := fakeServer(t, func(s net.Conn) {
		if _, err := readReq(s); err != nil {
			return
		}
		s.Write(headerFrame("id"))
		s.Write(chunkFrame(1, 2))
		s.Write(chunkFrame(3, 4))
		s.Write(endFrame())
		if _, err := readReq(s); err != nil { // the caller's NEXT statement
			return
		}
		s.Write(ddlFrame())
	})

	rs, err := c.streamQuery("SELECT id FROM t")
	if err != nil {
		t.Fatalf("streamQuery: %v", err)
	}
	dest := make([]driver.Value, 1)
	if err := rs.Next(dest); err != nil {
		t.Fatalf("first row: %v", err)
	}
	// The caller has seen enough: two rows and RowsEnd are still on the wire.
	if err := rs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !c.IsValid() {
		t.Fatal("a fully drained connection must stay usable")
	}
	if _, err := c.exec("CREATE TABLE t2 (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("statement after an abandoned stream: %v", err)
	}
}

// A stream too big to swallow must not be drained at the caller's expense:
// past the frame bound the connection is retired instead, and database/sql's
// pool check (IsValid) has to reject it.
func TestAbandonedStreamBeyondDrainBoundPoisonsConn(t *testing.T) {
	c := fakeServer(t, func(s net.Conn) {
		if _, err := readReq(s); err != nil {
			return
		}
		s.Write(headerFrame("id"))
		for i := 0; i < drainMaxFrames*3; i++ {
			if _, err := s.Write(chunkFrame(int64(i))); err != nil {
				return
			}
		}
		s.Write(endFrame())
	})

	rs, err := c.streamQuery("SELECT id FROM big")
	if err != nil {
		t.Fatalf("streamQuery: %v", err)
	}
	if err := rs.Next(make([]driver.Value, 1)); err != nil {
		t.Fatalf("first row: %v", err)
	}
	rs.Close()
	if c.IsValid() {
		t.Fatal("a stream that could not be drained must retire the connection")
	}
}

// Same when the peer simply stops talking: Close gives up after drainTimeout
// and retires the connection rather than blocking the caller forever.
func TestAbandonedStreamRetiresConnWhenPeerGoesQuiet(t *testing.T) {
	defer func(d time.Duration) { drainTimeout = d }(drainTimeout)
	drainTimeout = 50 * time.Millisecond

	quiet := make(chan struct{})
	c := fakeServer(t, func(s net.Conn) {
		if _, err := readReq(s); err != nil {
			return
		}
		s.Write(headerFrame("id"))
		s.Write(chunkFrame(1, 2))
		<-quiet // never sends RowsEnd
	})
	defer close(quiet)

	rs, err := c.streamQuery("SELECT id FROM t")
	if err != nil {
		t.Fatalf("streamQuery: %v", err)
	}
	if err := rs.Next(make([]driver.Value, 1)); err != nil {
		t.Fatalf("first row: %v", err)
	}
	start := time.Now()
	rs.Close()
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("Close blocked for %v on a silent peer", took)
	}
	if c.IsValid() {
		t.Fatal("an undrainable connection must not be handed back to the pool")
	}
}

// PROTOCOL.md §3.4: the connection is busy for the whole stream. database/sql
// takes its per-connection lock per CALL, not per stream, so two goroutines
// sharing one *sql.Conn can reach here concurrently — the second must be told
// so, not silently interleaved with the first. Closing the rows frees it.
func TestStatementDuringStreamIsRejected(t *testing.T) {
	c := fakeServer(t, func(s net.Conn) {
		if _, err := readReq(s); err != nil {
			return
		}
		s.Write(headerFrame("id"))
		s.Write(chunkFrame(1, 2))
		s.Write(endFrame())
		if _, err := readReq(s); err != nil {
			return
		}
		s.Write(mutationFrame(7))
	})

	rs, err := c.streamQuery("SELECT id FROM t")
	if err != nil {
		t.Fatalf("streamQuery: %v", err)
	}
	if err := rs.Next(make([]driver.Value, 1)); err != nil {
		t.Fatalf("first row: %v", err)
	}

	// In a goroutine with a timeout: an unguarded driver would write the
	// request and block, and a hung test reports nothing useful.
	busy := make(chan error, 1)
	go func() {
		_, err := c.ExecContext(context.Background(), "DELETE FROM t", nil)
		busy <- err
	}()
	select {
	case err := <-busy:
		if !errors.Is(err, errStreamBusy) {
			t.Fatalf("want errStreamBusy, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a statement issued during a stream neither failed nor returned")
	}

	rs.Close()
	res, err := c.ExecContext(context.Background(), "DELETE FROM t", nil)
	if err != nil {
		t.Fatalf("statement after the stream closed: %v", err)
	}
	if n, _ := res.RowsAffected(); n != 7 {
		t.Fatalf("affected = %d, want 7", n)
	}
}

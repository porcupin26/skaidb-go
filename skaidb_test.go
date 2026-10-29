package skaidb

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"strings"
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

	rs, err := c.streamQuery("SELECT id FROM t", nil)
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

	rs, err := c.streamQuery("SELECT id FROM big", nil)
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

	rs, err := c.streamQuery("SELECT id FROM t", nil)
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

	rs, err := c.streamQuery("SELECT id FROM t", nil)
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

// ---- parameterised streams -------------------------------------------------

// streamRequest builds the request a parameterless stream of q sends, by hand.
func streamRequest(level byte, q string) []byte {
	req := binary.LittleEndian.AppendUint32([]byte{5, level}, uint32(len(q)))
	return append(req, q...)
}

// Arguments travel in OP_QUERY_STREAM_PARAMS: opcode 9, the statement's
// consistency, the SQL, then the parameter vector OP_EXECUTE carries — u16
// count, each typed value behind its u32 length. The expected bytes are
// written out here rather than produced by the driver's encoder. A *sql.Stmt
// run with a streaming context sends the same request.
func TestStreamWithArgsSendsTheParameterisedOpcode(t *testing.T) {
	c, rec := recordingServer(t, func(req []byte) [][]byte {
		if req[0] != 9 {
			return [][]byte{errorFrame("unexpected request")}
		}
		return [][]byte{headerFrame("id"), chunkFrame(7), endFrame()}
	})
	db := sql.OpenDB(testConnector{c})
	defer db.Close()

	const q = "SELECT id FROM t WHERE n >= ? AND tag = ? AND tags = ?"
	ctx := WithConsistency(WithStreaming(context.Background()), "all")
	args := []any{5, "a", []string{"x"}}
	want := binary.LittleEndian.AppendUint32([]byte{9, consistencyAll}, uint32(len(q)))
	want = append(want, q...)
	want = append(want, 3, 0)                                           // three parameters
	want = append(want, 9, 0, 0, 0, 2, 5, 0, 0, 0, 0, 0, 0, 0)          // Int 5
	want = append(want, 6, 0, 0, 0, 5, 1, 0, 0, 0, 'a')                 // String "a"
	want = append(want, 11, 0, 0, 0, 9, 1, 0, 0, 0, 5, 1, 0, 0, 0, 'x') // Array ["x"]

	readID := func(rows *sql.Rows, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("scan: %v", err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil || len(ids) != 1 || ids[0] != 7 {
			t.Fatalf("rows = %v, err = %v; want [7]", ids, err)
		}
	}
	readID(db.QueryContext(ctx, q, args...))
	stmt, err := db.PrepareContext(ctx, q)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Close()
	readID(stmt.QueryContext(ctx, args...))

	reqs := rec.all()
	if len(reqs) != 2 {
		t.Fatalf("%d requests, want 2: %x", len(reqs), reqs)
	}
	for i, req := range reqs {
		if !bytes.Equal(req, want) {
			t.Errorf("request %d:\n  got  %x\n  want %x", i, req, want)
		}
	}
}

// No arguments, no change: the request is the original OP_QUERY_STREAM, byte
// for byte, so a parameterless stream still works against every server.
func TestStreamWithoutArgsKeepsTheOriginalOpcode(t *testing.T) {
	c, rec := recordingServer(t, func(req []byte) [][]byte {
		return [][]byte{headerFrame("id"), chunkFrame(1), endFrame()}
	})
	const q = "SELECT id FROM big"
	rs, err := c.QueryContext(WithStreaming(context.Background()), q, []driver.NamedValue{})
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	dest := make([]driver.Value, 1)
	if err := rs.Next(dest); err != nil || dest[0] != int64(1) {
		t.Fatalf("row = %v, err = %v", dest, err)
	}
	if err := rs.Next(dest); err != io.EOF {
		t.Fatalf("want io.EOF after the last row, got %v", err)
	}
	rs.Close()
	if reqs := rec.all(); len(reqs) != 1 || !bytes.Equal(reqs[0], streamRequest(consistencyQuorum, q)) {
		t.Fatalf("requests = %x, want %x", reqs, streamRequest(consistencyQuorum, q))
	}
}

// A server before 0.330 does not know OP_QUERY_STREAM_PARAMS and answers it
// with one ordinary Error frame. The connection is still on a frame
// boundary, so it stays usable; streamQuery names what the server lacks and
// the version that has it, and QueryContext answers the call on the
// buffered, prepared path — the result a call with arguments always gets
// from such a server.
func TestParameterisedStreamOnAnOldServerRunsBuffered(t *testing.T) {
	c, rec := recordingServer(t, func(req []byte) [][]byte {
		switch req[0] {
		case 9:
			return [][]byte{errorFrame("protocol error: malformed message: unknown opcode")}
		case 2:
			return [][]byte{preparedFrame(4, 2)}
		case 3:
			return [][]byte{serverFrame(fakeRowsFrame([]string{"name"}, [][]any{{"ada"}}))}
		}
		return [][]byte{errorFrame("unexpected request")}
	})
	const q = "SELECT name FROM t WHERE id = ? AND kind = ?"

	_, err := c.streamQuery(q, []driver.NamedValue{{Ordinal: 1, Value: int64(1)}, {Ordinal: 2, Value: "x"}})
	if !errors.Is(err, errNoParamStreaming) {
		t.Fatalf("err = %v, want errNoParamStreaming", err)
	}
	for _, part := range []string{"server does not support parameterised streaming (skaidb >= 0.330)", "unknown opcode"} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error %q does not name %q", err, part)
		}
	}
	if !c.IsValid() || c.streaming.Load() {
		t.Fatal("one Error frame must leave the connection usable and not streaming")
	}

	db := sql.OpenDB(testConnector{c})
	defer db.Close()
	var name string
	if err := db.QueryRowContext(WithStreaming(context.Background()), q, 1, "x").Scan(&name); err != nil {
		t.Fatalf("query against an old server: %v", err)
	}
	if name != "ada" {
		t.Errorf("name = %q, want ada", name)
	}
	if ops := rec.ops(); string(ops) != "\x09\x09\x02\x03" {
		t.Fatalf("requests = %v, want stream, stream, prepare, execute", ops)
	}
	if p := decodeExecuteParams(rec.all()[3]); len(p) != 2 || p[0] != int64(1) || p[1] != "x" {
		t.Errorf("execute params = %v", p)
	}
}

// A statement kind the server does not bind (DDL, session statements)
// answers the arguments with one Error before anything runs; it takes the
// client-side text path, exactly as it does when prepared.
func TestParameterisedStreamOfAnUnbindableKindFallsBackToText(t *testing.T) {
	c, rec := recordingServer(t, func(req []byte) [][]byte {
		switch req[0] {
		case 9, 2:
			return [][]byte{errorFrame("statement kind cannot be prepared")}
		case 1:
			return [][]byte{ddlFrame()}
		}
		return [][]byte{errorFrame("unexpected request")}
	})
	db := sql.OpenDB(testConnector{c})
	defer db.Close()
	rows, err := db.QueryContext(WithStreaming(context.Background()), "ALTER TABLE t SET shard = ?", 3)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if rows.Next() {
		t.Error("a DDL statement produced a row")
	}
	rows.Close()
	reqs := rec.all()
	if ops := rec.ops(); string(ops) != "\x09\x02\x01" {
		t.Fatalf("requests = %v, want stream, prepare, query", ops)
	}
	if got := string(reqs[2][6:]); got != "ALTER TABLE t SET shard = 3" {
		t.Errorf("text sent = %q", got)
	}
}

// Every other Error answering a parameterised stream is the statement's own
// failure: it is returned as the query error, nothing is sent again (that
// would run a statement twice), and the connection stays usable. The
// unbindable-kind answer only counts when it is the whole message.
func TestParameterisedStreamErrorIsTheQueryError(t *testing.T) {
	for _, msg := range []string{
		"statement expects 2 parameters, got 1",
		"procedure p failed: statement kind cannot be prepared",
	} {
		msg := msg
		c, rec := recordingServer(t, func(req []byte) [][]byte {
			if req[0] == 9 {
				return [][]byte{errorFrame(msg)}
			}
			return [][]byte{ddlFrame()}
		})
		_, err := c.QueryContext(WithStreaming(context.Background()), "SELECT id FROM t WHERE a = ? AND b = ?",
			[]driver.NamedValue{{Ordinal: 1, Value: int64(1)}})
		if err == nil || err.Error() != "skaidb: "+msg {
			t.Errorf("err = %v, want %q", err, "skaidb: "+msg)
		}
		if ops := rec.ops(); string(ops) != "\x09" {
			t.Errorf("%q: requests = %v, want the stream request alone", msg, ops)
		}
		if !c.IsValid() {
			t.Errorf("%q: a statement error retired the connection", msg)
		}
	}
}

// A procedure whose body EMITs answers even a streamed CALL with its result
// sets in one ordinary frame; they arrive through NextResultSet as on the
// buffered path — with arguments and without.
func TestStreamedCallReturnsItsResultSets(t *testing.T) {
	sets := binary.LittleEndian.AppendUint32([]byte{8}, 2) // ResultSets
	sets = append(sets, fakeRowsFrame([]string{"a"}, [][]any{{"x"}, {"y"}})[1:]...)
	sets = append(sets, fakeRowsFrame([]string{"b"}, [][]any{{"z"}})[1:]...)
	c, rec := recordingServer(t, func([]byte) [][]byte { return [][]byte{serverFrame(sets)} })
	db := sql.OpenDB(testConnector{c})
	defer db.Close()
	ctx := WithStreaming(context.Background())
	for _, call := range []struct {
		sql  string
		args []any
	}{{"CALL report(?)", []any{5}}, {"CALL report()", nil}} {
		rows, err := db.QueryContext(ctx, call.sql, call.args...)
		if err != nil {
			t.Fatalf("%s: %v", call.sql, err)
		}
		var got []string
		for {
			for rows.Next() {
				var v string
				if err := rows.Scan(&v); err != nil {
					t.Fatalf("scan: %v", err)
				}
				got = append(got, v)
			}
			if !rows.NextResultSet() {
				break
			}
			got = append(got, "|")
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("%s: %v", call.sql, err)
		}
		rows.Close()
		if strings.Join(got, "") != "xy|z" {
			t.Errorf("%s: result sets = %v, want [x y] then [z]", call.sql, got)
		}
	}
	if ops := rec.ops(); string(ops) != "\x09\x05" {
		t.Errorf("requests = %v, want a parameterised then a plain stream", ops)
	}
}

// The parameter count on the wire is 16 bits: more arguments than that are
// refused before anything is sent, instead of wrapping into a shorter vector.
func TestStreamRefusesMoreArgumentsThanTheWireCarries(t *testing.T) {
	c, rec := recordingServer(t, func([]byte) [][]byte { return [][]byte{ddlFrame()} })
	args := make([]driver.NamedValue, math.MaxUint16+1)
	for i := range args {
		args[i] = driver.NamedValue{Ordinal: i + 1, Value: int64(i)}
	}
	_, err := c.QueryContext(WithStreaming(context.Background()), "SELECT id FROM t WHERE id IN ?", args)
	if err == nil || !strings.Contains(err.Error(), "65536 parameters, over the protocol's 65535") {
		t.Fatalf("err = %v, want the 16-bit count refused", err)
	}
	if n := len(rec.all()); n != 0 || !c.IsValid() {
		t.Fatalf("%d requests sent, valid = %v: want nothing sent and the connection usable", n, c.IsValid())
	}
}

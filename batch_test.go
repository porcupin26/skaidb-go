package skaidb

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
)

// recorder is a fake server that answers every request with handler's
// frames and records the requests, in order.
type recorder struct {
	mu   sync.Mutex
	reqs [][]byte
}

func (r *recorder) all() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]byte(nil), r.reqs...)
}

// ops lists the opcodes received.
func (r *recorder) ops() []byte {
	var out []byte
	for _, q := range r.all() {
		out = append(out, q[0])
	}
	return out
}

func recordingServer(t *testing.T, handler func(req []byte) [][]byte) (*conn, *recorder) {
	rec := &recorder{}
	c := fakeServer(t, func(s net.Conn) {
		for {
			req, err := readReq(s)
			if err != nil {
				return
			}
			rec.mu.Lock()
			rec.reqs = append(rec.reqs, req)
			rec.mu.Unlock()
			for _, f := range handler(req) {
				if _, err := s.Write(f); err != nil {
					return
				}
			}
		}
	})
	return c, rec
}

func preparedFrame(id uint32, nparams uint16) []byte {
	p := binary.LittleEndian.AppendUint32([]byte{4}, id)
	return serverFrame(binary.LittleEndian.AppendUint16(p, nparams))
}

func errorFrame(msg string) []byte { return serverFrame(appendStr([]byte{3}, msg)) }

// decodeBatch parses an OP_EXECUTE_BATCH request.
func decodeBatch(t *testing.T, req []byte) (level byte, id uint32, rows [][]any) {
	t.Helper()
	if req[0] != 7 {
		t.Fatalf("opcode %d, want OP_EXECUTE_BATCH (7)", req[0])
	}
	r := &reader{buf: req[1:]}
	level, id = r.u8(), r.u32()
	n := int(r.u32())
	for i := 0; i < n; i++ {
		row := make([]any, r.u16())
		for j := range row {
			row[j] = decodeNative(&reader{buf: r.blob()})
		}
		rows = append(rows, row)
	}
	if r.err != nil || r.pos != len(r.buf) {
		t.Fatalf("malformed batch: err=%v, %d of %d bytes", r.err, r.pos, len(r.buf))
	}
	return level, id, rows
}

// batchHandler prepares every statement as id 9 with nparams parameters and
// answers each batch with the number of rows it carried.
func batchHandler(nparams uint16) func([]byte) [][]byte {
	return func(req []byte) [][]byte {
		switch req[0] {
		case 2:
			return [][]byte{preparedFrame(9, nparams)}
		case 7:
			r := &reader{buf: req[6:]}
			return [][]byte{mutationFrame(uint64(r.u32()))}
		}
		return [][]byte{errorFrame("unexpected request")}
	}
}

func intRows(n int) [][]any {
	rows := make([][]any, n)
	for i := range rows {
		rows[i] = []any{i, fmt.Sprintf("name-%d", i)}
	}
	return rows
}

// The point of the batch API: N rows cost ONE request, not N.
func TestExecBatchIsOneRequestForManyRows(t *testing.T) {
	c, rec := recordingServer(t, batchHandler(2))
	db := sql.OpenDB(testConnector{c})
	defer db.Close()

	n, err := ExecBatch(context.Background(), db, "INSERT INTO t (id, name) VALUES (?, ?)", intRows(1000))
	if err != nil {
		t.Fatalf("ExecBatch: %v", err)
	}
	if n != 1000 {
		t.Errorf("affected = %d, want 1000", n)
	}
	if ops := rec.ops(); string(ops) != "\x02\x07" {
		t.Fatalf("requests = %v, want one OP_PREPARE then ONE OP_EXECUTE_BATCH", ops)
	}
	level, id, rows := decodeBatch(t, rec.all()[1])
	if level != consistencyQuorum || id != 9 || len(rows) != 1000 {
		t.Fatalf("batch level=%d id=%d rows=%d", level, id, len(rows))
	}
	for i, row := range rows {
		if row[0] != int64(i) || row[1] != fmt.Sprintf("name-%d", i) {
			t.Fatalf("row %d = %v", i, row)
		}
	}

	// The prepared id is cached: a second batch is one request, no prepare.
	if _, err := ExecBatch(context.Background(), db, "INSERT INTO t (id, name) VALUES (?, ?)", intRows(3)); err != nil {
		t.Fatalf("second ExecBatch: %v", err)
	}
	if ops := rec.ops(); string(ops) != "\x02\x07\x07" {
		t.Fatalf("requests = %v, want the second batch to reuse the prepared id", ops)
	}
}

// The documented escape hatch: (*sql.Conn).Raw hands out the driver.Conn,
// which implements BatchExecer. The context's consistency override applies,
// and rows bind like ExecContext arguments (Valuers, pointers, composites).
func TestExecBatchThroughRawConn(t *testing.T) {
	c, rec := recordingServer(t, batchHandler(3))
	db := sql.OpenDB(testConnector{c})
	defer db.Close()
	ctx := WithConsistency(context.Background(), "all")
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	name := "ada"
	var affected int64
	err = conn.Raw(func(driverConn any) error {
		bc := driverConn.(BatchExecer)
		var err error
		affected, err = bc.ExecBatch(ctx, "INSERT INTO t (a, b, c) VALUES (?, ?, ?)", [][]any{
			{sql.NullString{String: "x", Valid: true}, &name, []string{"p", "q"}},
			{sql.NullInt64{}, uint8(7), map[string]any{"k": 1.5}},
		})
		return err
	})
	if err != nil {
		t.Fatalf("Raw ExecBatch: %v", err)
	}
	if affected != 2 {
		t.Errorf("affected = %d, want 2", affected)
	}
	level, _, rows := decodeBatch(t, rec.all()[1])
	if level != consistencyAll {
		t.Errorf("consistency = %d, want ALL (%d)", level, consistencyAll)
	}
	if rows[0][0] != "x" || rows[0][1] != "ada" || rows[1][0] != nil || rows[1][1] != int64(7) {
		t.Errorf("rows = %#v", rows)
	}
	if arr, ok := rows[0][2].([]any); !ok || len(arr) != 2 || arr[1] != "q" {
		t.Errorf("array param = %#v", rows[0][2])
	}
	if doc, ok := rows[1][2].(map[string]any); !ok || doc["k"] != 1.5 {
		t.Errorf("document param = %#v", rows[1][2])
	}
}

// A batch too large for one frame goes out in consecutive chunks, each one
// round trip, none over the limit, the rows in order.
func TestExecBatchChunksAtTheFrameLimit(t *testing.T) {
	old := batchFrameLimit
	batchFrameLimit = 300
	defer func() { batchFrameLimit = old }()

	c, rec := recordingServer(t, batchHandler(2))
	db := sql.OpenDB(testConnector{c})
	defer db.Close()
	n, err := ExecBatch(context.Background(), db, "INSERT INTO t (id, name) VALUES (?, ?)", intRows(50))
	if err != nil {
		t.Fatalf("ExecBatch: %v", err)
	}
	if n != 50 {
		t.Errorf("affected = %d, want 50", n)
	}
	reqs := rec.all()[1:]
	if len(reqs) < 2 {
		t.Fatalf("%d batch requests, want the 50 rows split into several", len(reqs))
	}
	next := 0
	for _, q := range reqs {
		if len(q) > batchFrameLimit {
			t.Errorf("a %d-byte request exceeds the %d-byte limit", len(q), batchFrameLimit)
		}
		_, _, rows := decodeBatch(t, q)
		for _, row := range rows {
			if row[0] != int64(next) {
				t.Fatalf("row %v arrived where row %d belongs", row, next)
			}
			next++
		}
	}
	if next != 50 {
		t.Errorf("%d rows sent, want 50", next)
	}
}

// One row bigger than a frame fails before anything is sent.
func TestExecBatchRejectsARowOverTheFrameLimit(t *testing.T) {
	old := batchFrameLimit
	batchFrameLimit = 64
	defer func() { batchFrameLimit = old }()
	c, rec := recordingServer(t, batchHandler(1))
	_, err := c.ExecBatch(context.Background(), "INSERT INTO t (v) VALUES (?)", [][]any{{strings.Repeat("x", 100)}})
	if err == nil || !strings.Contains(err.Error(), "frame limit") {
		t.Fatalf("err = %v, want a frame-limit error", err)
	}
	if ops := rec.ops(); string(ops) != "\x02" {
		t.Errorf("requests = %v, want only the prepare", ops)
	}
}

// A failure in a later chunk says which rows already applied, and the
// count of the chunks that did apply is returned.
func TestExecBatchErrorInALaterChunkNamesTheAppliedRows(t *testing.T) {
	old := batchFrameLimit
	batchFrameLimit = 300
	defer func() { batchFrameLimit = old }()
	batches := 0
	c, _ := recordingServer(t, func(req []byte) [][]byte {
		if req[0] == 7 {
			batches++
			if batches == 2 {
				return [][]byte{errorFrame("row 1: duplicate key (1 rows applied)")}
			}
		}
		return batchHandler(2)(req)
	})
	n, err := c.ExecBatch(context.Background(), "INSERT INTO t (id, name) VALUES (?, ?)", intRows(50))
	if err == nil || !strings.Contains(err.Error(), "duplicate key") {
		t.Fatalf("err = %v, want the server's error", err)
	}
	if n == 0 || n >= 50 || !strings.Contains(err.Error(), fmt.Sprintf("the %d rows before it applied", n)) {
		t.Errorf("affected = %d, err = %v: want the first chunk's count, named in the error", n, err)
	}
}

// A server that predates OP_EXECUTE_BATCH gets one OP_EXECUTE per row.
func TestExecBatchFallsBackToPerRowOnAnOldServer(t *testing.T) {
	c, rec := recordingServer(t, func(req []byte) [][]byte {
		switch req[0] {
		case 2:
			return [][]byte{preparedFrame(9, 2)}
		case 7:
			return [][]byte{errorFrame("unknown opcode 7")}
		case 3:
			return [][]byte{mutationFrame(1)}
		}
		return [][]byte{errorFrame("unexpected")}
	})
	n, err := c.ExecBatch(context.Background(), "INSERT INTO t (id, name) VALUES (?, ?)", intRows(3))
	if err != nil || n != 3 {
		t.Fatalf("n=%d err=%v, want 3 rows through the fallback", n, err)
	}
	if ops := rec.ops(); string(ops) != "\x02\x07\x03\x03\x03" {
		t.Errorf("requests = %v", ops)
	}
	if p := decodeExecuteParams(rec.all()[4]); p[0] != int64(2) || p[1] != "name-2" {
		t.Errorf("last row params = %v", p)
	}
}

// A statement the server will not prepare runs text-bound, one per row.
func TestExecBatchFallsBackToTextForAnUnpreparableStatement(t *testing.T) {
	c, rec := recordingServer(t, func(req []byte) [][]byte {
		switch req[0] {
		case 2:
			return [][]byte{errorFrame("cannot prepare")}
		case 1:
			return [][]byte{mutationFrame(2)}
		}
		return [][]byte{errorFrame("unexpected")}
	})
	n, err := c.ExecBatch(context.Background(), "CALL p(?)", [][]any{{1}, {"o'neil"}})
	if err != nil || n != 4 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	reqs := rec.all()
	if len(reqs) != 3 || string(reqs[2][6:]) != "CALL p('o''neil')" {
		t.Errorf("requests = %q", reqs)
	}
}

// A row whose width does not match the statement fails before the batch
// is sent, and an empty batch sends nothing at all.
func TestExecBatchChecksArityAndSkipsEmptyBatches(t *testing.T) {
	c, rec := recordingServer(t, batchHandler(2))
	if n, err := c.ExecBatch(context.Background(), "INSERT INTO t (id, name) VALUES (?, ?)", nil); err != nil || n != 0 {
		t.Fatalf("empty batch: n=%d err=%v", n, err)
	}
	if len(rec.all()) != 0 {
		t.Fatalf("an empty batch sent %d requests", len(rec.all()))
	}
	_, err := c.ExecBatch(context.Background(), "INSERT INTO t (id, name) VALUES (?, ?)", [][]any{{1, "a"}, {2}})
	if err == nil || !strings.Contains(err.Error(), "batch row 1") {
		t.Fatalf("err = %v, want an arity error naming row 1", err)
	}
	if ops := rec.ops(); string(ops) != "\x02" {
		t.Errorf("requests = %v, want no batch sent", ops)
	}
}

// Past the per-connection statement cache a prepared id is used once and
// then freed with OP_CLOSE; without that a long-lived connection ran into
// the server's cap of 256 open statements and silently lost the typed path.
func TestUncachedPreparedStatementIsClosed(t *testing.T) {
	c, rec := recordingServer(t, func(req []byte) [][]byte {
		switch req[0] {
		case 2:
			return [][]byte{preparedFrame(77, 1)}
		case 3:
			return [][]byte{mutationFrame(1)}
		case 7:
			return [][]byte{mutationFrame(2)}
		case 4:
			return [][]byte{ddlFrame()}
		}
		return [][]byte{errorFrame("unexpected")}
	})
	for i := 0; i < preparedCacheSize; i++ {
		c.prepared[fmt.Sprintf("SELECT %d", i)] = preparedStmt{id: uint32(i), nparams: 0}
	}
	db := sql.OpenDB(testConnector{c})
	defer db.Close()
	if _, err := db.Exec("UPDATE t SET v = ?", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := ExecBatch(context.Background(), db, "UPDATE t SET w = ?", [][]any{{1}, {2}}); err != nil {
		t.Fatal(err)
	}
	reqs := rec.all()
	if ops := rec.ops(); string(ops) != "\x02\x03\x04\x02\x07\x04" {
		t.Fatalf("requests = %v, want prepare, execute, CLOSE, prepare, batch, CLOSE", ops)
	}
	for _, i := range []int{2, 5} {
		if id := binary.LittleEndian.Uint32(reqs[i][1:]); id != 77 {
			t.Errorf("OP_CLOSE freed id %d, want 77", id)
		}
	}
}

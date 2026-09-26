package skaidb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

// BatchExecer is implemented by the driver's connection: the driver.Conn
// that database/sql hands to (*sql.Conn).Raw. ExecBatch runs one statement
// over many parameter rows with OP_EXECUTE_BATCH — one round trip for the
// whole batch (one per chunk when the batch exceeds a frame) instead of one
// per row — and returns the total affected-row count.
//
//	conn, _ := db.Conn(ctx)
//	defer conn.Close()
//	var n int64
//	err := conn.Raw(func(driverConn any) error {
//		bc := driverConn.(skaidb.BatchExecer)
//		var err error
//		n, err = bc.ExecBatch(ctx, "INSERT INTO t (id, v) VALUES (?, ?)", rows)
//		return err
//	})
//
// ExecBatch and ExecBatchConn wrap exactly this.
//
// Each row binds like the arguments of db.ExecContext: scalars, time.Time,
// []byte, driver.Valuer (sql.NullString and friends), pointers, named
// types, and slices and maps as arrays and documents. The context's
// deadline, cancellation and WithConsistency override apply.
//
// Rows run in order, each as its own autocommitted statement. On the first
// failing row the server stops and reports the row; every row before it
// stays applied.
type BatchExecer interface {
	ExecBatch(ctx context.Context, query string, rows [][]any) (int64, error)
}

var _ BatchExecer = (*conn)(nil)

// ExecBatch runs query once per parameter row on one pooled connection, in
// one round trip per chunk (see BatchExecer), and returns the total
// affected-row count. A failure before any byte reached the server is
// retried on a fresh connection, as database/sql does for its own calls.
//
//	n, err := skaidb.ExecBatch(ctx, db, "INSERT INTO t (id, v) VALUES (?, ?)",
//		[][]any{{1, "a"}, {2, "b"}, {3, "c"}})
func ExecBatch(ctx context.Context, db *sql.DB, query string, rows [][]any) (int64, error) {
	var (
		n   int64
		err error
	)
	for attempt := 0; attempt < 3; attempt++ {
		var c *sql.Conn
		c, err = db.Conn(ctx)
		if err != nil {
			return 0, err
		}
		n, err = ExecBatchConn(ctx, c, query, rows)
		c.Close()
		if !errors.Is(err, driver.ErrBadConn) {
			break
		}
	}
	return n, err
}

// ExecBatchConn is ExecBatch on a connection the caller has pinned with
// db.Conn — for a batch that must share the connection's session state
// (USE) or follow other statements on the same socket.
func ExecBatchConn(ctx context.Context, c *sql.Conn, query string, rows [][]any) (int64, error) {
	var n int64
	err := c.Raw(func(driverConn any) error {
		bc, ok := driverConn.(BatchExecer)
		if !ok {
			return fmt.Errorf("skaidb: %T is not a skaidb connection", driverConn)
		}
		var err error
		n, err = bc.ExecBatch(ctx, query, rows)
		return err
	})
	return n, err
}

// batchFrameLimit is the largest request payload ExecBatch sends: the
// server's frame limit (PROTOCOL.md §1, 64 MiB). A batch whose rows do not
// fit one frame is split into consecutive chunks, one round trip each. A
// var so the tests can shrink it.
var batchFrameLimit = 64 << 20

// batchHeaderLen is OP_EXECUTE_BATCH's fixed prefix: opcode, consistency,
// statement id, row count.
const batchHeaderLen = 1 + 1 + 4 + 4

// ExecBatch implements BatchExecer.
func (c *conn) ExecBatch(ctx context.Context, query string, rows [][]any) (int64, error) {
	lvl, err := c.consistencyFor(ctx)
	if err != nil {
		return 0, err
	}
	c.stmtLevel = lvl
	defer func() { c.stmtLevel = c.consistency }()
	finish := c.applyContext(ctx)
	n, err := c.execBatch(query, rows)
	if err != nil {
		return n, finish(err)
	}
	return n, finish(nil)
}

func (c *conn) execBatch(query string, rows [][]any) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if c.closed {
		return 0, fmt.Errorf("skaidb: connection closed")
	}
	if c.streaming.Load() {
		return 0, errStreamBusy
	}
	id, nparams, cached, err := c.prepareServer(query)
	if errors.Is(err, errUnpreparable) {
		// DDL and session statements: text-bound, one statement per row.
		return c.execBatchLoop(query, rows, nil)
	}
	if err != nil {
		return 0, err
	}
	if !cached {
		defer c.closePrepared(id)
	}
	for i, row := range rows {
		if len(row) != nparams {
			return 0, fmt.Errorf("skaidb: batch row %d: statement expects %d parameters, got %d", i, nparams, len(row))
		}
	}

	var (
		total   int64
		start   int // batch index of the chunk's first row
		count   int // rows in the chunk being built
		scratch []byte
	)
	body := make([]byte, batchHeaderLen, 4096)
	send := func() error {
		body[0] = 7 // OP_EXECUTE_BATCH
		body[1] = c.stmtLevel
		binary.LittleEndian.PutUint32(body[2:], id)
		binary.LittleEndian.PutUint32(body[6:], uint32(count))
		n, fallback, err := c.sendBatchChunk(body, start)
		if fallback {
			return errNoBatch
		}
		total += n
		return err
	}
	for i, row := range rows {
		scratch = binary.LittleEndian.AppendUint16(scratch[:0], uint16(len(row)))
		for j, v := range row {
			vb, err := encodeValue(nil, v)
			if err != nil {
				return total, fmt.Errorf("skaidb: batch row %d, parameter %d: %w", i, j+1, err)
			}
			scratch = binary.LittleEndian.AppendUint32(scratch, uint32(len(vb)))
			scratch = append(scratch, vb...)
		}
		if batchHeaderLen+len(scratch) > batchFrameLimit {
			return total, fmt.Errorf("skaidb: batch row %d is %d bytes, over the %d-byte frame limit", i, len(scratch), batchFrameLimit)
		}
		if count > 0 && len(body)+len(scratch) > batchFrameLimit {
			if err := send(); err != nil {
				return total, err
			}
			body, start, count = body[:batchHeaderLen], i, 0
		}
		body = append(body, scratch...)
		count++
	}
	err = send()
	if errors.Is(err, errNoBatch) {
		// A server that predates OP_EXECUTE_BATCH: the same statement, one
		// OP_EXECUTE per row.
		return c.execBatchLoop(query, rows, &id)
	}
	return total, err
}

// errNoBatch marks a server too old to know OP_EXECUTE_BATCH.
var errNoBatch = errors.New("skaidb: server does not support batches")

// sendBatchChunk sends one OP_EXECUTE_BATCH frame and reads its reply.
// start is the batch index of the chunk's first row: every row before it
// is already applied, which the errors say. fallback reports an
// "unknown opcode" answer to the first chunk.
func (c *conn) sendBatchChunk(req []byte, start int) (int64, bool, error) {
	if err := c.writeFrame(req); err != nil {
		if start > 0 {
			return 0, false, fmt.Errorf("skaidb: batch rows from %d not sent (the %d rows before them applied): %w",
				start, start, c.transportErr(err, true))
		}
		return 0, false, c.transportErr(err, false)
	}
	frame, err := c.readFrame()
	if err != nil {
		err = c.transportErr(err, true)
		if start > 0 {
			err = fmt.Errorf("skaidb: batch rows from %d (the %d rows before them applied): %w", start, start, err)
		}
		return 0, false, err
	}
	r := &reader{buf: frame}
	switch tag := r.u8(); tag {
	case 1: // Mutation: the chunk's total
		n := int64(r.u64())
		if r.err != nil {
			return 0, false, r.err
		}
		return n, false, nil
	case 2: // Ddl
		return 0, false, nil
	case 3:
		msg := r.text()
		if start == 0 && strings.Contains(msg, "unknown opcode") {
			return 0, true, nil
		}
		if start > 0 {
			return 0, false, fmt.Errorf("skaidb: %s (row numbers count from batch row %d; the %d rows before it applied)", msg, start, start)
		}
		return 0, false, fmt.Errorf("skaidb: %s", msg)
	default:
		c.broken.Store(true)
		return 0, false, fmt.Errorf("skaidb: unexpected response tag %d to a batch", tag)
	}
}

// execBatchLoop runs the batch one statement per row: with typed
// parameters on the prepared id when there is one, otherwise text-bound.
func (c *conn) execBatchLoop(query string, rows [][]any, id *uint32) (int64, error) {
	var total int64
	for i, row := range rows {
		n, err := c.execBatchRow(query, row, id)
		if err != nil {
			if i > 0 && errors.Is(err, driver.ErrBadConn) {
				// Nothing of THIS row was sent, but the rows before it
				// applied: a retry of the whole batch must not happen.
				err = fmt.Errorf("skaidb: connection lost before batch row %d was sent (the %d rows before it applied)", i, i)
			}
			return total, fmt.Errorf("skaidb: batch row %d: %w", i, err)
		}
		total += n
	}
	return total, nil
}

func (c *conn) execBatchRow(query string, row []any, id *uint32) (int64, error) {
	args, err := c.namedArgs(row)
	if err != nil {
		return 0, err
	}
	var res driver.Result
	if id != nil {
		r, err := c.sendPrepared(*id, args)
		if err != nil {
			return 0, err
		}
		if res, err = c.readResult(r); err != nil {
			return 0, err
		}
	} else {
		sqlText, err := bind(query, args)
		if err != nil {
			return 0, err
		}
		if res, err = c.exec(sqlText); err != nil {
			return 0, err
		}
	}
	return res.RowsAffected()
}

// namedArgs converts one row as database/sql would convert the arguments of
// ExecContext: CheckNamedValue, then the default converter for anything it
// hands back.
func (c *conn) namedArgs(row []any) ([]driver.NamedValue, error) {
	out := make([]driver.NamedValue, len(row))
	for i, v := range row {
		nv := driver.NamedValue{Ordinal: i + 1, Value: v}
		if err := c.CheckNamedValue(&nv); err != nil {
			if !errors.Is(err, driver.ErrSkip) {
				return nil, err
			}
			dv, err := driver.DefaultParameterConverter.ConvertValue(nv.Value)
			if err != nil {
				return nil, fmt.Errorf("parameter %d: %w", i+1, err)
			}
			nv.Value = dv
		}
		out[i] = nv
	}
	return out, nil
}

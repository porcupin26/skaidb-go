package skaidb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The server's whole message for a refused COMMIT. The driver matches its
// prefix only.
const conflictMsg = "transaction conflict: a row of table accounts was changed by another writer " +
	"after the transaction began — nothing was committed; run the transaction again"

// silent, returned by a txServer reply, leaves the statement unanswered.
var silent = []byte{}

// txServer is a fake server that answers each statement with the frame reply
// returns for its SQL text — nil is a Mutation of one row, silent no answer
// at all — whichever request carried it: OP_QUERY, or OP_PREPARE followed by
// OP_EXECUTE. reply runs on the server's goroutine.
func txServer(t *testing.T, reply func(sql string) []byte) (*conn, *recorder) {
	t.Helper()
	var prepared []string // by id - 1
	answer := func(sql string) [][]byte {
		f := reply(sql)
		if f == nil {
			return [][]byte{mutationFrame(1)}
		}
		if len(f) == 0 {
			return nil
		}
		return [][]byte{f}
	}
	return recordingServer(t, func(req []byte) [][]byte {
		switch req[0] {
		case 1: // OP_QUERY
			return answer(string(req[6:]))
		case 2: // OP_PREPARE
			prepared = append(prepared, string(req[5:]))
			return [][]byte{preparedFrame(uint32(len(prepared)), uint16(strings.Count(string(req[5:]), "?")))}
		case 3: // OP_EXECUTE
			return answer(prepared[binary.LittleEndian.Uint32(req[2:])-1])
		}
		return [][]byte{errorFrame("unexpected request")}
	})
}

// statements lists the SQL text of every statement a txServer received, in
// order; a prepared statement counts where it was executed.
func statements(rec *recorder) []string {
	var out, prepared []string
	for _, req := range rec.all() {
		switch req[0] {
		case 1:
			out = append(out, string(req[6:]))
		case 2:
			prepared = append(prepared, string(req[5:]))
		case 3:
			out = append(out, prepared[binary.LittleEndian.Uint32(req[2:])-1])
		}
	}
	return out
}

func wantStatements(t *testing.T, rec *recorder, want ...string) {
	t.Helper()
	if got := statements(rec); !reflect.DeepEqual(got, want) {
		t.Fatalf("the server received\n  %q\nwant\n  %q", got, want)
	}
}

// onCommit answers the n-th COMMIT with the n-th frame — with a success once
// they run out — and every other statement with a success.
func onCommit(frames ...[]byte) func(string) []byte {
	n := 0
	return func(sql string) []byte {
		if sql != "COMMIT" || n >= len(frames) {
			return nil
		}
		n++
		return frames[n-1]
	}
}

// onStatement answers the statements named in frames with their frame and
// every other statement with a success.
func onStatement(frames map[string][]byte) func(string) []byte {
	return func(sql string) []byte { return frames[sql] }
}

// countingConnector is testConnector that counts the connections
// database/sql asked for.
type countingConnector struct {
	testConnector
	dials *atomic.Int32
}

func (c countingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	c.dials.Add(1)
	return c.testConnector.Connect(ctx)
}

func txDB(t *testing.T, c *conn) (*sql.DB, *atomic.Int32) {
	t.Helper()
	dials := new(atomic.Int32)
	db := sql.OpenDB(countingConnector{testConnector{c}, dials})
	t.Cleanup(func() { db.Close() })
	return db, dials
}

// wantPooled checks that the transaction's one connection is back in the
// pool, healthy: it was never discarded and nothing else was dialled.
func wantPooled(t *testing.T, db *sql.DB, c *conn, dials *atomic.Int32) {
	t.Helper()
	if s := db.Stats(); s.InUse != 0 || s.OpenConnections != 1 {
		t.Errorf("pool: %d in use, %d open; want the one connection back in the pool", s.InUse, s.OpenConnections)
	}
	if n := dials.Load(); n != 1 {
		t.Errorf("%d connections dialled, want 1", n)
	}
	if c.closed || !c.IsValid() {
		t.Errorf("the connection was retired (closed = %v, valid = %v)", c.closed, c.IsValid())
	}
}

// update is the body most tests run: one write, counted.
func update(ctx context.Context, runs *int) func(*sql.Conn) error {
	return func(conn *sql.Conn) error {
		*runs++
		_, err := conn.ExecContext(ctx, "UPDATE accounts SET balance = 90 WHERE id = 1")
		return err
	}
}

const updateSQL = "UPDATE accounts SET balance = 90 WHERE id = 1"

// ---- the typed error --------------------------------------------------------

// A server Error whose message starts with "transaction conflict:" matches
// ErrTransactionConflict wherever a statement error is raised — text,
// prepared, streamed, mid-stream, batch. It still reads "skaidb: <server
// message>" like every statement error, it is not a connection failure, and
// the connection runs the next statement.
func TestConflictErrorOnEveryStatementPath(t *testing.T) {
	plain := "skaidb: " + conflictMsg
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		run  func(c *sql.Conn) error
		want string
	}{
		{"Exec, text", func(c *sql.Conn) error {
			_, err := c.ExecContext(ctx, "COMMIT")
			return err
		}, plain},
		{"Exec, prepared", func(c *sql.Conn) error {
			_, err := c.ExecContext(ctx, "UPDATE t SET v = ? WHERE id = ?", 1, 2)
			return err
		}, plain},
		{"Query, text", func(c *sql.Conn) error {
			rows, err := c.QueryContext(ctx, "COMMIT")
			if err == nil {
				rows.Close()
			}
			return err
		}, plain},
		{"Query, prepared", func(c *sql.Conn) error {
			var v int64
			return c.QueryRowContext(ctx, "SELECT v FROM t WHERE id = ?", 1).Scan(&v)
		}, plain},
		{"streamed query", func(c *sql.Conn) error {
			rows, err := c.QueryContext(WithStreaming(ctx), "SELECT v FROM t")
			if err == nil {
				rows.Close()
			}
			return err
		}, plain},
		{"streamed query with arguments", func(c *sql.Conn) error {
			rows, err := c.QueryContext(WithStreaming(ctx), "SELECT v FROM t WHERE id = ?", 1)
			if err == nil {
				rows.Close()
			}
			return err
		}, plain},
		{"part-way through a stream", func(c *sql.Conn) error {
			rows, err := c.QueryContext(WithStreaming(ctx), "SELECT v FROM mid")
			if err != nil {
				return errors.New("the stream did not start: " + err.Error())
			}
			defer rows.Close()
			n := 0
			for rows.Next() {
				n++
			}
			if n != 1 {
				return errors.New("the row before the error was not delivered")
			}
			return rows.Err()
		}, plain},
		{"batch", func(c *sql.Conn) error {
			_, err := ExecBatchConn(ctx, c, "INSERT INTO t (id) VALUES (?)", [][]any{{1}, {2}})
			return err
		}, plain},
		{"batch, one statement per row", func(c *sql.Conn) error {
			_, err := ExecBatchConn(ctx, c, "INSERT INTO old (id) VALUES (?)", [][]any{{1}, {2}})
			return err
		}, "skaidb: batch row 0: " + plain},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conflict := errorFrame(conflictMsg)
			old := false // the statement in hand is "INSERT INTO old …"
			c, _ := recordingServer(t, func(req []byte) [][]byte {
				switch req[0] {
				case 1:
					if string(req[6:]) == "DELETE FROM after" {
						return [][]byte{mutationFrame(4)}
					}
				case 2:
					old = strings.Contains(string(req[5:]), "old")
					return [][]byte{preparedFrame(7, uint16(strings.Count(string(req[5:]), "?")))}
				case 5:
					if strings.HasSuffix(string(req[6:]), "mid") {
						return [][]byte{headerFrame("v"), chunkFrame(1), conflict}
					}
				case 7:
					if old { // a server without OP_EXECUTE_BATCH
						return [][]byte{errorFrame("unknown opcode 7")}
					}
				}
				return [][]byte{conflict}
			})
			db, dials := txDB(t, c)
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			err = tc.run(conn)
			if !errors.Is(err, ErrTransactionConflict) {
				t.Fatalf("err = %v, want one that matches ErrTransactionConflict", err)
			}
			if err.Error() != tc.want {
				t.Errorf("message = %q\nwant      %q", err, tc.want)
			}
			if errors.Is(err, driver.ErrBadConn) {
				t.Error("a conflict must not read as a bad connection")
			}
			// The same connection takes the next statement.
			res, err := conn.ExecContext(ctx, "DELETE FROM after")
			if err != nil {
				t.Fatalf("the statement after a conflict: %v", err)
			}
			if n, _ := res.RowsAffected(); n != 4 {
				t.Errorf("affected = %d, want 4", n)
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			wantPooled(t, db, c, dials)
		})
	}
}

// A conflict in a later chunk of a batch keeps the note about the rows
// already applied and still matches the sentinel.
func TestConflictErrorInALaterBatchChunk(t *testing.T) {
	old := batchFrameLimit
	batchFrameLimit = 300
	defer func() { batchFrameLimit = old }()
	batches := 0
	c, _ := recordingServer(t, func(req []byte) [][]byte {
		if req[0] == 7 {
			if batches++; batches == 2 {
				return [][]byte{errorFrame(conflictMsg)}
			}
		}
		return batchHandler(2)(req)
	})
	n, err := c.ExecBatch(context.Background(), "INSERT INTO t (id, name) VALUES (?, ?)", intRows(50))
	if !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("err = %v, want one that matches ErrTransactionConflict", err)
	}
	want := "skaidb: " + conflictMsg + " (row numbers count from batch row"
	if n == 0 || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("affected = %d, err = %q; want the first chunk's count and the prefix %q", n, err, want)
	}
}

// Through a balancing Connector: a conflict is a statement error, not a
// connection failure. It does not count against the member — with
// eject_errors=1 one transport failure would eject it — and the connection
// stays open on it for the next statement.
func TestAConflictDoesNotCountAgainstTheMember(t *testing.T) {
	ms := fakeMembers(t, "a", "b")
	for _, m := range ms {
		m.conflict.Store(true)
	}
	c, db := lbDB(t, endpointsOf(ms), "load_balancing=round_robin&eject_errors=1")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	runs := 0
	err := Transaction(ctx, db, update(ctx, &runs), TxAttempts(4))
	if !errors.Is(err, ErrTransactionConflict) || runs != 4 {
		t.Fatalf("err = %v after %d runs, want the conflict after 4", err, runs)
	}
	open := func() int {
		n := 0
		for _, m := range c.Members() {
			if m.State != "ready" {
				t.Errorf("member %s is %q after a conflict, want ready", m.Endpoint, m.State)
			}
			n += m.Open
		}
		return n
	}
	for _, m := range ms {
		memberAt(t, c, m.endpoint, func(m *member) {
			if m.errors != 0 {
				t.Errorf("member %s has %d failures counted, want 0", m.endpoint, m.errors)
			}
		})
	}
	if s := db.Stats(); open() != 1 || s.OpenConnections != 1 || s.InUse != 0 {
		t.Fatalf("%d connections open (pool: %d open, %d in use); want the transaction's one, idle", open(), s.OpenConnections, s.InUse)
	}
	// The pool hands the same connection to the next statement.
	who(t, db)
	if n := open(); n != 1 {
		t.Errorf("%d connections open after the next statement, want the same one", n)
	}
}

// Only the prefix decides, and only at the start of the message: the rest of
// the text may change, and a message that merely mentions a conflict is an
// ordinary statement error. Either way the text is the server's, behind
// "skaidb: ".
func TestConflictIsMatchedOnThePrefixAlone(t *testing.T) {
	for msg, want := range map[string]bool{
		conflictMsg:                                                  true,
		"transaction conflict:":                                      true,
		"transaction conflict: reworded text":                        true,
		"transaction conflict":                                       false,
		"Transaction conflict: capitalised":                          false,
		" transaction conflict: indented":                            false,
		"procedure p failed: transaction conflict: a row of table t": false,
		"syntax error at 1:1":                                        false,
	} {
		err := serverError(msg)
		if got := errors.Is(err, ErrTransactionConflict); got != want {
			t.Errorf("%q: matches ErrTransactionConflict = %v, want %v", msg, got, want)
		}
		if err.Error() != "skaidb: "+msg {
			t.Errorf("%q: message = %q", msg, err)
		}
	}
	// Through a statement as well: an ordinary error is not a conflict.
	c, _ := txServer(t, onStatement(map[string][]byte{"COMMIT": errorFrame("no transaction in progress")}))
	_, err := c.ExecContext(context.Background(), "COMMIT", nil)
	if err == nil || err.Error() != "skaidb: no transaction in progress" || errors.Is(err, ErrTransactionConflict) {
		t.Errorf("err = %v, want the plain statement error", err)
	}
}

// ---- the helper -------------------------------------------------------------

// Success: the opening statement, the body's statements, COMMIT — once, all
// on one pooled connection, and what the body computed is the caller's.
func TestTransactionCommitsOnce(t *testing.T) {
	c, rec := txServer(t, onStatement(map[string][]byte{"INSERT INTO ledger (id) VALUES (7)": mutationFrame(5)}))
	db, dials := txDB(t, c)
	ctx := context.Background()
	runs := 0
	var got int64
	err := Transaction(ctx, db, func(conn *sql.Conn) error {
		runs++
		if _, err := conn.ExecContext(ctx, "UPDATE accounts SET balance = balance - ? WHERE id = ?", 10, 1); err != nil {
			return err
		}
		res, err := conn.ExecContext(ctx, "INSERT INTO ledger (id) VALUES (7)")
		if err != nil {
			return err
		}
		got, err = res.RowsAffected()
		return err
	})
	if err != nil {
		t.Fatalf("Transaction: %v", err)
	}
	if runs != 1 || got != 5 {
		t.Errorf("body ran %d times and left %d; want once, 5", runs, got)
	}
	wantStatements(t, rec, "BEGIN",
		"UPDATE accounts SET balance = balance - ? WHERE id = ?", "INSERT INTO ledger (id) VALUES (7)", "COMMIT")
	wantPooled(t, db, c, dials)
}

// A COMMIT refused with a conflict twice, then accepted: the body ran three
// times — three is the default, and TxAttempts(3) says the same — and the
// caller holds what the last run computed.
func TestTransactionRunsAgainAfterAConflict(t *testing.T) {
	for name, opts := range map[string][]TxOption{"default": nil, "TxAttempts(3)": {TxAttempts(3)}} {
		t.Run(name, func(t *testing.T) {
			c, rec := txServer(t, onCommit(errorFrame(conflictMsg), errorFrame(conflictMsg)))
			db, dials := txDB(t, c)
			ctx := context.Background()
			runs, got := 0, 0
			err := Transaction(ctx, db, func(conn *sql.Conn) error {
				runs++
				got = runs * 10
				_, err := conn.ExecContext(ctx, updateSQL)
				return err
			}, opts...)
			if err != nil {
				t.Fatalf("Transaction: %v", err)
			}
			if runs != 3 || got != 30 {
				t.Errorf("body ran %d times and left %d; want 3 runs, the last one's 30", runs, got)
			}
			wantStatements(t, rec,
				"BEGIN", updateSQL, "COMMIT",
				"BEGIN", updateSQL, "COMMIT",
				"BEGIN", updateSQL, "COMMIT")
			wantPooled(t, db, c, dials)
		})
	}
}

// A conflict on every COMMIT: after exactly TxAttempts runs the conflict is
// the result — the server's message, matching the sentinel — with no
// ROLLBACK (the refused COMMIT ended the transaction).
func TestTransactionReturnsTheConflictWhenAttemptsRunOut(t *testing.T) {
	for _, attempts := range []int{2, 1} {
		conflict := errorFrame(conflictMsg)
		c, rec := txServer(t, onCommit(conflict, conflict, conflict, conflict))
		db, dials := txDB(t, c)
		ctx := context.Background()
		runs := 0
		err := Transaction(ctx, db, update(ctx, &runs), TxAttempts(attempts))
		if !errors.Is(err, ErrTransactionConflict) || err.Error() != "skaidb: "+conflictMsg {
			t.Fatalf("TxAttempts(%d): err = %v, want the conflict", attempts, err)
		}
		if runs != attempts {
			t.Errorf("TxAttempts(%d): body ran %d times", attempts, runs)
		}
		want := []string{"BEGIN", updateSQL, "COMMIT", "BEGIN", updateSQL, "COMMIT"}[:3*attempts]
		wantStatements(t, rec, want...)
		wantPooled(t, db, c, dials)
	}
}

// A body that fails: ROLLBACK is sent and the body's own error comes back —
// the very value — whatever the server answers to the ROLLBACK. A server
// with nothing open may answer it with an Error.
func TestTransactionRollsBackWhenTheBodyFails(t *testing.T) {
	for name, rollbackReply := range map[string][]byte{
		"ROLLBACK succeeds":   nil,
		"ROLLBACK is refused": errorFrame("no transaction in progress"),
	} {
		t.Run(name, func(t *testing.T) {
			c, rec := txServer(t, onStatement(map[string][]byte{"ROLLBACK": rollbackReply}))
			db, dials := txDB(t, c)
			ctx := context.Background()
			bodyErr := errors.New("insufficient funds")
			runs := 0
			err := Transaction(ctx, db, func(conn *sql.Conn) error {
				if err := update(ctx, &runs)(conn); err != nil {
					return err
				}
				return bodyErr
			})
			if err != bodyErr {
				t.Fatalf("err = %v, want the body's own error", err)
			}
			if runs != 1 {
				t.Errorf("body ran %d times, want 1", runs)
			}
			wantStatements(t, rec, "BEGIN", updateSQL, "ROLLBACK")
			wantPooled(t, db, c, dials)
		})
	}
}

// A conflict that the BODY returns — a statement of its own refused with one
// — is a body error like any other: ROLLBACK, no second run.
func TestTransactionDoesNotRunAgainWhenTheBodyReturnsAConflict(t *testing.T) {
	c, rec := txServer(t, onStatement(map[string][]byte{updateSQL: errorFrame(conflictMsg)}))
	db, dials := txDB(t, c)
	ctx := context.Background()
	runs := 0
	err := Transaction(ctx, db, update(ctx, &runs))
	if !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("err = %v, want the body's conflict", err)
	}
	if runs != 1 {
		t.Errorf("body ran %d times, want 1", runs)
	}
	wantStatements(t, rec, "BEGIN", updateSQL, "ROLLBACK")
	wantPooled(t, db, c, dials)
}

// A refused opening statement ends it there: the body does not run — its
// statements would each commit on their own — and neither COMMIT nor
// ROLLBACK is sent. Not even a conflict-worded refusal is tried again.
func TestTransactionStopsWhenBeginIsRefused(t *testing.T) {
	for _, refusal := range []string{"transactions on a cluster open with BEGIN ATOMIC", conflictMsg} {
		c, rec := txServer(t, onStatement(map[string][]byte{"BEGIN": errorFrame(refusal)}))
		db, dials := txDB(t, c)
		ctx := context.Background()
		runs := 0
		err := Transaction(ctx, db, update(ctx, &runs))
		if err == nil || err.Error() != "skaidb: "+refusal {
			t.Fatalf("err = %v, want the server's refusal %q", err, refusal)
		}
		if runs != 0 {
			t.Errorf("body ran %d times after a refused BEGIN", runs)
		}
		wantStatements(t, rec, "BEGIN")
		wantPooled(t, db, c, dials)
	}
}

// The same when it is the opening statement of a second run that is refused.
func TestTransactionStopsWhenTheBeginOfARetryIsRefused(t *testing.T) {
	begins := 0
	c, rec := txServer(t, func(sql string) []byte {
		switch sql {
		case "BEGIN":
			if begins++; begins == 2 {
				return errorFrame("server is shutting down")
			}
		case "COMMIT":
			return errorFrame(conflictMsg)
		}
		return nil
	})
	db, dials := txDB(t, c)
	ctx := context.Background()
	runs := 0
	err := Transaction(ctx, db, update(ctx, &runs))
	if err == nil || err.Error() != "skaidb: server is shutting down" {
		t.Fatalf("err = %v, want the refusal of the second BEGIN", err)
	}
	if runs != 1 {
		t.Errorf("body ran %d times, want 1", runs)
	}
	wantStatements(t, rec, "BEGIN", updateSQL, "COMMIT", "BEGIN")
	wantPooled(t, db, c, dials)
}

// A COMMIT that fails for any other reason is returned as it is: the
// transaction is over, so it is neither run again nor rolled back.
func TestTransactionDoesNotRetryAnotherCommitError(t *testing.T) {
	c, rec := txServer(t, onCommit(errorFrame("write quorum unavailable")))
	db, dials := txDB(t, c)
	ctx := context.Background()
	runs := 0
	err := Transaction(ctx, db, update(ctx, &runs))
	if err == nil || err.Error() != "skaidb: write quorum unavailable" || errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("err = %v, want the COMMIT's own error", err)
	}
	if runs != 1 {
		t.Errorf("body ran %d times, want 1", runs)
	}
	wantStatements(t, rec, "BEGIN", updateSQL, "COMMIT")
	wantPooled(t, db, c, dials)
}

// TxAttempts below 1 is the caller's mistake, reported before anything
// happens: no connection is taken from the pool and nothing is sent.
func TestTransactionRefusesAttemptsBelowOne(t *testing.T) {
	for _, n := range []int{0, -1} {
		c, rec := txServer(t, onCommit())
		db, dials := txDB(t, c)
		ctx := context.Background()
		runs := 0
		err := Transaction(ctx, db, update(ctx, &runs), TxAttempts(n))
		if err == nil || !strings.Contains(err.Error(), "transaction attempts must be at least 1") {
			t.Fatalf("TxAttempts(%d): err = %v, want an argument error", n, err)
		}
		if dials.Load() != 0 {
			t.Errorf("TxAttempts(%d): a connection was dialled", n)
		}

		conn, cerr := db.Conn(ctx)
		if cerr != nil {
			t.Fatal(cerr)
		}
		err = TransactionConn(ctx, conn, update(ctx, &runs), TxAttempts(n))
		if err == nil || !strings.Contains(err.Error(), "transaction attempts must be at least 1") {
			t.Fatalf("TransactionConn, TxAttempts(%d): err = %v, want an argument error", n, err)
		}
		conn.Close()
		if runs != 0 || len(rec.all()) != 0 {
			t.Errorf("TxAttempts(%d): body ran %d times, %d requests sent; want nothing at all", n, runs, len(rec.all()))
		}
	}
}

// TxBegin's text opens the transaction, verbatim — every run of it.
func TestTransactionSendsACustomBeginVerbatim(t *testing.T) {
	const begin = "BEGIN ATOMIC PARTITION (tenant = 'acme')"
	c, rec := txServer(t, onCommit(errorFrame(conflictMsg)))
	db, dials := txDB(t, c)
	ctx := context.Background()
	runs := 0
	if err := Transaction(ctx, db, update(ctx, &runs), TxBegin(begin)); err != nil {
		t.Fatalf("Transaction: %v", err)
	}
	wantStatements(t, rec, begin, updateSQL, "COMMIT", begin, updateSQL, "COMMIT")
	wantPooled(t, db, c, dials)
}

// TransactionConn runs on the connection the caller pinned — the statements
// before and after it share the socket — and leaves it open.
func TestTransactionConnUsesTheCallersConnection(t *testing.T) {
	c, rec := txServer(t, onCommit(errorFrame(conflictMsg)))
	db, dials := txDB(t, c)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, `USE "app"`); err != nil {
		t.Fatal(err)
	}
	runs := 0
	var handed *sql.Conn
	err = TransactionConn(ctx, conn, func(inner *sql.Conn) error {
		handed = inner
		return update(ctx, &runs)(inner)
	})
	if err != nil {
		t.Fatalf("TransactionConn: %v", err)
	}
	if handed != conn || runs != 2 {
		t.Errorf("body got its own connection = %v, ran %d times; want the caller's, twice", handed == conn, runs)
	}
	if s := db.Stats(); s.InUse != 1 {
		t.Errorf("%d connections in use, want the caller's still checked out", s.InUse)
	}
	if _, err := conn.ExecContext(ctx, "DELETE FROM audit"); err != nil {
		t.Fatalf("the caller's connection after TransactionConn: %v", err)
	}
	wantStatements(t, rec, `USE "app"`, "BEGIN", updateSQL, "COMMIT", "BEGIN", updateSQL, "COMMIT", "DELETE FROM audit")
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	wantPooled(t, db, c, dials)
}

// A body that panics still gets its ROLLBACK, before the panic carries on:
// the connection goes back to the pool without an open transaction.
func TestTransactionRollsBackWhenTheBodyPanics(t *testing.T) {
	c, rec := txServer(t, onCommit())
	db, dials := txDB(t, c)
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_ = Transaction(context.Background(), db, func(*sql.Conn) error { panic("boom") })
	}()
	if recovered != "boom" {
		t.Fatalf("recovered %v, want the body's panic", recovered)
	}
	wantStatements(t, rec, "BEGIN", "ROLLBACK")
	wantPooled(t, db, c, dials)
}

// The body's error may be the context's own. The connection is healthy —
// no statement was interrupted — and returns to the pool, so the ROLLBACK
// must still go out, on a context of its own.
func TestTransactionRollsBackAfterTheContextIsCancelled(t *testing.T) {
	c, rec := txServer(t, onCommit())
	db, dials := txDB(t, c)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := Transaction(ctx, db, func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(context.Background(), updateSQL); err != nil {
			return err
		}
		cancel()
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	wantStatements(t, rec, "BEGIN", updateSQL, "ROLLBACK")
	wantPooled(t, db, c, dials)
}

// A statement cut short by its deadline leaves the socket mid-exchange and
// the driver retires the connection; the transaction ends with it. No
// ROLLBACK is written there — it could only wait for an answer — and the
// failure is not retried.
func TestTransactionSendsNoRollbackOnARetiredConnection(t *testing.T) {
	c, rec := txServer(t, onStatement(map[string][]byte{updateSQL: silent}))
	db, _ := txDB(t, c)
	runs := 0
	start := time.Now()
	err := Transaction(context.Background(), db, func(conn *sql.Conn) error {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		return update(ctx, &runs)(conn)
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if took := time.Since(start); took > rollbackTimeout/2 {
		t.Errorf("Transaction took %v on a connection that was already retired", took)
	}
	if runs != 1 {
		t.Errorf("body ran %d times, want 1", runs)
	}
	wantStatements(t, rec, "BEGIN", updateSQL)
	if c.IsValid() {
		t.Error("a connection interrupted mid-statement must not go back to the pool")
	}
}

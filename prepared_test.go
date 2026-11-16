package skaidb

// The server's prepared-statement table — 256 entries per connection — and
// the connection's own cache in front of it.

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// tableFull is the server's refusal of a prepare on a connection that holds
// 256 statements already.
const tableFull = "too many prepared statements (max 256); close some first"

// statementTable is a fake server that keeps prepared statements as the real
// one does: at most 256 on the connection, each until OP_CLOSE names it.
type statementTable struct {
	mu      sync.Mutex
	next    uint32
	open    map[uint32]string
	peak    int      // most statements open at once
	refused int      // prepares refused because the table was full
	typed   []string // statements run with typed parameters (OP_EXECUTE), in order
	params  [][]any  // their parameters, decoded
	literal []string // statements that arrived as text (OP_QUERY)
	batches []string // statements run as a batch (OP_EXECUTE_BATCH)
}

func (s *statementTable) handle(req []byte) [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.open == nil {
		s.open = map[uint32]string{}
	}
	switch req[0] {
	case 1: // OP_QUERY
		s.literal = append(s.literal, string(req[6:]))
		return [][]byte{mutationFrame(1)}
	case 2: // OP_PREPARE
		if len(s.open) >= 256 {
			s.refused++
			return [][]byte{errorFrame(tableFull)}
		}
		s.next++
		s.open[s.next] = string(req[5:])
		s.peak = max(s.peak, len(s.open))
		return [][]byte{preparedFrame(s.next, uint16(strings.Count(string(req[5:]), "?")))}
	case 3, 7: // OP_EXECUTE, OP_EXECUTE_BATCH
		text, ok := s.open[binary.LittleEndian.Uint32(req[2:])]
		if !ok {
			return [][]byte{errorFrame("unknown prepared statement id")}
		}
		if req[0] == 7 {
			s.batches = append(s.batches, text)
			return [][]byte{mutationFrame(1)}
		}
		s.typed = append(s.typed, text)
		s.params = append(s.params, decodeExecuteParams(req))
		return [][]byte{mutationFrame(1)}
	case 4: // OP_CLOSE
		delete(s.open, binary.LittleEndian.Uint32(req[1:]))
		return [][]byte{ddlFrame()}
	}
	return [][]byte{errorFrame("unexpected request")}
}

// Three hundred distinct statements with arguments on one connection — more
// than the server keeps for it. Every one is bound with typed parameters:
// the connection caches 240, below the server's 256, and a statement it
// does not keep is closed on the server as soon as it has run, so the table
// never fills and no statement is ever sent as text with its values
// written into it, which would type them differently (an array or a
// document has no literal form at all).
func TestThreeHundredStatementsOnOneConnectionAreAllBoundTyped(t *testing.T) {
	st := &statementTable{}
	c, _ := recordingServer(t, st.handle)
	db, _ := txDB(t, c)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	at := time.Date(2026, 11, 16, 1, 2, 3, 0, time.UTC)
	var want []string
	for i := 0; i < 300; i++ {
		q := fmt.Sprintf("INSERT INTO t (id, a%d, tags, doc, at, raw) VALUES (?, ?, ?, ?, ?, ?)", i)
		want = append(want, q)
		if _, err := conn.ExecContext(ctx, q, i, "v", []any{"x", int64(i)}, map[string]any{"n": int64(i)}, at, []byte{0xde, 0xad}); err != nil {
			t.Fatalf("statement %d: %v", i, err)
		}
	}
	// A *sql.Stmt past the cache: prepared, run and closed on each use,
	// and Stmt.Close has nothing left to release.
	const held = "UPDATE t SET v = ? WHERE id = ?"
	stmt, err := conn.PrepareContext(ctx, held)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := stmt.ExecContext(ctx, "w", i); err != nil {
			t.Fatalf("the *sql.Stmt, run %d: %v", i, err)
		}
		want = append(want, held)
	}
	if err := stmt.Close(); err != nil {
		t.Fatal(err)
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if st.refused != 0 {
		t.Errorf("the server refused %d prepares: its table was full", st.refused)
	}
	if len(st.literal) != 0 {
		t.Errorf("%d statements were sent as text, the first %q", len(st.literal), st.literal[0])
	}
	if !reflect.DeepEqual(st.typed, want) {
		t.Errorf("%d statements ran with typed parameters, want all %d, in order", len(st.typed), len(want))
	}
	if st.peak != preparedCacheSize+1 || len(st.open) != preparedCacheSize {
		t.Errorf("the server held up to %d statements and holds %d now; want the cache's %d and one in hand", st.peak, len(st.open), preparedCacheSize)
	}
	// The last of the 300, which the cache did not keep, arrived as typed
	// as the first.
	for _, i := range []int{0, 299} {
		p := st.params[i]
		_, isArray := p[2].([]any)
		_, isDoc := p[3].(map[string]any)
		_, isTime := p[4].(time.Time)
		_, isBytes := p[5].([]byte)
		if len(p) != 6 || !isArray || !isDoc || !isTime || !isBytes {
			t.Errorf("statement %d: parameters arrived as %T %T %T %T, want an array, a document, a timestamp and bytes", i, p[2], p[3], p[4], p[5])
		}
	}
}

// A prepare the server refuses because its table is full is the statement's
// error. It is not answered by sending the statement as text with the
// values written into it — the remedy for a statement KIND the server does
// not prepare — which would run it with other types than the caller bound.
func TestAPrepareRefusedForLackOfRoomIsNotAnsweredWithLiterals(t *testing.T) {
	ctx := context.Background()
	const insert = "INSERT INTO t (id, name) VALUES (?, ?)"
	for name, run := range map[string]func(db *sql.DB) error{
		"Exec": func(db *sql.DB) error {
			_, err := db.ExecContext(ctx, insert, 1, "a")
			return err
		},
		"Query": func(db *sql.DB) error {
			rows, err := db.QueryContext(ctx, insert, 1, "a")
			if err == nil {
				rows.Close()
			}
			return err
		},
		"ExecBatch": func(db *sql.DB) error {
			_, err := ExecBatch(ctx, db, insert, [][]any{{1, "a"}, {2, "b"}})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			st := &statementTable{open: map[uint32]string{}}
			for i := uint32(1); i <= 256; i++ { // somebody filled the table
				st.open[1000+i] = "held"
			}
			c, _ := recordingServer(t, st.handle)
			db, dials := txDB(t, c)
			err := run(db)
			if err == nil || err.Error() != "skaidb: "+tableFull {
				t.Errorf("err = %v, want the server's refusal", err)
			}
			st.mu.Lock()
			if len(st.literal) != 0 {
				t.Errorf("the statement was sent as text: %q", st.literal)
			}
			// A kind the server does not prepare still takes the text path.
			delete(st.open, 1001)
			st.mu.Unlock()
			// It is a statement error: the connection stays in the pool.
			if _, err := db.ExecContext(ctx, "DELETE FROM t WHERE id = ?", 1); err != nil {
				t.Fatalf("the next statement: %v", err)
			}
			wantPooled(t, db, c, dials)
		})
	}
}

// The remedy stays for what it is for: a statement kind the server does not
// prepare is bound as text.
func TestAStatementKindThatCannotBePreparedIsStillBoundAsText(t *testing.T) {
	c, rec := recordingServer(t, func(req []byte) [][]byte {
		if req[0] == 2 {
			return [][]byte{errorFrame(unbindableKind)}
		}
		return [][]byte{ddlFrame()}
	})
	db, _ := txDB(t, c)
	if _, err := db.ExecContext(context.Background(), "CREATE TABLE t (PRIMARY KEY (id)) WITH (ttl = ?)", 60); err != nil {
		t.Fatal(err)
	}
	reqs := rec.all()
	if len(reqs) != 2 || reqs[1][0] != 1 || string(reqs[1][6:]) != "CREATE TABLE t (PRIMARY KEY (id)) WITH (ttl = 60)" {
		t.Fatalf("requests = %q, want the prepare and then the statement as text", reqs)
	}
}

// ---- a statement id is its connection's -------------------------------------

const (
	stmtA = "UPDATE t SET v = ? WHERE id = ?"
	stmtB = "DELETE FROM t WHERE v = ? AND id = ?" // as many parameters, another effect
)

// A prepared statement's id is an index on the connection that issued it:
// on another connection the same id is unknown, or names whatever was
// prepared there first. A and B are prepared on one connection, which is
// then lost; on its replacement B runs first — and takes the id A had —
// then A, through what the application held before the loss. Each runs as
// itself: the ids live in the connection's own cache, a new connection
// starts with none, and database/sql prepares a *sql.Stmt anew on every
// connection it runs it on. The fake server resolves each execute through
// the ids it issued on that connection, so what it records is what ran.
func TestAPreparedStatementRunsAsItselfOnAReplacedConnection(t *testing.T) {
	ctx := context.Background()
	for name, handles := range map[string]func(t *testing.T, db *sql.DB) (a, b func() error){
		"*sql.Stmt": func(t *testing.T, db *sql.DB) (func() error, func() error) {
			a, err := db.PrepareContext(ctx, stmtA)
			if err != nil {
				t.Fatal(err)
			}
			b, err := db.PrepareContext(ctx, stmtB)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { a.Close(); b.Close() })
			return func() error { _, err := a.ExecContext(ctx, 1, 2); return err },
				func() error { _, err := b.ExecContext(ctx, 1, 2); return err }
		},
		"db.Exec with arguments": func(t *testing.T, db *sql.DB) (func() error, func() error) {
			return func() error { _, err := db.ExecContext(ctx, stmtA, 1, 2); return err },
				func() error { _, err := db.ExecContext(ctx, stmtB, 1, 2); return err }
		},
	} {
		t.Run(name, func(t *testing.T) {
			l, db := lossyServer(t, nil)
			a, b := handles(t, db)
			for _, run := range []func() error{a, b} { // connection 0: A is id 1, B id 2
				if err := run(); err != nil {
					t.Fatal(err)
				}
			}
			l.kill(0)
			for _, run := range []func() error{b, a} { // connection 1: B is id 1
				if err := run(); err != nil {
					t.Fatalf("after the connection was replaced: %v", err)
				}
			}
			l.wantReceived([]string{stmtA, stmtB}, []string{stmtB, stmtA})
			if n := l.unknownIDs.Load(); n != 0 {
				t.Errorf("%d executes named an id their connection never issued", n)
			}
		})
	}
}

// The same for a *sql.Stmt that database/sql runs on another pooled
// connection than the one it was first used on: it is prepared there, under
// that connection's id.
func TestASQLStmtIsPreparedOnEachConnectionItRunsOn(t *testing.T) {
	l, db := lossyServer(t, nil)
	ctx := context.Background()
	a, err := db.PrepareContext(ctx, stmtA)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, err := a.ExecContext(ctx, 1, 2); err != nil { // connection 0: A is id 1
		t.Fatal(err)
	}
	held, err := db.Conn(ctx) // connection 0 is taken ...
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if _, err := db.ExecContext(ctx, stmtB, 1, 2); err != nil { // ... so this is connection 1: B is id 1
		t.Fatal(err)
	}
	if _, err := a.ExecContext(ctx, 1, 2); err != nil { // and A's id 1 means B there
		t.Fatal(err)
	}
	l.wantReceived([]string{stmtA}, []string{stmtB, stmtA})
	if n := l.unknownIDs.Load(); n != 0 {
		t.Errorf("%d executes named an id their connection never issued", n)
	}
}

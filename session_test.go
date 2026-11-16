package skaidb

// What a borrower leaves on a pooled connection besides a USE: a consistency
// override, a scan budget, an open transaction. The pool hands every
// borrower a connection in its own state — the pool's database, no override,
// no budget, no transaction — whoever had it before.

import (
	"context"
	"database/sql"
	"testing"
)

const (
	setBudget        = "SET SCAN BUDGET ROWS 1"
	setConsistency   = "SET CONSISTENCY ALL"
	clearBudget      = "SET SCAN BUDGET DEFAULT"
	clearConsistency = "SET CONSISTENCY DEFAULT"
)

// exec1 runs each statement on q, in order.
func exec1(t *testing.T, q interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, statements ...string) {
	t.Helper()
	for _, s := range statements {
		if _, err := q.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
}

// One connection, two borrowers. The first enters another database, limits
// its scans to one row and asks for consistency ALL, then gives the
// connection back. The second gets the pool's connection, not the first
// borrower's: its statement runs in the pool's database, is not refused for
// a scan budget it never set, and reads at the level its own request
// carries. The driver put the connection back — USE, SET SCAN BUDGET
// DEFAULT, SET CONSISTENCY DEFAULT — before the second borrower's first
// statement, and only then: afterwards there is nothing left to put back.
func TestABorrowerLeavesNoSessionStateForTheNext(t *testing.T) {
	ctx := context.Background()
	for _, pool := range []struct{ name, dsnDatabase, home string }{
		{"a database in the DSN", "app", "app"},
		{"no database in the DSN", "", "default"},
	} {
		t.Run(pool.name, func(t *testing.T) {
			m := fakeMembers(t, "a")[0]
			db := memberDB(t, m, pool.dsnDatabase)
			db.SetMaxOpenConns(1)
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			exec1(t, conn, "USE other", setBudget, setConsistency)
			// For as long as the borrower holds it, the connection is as it
			// left it.
			if _, err := conn.ExecContext(ctx, addNote); err != nil {
				t.Fatal(err)
			}
			if got, want := m.ranIn(t, addNote), (fakeSession{database: "other", consistency: "ALL", scanBudget: true}); got != want {
				t.Errorf("the borrower's own statement ran in %+v, want %+v", got, want)
			}
			conn.Close()

			count1(t, db)
			if got, want := m.ranIn(t, countNotes), (fakeSession{database: pool.home}); got != want {
				t.Errorf("the next borrower's statement ran in %+v, want %+v", got, want)
			}
			count1(t, db)
			var first []string
			if pool.dsnDatabase != "" {
				first = []string{useSQL(pool.dsnDatabase)}
			}
			wantMemberReceived(t, m, append(first,
				"USE other", setBudget, setConsistency, addNote,
				useSQL(pool.home), clearBudget, clearConsistency,
				countNotes, countNotes))
		})
	}
}

// Through the *sql.DB every statement is a borrow of its own, so a setting
// sent that way is over when the statement returns: the next statement runs
// without it, on whichever connection it gets. Settings that are to hold go
// on a *sql.Conn.
func TestASettingSentThroughTheDBDoesNotOutliveItsStatement(t *testing.T) {
	m := fakeMembers(t, "a")[0]
	db := memberDB(t, m, "")
	exec1(t, db, setBudget, setConsistency)
	count1(t, db)
	if got, want := m.ranIn(t, countNotes), (fakeSession{database: "default"}); got != want {
		t.Errorf("the statement ran in %+v, want %+v", got, want)
	}
	wantMemberReceived(t, m, []string{setBudget, clearBudget, setConsistency, clearConsistency, countNotes})
}

// Only what the borrower changed is put back, so a borrower that changed
// nothing — or cleared its own settings before it gave the connection back
// — costs the next one no round trip.
func TestOnlyWhatABorrowerChangedIsPutBack(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name     string
		borrower []string
		putBack  []string
	}{
		{"nothing", []string{addNote}, nil},
		{"a scan budget", []string{setBudget}, []string{clearBudget}},
		{"a scan budget in bytes", []string{"SET SCAN BUDGET BYTES 4096"}, []string{clearBudget}},
		{"a consistency override", []string{"set consistency one;"}, []string{clearConsistency}},
		{"a scan budget it cleared itself", []string{setBudget, "set scan budget default"}, nil},
		{"a consistency override it cleared itself", []string{setConsistency, clearConsistency + ";"}, nil},
		{"settings changed twice", []string{setBudget, "SET SCAN BUDGET ROWS 5", setConsistency, "SET CONSISTENCY ONE"},
			[]string{clearBudget, clearConsistency}},
		{"settings the server refused", []string{"SET SCAN BUDGET ROWS 0 -- refused", "SET CONSISTENCY SOME -- refused"}, nil},
		{"a budget with a comment and odd spacing", []string{"SET -- for the export\n\tSCAN\u00a0BUDGET ROWS 10 -- rows"}, []string{clearBudget}},
		{"quoted words, as the server takes them", []string{`SET "scan" "budget" ROWS 1`, `SET "Consistency" ALL`},
			[]string{clearBudget, clearConsistency}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := fakeMembers(t, "a")[0]
			db := memberDB(t, m, "")
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, s := range tc.borrower {
				_, _ = conn.ExecContext(ctx, s)
			}
			conn.Close()
			exec1(t, db, addNote, addNote)
			want := append(append([]string{}, tc.borrower...), tc.putBack...)
			wantMemberReceived(t, m, append(want, addNote, addNote))
		})
	}
}

// A connection that cannot be put back is not handed out. A server before
// 0.349 has no SET CONSISTENCY DEFAULT and refuses it; the connection that
// carries a borrower's override is then closed, and the next borrower's
// statement runs on a new one — never under the override.
func TestAConnectionWhoseSettingsCannotBeClearedIsDropped(t *testing.T) {
	m := fakeMembers(t, "a")[0]
	m.refuseDefault.Store(true)
	db := memberDB(t, m, "")
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	exec1(t, conn, setBudget, setConsistency)
	conn.Close()
	count1(t, db)
	if got, want := m.ranIn(t, countNotes), (fakeSession{database: "default"}); got != want {
		t.Errorf("the statement ran in %+v, want %+v", got, want)
	}
	wantMemberReceived(t, m,
		[]string{setBudget, setConsistency, clearBudget, clearConsistency},
		[]string{countNotes})
	if s := db.Stats(); s.OpenConnections != 1 {
		t.Errorf("%d connections open, want the new one alone", s.OpenConnections)
	}
}

// A transaction lives on its connection, and one a borrower left open must
// not reach the next borrower: that one's statements would run inside it,
// seen by nobody else and never committed. A connection that comes back
// with a transaction open is closed — the server drops a transaction with
// its connection, at once — and the next borrower gets another. That is
// what becomes of a BEGIN sent through the *sql.DB, and of a *sql.Conn
// closed between its BEGIN and its COMMIT.
func TestATransactionLeftOpenDoesNotReachTheNextBorrower(t *testing.T) {
	ctx := context.Background()
	for name, borrow := range map[string]func(t *testing.T, db *sql.DB) []string{
		"a BEGIN through the *sql.DB": func(t *testing.T, db *sql.DB) []string {
			exec1(t, db, "BEGIN")
			return []string{"BEGIN"}
		},
		"a *sql.Conn closed before its COMMIT": func(t *testing.T, db *sql.DB) []string {
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			exec1(t, conn, "begin transaction;", updateSQL)
			return []string{"begin transaction;", updateSQL}
		},
		"a transaction a failed statement did not end": func(t *testing.T, db *sql.DB) []string {
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			exec1(t, conn, "BEGIN ATOMIC PARTITION (tenant = 'acme') -- one shard")
			const failing = "UPDATE accounts SET balance = 1/0 -- refused"
			if _, err := conn.ExecContext(ctx, failing); err == nil {
				t.Fatal("the failing statement succeeded")
			}
			return []string{"BEGIN ATOMIC PARTITION (tenant = 'acme') -- one shard", failing}
		},
		"a COMMIT the server could not read": func(t *testing.T, db *sql.DB) []string {
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			exec1(t, conn, "BEGIN")
			// Not a COMMIT to the server (a parse error): the transaction is
			// still open.
			const work = "COMMIT WORK -- refused"
			if _, err := conn.ExecContext(ctx, work); err == nil {
				t.Fatal("COMMIT WORK succeeded")
			}
			return []string{"BEGIN", work}
		},
		"a second BEGIN the server refused": func(t *testing.T, db *sql.DB) []string {
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			exec1(t, conn, "BEGIN")
			_, _ = conn.ExecContext(ctx, "BEGIN -- refused")
			return []string{"BEGIN", "BEGIN -- refused"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := fakeMembers(t, "a")[0]
			db := memberDB(t, m, "")
			db.SetMaxOpenConns(1)
			sent := borrow(t, db)
			exec1(t, db, addNote)
			if m.ranIn(t, addNote).transaction {
				t.Error("the next borrower's statement ran inside the transaction left open")
			}
			wantMemberReceived(t, m, sent, []string{addNote})
			if s := db.Stats(); s.OpenConnections != 1 {
				t.Errorf("%d connections open, want the new one alone", s.OpenConnections)
			}
		})
	}
}

// A transaction that was ended — committed, rolled back, or refused at its
// COMMIT, which ends it too — leaves its connection as the pool hands it
// out: it is used again, and nothing is sent to put it back. So does a
// transaction that was never opened, and so does Transaction, whose
// statements are noted like any other.
func TestAnEndedTransactionLeavesItsConnectionInThePool(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name       string
		statements []string
		conflict   bool // the member refuses COMMIT with a conflict
	}{
		{"committed", []string{"BEGIN", updateSQL, "COMMIT"}, false},
		{"rolled back", []string{"BEGIN", updateSQL, "ROLLBACK"}, false},
		{"other spellings", []string{"begin transaction", updateSQL, " Commit\tTransaction ; -- done"}, false},
		{"rolled back, with a comment", []string{"BEGIN", updateSQL, "rollback -- not this time"}, false},
		{"refused at COMMIT", []string{"BEGIN", updateSQL, "COMMIT"}, true},
		{"never opened", []string{"BEGIN -- refused", updateSQL}, false},
		{"a ROLLBACK with nothing open", []string{"ROLLBACK"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := fakeMembers(t, "a")[0]
			m.conflict.Store(tc.conflict)
			db := memberDB(t, m, "")
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, s := range tc.statements {
				_, _ = conn.ExecContext(ctx, s)
			}
			conn.Close()
			m.conflict.Store(false)
			exec1(t, db, addNote)
			if m.ranIn(t, addNote).transaction {
				t.Error("the next statement ran inside a transaction")
			}
			wantMemberReceived(t, m, append(append([]string{}, tc.statements...), addNote))
		})
	}
	t.Run("Transaction", func(t *testing.T) {
		m := fakeMembers(t, "a")[0]
		db := memberDB(t, m, "")
		runs := 0
		for i := 0; i < 2; i++ {
			if err := Transaction(ctx, db, update(ctx, &runs)); err != nil {
				t.Fatal(err)
			}
		}
		exec1(t, db, addNote)
		wantMemberReceived(t, m, []string{"BEGIN", updateSQL, "COMMIT", "BEGIN", updateSQL, "COMMIT", addNote})
	})
}

// What the driver reads as a session statement, besides USE: a text that is
// one statement whose first words are SET CONSISTENCY or SET SCAN BUDGET —
// with DEFAULT behind them it clears the setting, with anything else it
// sets one; a statement whose first word is BEGIN, which opens a
// transaction; and exactly COMMIT [TRANSACTION] or ROLLBACK [TRANSACTION],
// which ends one. Words are apart by whatever the server takes for
// whitespace, `--` comments are skipped, and the words that are names to
// the server may be written in double quotes. Anything else is left alone.
func TestSessionStatement(t *testing.T) {
	for sql, want := range map[string]sessionKind{
		"SET CONSISTENCY ALL":                    consistencySet,
		"set consistency one":                    consistencySet,
		"SET CONSISTENCY QUORUM;":                consistencySet,
		"  SET\tCONSISTENCY\nALL  ;  ":           consistencySet,
		"SET CONSISTENCY ALL -- every replica":   consistencySet,
		"-- strict\nSET CONSISTENCY ALL":         consistencySet,
		"SET -- strict\n CONSISTENCY ALL":        consistencySet,
		"SET\u00a0CONSISTENCY\u3000ALL":          consistencySet,
		`SET "consistency" ALL`:                  consistencySet,
		`SET"consistency"ALL`:                    consistencySet,
		"SET CONSISTENCY DEFAULT":                consistencyCleared,
		"set consistency default;":               consistencyCleared,
		`SET CONSISTENCY "default"`:              consistencyCleared,
		"SET CONSISTENCY -- back\nDEFAULT":       consistencyCleared,
		"SET CONSISTENCY DEFAULTS":               consistencySet,
		"SET SCAN BUDGET ROWS 1":                 scanBudgetSet,
		"set scan budget rows 100 bytes 4096;":   scanBudgetSet,
		"SET SCAN BUDGET BYTES 4096 ROWS 100":    scanBudgetSet,
		"SET\vSCAN\fBUDGET\u00a0ROWS 1":          scanBudgetSet,
		`SET "scan" "budget" ROWS 1`:             scanBudgetSet,
		"SET SCAN BUDGET -- tight\nROWS 1":       scanBudgetSet,
		"SET SCAN BUDGET DEFAULT":                scanBudgetCleared,
		"set scan budget default ;":              scanBudgetCleared,
		`SET SCAN BUDGET "DEFAULT"`:              scanBudgetCleared,
		"SET SCAN BUDGET DEFAULT -- back":        scanBudgetCleared,
		"BEGIN":                                  transactionBegin,
		"begin;":                                 transactionBegin,
		"BEGIN TRANSACTION":                      transactionBegin,
		"BEGIN ATOMIC":                           transactionBegin,
		"BEGIN ATOMIC PARTITION (tenant = 'a')":  transactionBegin,
		"BEGIN -- the transfer":                  transactionBegin,
		"-- the transfer\n  BEGIN":               transactionBegin,
		"BEGIN\u00a0TRANSACTION":                 transactionBegin,
		"COMMIT":                                 transactionCommit,
		"commit;":                                transactionCommit,
		"COMMIT TRANSACTION":                     transactionCommit,
		" Commit\tTransaction ; -- done":         transactionCommit,
		"COMMIT -- all of it\n;":                 transactionCommit,
		"ROLLBACK":                               transactionRollback,
		"rollback transaction;":                  transactionRollback,
		"ROLLBACK -- not this time":              transactionRollback,
		"COMMIT WORK":                            otherStatement,
		"COMMIT;;":                               otherStatement,
		"COMMIT; BEGIN":                          otherStatement,
		"COMMIT TRANSACTION now":                 otherStatement,
		`COMMIT "TRANSACTION"`:                   otherStatement,
		"ROLLBACK TO savepoint":                  otherStatement,
		"ROLLBACK TRANSACTION TRANSACTION":       otherStatement,
		`"BEGIN"`:                                otherStatement,
		`"COMMIT"`:                               otherStatement,
		"BEGINS":                                 otherStatement,
		"COMMITTED":                              otherStatement,
		"END":                                    otherStatement,
		"START TRANSACTION":                      otherStatement,
		"EXPLAIN COMMIT":                         otherStatement,
		"CREATE TABLE begin (PRIMARY KEY (id))":  otherStatement,
		"USE app":                                useDatabase,
		`USE DATABASE "app";`:                    useDatabase,
		"SET":                                    otherStatement,
		"SET CONSISTENCY":                        otherStatement,
		"SET SCAN":                               otherStatement,
		"SET SCAN BUDGET":                        otherStatement,
		"SET SCAN ROWS 1":                        otherStatement,
		"SET CONFIG storage.scan_row_budget = 5": otherStatement,
		`"SET" CONSISTENCY ALL`:                  otherStatement,
		"SETCONSISTENCY ALL":                     otherStatement,
		"SET CONSISTENCYALL":                     otherStatement,
		"RESET CONSISTENCY":                      otherStatement,
		"EXPLAIN SET CONSISTENCY ALL":            otherStatement,
		"SELECT 'SET CONSISTENCY ALL'":           otherStatement,
		"SELECT 1":                               otherStatement,
		"SHOW DATABASES":                         otherStatement,
		"-- SET CONSISTENCY ALL":                 otherStatement,
		"/* x */ SET CONSISTENCY ALL":            otherStatement,
		"USE":                                    otherStatement,
		"":                                       otherStatement,
	} {
		if got, _ := sessionStatement(sql); got != want {
			t.Errorf("sessionStatement(%q) = %v, want %v", sql, got, want)
		}
	}
	if kind, name := sessionStatement(`USE "app" -- there`); kind != useDatabase || name != "app" {
		t.Errorf(`a USE: %v, %q; want the database it names`, kind, name)
	}
}

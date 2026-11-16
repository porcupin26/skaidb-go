package skaidb

// A transaction the application writes itself — BEGIN … COMMIT as statements
// on one *sql.Conn — and what becomes of it when its connection is lost
// (PROTOCOL.md §6 rule 7): nothing of it is sent on another session, and it
// ends on the connection object it was opened on.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"
)

const (
	row1    = "INSERT INTO ledger (id) VALUES (1)"
	row2    = "INSERT INTO ledger (id) VALUES (2)"
	row3    = "INSERT INTO ledger (id) VALUES (3)"
	ledger  = "SELECT id FROM ledger"
	theLoss = "connection lost mid-statement"
)

// opened is a *sql.Conn with a transaction open on it and one row written.
func opened(t *testing.T, db *sql.DB) *sql.Conn {
	t.Helper()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	exec1(t, conn, "BEGIN", row1)
	return conn
}

// readsLedger runs a statement through the pool: after a lost transaction it
// works, on a connection of its own.
func readsLedger(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), ledger)
	if err != nil {
		t.Fatalf("a statement through the pool after the lost transaction: %v", err)
	}
	rows.Close()
}

// The connection is found dead before the transaction's next statement is
// written: by the write itself, or — when the socket still takes writes — by
// the look at it. database/sql does the rest: the statement comes back as
// driver.ErrBadConn, which closes the *sql.Conn, and it puts no other
// connection under a *sql.Conn. So the rest of the transaction — the
// statements after it, its COMMIT, its ROLLBACK — fails on the closed
// *sql.Conn without the driver being asked, nothing reaches the server on
// the dead connection or on a new one, and none of the rows exists. The
// pool is as it is after any loss.
func TestAHandWrittenTransactionFoundLostSendsNothingMore(t *testing.T) {
	ctx := context.Background()
	for _, lose := range []struct {
		name string
		lose func(l *lossy)
	}{
		{"the write fails", func(l *lossy) { l.kill(0) }},
		{"the server closed the connection", func(l *lossy) { l.halfClose(0) }},
		{"the server reset the connection", func(l *lossy) { l.reset(0) }},
	} {
		for _, end := range []string{"COMMIT", "ROLLBACK"} {
			t.Run(lose.name+", "+end, func(t *testing.T) {
				l, db := lossyServer(t, nil)
				conn := opened(t, db)
				lose.lose(l)
				if _, err := conn.ExecContext(ctx, row2); !errors.Is(err, driver.ErrBadConn) {
					t.Errorf("the statement that found the connection lost: err = %v, want driver.ErrBadConn", err)
				}
				for _, s := range []string{row3, end} {
					if _, err := conn.ExecContext(ctx, s); !errors.Is(err, sql.ErrConnDone) {
						t.Errorf("%s on the lost *sql.Conn: err = %v, want sql.ErrConnDone", s, err)
					}
				}
				l.wantReceived([]string{"BEGIN", row1})
				if n := l.lateBytes(0); n != 0 {
					t.Errorf("%d bytes were written to the dead connection", n)
				}
				readsLedger(t, db)
				l.wantReceived([]string{"BEGIN", row1}, []string{ledger})
			})
		}
	}
}

// The connection is lost under a statement of the transaction — the request
// left, the answer never came — or the statement was cut short by its
// deadline, which retires the connection. Such a statement's error is not
// driver.ErrBadConn, so the *sql.Conn stays with its owner, and the driver
// answers what comes next on it:
//
//   - a further statement fails, and nothing is sent;
//   - ROLLBACK succeeds, without being sent: the server dropped the
//     transaction with the connection, which is what a ROLLBACK asks for;
//   - COMMIT fails with an error that says the connection was lost and
//     nothing was committed, without being sent.
//
// Either ends the transaction on this connection object: what follows is a
// statement on a retired connection like any other.
func TestAHandWrittenTransactionLostUnderAStatementIsEndedByItsOwner(t *testing.T) {
	ctx := context.Background()
	for _, lose := range []struct {
		name string
		// second runs the statement the connection is lost under and checks
		// its error.
		second func(t *testing.T, conn *sql.Conn)
	}{
		{"the answer never comes", func(t *testing.T, conn *sql.Conn) {
			if _, err := conn.ExecContext(ctx, row2); err == nil || !strings.Contains(err.Error(), theLoss) {
				t.Errorf("the statement the connection was lost under: err = %v, want the mid-statement loss", err)
			}
		}},
		{"the statement is cut short by its deadline", func(t *testing.T, conn *sql.Conn) {
			short, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
			defer cancel()
			if _, err := conn.ExecContext(short, row2); !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("the statement cut short: err = %v, want context.DeadlineExceeded", err)
			}
		}},
	} {
		for _, end := range []string{"COMMIT", "ROLLBACK", "rollback transaction; -- give up"} {
			t.Run(lose.name+", "+end, func(t *testing.T) {
				l, db := lossyServer(t, func(_ int, sql string) []byte {
					if sql == row2 {
						if strings.HasPrefix(lose.name, "the answer") {
							return hangup
						}
						return silent
					}
					return nil
				})
				conn := opened(t, db)
				lose.second(t, conn)
				if _, err := conn.ExecContext(ctx, row3); !errors.Is(err, errRetired) {
					t.Errorf("the statement after the loss: err = %v, want the retired-connection error", err)
				}
				_, err := conn.ExecContext(ctx, end)
				if end == "COMMIT" {
					if !errors.Is(err, driver.ErrBadConn) || !strings.Contains(err.Error(), "transaction not committed") ||
						!strings.Contains(err.Error(), "before COMMIT was sent") {
						t.Errorf("COMMIT after the loss: err = %v, want the lost connection reported, nothing committed", err)
					}
				} else if err != nil {
					t.Errorf("%s after the loss: err = %v, want success — the transaction went with the connection", end, err)
				}
				// The transaction is over on this *sql.Conn; the connection
				// under it is as dead as before.
				if _, err := conn.ExecContext(ctx, row3); err == nil {
					t.Error("a statement on the lost connection succeeded after the transaction was ended")
				}
				if _, err := conn.ExecContext(ctx, "ROLLBACK"); err == nil {
					t.Error("a second ROLLBACK succeeded: there is no transaction left to end")
				}
				l.wantReceived([]string{"BEGIN", row1, row2})
				conn.Close()
				readsLedger(t, db)
				l.wantReceived([]string{"BEGIN", row1, row2}, []string{ledger})
			})
		}
	}
}

// With rows still open on the *sql.Conn a statement that cannot be written
// is not driver.ErrBadConn either — database/sql would wait for those rows —
// so here too the transaction is ended by its owner: ROLLBACK succeeds
// unsent, COMMIT says nothing was committed, and neither closes the
// *sql.Conn under the rows.
func TestAHandWrittenTransactionLostWithRowsOpenIsEndedByItsOwner(t *testing.T) {
	ctx := context.Background()
	for _, end := range []string{"COMMIT", "ROLLBACK"} {
		t.Run(end, func(t *testing.T) {
			l, db := listServer(t)
			conn := opened(t, db)
			rows, err := conn.QueryContext(ctx, list)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			l.kill(0)
			if _, err := conn.ExecContext(ctx, row2); !errors.Is(err, errNotSent) {
				t.Fatalf("the statement that could not be written: err = %v, want the not-sent error", err)
			}
			err, returned := within(2*time.Second, func() error {
				_, err := conn.ExecContext(ctx, end)
				return err
			})
			if !returned {
				t.Fatalf("%s did not return while rows were open", end)
			}
			if end == "COMMIT" {
				if err == nil || errors.Is(err, driver.ErrBadConn) || !strings.Contains(err.Error(), "transaction not committed") {
					t.Errorf("COMMIT: err = %v, want nothing committed, and not driver.ErrBadConn while rows are open", err)
				}
			} else if err != nil {
				t.Errorf("ROLLBACK: err = %v, want success", err)
			}
			l.wantReceived([]string{"BEGIN", row1, list})
		})
	}
}

// A COMMIT that cannot be written — the connection is found lost when the
// COMMIT itself is about to be sent — did not commit, and its error says so,
// in the words Transaction uses for it. A ROLLBACK in that place succeeds:
// the transaction is gone with the connection either way.
func TestAHandWrittenCommitThatCannotBeSentWasNotCommitted(t *testing.T) {
	ctx := context.Background()
	for _, lose := range []struct {
		name string
		lose func(l *lossy)
	}{
		{"the write fails", func(l *lossy) { l.kill(0) }},
		{"the server closed the connection", func(l *lossy) { l.halfClose(0) }},
	} {
		t.Run(lose.name, func(t *testing.T) {
			l, db := lossyServer(t, nil)
			conn := opened(t, db)
			lose.lose(l)
			_, err := conn.ExecContext(ctx, "COMMIT")
			if !errors.Is(err, driver.ErrBadConn) || !strings.Contains(err.Error(), "transaction not committed") {
				t.Errorf("COMMIT: err = %v, want the lost connection reported, nothing committed", err)
			}
			l.wantReceived([]string{"BEGIN", row1})
			if n := l.lateBytes(0); n != 0 {
				t.Errorf("%d bytes were written to the dead connection", n)
			}
		})
		t.Run(lose.name+", ROLLBACK", func(t *testing.T) {
			l, db := lossyServer(t, nil)
			conn := opened(t, db)
			lose.lose(l)
			if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
				t.Errorf("ROLLBACK: err = %v, want success — the transaction went with the connection", err)
			}
			if _, err := conn.ExecContext(ctx, row3); err == nil {
				t.Error("a statement on the lost connection succeeded")
			}
			l.wantReceived([]string{"BEGIN", row1})
			conn.Close()
			readsLedger(t, db)
			l.wantReceived([]string{"BEGIN", row1}, []string{ledger})
		})
	}
}

// Lost under the COMMIT itself — sent, and never answered — the outcome is
// not known, and the error says that and nothing else: not that nothing was
// committed, and not driver.ErrBadConn. A ROLLBACK afterwards is not
// answered with success: the driver does not know that anything was rolled
// back.
func TestAHandWrittenTransactionLostUnderItsCommitClaimsNothing(t *testing.T) {
	l, db := lossyServer(t, func(_ int, sql string) []byte {
		if sql == "COMMIT" {
			return hangup
		}
		return nil
	})
	ctx := context.Background()
	conn := opened(t, db)
	_, err := conn.ExecContext(ctx, "COMMIT")
	if err == nil || !strings.Contains(err.Error(), "statement may or may not have applied") {
		t.Fatalf("COMMIT: err = %v, want the mid-statement loss", err)
	}
	if errors.Is(err, driver.ErrBadConn) || strings.Contains(err.Error(), "not committed") {
		t.Errorf("COMMIT: err = %v — it was sent, its outcome is unknown", err)
	}
	if _, err := conn.ExecContext(ctx, "ROLLBACK"); !errors.Is(err, errRetired) {
		t.Errorf("ROLLBACK after the lost COMMIT: err = %v, want the retired-connection error", err)
	}
	l.wantReceived([]string{"BEGIN", row1, "COMMIT"})
}

// A statement the connection refuses for another reason — a streamed result
// is still open on it — has not lost anything: the transaction is still
// open, its COMMIT is not answered by the driver, and once the rows are
// closed it is sent.
func TestAHandWrittenCommitRefusedWhileStreamingIsStillOpen(t *testing.T) {
	l, db := streamServer(t)
	ctx := context.Background()
	conn := opened(t, db)
	rows, err := conn.QueryContext(WithStreaming(ctx), scan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); !errors.Is(err, errStreamBusy) {
		t.Fatalf("COMMIT with a stream open: err = %v, want the busy-streaming error", err)
	}
	rows.Close()
	exec1(t, conn, "COMMIT")
	l.wantReceived([]string{"BEGIN", row1, scan, "COMMIT"})
	conn.Close()
	exec1(t, db, debit)
	l.wantReceived([]string{"BEGIN", row1, scan, "COMMIT", debit})
}

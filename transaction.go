package skaidb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrTransactionConflict matches, with errors.Is, the error of a statement
// the server refused with a transaction conflict: a COMMIT whose transaction
// writes a row another writer changed after the transaction began.
//
//	if _, err := conn.ExecContext(ctx, "COMMIT"); errors.Is(err, skaidb.ErrTransactionConflict) {
//		// nothing was committed: run the transaction again from BEGIN
//	}
//
// Nothing of the transaction was applied and the transaction is over; the
// connection is back in autocommit and stays usable, so running the whole
// transaction again from its opening statement is safe. Transaction and
// TransactionConn do exactly that.
//
// The error itself reads like every other statement error —
// "skaidb: <server message>" — so code that matches on the text keeps
// working. It is a statement error, not a connection failure: the connection
// stays in the pool and nothing counts against its member.
var ErrTransactionConflict = errors.New("skaidb: transaction conflict")

// conflictPrefix is how the server's message for a transaction conflict
// starts (PROTOCOL.md §6 rule 6). Only the prefix is matched: the rest of the
// text names the table today and may change.
const conflictPrefix = "transaction conflict:"

// conflictError is the statement error for such a message: the text every
// statement error has, plus the identity of ErrTransactionConflict.
type conflictError struct{ text string }

func (e *conflictError) Error() string        { return e.text }
func (e *conflictError) Is(target error) bool { return target == ErrTransactionConflict }

// serverError is the error for a server Error response to a statement:
// "skaidb: <message>", whichever request the statement travelled in. A
// message that starts with conflictPrefix also matches
// ErrTransactionConflict.
func serverError(msg string) error {
	if strings.HasPrefix(msg, conflictPrefix) {
		return &conflictError{text: "skaidb: " + msg}
	}
	return fmt.Errorf("skaidb: %s", msg)
}

// TxOption adjusts Transaction and TransactionConn: TxAttempts, TxBegin.
type TxOption func(*txConfig)

type txConfig struct {
	attempts int
	begin    string
}

// TxAttempts sets how many times the transaction may run in all when its
// COMMIT is refused with a conflict (default 3). 1 runs it once, with no
// retry; below 1 is an error, returned before anything is sent.
func TxAttempts(n int) TxOption { return func(c *txConfig) { c.attempts = n } }

// TxBegin sets the statement that opens the transaction, sent as it is
// (default "BEGIN", which a standalone server takes). A cluster opens one
// with "BEGIN ATOMIC" or "BEGIN ATOMIC PARTITION (col = literal)".
func TxBegin(statement string) TxOption { return func(c *txConfig) { c.begin = statement } }

func newTxConfig(opts []TxOption) (txConfig, error) {
	cfg := txConfig{attempts: 3, begin: "BEGIN"}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.attempts < 1 {
		return cfg, fmt.Errorf("skaidb: transaction attempts must be at least 1, got %d", cfg.attempts)
	}
	return cfg, nil
}

// Transaction runs body inside a transaction and commits it, running it
// again when the COMMIT is refused with a conflict.
//
//	var left int64
//	err := skaidb.Transaction(ctx, db, func(conn *sql.Conn) error {
//		if err := conn.QueryRowContext(ctx, "SELECT qty FROM stock WHERE id = ?", id).Scan(&left); err != nil {
//			return err
//		}
//		left--
//		_, err := conn.ExecContext(ctx, "UPDATE stock SET qty = ? WHERE id = ?", left, id)
//		return err
//	})
//
// A transaction lives on ONE connection, and a *sql.DB hands each statement
// to whichever pooled connection is free. So Transaction takes one
// connection out of the pool for the whole transaction, hands it to body and
// puts it back afterwards: run every statement of the transaction on that
// *sql.Conn. A statement that body sends through the *sql.DB instead is
// outside the transaction.
//
// What is sent, in order:
//
//   - The opening statement: "BEGIN", or TxBegin's. If it fails, its error
//     is returned, body does not run and nothing else is sent — without a
//     transaction each statement of body would commit on its own. A pooled
//     connection that turns out to be dead — the server restarted while it
//     sat idle — is not such a failure: nothing is open yet, so the
//     statement is sent on another connection, as database/sql does for a
//     statement run through the *sql.DB. Once it has been answered the
//     transaction stays on its connection, whatever happens to it.
//   - body's statements. If body returns an error, ROLLBACK is sent and
//     body's error is returned as it is, whatever the server answered to the
//     ROLLBACK. The transaction is not run again, even when body's error is
//     a conflict.
//   - COMMIT. A conflict (ErrTransactionConflict) starts over from the
//     opening statement, at once, on the same connection, until the
//     transaction has run TxAttempts times in all (default 3); the last
//     conflict is returned. Any other COMMIT error is returned as it is:
//     no retry and no ROLLBACK, the transaction is already over.
//
// body may therefore run more than once. It must be safe to repeat: let it
// read what it needs inside the transaction, and do nothing outside the
// database that a second run would do twice. A value it computes is the
// caller's to capture, as left above; after a retry it holds what the last
// run set. Close every *sql.Rows body opens before it returns.
//
// A transport failure or an expired context anywhere is the usual error for
// it and is not retried. If body panics, ROLLBACK is sent before the panic
// carries on.
func Transaction(ctx context.Context, db *sql.DB, body func(conn *sql.Conn) error, opts ...TxOption) error {
	cfg, err := newTxConfig(opts)
	if err != nil {
		return err
	}
	var c *sql.Conn
	defer func() {
		if c != nil {
			c.Close()
		}
	}()
	return runTransaction(ctx, func() (*sql.Conn, error) {
		if c != nil {
			// A run after a conflict: on the same connection, unless that
			// one has died since the refused COMMIT.
			_, err := c.ExecContext(ctx, cfg.begin)
			if !errors.Is(err, driver.ErrBadConn) {
				return c, err
			}
			c.Close()
		}
		var err error
		c, err = beginPooled(ctx, db, cfg.begin)
		return c, err
	}, body, cfg)
}

// beginPooled takes a connection from the pool and opens the transaction on
// it, moving to another connection when the one it got is dead.
//
// Nothing has been answered at that point, so nothing is lost by opening the
// transaction elsewhere. That ends with the opening statement: a transaction
// is its connection's, and no later statement of it is sent on another one.
func beginPooled(ctx context.Context, db *sql.DB, begin string) (*sql.Conn, error) {
	var retry badConnRetry
	for {
		c, err := db.Conn(ctx)
		if err != nil {
			return nil, err
		}
		if _, err = c.ExecContext(ctx, begin); err == nil {
			return c, nil
		}
		c.Close()
		if !retry.again(db, err) {
			return nil, err
		}
	}
}

// badConnRetry bounds the tries of a call that takes its own connection from
// the pool and finds it dead before anything reached the server
// (driver.ErrBadConn).
//
// database/sql answers that, for the statements it runs itself, with two
// tries on pooled connections and a last one on a new connection. A pinned
// *sql.Conn gets no retry at all, and the pool cannot be asked for a new
// connection. But each dead connection found is closed and gone from the
// pool, so the search ends by itself: after the first failure it may go on
// for as many tries as the pool has idle connections — when the server
// restarted, each is as dead as the first — and two more.
type badConnRetry struct{ tries, limit int }

// again reports whether the call that failed with err is to be tried on
// another connection.
func (r *badConnRetry) again(db *sql.DB, err error) bool {
	if !errors.Is(err, driver.ErrBadConn) {
		return false
	}
	if r.tries == 0 {
		r.limit = db.Stats().Idle + 2
	}
	r.tries++
	return r.tries <= r.limit
}

// TransactionConn is Transaction on a connection the caller has pinned with
// db.Conn — for a transaction that must share the connection's session state
// (USE) or follow other statements on the same socket. The connection stays
// the caller's: it is not closed, and the transaction never leaves it — on
// a dead connection the opening statement fails with driver.ErrBadConn, as
// any statement on that *sql.Conn does.
func TransactionConn(ctx context.Context, c *sql.Conn, body func(conn *sql.Conn) error, opts ...TxOption) error {
	cfg, err := newTxConfig(opts)
	if err != nil {
		return err
	}
	return runTransaction(ctx, func() (*sql.Conn, error) {
		_, err := c.ExecContext(ctx, cfg.begin)
		return c, err
	}, body, cfg)
}

// runTransaction runs the transaction until it commits or fails. begin
// sends the opening statement of a run and returns the connection that
// answered it: the transaction's, until its COMMIT or ROLLBACK.
func runTransaction(ctx context.Context, begin func() (*sql.Conn, error), body func(*sql.Conn) error, cfg txConfig) error {
	for attempt := 1; ; attempt++ {
		c, err := begin()
		if err != nil {
			return err
		}
		if err := runBody(ctx, c, body); err != nil {
			return err
		}
		// A COMMIT that fails has ended the transaction, whatever the reason,
		// so no ROLLBACK follows it.
		_, err = c.ExecContext(ctx, "COMMIT")
		if err == nil || attempt >= cfg.attempts || !errors.Is(err, ErrTransactionConflict) {
			return err
		}
	}
}

// runBody runs body and rolls the transaction back when it does not return
// nil: an error, a panic, or runtime.Goexit (a t.Fatal inside body).
func runBody(ctx context.Context, c *sql.Conn, body func(*sql.Conn) error) (err error) {
	returned := false
	defer func() {
		if err != nil || !returned {
			rollback(ctx, c)
		}
	}()
	err = body(c)
	returned = true
	return err
}

// rollbackTimeout bounds the ROLLBACK that follows a failed body. A var so
// the tests can shorten it.
var rollbackTimeout = 5 * time.Second

// rollback ends the transaction after a failed body. It reports nothing:
// body's error is the one the caller gets, and a server without an open
// transaction may answer the ROLLBACK with an error of its own.
func rollback(ctx context.Context, c *sql.Conn) {
	// A connection the driver has retired — its socket failed, or a cancelled
	// statement left it mid-frame — is closed by the pool, and the
	// transaction ends with it. A ROLLBACK written there could only wait for
	// an answer that is not coming.
	retired := false
	_ = c.Raw(func(driverConn any) error {
		if dc, ok := driverConn.(*conn); ok {
			retired = dc.closed || dc.broken.Load()
		}
		return nil
	})
	if retired {
		return
	}
	// body's error may be the context's own, and the ROLLBACK still has to
	// go out: a healthy connection goes back to the pool, and must not take
	// an open transaction with it.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
	defer cancel()
	_, _ = c.ExecContext(ctx, "ROLLBACK")
}

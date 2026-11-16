package skaidb

// The session database: the DSN's, on every connection of the pool — a
// re-dialled one included.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

// memberDB opens a pool on one fake member, with database as the DSN's
// ("" for a DSN that names none).
func memberDB(t *testing.T, m *fakeMember, database string) *sql.DB {
	t.Helper()
	dsn := "skaidb://" + url.UserPassword(m.v.Auth.Username, m.v.Auth.Password).String() + "@" + m.endpoint + "/" + database
	db, err := sql.Open("skaidb", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// killIdle closes the socket under the pool's idle connection, as a member
// restart does: the pool finds out at the next write.
func killIdle(t *testing.T, db *sql.DB) {
	t.Helper()
	c, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var dc *conn
	if err := c.Raw(func(x any) error { dc = x.(*conn); return nil }); err != nil {
		t.Fatal(err)
	}
	c.Close()
	dc.nc.Close()
}

func wantMemberReceived(t *testing.T, m *fakeMember, want ...[]string) {
	t.Helper()
	if got := m.received(); !reflect.DeepEqual(got, want) {
		t.Fatalf("the member received, connection by connection,\n  %q\nwant\n  %q", got, want)
	}
}

const (
	countNotes = "SELECT count(*) FROM notes"
	addNote    = "INSERT INTO notes (id) VALUES (7)"
	useApp     = `USE "app"`
)

func count1(t *testing.T, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) {
	t.Helper()
	var x string
	if err := q.QueryRowContext(context.Background(), countNotes).Scan(&x); err != nil {
		t.Fatalf("%s: %v", countNotes, err)
	}
}

// The pool's connection is lost while idle and database/sql dials another
// for the next statement. The new session starts in no database of its own:
// the DSN's is entered on it before anything else is sent, so the statement
// — and the write after it — run where the DSN says, not against a table of
// the same name in the default database.
func TestARedialledConnectionEntersTheDSNDatabaseFirst(t *testing.T) {
	m := fakeMembers(t, "a")[0]
	db := memberDB(t, m, "app")
	ctx := context.Background()
	count1(t, db)
	killIdle(t, db)
	count1(t, db)
	if _, err := db.ExecContext(ctx, addNote); err != nil {
		t.Fatalf("%s: %v", addNote, err)
	}
	wantMemberReceived(t, m,
		[]string{useApp, countNotes},
		[]string{useApp, countNotes, addNote})
}

// If the DSN's database cannot be entered on the new connection, the
// statement fails with that error. It is not run in whatever database the
// new session happens to be in.
func TestAStatementFailsWhenTheDSNDatabaseCannotBeEnteredAgain(t *testing.T) {
	m := fakeMembers(t, "a")[0]
	db := memberDB(t, m, "app")
	ctx := context.Background()
	count1(t, db)
	killIdle(t, db)
	m.refuseUse.Store(true)
	_, err := db.ExecContext(ctx, addNote)
	if err == nil || !strings.Contains(err.Error(), "database does not exist") {
		t.Fatalf("err = %v, want the refusal of the USE", err)
	}
	for i, c := range m.received() {
		for _, s := range c {
			if s == addNote {
				t.Errorf("connection %d ran the statement outside the DSN's database: %q", i, c)
			}
		}
	}
}

// A USE on a *sql.Conn is that connection's. database/sql never puts
// another connection under a *sql.Conn, so when it is lost the statements
// on it fail; none is run on a session that is not in the database.
func TestAStatementOnALostConnAfterUseFails(t *testing.T) {
	l, db := lossyServer(t, nil)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "USE app"); err != nil {
		t.Fatal(err)
	}
	l.kill(0)
	var n int
	if err := conn.QueryRowContext(ctx, countNotes).Scan(&n); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("err = %v, want driver.ErrBadConn", err)
	}
	if _, err := conn.ExecContext(ctx, addNote); err == nil {
		t.Fatal("the write on the lost connection succeeded")
	}
	l.wantReceived([]string{"USE app"})
}

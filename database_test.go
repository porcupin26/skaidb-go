package skaidb

// The session database: the DSN's, on every connection of the pool — a
// re-dialled one included — and a USE that lasts as long as the borrow it
// was sent in.

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

// A connection goes back to the pool in the DSN's database. A borrower's
// USE — on a *sql.Conn it held, or sent through the *sql.DB — would
// otherwise stay on that one connection, and whoever got it next would run
// in the borrower's database instead of the pool's. The DSN's database is
// entered again before the next borrower's first statement, and only then:
// a connection that never left it costs nothing.
func TestABorrowersUseDoesNotOutliveItsBorrow(t *testing.T) {
	ctx := context.Background()
	for name, borrow := range map[string]func(t *testing.T, db *sql.DB) []string{
		"on a *sql.Conn": func(t *testing.T, db *sql.DB) []string {
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if _, err := conn.ExecContext(ctx, "USE other"); err != nil {
				t.Fatal(err)
			}
			count1(t, conn)
			return []string{"USE other", countNotes}
		},
		"through the *sql.DB": func(t *testing.T, db *sql.DB) []string {
			if _, err := db.ExecContext(ctx, `USE DATABASE "other";`); err != nil {
				t.Fatal(err)
			}
			return []string{`USE DATABASE "other";`}
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := fakeMembers(t, "a")[0]
			db := memberDB(t, m, "app")
			count1(t, db)
			count1(t, db) // still in app: nothing to restore
			sent := borrow(t, db)
			count1(t, db)
			count1(t, db)
			want := append([]string{useApp, countNotes, countNotes}, sent...)
			wantMemberReceived(t, m, append(want, useApp, countNotes, countNotes))
		})
	}
}

// When the DSN's database cannot be entered again on a connection a
// borrower took elsewhere, that connection is dropped: the next statement
// runs on one that is in the DSN's database, or not at all.
func TestAConnectionThatCannotGoBackToTheDSNDatabaseIsDropped(t *testing.T) {
	m := fakeMembers(t, "a")[0]
	db := memberDB(t, m, "app")
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "USE other"); err != nil {
		t.Fatal(err)
	}
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
	if s := db.Stats(); s.OpenConnections != 0 {
		t.Errorf("%d connections open, want the one left in another database dropped", s.OpenConnections)
	}
}

// A DSN that names no database gives the pool none to go back to: a USE
// stays on the connection it was sent on, and the driver sends none of its
// own.
func TestWithoutADSNDatabaseTheDriverSendsNoUse(t *testing.T) {
	m := fakeMembers(t, "a")[0]
	db := memberDB(t, m, "")
	ctx := context.Background()
	count1(t, db)
	if _, err := db.ExecContext(ctx, "USE other"); err != nil {
		t.Fatal(err)
	}
	count1(t, db)
	wantMemberReceived(t, m, []string{countNotes, "USE other", countNotes})
}

// What the driver reads as a USE: exactly one `USE [DATABASE] <name>`, the
// name bare or in double quotes with "" for a quote inside, an optional
// trailing semicolon. Anything else is left alone.
func TestParseUse(t *testing.T) {
	for sql, want := range map[string]string{
		"USE app":                   "app",
		"use app":                   "app",
		"  USE   app  ;  ":          "app",
		"USE DATABASE app":          "app",
		"use database app;":         "app",
		`USE "app"`:                 "app",
		`USE "My ""quoted"" db";`:   `My "quoted" db`,
		`USE DATABASE "app"`:        "app",
		`USE"app"`:                  "app",
		"USE database":              "database",
		"USE\n\tapp_2":              "app_2",
		`USE "a;b"`:                 "a;b",
		"USE żółw":                  "żółw",
		"USE DATABASE DATABASE":     "DATABASE",
		`USE DATABASE "database";`:  "database",
		"USE app -- comment":        "",
		"USE app; USE other":        "",
		"USE app other":             "",
		"USE":                       "",
		"USE ;":                     "",
		`USE "app`:                  "",
		`USE "app" x`:               "",
		`USE ""`:                    "",
		"USE 'app'":                 "",
		"USE a.b":                   "",
		"USE a-b":                   "",
		"USER app":                  "",
		"USEapp":                    "",
		"SELECT 1":                  "",
		"EXPLAIN USE app":           "",
		"/* x */ USE app":           "",
		"USE app;;":                 "",
		"":                          "",
		"USE DATABASE":              "DATABASE",
		`USE DATABASE"app"`:         "app",
		"USE DATABASEapp":           "DATABASEapp",
		`USE "app"";"`:              `app";`,
		"USE \"app\"\n;":            "app",
		"USE app\x00":               "",
		"USE DATABASE app other":    "",
		`USE DATABASE "a" "b"`:      "",
		"USE DATABASE ;":            "DATABASE",
		"use DataBase App":          "App",
		`USE "DATABASE" app`:        "",
		"USE database database x":   "",
		"USE 1app":                  "1app",
		"USE app$":                  "",
		"USE \"\"\"\"":              `"`,
		"USE app ; ":                "app",
		"\ufeffUSE app":             "",
		"USE\u00a0app":              "",
		"USE app\u00a0":             "",
		"USE \"app\" ; x":           "",
		"USE \"unterminated\"\"":    "",
		"USE database \"\"":         "",
		"USE DATABASE \"x\";":       "x",
		"use\tdatabase\tx":          "x",
		"USE x;\n":                  "x",
		"USE  DATABASE  \"x y\"  ;": "x y",
	} {
		got, ok := parseUse(sql)
		if ok != (want != "") || got != want {
			t.Errorf("parseUse(%q) = %q, %v; want %q", sql, got, ok, want)
		}
	}
}

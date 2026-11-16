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
		"with a comment behind it": func(t *testing.T, db *sql.DB) []string {
			const use = "USE other -- for the nightly report"
			if _, err := db.ExecContext(ctx, use); err != nil {
				t.Fatal(err)
			}
			return []string{use}
		},
		// Pasted from a document, the spaces are not always U+0020. The
		// server takes any Unicode space, a vertical tab and a form feed
		// for whitespace, so the session moves — and the driver has to see
		// that it did.
		"with a no-break space in it": func(t *testing.T, db *sql.DB) []string {
			const use = "USE\u00a0other"
			if _, err := db.ExecContext(ctx, use); err != nil {
				t.Fatal(err)
			}
			return []string{use}
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

// A DSN that names no database opens the pool on the database every session
// starts in, `default`, and that is where its connections go back to. A
// borrower's USE lasts for its borrow, as with a database in the DSN: sent
// through the *sql.DB it is over when the statement returns, on a *sql.Conn
// when the *sql.Conn is closed. Left on the connection it would hold for
// whoever got that one connection next and for nobody else — and be gone
// with the connection, at the next restart.
func TestWithoutADSNDatabaseABorrowersUseDoesNotOutliveItsBorrow(t *testing.T) {
	ctx := context.Background()
	const useDefault = `USE "default"`
	for name, borrow := range map[string]func(t *testing.T, db *sql.DB) []string{
		"through the *sql.DB": func(t *testing.T, db *sql.DB) []string {
			if _, err := db.ExecContext(ctx, "USE other"); err != nil {
				t.Fatal(err)
			}
			return []string{"USE other"}
		},
		"on a *sql.Conn": func(t *testing.T, db *sql.DB) []string {
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if _, err := conn.ExecContext(ctx, "USE other"); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.ExecContext(ctx, addNote); err != nil {
				t.Fatal(err)
			}
			return []string{"USE other", addNote}
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := fakeMembers(t, "a")[0]
			db := memberDB(t, m, "")
			count1(t, db) // a new session is in default: the driver sends no USE
			count1(t, db)
			sent := borrow(t, db)
			if len(sent) > 1 {
				// On the *sql.Conn the USE held for as long as it was kept.
				if got := m.ranIn(t, addNote).database; got != "other" {
					t.Errorf("the statement behind the USE on its *sql.Conn ran in %q, want other", got)
				}
			}
			count1(t, db)
			if got := m.ranIn(t, countNotes).database; got != "default" {
				t.Errorf("the next borrower's statement ran in %q, want default", got)
			}
			count1(t, db) // back in default: nothing more to restore
			want := append([]string{countNotes, countNotes}, sent...)
			wantMemberReceived(t, m, append(want, useDefault, countNotes, countNotes))
			// A restart changes nothing: the pool's database is default before
			// and after it.
			m.hangUp()
			count1(t, db)
			if got := m.ranIn(t, countNotes).database; got != "default" {
				t.Errorf("after the connection was replaced the statement ran in %q, want default", got)
			}
			if got := m.received(); len(got) != 2 || !reflect.DeepEqual(got[1], []string{countNotes}) {
				t.Errorf("the replacing connection received %q, want the statement alone", got[1:])
			}
		})
	}
}

// A connection that cannot be taken back to `default` is dropped, like one
// that cannot be taken back to the DSN's database: the next statement runs
// on a connection that is there.
func TestAConnectionThatCannotGoBackToTheDefaultDatabaseIsDropped(t *testing.T) {
	m := fakeMembers(t, "a")[0]
	db := memberDB(t, m, "")
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "USE other"); err != nil {
		t.Fatal(err)
	}
	m.refuseUse.Store(true)
	if _, err := db.ExecContext(ctx, addNote); err != nil {
		t.Fatalf("%s: %v", addNote, err)
	}
	wantMemberReceived(t, m, []string{"USE other", `USE "default"`}, []string{addNote})
	if got := m.ranIn(t, addNote).database; got != "default" {
		t.Errorf("the statement ran in %q, want default", got)
	}
}

// What the driver reads as a USE: exactly one `USE [DATABASE] <name>`, the
// name bare or in double quotes with "" for a quote inside, an optional
// trailing semicolon — and `--` comments, which the server skips to the end
// of their line, outside a quoted name. Whitespace is what the server takes
// for it: the ASCII blanks, vertical tab and form feed included, and every
// Unicode space. Anything else is left alone.
func TestParseUse(t *testing.T) {
	for sql, want := range map[string]string{
		"USE app":                          "app",
		"use app":                          "app",
		"  USE   app  ;  ":                 "app",
		"USE DATABASE app":                 "app",
		"use database app;":                "app",
		`USE "app"`:                        "app",
		`USE "My ""quoted"" db";`:          `My "quoted" db`,
		`USE DATABASE "app"`:               "app",
		`USE"app"`:                         "app",
		"USE database":                     "database",
		"USE\n\tapp_2":                     "app_2",
		`USE "a;b"`:                        "a;b",
		"USE żółw":                         "żółw",
		"USE DATABASE DATABASE":            "DATABASE",
		`USE DATABASE "database";`:         "database",
		"USE app -- the app database":      "app",
		"USE app -- comment":               "app",
		"USE app --":                       "app",
		"USE app--x":                       "app",
		"USE app; -- done":                 "app",
		"USE app -- one\n;":                "app",
		"-- entering app\nUSE app":         "app",
		"USE -- which one?\n app":          "app",
		"USE DATABASE -- the keyword\napp": "app",
		`USE "app" -- quoted`:              "app",
		`USE "a--b"`:                       "a--b",
		`USE "a--b" -- and a comment`:      "a--b",
		`USE "a""--b"`:                     `a"--b`,
		"-- USE app":                       "",
		"USE -- x":                         "",
		"USE app -- one\nother":            "",
		"USE app - - x":                    "",
		"USE app -x":                       "",
		"USE 'a--b'":                       "",
		"USE app /* x */":                  "",
		"USE app; USE other":               "",
		"USE app other":                    "",
		"USE":                              "",
		"USE ;":                            "",
		`USE "app`:                         "",
		`USE "app" x`:                      "",
		`USE ""`:                           "",
		"USE 'app'":                        "",
		"USE a.b":                          "",
		"USE a-b":                          "",
		"USER app":                         "",
		"USEapp":                           "",
		"SELECT 1":                         "",
		"EXPLAIN USE app":                  "",
		"/* x */ USE app":                  "",
		"USE app;;":                        "",
		"":                                 "",
		"USE DATABASE":                     "DATABASE",
		`USE DATABASE"app"`:                "app",
		"USE DATABASEapp":                  "DATABASEapp",
		`USE "app"";"`:                     `app";`,
		"USE \"app\"\n;":                   "app",
		"USE app\x00":                      "",
		"USE DATABASE app other":           "",
		`USE DATABASE "a" "b"`:             "",
		"USE DATABASE ;":                   "DATABASE",
		"use DataBase App":                 "App",
		`USE "DATABASE" app`:               "",
		"USE database database x":          "",
		"USE 1app":                         "1app",
		"USE app$":                         "",
		"USE \"\"\"\"":                     `"`,
		"USE app ; ":                       "app",
		"\ufeffUSE app":                    "",
		"USE\u00a0app":                     "app",
		"USE app\u00a0":                    "app",
		"\u00a0USE app":                    "app",
		"USE\vapp":                         "app",
		"USE\fapp":                         "app",
		"USE\u3000app":                     "app",
		"USE\u0085app\u2003;\u00a0":        "app",
		"USE\u2003DATABASE\u00a0app":       "app",
		"USE\u00a0\"app\"\u00a0;":          "app",
		"USE\u00a0-- which\n\u00a0app":     "app",
		"USE app\u200b":                    "",
		"USE app\u00a0other":               "",
		"USE \"app\" ; x":                  "",
		"USE \"unterminated\"\"":           "",
		"USE database \"\"":                "",
		"USE DATABASE \"x\";":              "x",
		"use\tdatabase\tx":                 "x",
		"USE x;\n":                         "x",
		"USE  DATABASE  \"x y\"  ;":        "x y",
	} {
		got, ok := parseUse(sql)
		if ok != (want != "") || got != want {
			t.Errorf("parseUse(%q) = %q, %v; want %q", sql, got, ok, want)
		}
	}
}

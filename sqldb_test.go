package skaidb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// testConnector hands a scripted conn to database/sql, so a test can drive
// the real *sql.DB API end to end — argument conversion included.
type testConnector struct{ c *conn }

func (t testConnector) Connect(context.Context) (driver.Conn, error) { return t.c, nil }
func (t testConnector) Driver() driver.Driver                        { return &drv{} }

// decodeExecuteParams parses an OP_EXECUTE request into its typed parameters.
func decodeExecuteParams(req []byte) []interface{} {
	r := &reader{buf: req[1:]}
	_ = r.u8()  // consistency
	_ = r.u32() // prepared id
	n := int(r.u16())
	out := make([]interface{}, n)
	for i := range out {
		out[i] = decodeNative(&reader{buf: r.blob()})
	}
	return out
}

// Through the real database/sql API, every kind of argument a Go program
// reasonably passes must bind: Valuers (sql.Null*), unsigned and named
// integers, pointers, and the composites only the typed path can carry.
func TestSQLArgumentsConvertThroughDatabaseSQL(t *testing.T) {
	type userID uint16
	params := make(chan []interface{}, 1)
	c := fakeServer(t, func(s net.Conn) {
		req, err := readReq(s)
		if err != nil || req[0] != 2 {
			return
		}
		p := binary.LittleEndian.AppendUint32([]byte{4}, 1)
		p = binary.LittleEndian.AppendUint16(p, 9)
		s.Write(serverFrame(p))
		req, err = readReq(s)
		if err != nil || req[0] != 3 {
			return
		}
		params <- decodeExecuteParams(req)
		s.Write(mutationFrame(1))
	})
	db := sql.OpenDB(testConnector{c})
	defer db.Close()

	name := "ada"
	when := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	_, err := db.Exec("INSERT INTO t VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		sql.NullString{String: "x", Valid: true}, // Valuer → String
		sql.NullInt64{},                          // Valuer, invalid → Null
		uint8(7),                                 // unsigned → Int
		userID(42),                               // named integer → Int
		&name,                                    // pointer → String
		(*string)(nil),                           // nil pointer → Null
		[]string{"a", "b"},                       // slice → Array
		map[string]any{"k": 1.5},                 // map → Document
		when,                                     // Timestamp
	)
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	got := <-params
	want := []interface{}{"x", nil, int64(7), int64(42), "ada", nil}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("param %d = %#v, want %#v", i, got[i], w)
		}
	}
	if arr, ok := got[6].([]interface{}); !ok || len(arr) != 2 || arr[0] != "a" {
		t.Errorf("param 6 = %#v, want [a b]", got[6])
	}
	if doc, ok := got[7].(map[string]interface{}); !ok || doc["k"] != 1.5 {
		t.Errorf("param 7 = %#v, want {k:1.5}", got[7])
	}
	if ts, ok := got[8].(time.Time); !ok || !ts.Equal(when) {
		t.Errorf("param 8 = %#v, want %v", got[8], when)
	}
}

// The client-side fallback (a statement the server will not prepare) sees
// the same normalised scalars, so a named integer binds there too.
func TestSQLFallbackBindsNormalisedScalars(t *testing.T) {
	type shard int8
	text := make(chan string, 1)
	c := fakeServer(t, func(s net.Conn) {
		req, err := readReq(s)
		if err != nil || req[0] != 2 {
			return
		}
		s.Write(serverFrame(appendStr([]byte{3}, "cannot prepare DDL"))) // Error → fallback
		req, err = readReq(s)
		if err != nil || req[0] != 1 {
			return
		}
		r := &reader{buf: req[2:]}
		text <- r.text()
		s.Write(ddlFrame())
	})
	db := sql.OpenDB(testConnector{c})
	defer db.Close()
	if _, err := db.Exec("ALTER TABLE t SET shard = ?", shard(3)); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if got := <-text; got != "ALTER TABLE t SET shard = 3" {
		t.Errorf("sent %q", got)
	}
}

// Elements inside a composite never see database/sql's converter, so the
// encoder must normalise them itself: every integer width, named scalar
// types, Valuers, pointers and nested composites.
func TestCompositeElementsNormalise(t *testing.T) {
	type label string
	n := 5
	buf, err := encodeValue(nil, []any{
		[]uint16{1, 2},
		[]label{"a"},
		[]sql.NullString{{String: "v", Valid: true}, {}},
		[]*int{&n, nil},
		map[string]int8{"k": -3},
		[2]bool{true, false},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := decodeNative(&reader{buf: buf}).([]interface{})
	if u := got[0].([]interface{}); u[0] != int64(1) || u[1] != int64(2) {
		t.Errorf("[]uint16: %#v", got[0])
	}
	if l := got[1].([]interface{}); l[0] != "a" {
		t.Errorf("[]label: %#v", got[1])
	}
	if v := got[2].([]interface{}); v[0] != "v" || v[1] != nil {
		t.Errorf("[]sql.NullString: %#v", got[2])
	}
	if p := got[3].([]interface{}); p[0] != int64(5) || p[1] != nil {
		t.Errorf("[]*int: %#v", got[3])
	}
	if m := got[4].(map[string]interface{}); m["k"] != int64(-3) {
		t.Errorf("map[string]int8: %#v", got[4])
	}
	if b := got[5].([]interface{}); b[0] != true || b[1] != false {
		t.Errorf("[2]bool: %#v", got[5])
	}
	if _, err := encodeValue(nil, []uint64{1 << 63}); err == nil {
		t.Error("a uint64 above MaxInt64 must be refused")
	}
	if _, err := encodeValue(nil, []any{struct{}{}}); err == nil {
		t.Error("a struct element must be refused")
	}
}

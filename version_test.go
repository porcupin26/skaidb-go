package skaidb

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"net"
	"runtime/debug"
	"testing"
)

// The version Hello reports must be the module version the program was
// built against — never a literal that drifts from the tag. Each shape the
// toolchain can hand us resolves to the wire form the other drivers use.
func TestVersionFromBuildInfo(t *testing.T) {
	dep := func(v string, repl *debug.Module) *debug.BuildInfo {
		return &debug.BuildInfo{
			Main: debug.Module{Path: "example.com/app", Version: "(devel)"},
			Deps: []*debug.Module{
				{Path: "golang.org/x/other", Version: "v9.9.9"},
				{Path: modulePath, Version: v, Replace: repl},
			},
		}
	}
	for _, tc := range []struct {
		name string
		bi   *debug.BuildInfo
		ok   bool
		want string
	}{
		{"no build info", nil, false, "devel"},
		{"nil info", nil, true, "devel"},
		{"tagged dependency", dep("v1.0.0", nil), true, "1.0.0"},
		{"pseudo-version", dep("v1.0.1-0.20260919120000-abcdef123456", nil), true, "1.0.1-0.20260919120000-abcdef123456"},
		{"replace to a tagged fork", dep("v1.0.0", &debug.Module{Path: "example.com/fork", Version: "v1.2.3"}), true, "1.2.3"},
		{"replace to a local checkout", dep("v1.0.0", &debug.Module{Path: "../skaidb-go", Version: ""}), true, "devel"},
		{"main module untagged", &debug.BuildInfo{Main: debug.Module{Path: modulePath, Version: "(devel)"}}, true, "devel"},
		{"main module at a tag", &debug.BuildInfo{Main: debug.Module{Path: modulePath, Version: "v1.0.0"}}, true, "1.0.0"},
		{"not a dependency at all", &debug.BuildInfo{Main: debug.Module{Path: "example.com/app"}}, true, "devel"},
	} {
		if got := versionFromBuildInfo(tc.bi, tc.ok); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

// Version() is what the process actually reports, and it is never empty.
func TestVersionIsSet(t *testing.T) {
	if Version() == "" {
		t.Fatal("Version() must never be empty")
	}
	if Version() != driverVersion {
		t.Fatalf("Version() = %q but Hello sends %q", Version(), driverVersion)
	}
}

// The Hello frame carries exactly Version(): opcode 8, then two
// length-prefixed strings, client name "go" and the version.
func TestHelloFrameCarriesVersion(t *testing.T) {
	got := make(chan []byte, 1)
	c := fakeServer(t, func(s net.Conn) {
		req, err := readReq(s)
		if err != nil {
			got <- nil
			return
		}
		got <- req
		s.Write(serverFrame([]byte{3, 0, 0, 0, 0})) // an old server's "unknown opcode" Error: ignored
	})
	c.sendHello()
	req := <-got
	if req == nil || req[0] != 8 {
		t.Fatalf("expected OP_HELLO (8), got %v", req)
	}
	r := &reader{buf: req[1:]}
	name, ver := r.text(), r.text()
	if r.err != nil || r.pos != len(r.buf) {
		t.Fatalf("malformed Hello payload: %v (pos %d of %d)", r.err, r.pos, len(r.buf))
	}
	if name != "go" {
		t.Errorf("client name = %q, want go", name)
	}
	if ver != Version() {
		t.Errorf("client version = %q, want %q", ver, Version())
	}
}

// The DSN grammar the README documents: seeds, database path, consistency
// and the three TLS knobs — and the malformed inputs that must be refused.
func TestParseDSN(t *testing.T) {
	cfg, err := parseDSN("skaidb://ada:s%40cret@h1,h2:7001,h3,[::1],[fe80::1]:7002/app?consistency=one&tls_ca=/x/ca.pem&tls_server_name=node1")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.user != "ada" || cfg.password != "s@cret" {
		t.Errorf("credentials: %q/%q", cfg.user, cfg.password)
	}
	if want := []string{"h1:7000", "h2:7001", "h3:7000", "[::1]:7000", "[fe80::1]:7002"}; fmt.Sprint(cfg.addrs) != fmt.Sprint(want) {
		t.Errorf("addrs = %v, want %v", cfg.addrs, want)
	}
	if cfg.database != "app" || cfg.consistency != consistencyOne {
		t.Errorf("database=%q consistency=%d", cfg.database, cfg.consistency)
	}
	if !cfg.tls || cfg.tlsCA != "/x/ca.pem" || cfg.tlsInsecure || cfg.tlsName != "node1" {
		t.Errorf("tls: %+v", cfg)
	}

	cfg, err = parseDSN("skaidb://localhost")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.user != "anonymous" || cfg.consistency != consistencyQuorum || cfg.tls || cfg.tlsName != "skaidb" || cfg.database != "" {
		t.Errorf("defaults: %+v", cfg)
	}
	if cfg, _ = parseDSN("skaidb://localhost/?tls_insecure=1"); !cfg.tls || !cfg.tlsInsecure {
		t.Errorf("tls_insecure must imply tls: %+v", cfg)
	}
	if cfg, _ = parseDSN("skaidb://localhost/?tls=true"); !cfg.tls {
		t.Errorf("tls=true: %+v", cfg)
	}
	for _, bad := range []string{
		"mysql://localhost",
		"skaidb://",
		"skaidb://localhost/?consistency=eventual",
		"skaidb://localhost/?tls=maybe",
		"skaidb://localhost/?tls_insecure=yes",
		"skaidb://h1:notaport",
		"skaidb://[::1",
	} {
		if _, err := parseDSN(bad); err == nil {
			t.Errorf("%q must be rejected", bad)
		}
	}
}

// Client-side binding: the fallback for statements the server will not
// prepare. Quoting must be injection-safe and count placeholders exactly.
func TestBindQuotesAndCounts(t *testing.T) {
	got, err := bind("INSERT INTO t (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?) -- 'lit?'",
		named([]driver.Value{int64(1), "O'Brien", true, nil, 2.5, []byte{0xde, 0xad}}))
	if err != nil {
		t.Fatal(err)
	}
	want := "INSERT INTO t (a, b, c, d, e, f) VALUES (1, 'O''Brien', TRUE, NULL, 2.5, 'dead') -- 'lit?'"
	if got != want {
		t.Errorf("\n got %s\nwant %s", got, want)
	}
	if _, err := bind("SELECT ?", nil); err != nil {
		t.Errorf("no args leaves the text alone: %v", err)
	}
	if _, err := bind("SELECT ?, ?", named([]driver.Value{int64(1)})); err == nil {
		t.Error("more placeholders than parameters must fail")
	}
	if _, err := bind("SELECT ?", named([]driver.Value{int64(1), int64(2)})); err == nil {
		t.Error("more parameters than placeholders must fail")
	}
}

// Typed encoding round-trips through the decoder for every value kind the
// driver binds, including the composites that only travel prepared.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	ts := int64(1_700_000_000_123)
	in := map[string]any{
		"n":    int64(-7),
		"f":    1.5,
		"s":    "héllo",
		"b":    true,
		"z":    nil,
		"arr":  []any{int64(1), "two", []any{int64(3)}},
		"doc":  map[string]any{"k": "v"},
		"tags": []string{"x", "y"}, // a named slice type goes through reflection
	}
	buf, err := encodeValue(nil, in)
	if err != nil {
		t.Fatal(err)
	}
	out, ok := decodeNative(&reader{buf: buf}).(map[string]interface{})
	if !ok {
		t.Fatalf("decoded %T", out)
	}
	if out["n"] != int64(-7) || out["f"] != 1.5 || out["s"] != "héllo" || out["b"] != true || out["z"] != nil {
		t.Errorf("scalars: %v", out)
	}
	if arr := out["arr"].([]interface{}); len(arr) != 3 || arr[1] != "two" || arr[2].([]interface{})[0] != int64(3) {
		t.Errorf("array: %v", out["arr"])
	}
	if tags := out["tags"].([]interface{}); len(tags) != 2 || tags[0] != "x" {
		t.Errorf("named slice: %v", out["tags"])
	}
	if doc := out["doc"].(map[string]interface{}); doc["k"] != "v" {
		t.Errorf("document: %v", out["doc"])
	}
	// Timestamps are milliseconds on the wire and come back in UTC.
	tb, _ := encodeValue(nil, timeFromMilli(ts))
	if got := decodeNative(&reader{buf: tb}); got.(interface{ UnixMilli() int64 }).UnixMilli() != ts {
		t.Errorf("timestamp: %v", got)
	}
	// NaN has no wire form and must be refused rather than sent as garbage.
	if _, err := encodeValue(nil, nanFloat()); err == nil {
		t.Error("NaN must not encode")
	}
	// A decimal decodes to its exact text.
	dec := append([]byte{4}, make([]byte, 16)...)
	dec[1] = 0xd2 // 1234 little-endian mantissa
	dec[2] = 0x04
	dec = binary.LittleEndian.AppendUint32(dec, 2)
	if got := decodeNative(&reader{buf: dec}); got != "12.34" {
		t.Errorf("decimal: %v", got)
	}
}

// A *sql.Stmt must take the server-prepared, typed-parameter path — the
// same one db.Exec with arguments takes — not client-side text binding.
// The fake server expects OP_PREPARE (2) then OP_EXECUTE (3).
func TestStmtUsesServerPreparedPath(t *testing.T) {
	seen := make(chan byte, 2)
	c := fakeServer(t, func(s net.Conn) {
		req, err := readReq(s)
		if err != nil {
			return
		}
		seen <- req[0]
		p := binary.LittleEndian.AppendUint32([]byte{4}, 77) // Prepared: id 77
		p = binary.LittleEndian.AppendUint16(p, 2)           // two params
		s.Write(serverFrame(p))
		req, err = readReq(s)
		if err != nil {
			return
		}
		seen <- req[0]
		s.Write(mutationFrame(1))
	})
	st, err := c.Prepare("INSERT INTO t (id, tags) VALUES (?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	if n := st.NumInput(); n != 2 {
		t.Fatalf("NumInput = %d, want 2", n)
	}
	res, err := st.(driver.StmtExecContext).ExecContext(context.Background(),
		named([]driver.Value{int64(1), []string{"a", "b"}})) // a slice: only the typed path can carry it
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("affected = %d, want 1", n)
	}
	if op := <-seen; op != 2 {
		t.Fatalf("first request opcode = %d, want 2 (prepare)", op)
	}
	if op := <-seen; op != 3 {
		t.Fatalf("second request opcode = %d, want 3 (execute)", op)
	}
}

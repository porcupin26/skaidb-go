package skaidb

// The shared skaidb wire-protocol conformance suite.
//
// conformance/vectors.json is generated from the server's reference encoders
// (https://skaidb.org/conformance/vectors.json; contract in
// conformance/README.md). This harness runs it against the driver's PUBLIC
// API — database/sql plus the package's own helpers — through a fake server
// that sends the reference bytes, never bytes this driver encoded, and
// checks the exact bytes the driver sends. The fake server verifies the
// client proof with its own PBKDF2/HMAC code, not the driver's.
//
// Anything the harness cannot compare is printed as a "conformance: SKIP"
// line; `go test -v -run TestConformance .` shows them.

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

type tagged = map[string]any

type vectors struct {
	FormatVersion int `json:"format_version"`
	Auth          struct {
		Username  string `json:"username"`
		Password  string `json:"password"`
		Challenge struct {
			Salt              string `json:"salt"`
			Iterations        uint32 `json:"iterations"`
			ServerNonceSuffix string `json:"server_nonce_suffix"`
		} `json:"challenge"`
		Outcomes []struct {
			Name    string `json:"name"`
			Expect  string `json:"expect"`
			Payload string `json:"payload"`
			Reason  string `json:"reason"`
		} `json:"outcomes"`
	} `json:"auth"`
	Ignorable struct {
		DdlPayload string `json:"ddl_payload"`
	} `json:"ignorable_requests"`
	Values []struct {
		Name    string `json:"name"`
		Value   tagged `json:"value"`
		Encoded string `json:"encoded"`
	} `json:"values"`
	Scram []struct {
		Username        string `json:"username"`
		Password        string `json:"password"`
		Salt            string `json:"salt"`
		Iterations      uint32 `json:"iterations"`
		ClientNonce     string `json:"client_nonce"`
		ServerNonce     string `json:"server_nonce"`
		AuthMessage     string `json:"auth_message"`
		SaltedPassword  string `json:"salted_password"`
		ClientProof     string `json:"client_proof"`
		ServerSignature string `json:"server_signature"`
	} `json:"scram"`
	Cases []struct {
		Name      string `json:"name"`
		Call      call   `json:"call"`
		Exchanges []struct {
			Request   string   `json:"request"`
			Responses []string `json:"responses"`
		} `json:"exchanges"`
		Expect map[string]any `json:"expect"`
	} `json:"cases"`
}

type call struct {
	Method      string     `json:"method"`
	SQL         string     `json:"sql"`
	Consistency string     `json:"consistency"`
	Params      []tagged   `json:"params"`
	Rows        [][]tagged `json:"rows"`
	Calls       []call     `json:"calls"`
}

func loadVectors(t *testing.T) *vectors {
	t.Helper()
	raw, err := os.ReadFile("conformance/vectors.json")
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var v vectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}
	if v.FormatVersion != 1 {
		t.Fatalf("format_version %d: this harness knows 1", v.FormatVersion)
	}
	return &v
}

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex %q: %v", s, err)
	}
	return b
}

// ---- tagged JSON <-> the driver's documented Go forms --------------------

// goForm maps a tagged value onto the Go value the driver documents for it
// (README "Types"): Int int64, Float float64, Decimal and Uuid as strings,
// Bytes []byte, Timestamp a UTC time.Time, Array []any and Document
// map[string]any.
func goForm(t *testing.T, v tagged) any {
	t.Helper()
	switch {
	case v["null"] != nil:
		return nil
	case v["bool"] != nil:
		return v["bool"].(bool)
	case v["int"] != nil:
		n, err := strconv.ParseInt(v["int"].(string), 10, 64)
		if err != nil {
			t.Fatalf("int %v: %v", v["int"], err)
		}
		return n
	case v["float_bits"] != nil:
		bits, err := strconv.ParseUint(v["float_bits"].(string), 16, 64)
		if err != nil {
			t.Fatalf("float_bits %v: %v", v["float_bits"], err)
		}
		return math.Float64frombits(bits)
	case v["decimal"] != nil:
		d := v["decimal"].(map[string]any)
		return decimalText(t, d["mantissa"].(string), int(d["scale"].(float64)))
	case v["string"] != nil:
		return v["string"].(string)
	case v["bytes"] != nil:
		return unhex(t, v["bytes"].(string))
	case v["uuid"] != nil:
		return v["uuid"].(string)
	case v["timestamp_ms"] != nil:
		ms, err := strconv.ParseInt(v["timestamp_ms"].(string), 10, 64)
		if err != nil {
			t.Fatalf("timestamp_ms %v: %v", v["timestamp_ms"], err)
		}
		return time.UnixMilli(ms).UTC()
	case v["array"] != nil:
		items := v["array"].([]any)
		out := make([]any, len(items))
		for i, it := range items {
			out[i] = goForm(t, it.(map[string]any))
		}
		return out
	case v["document"] != nil:
		out := map[string]any{}
		for _, e := range v["document"].([]any) {
			kv := e.(map[string]any)
			out[kv["key"].(string)] = goForm(t, kv["value"].(map[string]any))
		}
		return out
	}
	t.Fatalf("unknown tagged value %v", v)
	return nil
}

// decimalText renders mantissa / 10^scale as the driver's decimal string,
// computed with math/big rather than the driver's own formatter.
func decimalText(t *testing.T, mantissa string, scale int) string {
	t.Helper()
	m, ok := new(big.Int).SetString(mantissa, 10)
	if !ok {
		t.Fatalf("decimal mantissa %q", mantissa)
	}
	if scale == 0 {
		return m.String()
	}
	r := new(big.Rat).SetFrac(m, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil))
	return r.FloatString(scale)
}

// cellForm is what a cell looks like through database/sql: the Go form,
// except that arrays and documents surface as JSON text.
func cellForm(t *testing.T, v tagged) any {
	g := goForm(t, v)
	switch g.(type) {
	case []any, map[string]any:
		b, err := json.Marshal(g)
		if err != nil {
			t.Fatalf("marshal %v: %v", g, err)
		}
		return string(b)
	}
	return g
}

// encodable reports whether the driver can bind v as a parameter with the
// exact wire type: a Decimal and a Uuid have no Go form other than a string
// (which binds as String), and a Go map cannot hold a Document whose keys
// are not in sorted order (the driver writes a map's keys sorted).
func encodable(v tagged) (bool, string) {
	switch {
	case v["decimal"] != nil:
		return false, "a Decimal surfaces as a string and binds as String"
	case v["uuid"] != nil:
		return false, "a Uuid surfaces as a string and binds as String"
	case v["array"] != nil:
		for _, it := range v["array"].([]any) {
			if ok, why := encodable(it.(map[string]any)); !ok {
				return false, why
			}
		}
	case v["document"] != nil:
		var keys []string
		for _, e := range v["document"].([]any) {
			kv := e.(map[string]any)
			keys = append(keys, kv["key"].(string))
			if ok, why := encodable(kv["value"].(map[string]any)); !ok {
				return false, why
			}
		}
		if !sort.StringsAreSorted(keys) {
			return false, "a Document binds from a map, whose keys the driver writes sorted"
		}
	}
	return true, ""
}

// sameValue compares two Go-form values, floats by their bits (so -0.0 and
// NaN payloads count).
func sameValue(a, b any) bool {
	switch x := a.(type) {
	case float64:
		y, ok := b.(float64)
		return ok && math.Float64bits(x) == math.Float64bits(y)
	case []byte:
		y, ok := b.([]byte)
		return ok && bytes.Equal(x, y)
	case time.Time:
		y, ok := b.(time.Time)
		return ok && x.Equal(y) && y.Location() == time.UTC
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !sameValue(x[i], y[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for k, xv := range x {
			yv, ok := y[k]
			if !ok || !sameValue(xv, yv) {
				return false
			}
		}
		return true
	}
	return reflect.DeepEqual(a, b)
}

// ---- part 1: pure vectors -------------------------------------------------

func TestConformanceValues(t *testing.T) {
	v := loadVectors(t)
	for _, e := range v.Values {
		t.Run(e.Name, func(t *testing.T) {
			enc := unhex(t, e.Encoded)
			r := &reader{buf: enc}
			got := decodeNative(r)
			if r.err != nil || r.pos != len(enc) {
				t.Fatalf("decode: err=%v, consumed %d of %d bytes", r.err, r.pos, len(enc))
			}
			if want := goForm(t, e.Value); !sameValue(got, want) {
				t.Errorf("decoded %#v, want %#v", got, want)
			}
			if got, want := decodeValue(&reader{buf: enc}), cellForm(t, e.Value); !sameValue(got, want) {
				t.Errorf("database/sql cell %#v, want %#v", got, want)
			}
			if ok, why := encodable(e.Value); !ok {
				fmt.Printf("conformance: SKIP encode of value %s: %s\n", e.Name, why)
				return
			}
			out, err := encodeValue(nil, goForm(t, e.Value))
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if hex.EncodeToString(out) != e.Encoded {
				t.Errorf("encoded %x, want %s", out, e.Encoded)
			}
		})
	}
}

func TestConformanceScram(t *testing.T) {
	v := loadVectors(t)
	for _, s := range v.Scram {
		t.Run(s.Username, func(t *testing.T) {
			salt := unhex(t, s.Salt)
			am := scramAuthMessage(s.Username, s.ClientNonce, s.ServerNonce, salt, s.Iterations)
			if hex.EncodeToString(am) != s.AuthMessage {
				t.Errorf("auth message %x, want %s", am, s.AuthMessage)
			}
			if got := hex.EncodeToString(pbkdf2SHA256([]byte(s.Password), salt, int(s.Iterations), 32)); got != s.SaltedPassword {
				t.Errorf("salted password %s, want %s", got, s.SaltedPassword)
			}
			proof, sig := scramProof(s.Password, salt, s.Iterations, am)
			if hex.EncodeToString(proof) != s.ClientProof {
				t.Errorf("client proof %x, want %s", proof, s.ClientProof)
			}
			if hex.EncodeToString(sig) != s.ServerSignature {
				t.Errorf("server signature %x, want %s", sig, s.ServerSignature)
			}
		})
	}
}

// ---- part 2: the scripted fake server -------------------------------------

// Independent SCRAM primitives for the fake server: none of the driver's
// crypto helpers are used on this side.
func refHMAC(key, msg []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(msg)
	return m.Sum(nil)
}

func refPBKDF2(password, salt []byte, iter int) []byte {
	u := refHMAC(password, append(append([]byte{}, salt...), 0, 0, 0, 1))
	out := append([]byte{}, u...)
	for i := 1; i < iter; i++ {
		u = refHMAC(password, u)
		for j := range out {
			out[j] ^= u[j]
		}
	}
	return out
}

type fakeExchange struct {
	request   []byte
	responses [][]byte
}

func fakeWrite(w io.Writer, payload []byte) error {
	var head [4]byte
	binary.BigEndian.PutUint32(head[:], uint32(len(payload)))
	_, err := w.Write(append(head[:], payload...))
	return err
}

func fakeRead(r io.Reader) ([]byte, error) {
	var head [4]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return nil, err
	}
	buf := make([]byte, binary.BigEndian.Uint32(head[:]))
	_, err := io.ReadFull(r, buf)
	return buf, err
}

// startConformanceServer serves ONE connection: the handshake per outcome,
// then the exchanges in order. The returned channel yields the server's
// verdict (nil when everything matched) once the connection ends.
func startConformanceServer(t *testing.T, v *vectors, outcome string, exchanges []fakeExchange) (string, <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("no loopback listener available: %v", err)
	}
	verdict := make(chan error, 1)
	go func() {
		defer ln.Close()
		s, err := ln.Accept()
		if err != nil {
			verdict <- err
			return
		}
		defer s.Close()
		_ = s.SetDeadline(time.Now().Add(20 * time.Second))
		verdict <- serveConformance(s, v, outcome, exchanges)
	}()
	return ln.Addr().String(), verdict
}

func serveConformance(s net.Conn, v *vectors, outcome string, exchanges []fakeExchange) error {
	start, err := fakeRead(s)
	if err != nil || len(start) == 0 || start[0] != 10 {
		return fmt.Errorf("expected AuthStart, got %x (%v)", start, err)
	}
	r := &reader{buf: start[1:]}
	user, clientNonce := r.text(), r.text()
	if r.err != nil {
		return fmt.Errorf("AuthStart: %v", r.err)
	}
	salt, _ := hex.DecodeString(v.Auth.Challenge.Salt)
	iter := v.Auth.Challenge.Iterations
	serverNonce := clientNonce + v.Auth.Challenge.ServerNonceSuffix
	ch := binary.LittleEndian.AppendUint32([]byte{11}, uint32(len(salt)))
	ch = append(ch, salt...)
	ch = binary.LittleEndian.AppendUint32(ch, iter)
	ch = binary.LittleEndian.AppendUint32(ch, uint32(len(serverNonce)))
	ch = append(ch, serverNonce...)
	if err := fakeWrite(s, ch); err != nil {
		return err
	}
	finish, err := fakeRead(s)
	if err != nil || len(finish) != 33 || finish[0] != 12 {
		return fmt.Errorf("expected AuthFinish, got %x (%v)", finish, err)
	}
	am := []byte(strings.Join([]string{user, clientNonce, serverNonce, hex.EncodeToString(salt),
		strconv.FormatUint(uint64(iter), 10)}, "\x00"))
	salted := refPBKDF2([]byte(v.Auth.Password), salt, int(iter))
	clientKey := refHMAC(salted, []byte("Client Key"))
	stored := sha256.Sum256(clientKey)
	clientSig := refHMAC(stored[:], am)
	for i := range clientKey {
		if finish[1+i] != clientKey[i]^clientSig[i] {
			return errors.New("client proof did not verify")
		}
	}
	switch outcome {
	case "ok":
		sig := refHMAC(refHMAC(salted, []byte("Server Key")), am)
		if err := fakeWrite(s, append([]byte{13, 1}, sig...)); err != nil {
			return err
		}
	case "bad_server_signature":
		return fakeWrite(s, append([]byte{13, 1}, bytes.Repeat([]byte{0xaa}, 32)...))
	case "denied":
		for _, o := range v.Auth.Outcomes {
			if o.Name == "denied" {
				p, _ := hex.DecodeString(o.Payload)
				return fakeWrite(s, p)
			}
		}
		return errors.New("no denied outcome in the vectors")
	}
	ddl, _ := hex.DecodeString(v.Ignorable.DdlPayload)
	pending := exchanges
	for {
		req, err := fakeRead(s)
		if err != nil {
			if len(pending) > 0 {
				return fmt.Errorf("never received request %x", pending[0].request)
			}
			return nil
		}
		if req[0] == 4 || req[0] == 8 { // OP_CLOSE, OP_HELLO
			if err := fakeWrite(s, ddl); err != nil {
				return err
			}
			continue
		}
		if len(pending) == 0 {
			return fmt.Errorf("unexpected extra request %x", req)
		}
		want := pending[0]
		pending = pending[1:]
		if !bytes.Equal(req, want.request) {
			return fmt.Errorf("request mismatch:\n  got  %x\n  want %x", req, want.request)
		}
		for _, resp := range want.responses {
			if err := fakeWrite(s, resp); err != nil {
				return err
			}
		}
	}
}

func conformanceDSN(v *vectors, addr string) string {
	return "skaidb://" + url.UserPassword(v.Auth.Username, v.Auth.Password).String() + "@" + addr + "/"
}

func TestConformanceAuthOutcomes(t *testing.T) {
	v := loadVectors(t)
	for _, o := range v.Auth.Outcomes {
		t.Run(o.Name, func(t *testing.T) {
			addr, verdict := startConformanceServer(t, v, o.Name, nil)
			db, err := sql.Open("skaidb", conformanceDSN(v, addr))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			c, err := db.Conn(ctx)
			switch o.Expect {
			case "connected":
				if err != nil {
					t.Fatalf("connect: %v", err)
				}
				c.Close()
			default:
				if err == nil {
					c.Close()
					t.Fatalf("connect succeeded, want an auth error")
				}
				if o.Reason != "" && !strings.Contains(err.Error(), o.Reason) {
					t.Errorf("error %q does not name %q", err, o.Reason)
				}
			}
			db.Close()
			if err := <-verdict; err != nil {
				t.Errorf("fake server: %v", err)
			}
		})
	}
}

// ---- part 3: the cases through the public API ------------------------------

// result is what one call surfaced.
type callResult struct {
	sets     []gotSet // row-producing
	affected *int64   // Exec
	ok       bool     // succeeded (a Ddl or a statement with no rows)
	err      string
	seq      []callResult
	// rows then an error, for a stream failing part-way
	rowsThenErr bool
}

func toNative(t *testing.T, vals []tagged) []any {
	out := make([]any, len(vals))
	for i, p := range vals {
		out[i] = goForm(t, p)
	}
	return out
}

// wantsRows reports whether an expectation is answered by a row-producing
// API. database/sql makes the caller choose between Query (rows) and Exec
// (an affected count) up front, so the harness picks the one a program
// would use for that statement.
func wantsRows(expect map[string]any) bool {
	_, affected := expect["affected"]
	_, ddl := expect["ddl"]
	return !affected && !ddl
}

func runCall(t *testing.T, ctx context.Context, c *sql.Conn, cl call, expect map[string]any) callResult {
	switch cl.Method {
	case "sequence":
		subs, _ := expect["sequence"].([]any)
		var res callResult
		for i, sub := range cl.Calls {
			var e map[string]any
			if i < len(subs) {
				e, _ = subs[i].(map[string]any)
			}
			res.seq = append(res.seq, runCall(t, ctx, c, sub, e))
		}
		return res
	case "query", "execute_prepared":
		if cl.Consistency != "" {
			ctx = WithConsistency(ctx, cl.Consistency)
		}
		args := toNative(t, cl.Params)
		if !wantsRows(expect) {
			r, err := c.ExecContext(ctx, cl.SQL, args...)
			if err != nil {
				return callResult{err: err.Error()}
			}
			n, _ := r.RowsAffected()
			return callResult{affected: &n, ok: true}
		}
		rows, err := c.QueryContext(ctx, cl.SQL, args...)
		if err != nil {
			return callResult{err: err.Error()}
		}
		return collect(t, rows)
	case "query_stream":
		rows, err := c.QueryContext(WithStreaming(ctx), cl.SQL)
		if err != nil {
			return callResult{err: err.Error()}
		}
		return collect(t, rows)
	case "execute_batch":
		if cl.Consistency != "" {
			ctx = WithConsistency(ctx, cl.Consistency)
		}
		batch := make([][]any, len(cl.Rows))
		for i, r := range cl.Rows {
			batch[i] = toNative(t, r)
		}
		n, err := ExecBatchConn(ctx, c, cl.SQL, batch)
		if err != nil {
			return callResult{err: err.Error()}
		}
		return callResult{affected: &n, ok: true}
	}
	fmt.Printf("conformance: SKIP call.method %q: no API in this harness\n", cl.Method)
	return callResult{err: "skipped"}
}

func collect(t *testing.T, rows *sql.Rows) callResult {
	defer rows.Close()
	var res callResult
	for {
		cols, err := rows.Columns()
		if err != nil {
			return callResult{err: err.Error()}
		}
		set := gotSet{cols: cols}
		for rows.Next() {
			cells := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range cells {
				ptrs[i] = &cells[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatalf("scan: %v", err)
			}
			row := make([]any, len(cells))
			copy(row, cells)
			set.data = append(set.data, row)
		}
		res.sets = append(res.sets, set)
		if err := rows.Err(); err != nil {
			res.err = err.Error()
			res.rowsThenErr = true
			return res
		}
		if !rows.NextResultSet() {
			break
		}
	}
	res.ok = true
	return res
}

func wantSet(t *testing.T, raw any) gotSet {
	m := raw.(map[string]any)
	var set gotSet
	for _, c := range m["columns"].([]any) {
		set.cols = append(set.cols, c.(string))
	}
	for _, r := range m["rows"].([]any) {
		var row []any
		for _, cell := range r.([]any) {
			row = append(row, cellForm(t, cell.(map[string]any)))
		}
		set.data = append(set.data, row)
	}
	return set
}

func sameSet(a, b gotSet) bool {
	if !reflect.DeepEqual(append([]string{}, a.cols...), append([]string{}, b.cols...)) || len(a.data) != len(b.data) {
		return false
	}
	for i := range a.data {
		if len(a.data[i]) != len(b.data[i]) {
			return false
		}
		for j := range a.data[i] {
			if !sameValue(a.data[i][j], b.data[i][j]) {
				return false
			}
		}
	}
	return true
}

// check compares a call's result to its expectation; "" means it matched.
func check(t *testing.T, name string, expect map[string]any, got callResult) string {
	switch {
	case expect["error"] != nil:
		if got.err == "" || !strings.Contains(got.err, expect["error"].(string)) || got.rowsThenErr && len(got.sets[0].data) > 0 {
			return fmt.Sprintf("want error containing %q, got %+v", expect["error"], got)
		}
	case expect["sequence"] != nil:
		subs := expect["sequence"].([]any)
		if len(subs) != len(got.seq) {
			return fmt.Sprintf("want %d results, got %d", len(subs), len(got.seq))
		}
		for i, s := range subs {
			if msg := check(t, fmt.Sprintf("%s[%d]", name, i), s.(map[string]any), got.seq[i]); msg != "" {
				return fmt.Sprintf("call %d: %s", i, msg)
			}
		}
	case expect["rows_then_error"] != nil:
		e := expect["rows_then_error"].(map[string]any)
		if !got.rowsThenErr || !strings.Contains(got.err, e["error"].(string)) || !sameSet(got.sets[0], wantSet(t, e["rows"])) {
			return fmt.Sprintf("want rows %v then error %q, got %+v", e["rows"], e["error"], got)
		}
	case expect["rows"] != nil:
		if got.err != "" || len(got.sets) != 1 || !sameSet(got.sets[0], wantSet(t, expect["rows"])) {
			return fmt.Sprintf("want rows %v, got %+v", expect["rows"], got)
		}
	case expect["result_sets"] != nil:
		sets := expect["result_sets"].([]any)
		if got.err != "" || len(sets) != len(got.sets) {
			return fmt.Sprintf("want %d result sets, got %+v", len(sets), got)
		}
		for i, s := range sets {
			if !sameSet(got.sets[i], wantSet(t, s)) {
				return fmt.Sprintf("result set %d: want %v, got %+v", i, s, got.sets[i])
			}
		}
	case expect["affected"] != nil:
		if got.err != "" {
			return fmt.Sprintf("want affected %v, got error %q", expect["affected"], got.err)
		}
		if got.affected == nil {
			// The streaming API is database/sql's Query, which has no channel
			// for an affected count: check that the statement succeeded with
			// no columns and no rows, and say the count went unchecked.
			if !got.ok || len(got.sets) != 1 || len(got.sets[0].cols) != 0 || len(got.sets[0].data) != 0 {
				return fmt.Sprintf("want a non-row result, got %+v", got)
			}
			fmt.Printf("conformance: SKIP %s: affected count %v not comparable (database/sql Rows carry no affected count)\n", name, expect["affected"])
			return ""
		}
		if strconv.FormatInt(*got.affected, 10) != expect["affected"].(string) {
			return fmt.Sprintf("want affected %v, got %d", expect["affected"], *got.affected)
		}
	case expect["ddl"] != nil:
		if got.err != "" || !got.ok {
			return fmt.Sprintf("want success, got %+v", got)
		}
	default:
		fmt.Printf("conformance: SKIP %s: unknown expect keys %v\n", name, reflect.ValueOf(expect).MapKeys())
	}
	return ""
}

func TestConformanceCases(t *testing.T) {
	v := loadVectors(t)
	for _, cs := range v.Cases {
		cs := cs
		t.Run(cs.Name, func(t *testing.T) {
			var ex []fakeExchange
			for _, e := range cs.Exchanges {
				fe := fakeExchange{request: unhex(t, e.Request)}
				for _, r := range e.Responses {
					fe.responses = append(fe.responses, unhex(t, r))
				}
				ex = append(ex, fe)
			}
			addr, verdict := startConformanceServer(t, v, "ok", ex)
			db, err := sql.Open("skaidb", conformanceDSN(v, addr))
			if err != nil {
				t.Fatal(err)
			}
			db.SetMaxOpenConns(1)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			c, err := db.Conn(ctx)
			if err != nil {
				db.Close()
				t.Fatalf("connect: %v", err)
			}
			got := runCall(t, ctx, c, cs.Call, cs.Expect)
			c.Close()
			db.Close()
			if err := <-verdict; err != nil {
				t.Errorf("fake server: %v", err)
			}
			if msg := check(t, cs.Name, cs.Expect, got); msg != "" {
				t.Errorf("%s: %s", cs.Name, msg)
			}
		})
	}
}

// gotSet is one result set as the harness compares it.
type gotSet struct {
	cols []string
	data [][]any
}

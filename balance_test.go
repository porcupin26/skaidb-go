package skaidb

// Pool load balancing (PROTOCOL.md §6.1): policies, health ejection and
// re-admission, member discovery (SHOW ENDPOINTS), and how the Connector
// retires connections through database/sql — against fake members that
// each serve many connections.

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// fakeMember is a fake skaidb member: SCRAM per the conformance vectors on
// every connection, then `SELECT …` answers one row naming the member
// (after delay), SHOW ENDPOINTS answers listing, SHOW DATABASES (the
// probe) answers no rows, anything else is a one-row mutation. refuse
// closes each new connection before the handshake; deny refuses the login;
// oldServer answers SHOW ENDPOINTS with a parse error.
type fakeMember struct {
	name      string
	endpoint  string
	ln        net.Listener
	v         *vectors
	salted    []byte
	delay     atomic.Int64 // nanoseconds
	refuse    atomic.Bool
	deny      atomic.Bool
	oldServer atomic.Bool
	listings  atomic.Int64 // SHOW ENDPOINTS asked

	mu      sync.Mutex
	listing func() []listedEndpoint
}

func newFakeMember(t *testing.T, v *vectors, name string) *fakeMember {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("no loopback listener available: %v", err)
	}
	salt, _ := hex.DecodeString(v.Auth.Challenge.Salt)
	m := &fakeMember{name: name, endpoint: ln.Addr().String(), ln: ln, v: v,
		salted: refPBKDF2([]byte(v.Auth.Password), salt, int(v.Auth.Challenge.Iterations))}
	go m.accept()
	t.Cleanup(func() { ln.Close() })
	return m
}

func (m *fakeMember) port() string { _, p, _ := net.SplitHostPort(m.endpoint); return p }

func (m *fakeMember) setListing(f func() []listedEndpoint) {
	m.mu.Lock()
	m.listing = f
	m.mu.Unlock()
}

func (m *fakeMember) accept() {
	for {
		s, err := m.ln.Accept()
		if err != nil {
			return
		}
		if m.refuse.Load() {
			s.Close()
			continue
		}
		go func() {
			defer s.Close()
			m.serve(s)
		}()
	}
}

func (m *fakeMember) serve(s net.Conn) {
	start, err := fakeRead(s)
	if err != nil || len(start) == 0 || start[0] != 10 {
		return
	}
	r := &reader{buf: start[1:]}
	user, clientNonce := r.text(), r.text()
	salt, _ := hex.DecodeString(m.v.Auth.Challenge.Salt)
	iter := m.v.Auth.Challenge.Iterations
	serverNonce := clientNonce + m.v.Auth.Challenge.ServerNonceSuffix
	ch := binary.LittleEndian.AppendUint32([]byte{11}, uint32(len(salt)))
	ch = append(ch, salt...)
	ch = binary.LittleEndian.AppendUint32(ch, iter)
	ch = binary.LittleEndian.AppendUint32(ch, uint32(len(serverNonce)))
	ch = append(ch, serverNonce...)
	if fakeWrite(s, ch) != nil {
		return
	}
	if _, err := fakeRead(s); err != nil {
		return
	}
	if m.deny.Load() {
		_ = fakeWrite(s, appendStr([]byte{13, 0}, "bad password"))
		return
	}
	am := scramAuthMessage(user, clientNonce, serverNonce, salt, iter)
	if fakeWrite(s, append([]byte{13, 1}, refHMAC(refHMAC(m.salted, []byte("Server Key")), am)...)) != nil {
		return
	}
	for {
		req, err := fakeRead(s)
		if err != nil {
			return
		}
		if req[0] != 1 { // Hello, Close, anything else: an ack
			if fakeWrite(s, []byte{2}) != nil {
				return
			}
			continue
		}
		n := binary.LittleEndian.Uint32(req[2:])
		text := string(req[6 : 6+n])
		var resp []byte
		switch {
		case text == "SHOW ENDPOINTS":
			m.listings.Add(1)
			m.mu.Lock()
			listing := m.listing
			m.mu.Unlock()
			if m.oldServer.Load() || listing == nil {
				resp = appendStr([]byte{3}, "parse error: expected TABLES, found ENDPOINTS")
			} else {
				var rows [][]any
				for _, e := range listing() {
					rows = append(rows, []any{e.endpoint, e.state, e.local})
				}
				resp = fakeRowsFrame([]string{"endpoint", "state", "local"}, rows)
			}
		case text == "SHOW DATABASES":
			resp = fakeRowsFrame([]string{"name"}, nil)
		case strings.HasPrefix(text, "SELECT"):
			time.Sleep(time.Duration(m.delay.Load()))
			resp = fakeRowsFrame([]string{"member"}, [][]any{{m.name}})
		default:
			resp = binary.LittleEndian.AppendUint64([]byte{1}, 1)
		}
		if fakeWrite(s, resp) != nil {
			return
		}
	}
}

// fakeRowsFrame encodes a Rows reply of Text and Bool cells.
func fakeRowsFrame(cols []string, rows [][]any) []byte {
	p := binary.LittleEndian.AppendUint32([]byte{0}, uint32(len(cols)))
	for _, c := range cols {
		p = appendStr(p, c)
	}
	p = binary.LittleEndian.AppendUint32(p, uint32(len(rows)))
	for _, row := range rows {
		p = binary.LittleEndian.AppendUint32(p, uint32(len(row)))
		for _, v := range row {
			var cell []byte
			switch x := v.(type) {
			case string:
				cell = appendStr([]byte{5}, x)
			case bool:
				cell = []byte{1, 0}
				if x {
					cell[1] = 1
				}
			}
			p = binary.LittleEndian.AppendUint32(p, uint32(len(cell)))
			p = append(p, cell...)
		}
	}
	return p
}

func fakeMembers(t *testing.T, names ...string) []*fakeMember {
	v := loadVectors(t)
	out := make([]*fakeMember, len(names))
	for i, n := range names {
		out[i] = newFakeMember(t, v, n)
	}
	return out
}

func endpointsOf(ms []*fakeMember) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.endpoint
	}
	return out
}

// lbDB opens a *sql.DB over a balancing Connector: seeds, DSN query
// options, then Go options.
func lbDB(t *testing.T, seeds []string, query string, opts ...Option) (*Connector, *sql.DB) {
	t.Helper()
	v := loadVectors(t)
	dsn := "skaidb://" + url.UserPassword(v.Auth.Username, v.Auth.Password).String() + "@" + strings.Join(seeds, ",") + "/"
	if query != "" {
		dsn += "?" + query
	}
	c, err := NewConnector(dsn, opts...)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	db := sql.OpenDB(c)
	t.Cleanup(func() { db.Close() })
	return c, db
}

// who runs a SELECT and returns the member that answered it.
func who(t *testing.T, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var name string
	if err := q.QueryRowContext(ctx, "SELECT member()").Scan(&name); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	return name
}

func whoN(t *testing.T, db *sql.DB, n int) []string {
	t.Helper()
	out := make([]string, n)
	for i := range out {
		out[i] = who(t, db)
	}
	return out
}

func distinct(names []string) []string {
	set := map[string]bool{}
	for _, n := range names {
		set[n] = true
	}
	out := make([]string, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func count(names []string, name string) int {
	n := 0
	for _, x := range names {
		if x == name {
			n++
		}
	}
	return n
}

func stateOf(c *Connector, endpoint string) string {
	for _, m := range c.Members() {
		if m.Endpoint == endpoint {
			return m.State
		}
	}
	return ""
}

// memberAt runs f on the Connector's member for endpoint, under its lock.
func memberAt(t *testing.T, c *Connector, endpoint string, f func(m *member)) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.members[endpoint]
	if m == nil {
		t.Fatalf("no member %s", endpoint)
	}
	f(m)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ---- policies ---------------------------------------------------------------

// Each new connection goes to the next member: nine connections opened
// while the others are held land three on each, in strict rotation.
func TestRoundRobinRotatesOverEveryMember(t *testing.T) {
	ms := fakeMembers(t, "a", "b", "c")
	c, db := lbDB(t, endpointsOf(ms), "load_balancing=round_robin")
	ctx := context.Background()
	var served []string
	for i := 0; i < 9; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		served = append(served, who(t, conn))
	}
	for _, name := range []string{"a", "b", "c"} {
		if count(served, name) != 3 {
			t.Fatalf("served %v, want three each", served)
		}
	}
	for i := 0; i < 9; i += 3 {
		if len(distinct(served[i:i+3])) != 3 {
			t.Fatalf("served %v: three in a row must be three members", served)
		}
	}
	for _, m := range c.Members() {
		if m.Open != 3 || m.State != "ready" || !m.Seed || m.Latency <= 0 {
			t.Errorf("member %+v, want 3 open, ready, seed, measured", m)
		}
	}
}

func TestLeastLatencyPrefersTheFasterMember(t *testing.T) {
	ms := fakeMembers(t, "a", "b")
	ms[1].delay.Store(int64(20 * time.Millisecond))
	_, db := lbDB(t, endpointsOf(ms), "", LoadBalancing(LeastLatency), EjectLatencyFactor(0))
	db.SetMaxIdleConns(0) // every statement dials: the policy decides each one
	served := whoN(t, db, 60)
	if count(served, "a") <= 48 {
		t.Errorf("served %v: the faster member must take most connections", served)
	}
	if count(served, "b") == 0 {
		t.Errorf("b was never measured")
	}
}

func TestLeastLatencyReMeasuresAMemberItHasNotUsedLately(t *testing.T) {
	ms := fakeMembers(t, "a", "b")
	ms[1].delay.Store(int64(20 * time.Millisecond))
	c, db := lbDB(t, endpointsOf(ms), "load_balancing=least_latency&eject_latency_factor=0")
	db.SetMaxIdleConns(0)
	whoN(t, db, 20)
	// b is slow, then recovers: an idle pool notices once b's average goes
	// stale instead of never asking b again.
	ms[1].delay.Store(0)
	memberAt(t, c, ms[1].endpoint, func(m *member) { m.lastSample = m.lastSample.Add(-10 * time.Second) })
	if served := whoN(t, db, 3); count(served, "b") == 0 {
		t.Errorf("served %v: a stale member must be re-measured", served)
	}
}

// The default policy keeps 1.1's behaviour: a quiet pool reuses its one
// connection (database/sql's idle list), whichever member it reached.
func TestRandomPolicyKeepsThe11Behaviour(t *testing.T) {
	ms := fakeMembers(t, "a", "b", "c")
	_, db := lbDB(t, endpointsOf(ms), "")
	if served := whoN(t, db, 5); len(distinct(served)) != 1 {
		t.Errorf("served %v, want one member", served)
	}
}

func TestBalancingOptionsParseAndValidate(t *testing.T) {
	c, err := NewConnector("skaidb://h1,h2/app")
	if err != nil {
		t.Fatal(err)
	}
	if c.cfg.lb != defaultBalance() {
		t.Errorf("defaults = %+v", c.cfg.lb)
	}
	c, err = NewConnector("skaidb://h1/?load_balancing=ROUND_ROBIN&discover=true&refresh_interval=30s" +
		"&eject_errors=5&eject_latency_factor=2.5&eject_latency_floor=20ms&eject_backoff=1s" +
		"&eject_backoff_max=10s&idle_order=lifo&read_your_writes=0")
	if err != nil {
		t.Fatal(err)
	}
	want := balance{policy: RoundRobin, discover: true, refreshInterval: 30 * time.Second, ejectErrors: 5,
		ejectFactor: 2.5, ejectFloor: 20 * time.Millisecond, ejectBackoff: time.Second, ejectBackoffMax: 10 * time.Second}
	if c.cfg.lb != want {
		t.Errorf("parsed %+v, want %+v", c.cfg.lb, want)
	}
	// Options apply on top of the DSN.
	c, err = NewConnector("skaidb://h1/?load_balancing=round_robin&eject_errors=5",
		LoadBalancing(LeastLatency), Discover(true), RefreshInterval(time.Second), EjectErrors(2),
		EjectLatencyFactor(0), EjectLatencyFloor(time.Millisecond), EjectBackoff(time.Second), EjectBackoffMax(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	want = balance{policy: LeastLatency, discover: true, refreshInterval: time.Second, ejectErrors: 2,
		ejectFactor: 0, ejectFloor: time.Millisecond, ejectBackoff: time.Second, ejectBackoffMax: 2 * time.Second}
	if c.cfg.lb != want {
		t.Errorf("with options %+v, want %+v", c.cfg.lb, want)
	}
	for _, tc := range []struct{ query, want string }{
		{"load_balancing=nearest", `skaidb: bad load_balancing "nearest"`},
		{"idle_order=random", `skaidb: bad idle_order "random"`},
		{"idle_order=fifo", errIdleFIFO.Error()},
		{"read_your_writes=2s", errReadYourWrites.Error()},
		{"read_your_writes=soon", `skaidb: bad read_your_writes "soon"`},
		{"discover=maybe", `skaidb: bad discover "maybe"`},
		{"refresh_interval=60", `skaidb: bad refresh_interval "60"`},
		{"refresh_interval=0s", "skaidb: refresh_interval must be positive"},
		{"eject_errors=0", "skaidb: eject_errors must be at least 1"},
		{"eject_errors=x", `skaidb: bad eject_errors "x"`},
		{"eject_latency_factor=-1", "skaidb: eject_latency_factor must be a finite number"},
		{"eject_backoff=-1s", "skaidb: eject_backoff and eject_backoff_max must be positive"},
	} {
		_, err := sql.Open("skaidb", "skaidb://h1/?"+tc.query)
		if err == nil || !strings.HasPrefix(err.Error(), tc.want) {
			t.Errorf("sql.Open(?%s) = %v, want prefix %q", tc.query, err, tc.want)
		}
	}
	if _, err := NewConnector("skaidb://h1", LoadBalancing("nearest")); err == nil ||
		!strings.HasPrefix(err.Error(), `skaidb: bad load_balancing "nearest"`) {
		t.Errorf("an unknown policy option = %v", err)
	}
	if _, err := NewConnector("skaidb://h1", EjectErrors(0)); err == nil {
		t.Error("EjectErrors(0) must be refused")
	}
}

// ---- health ejection --------------------------------------------------------

func TestAFailingMemberIsEjectedThenReadmittedByTheProbe(t *testing.T) {
	ms := fakeMembers(t, "a", "b", "c")
	b := ms[1]
	b.refuse.Store(true)
	c, db := lbDB(t, endpointsOf(ms), "load_balancing=round_robin&eject_errors=2&eject_backoff=300ms")
	db.SetMaxIdleConns(0)
	if served := whoN(t, db, 12); count(served, "b") != 0 {
		t.Fatalf("served %v from a refusing member", served)
	}
	if s := stateOf(c, b.endpoint); s != "ejected" {
		t.Fatalf("b is %q, want ejected", s)
	}
	// Healthy again: the background probe re-admits it after the backoff.
	b.refuse.Store(false)
	waitFor(t, "the probe to re-admit b", func() bool { return stateOf(c, b.endpoint) == "ready" })
	if served := whoN(t, db, 6); count(served, "b") == 0 {
		t.Errorf("served %v: a re-admitted member gets connections", served)
	}
}

func TestAProbeThatFailsDoublesTheBackoff(t *testing.T) {
	ms := fakeMembers(t, "a", "b", "c")
	b := ms[1]
	b.refuse.Store(true)
	c, db := lbDB(t, endpointsOf(ms), "load_balancing=round_robin&eject_errors=1&eject_backoff=200ms")
	db.SetMaxIdleConns(0)
	whoN(t, db, 6)
	waitFor(t, "a failed probe", func() bool {
		n := 0
		memberAt(t, c, b.endpoint, func(m *member) { n = m.ejections })
		return n >= 2
	})
	if s := stateOf(c, b.endpoint); s != "ejected" {
		t.Fatalf("b is %q, want ejected", s)
	}
	memberAt(t, c, b.endpoint, func(m *member) {
		if d := c.backoffFor(m); d < 400*time.Millisecond {
			t.Errorf("backoff after a failed probe = %v, want >= 400ms", d)
		}
	})
}

func TestAConsistentlySlowMemberIsEjected(t *testing.T) {
	ms := fakeMembers(t, "a", "b", "c")
	b := ms[1]
	b.delay.Store(int64(50 * time.Millisecond))
	c, db := lbDB(t, endpointsOf(ms), "load_balancing=round_robin&eject_latency_floor=20ms&eject_backoff=30s")
	db.SetMaxIdleConns(0)
	whoN(t, db, 45)
	if s := stateOf(c, b.endpoint); s != "ejected" {
		t.Fatalf("b is %q, want ejected", s)
	}
	if served := whoN(t, db, 6); count(served, "b") != 0 {
		t.Errorf("served %v from an ejected member", served)
	}
}

func TestAnEjectedMemberIsNotEjectedAgainByLateRoundTrips(t *testing.T) {
	ms := fakeMembers(t, "a", "b", "c")
	b := ms[1]
	b.delay.Store(int64(50 * time.Millisecond))
	c, db := lbDB(t, endpointsOf(ms), "load_balancing=round_robin&eject_latency_floor=20ms&eject_backoff=30s")
	db.SetMaxIdleConns(0)
	whoN(t, db, 45)
	if s := stateOf(c, b.endpoint); s != "ejected" {
		t.Fatalf("b is %q, want ejected", s)
	}
	var m *member
	var until time.Time
	memberAt(t, c, b.endpoint, func(mm *member) { m, until = mm, mm.ejectedUntil })
	// Slow round trips still finishing on connections to b that were checked
	// out before the ejection.
	for i := 0; i < 5; i++ {
		c.observe(m, 50*time.Millisecond)
	}
	memberAt(t, c, b.endpoint, func(m *member) {
		if m.ejections != 1 || !m.ejectedUntil.Equal(until) {
			t.Errorf("late round trips re-ejected b: %d ejections, until moved by %v", m.ejections, m.ejectedUntil.Sub(until))
		}
	})
}

func TestOneSlowStatementDoesNotEjectAMember(t *testing.T) {
	ms := fakeMembers(t, "a", "b", "c")
	b := ms[1]
	c, db := lbDB(t, endpointsOf(ms), "load_balancing=round_robin&eject_latency_floor=50ms")
	db.SetMaxIdleConns(0)
	whoN(t, db, 36)
	b.delay.Store(int64(500 * time.Millisecond))
	for who(t, db) != "b" {
	}
	b.delay.Store(0)
	whoN(t, db, 6)
	if s := stateOf(c, b.endpoint); s != "ready" {
		t.Errorf("b is %q after one slow statement, want ready", s)
	}
}

func TestTheLastUsableMemberIsNeverEjected(t *testing.T) {
	ms := fakeMembers(t, "a", "b", "c")
	a := ms[0]
	ms[1].refuse.Store(true)
	ms[2].refuse.Store(true)
	c, db := lbDB(t, endpointsOf(ms), "load_balancing=round_robin&eject_errors=1")
	db.SetMaxIdleConns(0)
	for _, name := range whoN(t, db, 6) {
		if name != "a" {
			t.Fatalf("served by %s, want a", name)
		}
	}
	a.refuse.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var name string
	if err := db.QueryRowContext(ctx, "SELECT member()").Scan(&name); err == nil {
		t.Fatalf("every member refuses, yet %s answered", name)
	}
	if s := stateOf(c, a.endpoint); s != "ready" {
		t.Errorf("the last usable member is %q, want ready", s)
	}
}

func TestARefusedLoginIsNotAMemberFault(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{errors.New("skaidb: authentication denied: bad password"), false},
		{errors.New("skaidb: authentication denied: too many connections: 500"), true},
		{errors.New("skaidb: server signature mismatch (mutual auth failed)"), false},
		{errors.New("skaidb: bad handshake challenge"), true}, // closed before the handshake
		{fmt.Errorf("skaidb: connect failed: %w", syscall.ECONNREFUSED), true},
		{io.EOF, true},
		{callerFault{errors.New(`skaidb: database "nope" does not exist`)}, false},
		{callerFault{errors.New(`skaidb: cannot read tls_ca "x"`)}, false},
	} {
		if got := memberFault(tc.err); got != tc.want {
			t.Errorf("memberFault(%q) = %v, want %v", tc.err, got, tc.want)
		}
	}
	// Through the pool: a member that refuses the credentials stays in
	// rotation; the connections land on the other one.
	ms := fakeMembers(t, "a", "b")
	ms[1].deny.Store(true)
	c, db := lbDB(t, endpointsOf(ms), "load_balancing=round_robin&eject_errors=1")
	db.SetMaxIdleConns(0)
	for _, name := range whoN(t, db, 4) {
		if name != "a" {
			t.Fatalf("served by %s, want a", name)
		}
	}
	if s := stateOf(c, ms[1].endpoint); s != "ready" {
		t.Errorf("a member refusing a login is %q, want ready", s)
	}
}

// ---- connections retired through database/sql ------------------------------

// An idle connection whose member is ejected is not handed out again:
// ResetSession turns it down, database/sql closes it and uses another.
func TestAnIdleConnectionToAnEjectedMemberIsNotHandedOut(t *testing.T) {
	ms := fakeMembers(t, "a", "b")
	c, db := lbDB(t, endpointsOf(ms), "load_balancing=round_robin&eject_backoff=30s")
	ctx := context.Background()
	c1, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]*sql.Conn{who(t, c1): c1, who(t, c2): c2}
	if byName["a"] == nil || byName["b"] == nil {
		t.Fatalf("round robin opened %v", byName)
	}
	// b's connection goes back last, so database/sql offers it first.
	byName["a"].Close()
	byName["b"].Close()
	memberAt(t, c, ms[1].endpoint, func(m *member) { c.eject(m) })
	if served := whoN(t, db, 4); count(served, "b") != 0 {
		t.Errorf("served %v from an ejected member's idle connection", served)
	}
	for _, m := range c.Members() {
		if m.Endpoint == ms[1].endpoint && m.Open != 0 {
			t.Errorf("the ejected member still has %d connections open", m.Open)
		}
	}
}

// A connection in use when its member is ejected keeps working (it is the
// caller's), and is closed when it goes back to the pool.
func TestAConnectionToAnEjectedMemberIsClosedOnReturn(t *testing.T) {
	ms := fakeMembers(t, "a", "b")
	c, db := lbDB(t, endpointsOf(ms), "load_balancing=round_robin&eject_backoff=30s")
	ctx := context.Background()
	held := map[string]*sql.Conn{}
	for i := 0; i < 2; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		held[who(t, conn)] = conn
	}
	memberAt(t, c, ms[1].endpoint, func(m *member) { c.eject(m) })
	if name := who(t, held["b"]); name != "b" {
		t.Fatalf("a pinned connection moved to %s", name)
	}
	held["b"].Close()
	held["a"].Close()
	for _, m := range c.Members() {
		want := map[string]int{ms[0].endpoint: 1, ms[1].endpoint: 0}[m.Endpoint]
		if m.Open != want {
			t.Errorf("%s has %d connections open, want %d", m.Endpoint, m.Open, want)
		}
	}
}

// The read-your-writes idiom under database/sql: a *sql.Conn stays on the
// member that coordinated its write, while the pool keeps rotating.
func TestASQLConnStaysOnTheMemberThatTookTheWrite(t *testing.T) {
	ms := fakeMembers(t, "a", "b", "c")
	_, db := lbDB(t, endpointsOf(ms), "load_balancing=round_robin")
	db.SetMaxIdleConns(0)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "INSERT INTO t (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	writer := who(t, conn)
	for i := 0; i < 5; i++ {
		if name := who(t, conn); name != writer {
			t.Fatalf("the *sql.Conn moved from %s to %s", writer, name)
		}
	}
	if served := whoN(t, db, 3); len(distinct(served)) != 3 {
		t.Errorf("the pool served %v, want all three members", served)
	}
}

func TestConnectorCloseStopsTheBackgroundGoroutine(t *testing.T) {
	ms := fakeMembers(t, "a", "b")
	c, db := lbDB(t, endpointsOf(ms), "eject_backoff=30s")
	memberAt(t, c, ms[1].endpoint, func(m *member) { c.eject(m) })
	running := func() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.maintaining }
	if !running() {
		t.Fatal("an ejection must start the background goroutine")
	}
	db.Close()
	waitFor(t, "the goroutine to stop", func() bool { return !running() })
	if _, err := c.Connect(context.Background()); !errors.Is(err, errConnectorClosed) {
		t.Errorf("Connect after Close = %v", err)
	}
}

// ---- discovery --------------------------------------------------------------

// discoveryCluster gives three members a shared SHOW ENDPOINTS answer: a and
// b ready, c resyncing; each marks its own row local.
func discoveryCluster(t *testing.T) ([]*fakeMember, *sync.Mutex, map[string]string) {
	ms := fakeMembers(t, "a", "b", "c")
	var mu sync.Mutex
	states := map[string]string{ms[0].endpoint: "ready", ms[1].endpoint: "ready", ms[2].endpoint: "resyncing"}
	for _, m := range ms {
		me := m.endpoint
		m.setListing(func() []listedEndpoint {
			mu.Lock()
			defer mu.Unlock()
			var out []listedEndpoint
			for ep, st := range states {
				out = append(out, listedEndpoint{ep, st, ep == me})
			}
			sort.Slice(out, func(i, j int) bool { return out[i].endpoint < out[j].endpoint })
			return out
		})
	}
	return ms, &mu, states
}

// listNow runs SHOW ENDPOINTS through a fresh connection, as a refresh does.
func listNow(t *testing.T, c *Connector) {
	t.Helper()
	dc, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c.listThrough(dc.(*conn))
	dc.Close()
}

func TestMembersAreDiscoveredAndResyncingOnesGetNoConnections(t *testing.T) {
	ms, mu, states := discoveryCluster(t)
	a, cm := ms[0], ms[2]
	c, db := lbDB(t, []string{a.endpoint}, "load_balancing=round_robin&discover=true")
	db.SetMaxIdleConns(0)
	if served := distinct(whoN(t, db, 8)); strings.Join(served, "") != "ab" {
		t.Fatalf("served %v, want a and b", served)
	}
	if s := stateOf(c, cm.endpoint); s != "resyncing" {
		t.Fatalf("c is %q, want resyncing", s)
	}
	// c catches up: the next refresh lets it in, measured afresh.
	memberAt(t, c, cm.endpoint, func(m *member) { m.ewma, m.samples = 9900*time.Millisecond, 50 })
	mu.Lock()
	states[cm.endpoint] = "ready"
	mu.Unlock()
	listNow(t, c)
	memberAt(t, c, cm.endpoint, func(m *member) {
		if m.ewma != 0 || m.samples != 0 {
			t.Errorf("c back to ready kept ewma %v over %d samples", m.ewma, m.samples)
		}
	})
	if served := whoN(t, db, 6); count(served, "c") == 0 {
		t.Errorf("served %v: c is ready again", served)
	}
}

func TestAListingWithAMemberNotReadyRefreshesSooner(t *testing.T) {
	ms, mu, states := discoveryCluster(t)
	c, db := lbDB(t, []string{ms[0].endpoint}, "load_balancing=round_robin&discover=true&refresh_interval=60s")
	who(t, db)
	next := func() time.Duration { c.mu.Lock(); defer c.mu.Unlock(); return time.Until(c.nextRefresh) }
	if d := next(); d > refreshNotReady {
		t.Errorf("next refresh in %v with c resyncing, want <= %v", d, refreshNotReady)
	}
	mu.Lock()
	states[ms[2].endpoint] = "ready"
	mu.Unlock()
	listNow(t, c)
	if d := next(); d < 50*time.Second {
		t.Errorf("next refresh in %v with every member ready, want the full interval", d)
	}
}

// The background goroutine refreshes the listing on its own.
func TestDiscoveryRefreshesInTheBackground(t *testing.T) {
	ms, mu, states := discoveryCluster(t)
	c, db := lbDB(t, []string{ms[0].endpoint}, "", Discover(true), RefreshInterval(200*time.Millisecond))
	who(t, db)
	mu.Lock()
	states[ms[2].endpoint] = "ready"
	mu.Unlock()
	waitFor(t, "a background refresh", func() bool { return stateOf(c, ms[2].endpoint) == "ready" })
}

func TestASeedGivenByAnotherNameIsNotCountedTwice(t *testing.T) {
	ms, _, _ := discoveryCluster(t)
	a := ms[0]
	c, db := lbDB(t, []string{"localhost:" + a.port()}, "load_balancing=round_robin&discover=true")
	who(t, db)
	rows := c.Members()
	if len(rows) != 3 {
		t.Fatalf("members %+v, want 3", rows)
	}
	for _, r := range rows {
		if r.Seed && (r.Endpoint != "localhost:"+a.port() || r.Name != a.endpoint) {
			t.Errorf("seed %+v, want endpoint localhost:%s named %s", r, a.port(), a.endpoint)
		}
	}
}

func TestAMemberThatLeavesTheListingIsDropped(t *testing.T) {
	ms, mu, states := discoveryCluster(t)
	c, db := lbDB(t, []string{ms[0].endpoint}, "load_balancing=round_robin&discover=true")
	who(t, db)
	mu.Lock()
	delete(states, ms[1].endpoint)
	mu.Unlock()
	listNow(t, c)
	for _, m := range c.Members() {
		if m.Endpoint == ms[1].endpoint {
			t.Errorf("b left the ring but is still listed: %+v", m)
		}
	}
}

func TestASeedThatLeavesTheListingIsMarkedUnlisted(t *testing.T) {
	ms, mu, states := discoveryCluster(t)
	c, db := lbDB(t, []string{ms[0].endpoint, ms[1].endpoint}, "load_balancing=round_robin&discover=true")
	who(t, db)
	who(t, db)
	mu.Lock()
	delete(states, ms[1].endpoint)
	mu.Unlock()
	listNow(t, c)
	if s := stateOf(c, ms[1].endpoint); s != "unlisted" {
		t.Errorf("a seed that left the ring is %q, want unlisted", s)
	}
}

func TestAStandaloneServerTurnsDiscoveryOff(t *testing.T) {
	ms, _, _ := discoveryCluster(t)
	a := ms[0]
	a.setListing(func() []listedEndpoint { return nil })
	c, db := lbDB(t, []string{a.endpoint}, "load_balancing=round_robin&discover=true")
	db.SetMaxIdleConns(0)
	if served := distinct(whoN(t, db, 4)); strings.Join(served, "") != "a" {
		t.Errorf("served %v, want a only", served)
	}
	c.mu.Lock()
	off := c.discoveryOff
	c.mu.Unlock()
	if !off {
		t.Error("no rows from SHOW ENDPOINTS must turn discovery off")
	}
	if n := a.listings.Load(); n != 1 {
		t.Errorf("SHOW ENDPOINTS asked %d times, want once", n)
	}
}

func TestAnOldServerTurnsDiscoveryOffAndKeepsTheSeeds(t *testing.T) {
	ms, _, _ := discoveryCluster(t)
	a := ms[0]
	a.oldServer.Store(true)
	c, db := lbDB(t, []string{a.endpoint}, "load_balancing=round_robin&discover=true")
	db.SetMaxIdleConns(0)
	if served := distinct(whoN(t, db, 4)); strings.Join(served, "") != "a" {
		t.Errorf("served %v, want a only", served)
	}
	c.mu.Lock()
	off := c.discoveryOff
	c.mu.Unlock()
	if !off {
		t.Error("a statement error on SHOW ENDPOINTS must turn discovery off")
	}
	if n := len(c.Members()); n != 1 {
		t.Errorf("%d members, want the one seed", n)
	}
}

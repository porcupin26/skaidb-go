package skaidb

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---- load balancing across members (PROTOCOL.md §6.1) ----------------------
//
// database/sql owns the pool: it keeps the idle connections, chooses which one
// serves a statement, and asks the driver.Connector for a new connection only
// when it has none to spare. The balancing therefore lives in the Connector:
// it keeps the member table (latency averages, health, discovery), picks the
// member each NEW connection is dialled to, and makes a connection whose
// member left the rotation fail IsValid / ResetSession, so database/sql closes
// it and dials a replacement through the Connector.

// Policy chooses the member each new pooled connection is dialled to.
type Policy string

const (
	// Random dials the usable members in random order; each connection stays
	// on the member it reached. The default.
	Random Policy = "random"
	// RoundRobin dials the next usable member in endpoint order.
	RoundRobin Policy = "round_robin"
	// LeastLatency dials, of two usable members picked at random, the one
	// with the lower round-trip average × (open connections + 1). A member
	// not measured in the last 2 s scores 0, so it is tried.
	LeastLatency Policy = "least_latency"
)

const (
	// Latency EWMA: each round trip moves a member's average 5% toward it,
	// and no sample counts for more than 10× the larger of the current
	// average and the ejection floor — one slow statement does not eject a
	// healthy member, a member that stays slow is ejected within a few dozen
	// statements.
	ewmaWeight = 0.05
	ewmaClamp  = 10
	// Round trips a member (and at least one other) needs before its average
	// can eject it.
	ewmaMinSamples = 10
	// LeastLatency: a member whose last round trip is older than this scores
	// 0, so a lightly loaded pool keeps re-measuring every member instead of
	// settling forever on whichever answered fastest once.
	staleSample = 2 * time.Second
	// Discovery refresh while a listed member is not ready: a member is often
	// resyncing or unreachable only for seconds (a peer's circuit breaker
	// right after it restarts), and waiting a whole refresh interval to let
	// it back in leaves the load on the others.
	refreshNotReady = 5 * time.Second
	// How often the background goroutine checks for members due a probe and
	// for a due discovery refresh.
	maintainTick = 250 * time.Millisecond
	// Bound on a probe's or a refresh's dial and statement, so a member that
	// accepts and then hangs cannot stall the background goroutine.
	backgroundTimeout = 10 * time.Second
)

// balance holds the balancing options: the DSN's, then any Option on top.
type balance struct {
	policy          Policy
	discover        bool
	refreshInterval time.Duration
	ejectErrors     int
	ejectFactor     float64
	ejectFloor      time.Duration
	ejectBackoff    time.Duration
	ejectBackoffMax time.Duration
}

func defaultBalance() balance {
	return balance{
		policy:          Random,
		refreshInterval: 60 * time.Second,
		ejectErrors:     3,
		ejectFactor:     3,
		ejectFloor:      50 * time.Millisecond,
		ejectBackoff:    5 * time.Second,
		ejectBackoffMax: 60 * time.Second,
	}
}

// errIdleFIFO and errReadYourWrites refuse the two options of the shared spec
// that database/sql leaves no room for, rather than accepting and ignoring
// them.
var (
	errIdleFIFO = errors.New("skaidb: idle_order=fifo is not available: database/sql keeps the idle " +
		"connections and hands out the most recently returned one first (lifo); " +
		"use db.SetConnMaxIdleTime / SetConnMaxLifetime to keep them rotating")
	errReadYourWrites = errors.New("skaidb: read_your_writes is not available: database/sql chooses " +
		"the connection for each statement; run a write and the reads that must see it " +
		"on one *sql.Conn from db.Conn(ctx), which stays on the member that took the write")
)

// parseBalancing reads the balancing options from the DSN query.
func parseBalancing(q url.Values) (balance, error) {
	b := defaultBalance()
	if v := q.Get("load_balancing"); v != "" {
		b.policy = Policy(strings.ToLower(v))
	}
	switch strings.ToLower(q.Get("idle_order")) {
	case "", "lifo":
	case "fifo":
		return b, errIdleFIFO
	default:
		return b, fmt.Errorf("skaidb: bad idle_order %q (database/sql's order is lifo)", q.Get("idle_order"))
	}
	switch strings.ToLower(q.Get("discover")) {
	case "", "false", "0":
	case "true", "1":
		b.discover = true
	default:
		return b, fmt.Errorf("skaidb: bad discover %q", q.Get("discover"))
	}
	for _, d := range []struct {
		name string
		dst  *time.Duration
	}{
		{"refresh_interval", &b.refreshInterval},
		{"eject_latency_floor", &b.ejectFloor},
		{"eject_backoff", &b.ejectBackoff},
		{"eject_backoff_max", &b.ejectBackoffMax},
	} {
		if v := q.Get(d.name); v != "" {
			parsed, err := time.ParseDuration(v)
			if err != nil {
				return b, fmt.Errorf("skaidb: bad %s %q (a duration such as 5s or 50ms)", d.name, v)
			}
			*d.dst = parsed
		}
	}
	if v := q.Get("read_your_writes"); v != "" {
		window, err := time.ParseDuration(v)
		if err != nil {
			return b, fmt.Errorf("skaidb: bad read_your_writes %q (a duration such as 2s)", v)
		}
		if window != 0 {
			return b, errReadYourWrites
		}
	}
	if v := q.Get("eject_errors"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return b, fmt.Errorf("skaidb: bad eject_errors %q", v)
		}
		b.ejectErrors = n
	}
	if v := q.Get("eject_latency_factor"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return b, fmt.Errorf("skaidb: bad eject_latency_factor %q", v)
		}
		b.ejectFactor = f
	}
	return b, b.validate()
}

func (b balance) validate() error {
	switch b.policy {
	case Random, RoundRobin, LeastLatency:
	default:
		return fmt.Errorf("skaidb: bad load_balancing %q (use random, round_robin or least_latency)", string(b.policy))
	}
	switch {
	case b.ejectErrors < 1:
		return fmt.Errorf("skaidb: eject_errors must be at least 1, got %d", b.ejectErrors)
	case b.ejectFactor < 0 || math.IsNaN(b.ejectFactor) || math.IsInf(b.ejectFactor, 0):
		return fmt.Errorf("skaidb: eject_latency_factor must be a finite number >= 0 (0 disables), got %v", b.ejectFactor)
	case b.ejectFloor < 0:
		return fmt.Errorf("skaidb: eject_latency_floor must not be negative, got %v", b.ejectFloor)
	case b.refreshInterval <= 0:
		return fmt.Errorf("skaidb: refresh_interval must be positive, got %v", b.refreshInterval)
	case b.ejectBackoff <= 0 || b.ejectBackoffMax <= 0:
		return fmt.Errorf("skaidb: eject_backoff and eject_backoff_max must be positive, got %v and %v", b.ejectBackoff, b.ejectBackoffMax)
	}
	return nil
}

// Option tunes a Connector made by NewConnector. Options apply after the
// DSN, so they override its query parameters of the same name.
type Option func(*balance)

// LoadBalancing sets the policy for new connections (DSN load_balancing):
// Random (default), RoundRobin or LeastLatency.
func LoadBalancing(p Policy) Option { return func(b *balance) { b.policy = p } }

// Discover reads the cluster's members with SHOW ENDPOINTS (DSN discover;
// server >= 0.327): members joining the ring are added, members leaving it
// dropped, and members reported resyncing or unreachable get no new
// connections until they are ready again.
func Discover(on bool) Option { return func(b *balance) { b.discover = on } }

// RefreshInterval sets how often discovery re-reads the members (DSN
// refresh_interval, default 60 s; every 5 s while one is not ready).
func RefreshInterval(d time.Duration) Option { return func(b *balance) { b.refreshInterval = d } }

// EjectErrors sets how many consecutive transport failures eject a member
// (DSN eject_errors, default 3).
func EjectErrors(n int) Option { return func(b *balance) { b.ejectErrors = n } }

// EjectLatencyFactor ejects a member whose round-trip average exceeds this
// many times the other members' median (DSN eject_latency_factor, default 3;
// 0 disables latency ejection).
func EjectLatencyFactor(f float64) Option { return func(b *balance) { b.ejectFactor = f } }

// EjectLatencyFloor is how far above the median a member's average must
// also be before it is ejected for latency (DSN eject_latency_floor,
// default 50 ms).
func EjectLatencyFloor(d time.Duration) Option { return func(b *balance) { b.ejectFloor = d } }

// EjectBackoff sets how long an ejection lasts (DSN eject_backoff, default
// 5 s), doubling for each repeat.
func EjectBackoff(d time.Duration) Option { return func(b *balance) { b.ejectBackoff = d } }

// EjectBackoffMax caps the doubling ejection (DSN eject_backoff_max, default
// 60 s). A member healthy for this long after a re-admission starts over at
// EjectBackoff.
func EjectBackoffMax(d time.Duration) Option { return func(b *balance) { b.ejectBackoffMax = d } }

// member is one cluster member as the Connector sees it. Guarded by
// Connector.mu.
type member struct {
	endpoint     string // what the Connector dials, host:port
	seed         bool   // configured in the DSN: discovery never drops it
	canonical    string // its own name in SHOW ENDPOINTS; "" until learned
	listed       string // ready | resyncing | unreachable | unlisted
	ewma         time.Duration
	samples      int
	lastSample   time.Time
	errors       int       // consecutive transport failures
	ejectedUntil time.Time // zero = admitted
	ejections    int
	readmittedAt time.Time
	open         int // connections open to it
	probing      bool
}

// usable: admitted and listed ready — the members that get new connections.
func (m *member) usable() bool { return m.ejectedUntil.IsZero() && m.listed == "ready" }

// freshStart forgets a member's latency and errors: a re-admission, or a
// member back to ready in a listing, is measured afresh.
func (m *member) freshStart() {
	m.ewma, m.samples, m.errors = 0, 0, 0
}

// MemberInfo is one member as Connector.Members reports it.
type MemberInfo struct {
	Endpoint string        // what the Connector dials, host:port
	Name     string        // the member's own endpoint from SHOW ENDPOINTS; "" until discovered
	State    string        // ready, ejected, resyncing, unreachable or unlisted
	Latency  time.Duration // the round-trip average; 0 until measured
	Open     int           // connections open to it (idle or in use: database/sql keeps that split)
	Seed     bool          // configured in the DSN
}

// errConnectorClosed is Connect's answer after Close.
var errConnectorClosed = errors.New("skaidb: connector is closed")

// Connector is the driver.Connector a *sql.DB dials through: the parsed DSN
// plus the member table the pool is balanced over (PROTOCOL.md §6.1).
// sql.Open builds one from the DSN; NewConnector builds one with Options on
// top and keeps it reachable for Members. Use one Connector per *sql.DB:
// DB.Close closes it.
type Connector struct {
	cfg config
	d   *drv

	mu           sync.Mutex
	members      map[string]*member // by endpoint dialled
	rr           int
	discoveryOff bool
	nextRefresh  time.Time
	maintaining  bool
	closed       bool
	stop         chan struct{}
}

// NewConnector parses dsn as sql.Open does and applies opts on top — the
// way to set the balancing options in code and to keep the Connector for
// Members. Hand it to sql.OpenDB:
//
//	c, err := skaidb.NewConnector("skaidb://app:pw@db1,db2,db3/app",
//		skaidb.LoadBalancing(skaidb.LeastLatency), skaidb.Discover(true))
//	if err != nil {
//		return err
//	}
//	db := sql.OpenDB(c)
//	defer db.Close() // closes c too, stopping its background goroutine
//
// Nothing is dialled until the first statement.
func NewConnector(dsn string, opts ...Option) (*Connector, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	for _, o := range opts {
		if o != nil {
			o(&cfg.lb)
		}
	}
	if err := cfg.lb.validate(); err != nil {
		return nil, err
	}
	return newConnector(cfg, &drv{}), nil
}

func newConnector(cfg config, d *drv) *Connector {
	c := &Connector{cfg: cfg, d: d, members: map[string]*member{}, rr: -1, stop: make(chan struct{})}
	for _, addr := range cfg.addrs {
		if c.members[addr] == nil {
			c.members[addr] = &member{endpoint: addr, seed: true, listed: "ready"}
		}
	}
	return c
}

// Connect implements driver.Connector: one dial per pooled connection, to
// the member the policy picks, falling back to the other usable members in
// random order and then to the unusable ones (resyncing/unreachable before
// ejected, the soonest re-admission first). With discovery on, the first
// connection to a member not yet named runs SHOW ENDPOINTS. The dial has a
// fixed connect timeout and is not interruptible mid-handshake, so ctx only
// short-circuits an attempt that is already cancelled when the pool asks.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for attempt := 0; ; attempt++ {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, errConnectorClosed
		}
		order := c.order(c.pick())
		c.mu.Unlock()
		cn, err := c.dialMember(order, time.Time{})
		if err != nil {
			return nil, err
		}
		c.discoverThrough(cn)
		switch {
		case cn.broken.Load():
			// The listing lost the connection; database/sql retries an
			// ErrBadConn on a fresh one.
			cn.Close()
			if attempt == 2 {
				return nil, driver.ErrBadConn
			}
		case attempt == 2 || c.keep(cn):
			return cn, nil
		default:
			// The listing says the member just reached is not ready: dial
			// again, now that the order knows it.
			cn.Close()
		}
	}
}

// Driver implements driver.Connector; it is what db.Driver() returns.
func (c *Connector) Driver() driver.Driver { return c.d }

// Close implements io.Closer, which DB.Close calls: it stops the background
// goroutine that probes ejected members and refreshes discovery. Connections
// already open are database/sql's to close.
func (c *Connector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		close(c.stop)
	}
	return nil
}

// Members lists the members as the Connector sees them, sorted by endpoint,
// for monitoring.
func (c *Connector) Members() []MemberInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]MemberInfo, 0, len(c.members))
	for _, m := range c.sorted() {
		state := m.listed
		if !m.ejectedUntil.IsZero() {
			state = "ejected"
		}
		out = append(out, MemberInfo{
			Endpoint: m.endpoint,
			Name:     m.canonical,
			State:    state,
			Latency:  m.ewma,
			Open:     m.open,
			Seed:     m.seed,
		})
	}
	return out
}

// -- choosing a member (callers hold c.mu) --

func (c *Connector) sorted() []*member {
	ms := make([]*member, 0, len(c.members))
	for _, m := range c.members {
		ms = append(ms, m)
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].endpoint < ms[j].endpoint })
	return ms
}

// pick is the member the next connection should go to, or nil for "any"
// (the random policy, or no usable member at all).
func (c *Connector) pick() *member {
	if c.cfg.lb.policy == Random {
		return nil
	}
	var usable []*member
	for _, m := range c.sorted() {
		if m.usable() {
			usable = append(usable, m)
		}
	}
	switch {
	case len(usable) == 0:
		return nil
	case c.cfg.lb.policy == RoundRobin:
		c.rr = (c.rr + 1) % len(usable)
		return usable[c.rr]
	case len(usable) == 1:
		return usable[0]
	}
	i := rand.Intn(len(usable))
	j := rand.Intn(len(usable) - 1)
	if j >= i {
		j++
	}
	now := time.Now()
	if score(usable[i], now) <= score(usable[j], now) {
		return usable[i]
	}
	return usable[j]
}

// score is LeastLatency's load estimate. A member never measured, or not
// lately, scores 0: it is tried and measured.
func score(m *member, now time.Time) float64 {
	if m.samples == 0 || now.Sub(m.lastSample) > staleSample {
		return 0
	}
	return float64(m.ewma) * float64(m.open+1)
}

// order is the dial order, best first: first, the other usable members in
// random order, then the rest (resyncing/unreachable/unlisted before
// ejected, the soonest re-admission first) — a dial never has nowhere to go
// while any member is configured.
func (c *Connector) order(first *member) []*member {
	var usable, rest []*member
	for _, m := range c.sorted() {
		switch {
		case m == first:
		case m.usable():
			usable = append(usable, m)
		default:
			rest = append(rest, m)
		}
	}
	rand.Shuffle(len(usable), func(i, j int) { usable[i], usable[j] = usable[j], usable[i] })
	sort.SliceStable(rest, func(i, j int) bool {
		ei, ej := !rest[i].ejectedUntil.IsZero(), !rest[j].ejectedUntil.IsZero()
		if ei != ej {
			return ej
		}
		return rest[i].ejectedUntil.Before(rest[j].ejectedUntil)
	})
	out := make([]*member, 0, len(c.members))
	if first != nil {
		out = append(out, first)
	}
	return append(append(out, usable...), rest...)
}

// dialMember dials the members in order until one connects and
// authenticates, counting each failure that is the member's against it.
// A non-zero deadline bounds the dial and stays on the connection.
func (c *Connector) dialMember(order []*member, deadline time.Time) (*conn, error) {
	var lastErr error
	tried := make([]string, 0, len(order))
	for _, m := range order {
		cn, err := dialOneDeadline(c.cfg, m.endpoint, deadline)
		if err != nil {
			if memberFault(err) {
				c.failed(m)
			}
			tried = append(tried, m.endpoint)
			lastErr = err
			continue
		}
		c.mu.Lock()
		m.open++
		c.mu.Unlock()
		cn.lb, cn.member = c, m
		return cn, nil
	}
	sort.Strings(tried)
	return nil, fmt.Errorf("skaidb: no reachable endpoint in %v: %w", tried, lastErr)
}

// -- what connections report (any goroutine) --

// keep reports whether a pooled connection may stay in the pool: its member
// is still a member and usable. When no member is usable at all, every
// connection is kept — closing them would only re-dial the same members.
func (c *Connector) keep(cn *conn) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := cn.member
	if c.members[m.endpoint] == m && m.usable() {
		return true
	}
	for _, o := range c.members {
		if o.usable() {
			return false
		}
	}
	return true
}

// released: a connection to m closed.
func (c *Connector) released(m *member) {
	c.mu.Lock()
	if m.open > 0 {
		m.open--
	}
	c.mu.Unlock()
}

// observe folds one round trip into the member's average and ejects a
// member that stays far slower than the others.
func (c *Connector) observe(m *member, elapsed time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.members[m.endpoint] != m {
		return // dropped by discovery meanwhile
	}
	m.errors = 0
	if m.samples == 0 {
		m.ewma = elapsed
	} else {
		limit := ewmaClamp * max(m.ewma, c.cfg.lb.ejectFloor)
		m.ewma += time.Duration(ewmaWeight * float64(min(elapsed, limit)-m.ewma))
	}
	m.samples++
	m.lastSample = time.Now()
	// Only a member still in rotation: round trips finishing on connections
	// checked out before an ejection must not eject it again.
	if m.samples < ewmaMinSamples || c.cfg.lb.ejectFactor <= 0 || !m.usable() {
		return
	}
	var others []time.Duration
	for _, o := range c.members {
		if o != m && o.usable() && o.samples >= ewmaMinSamples {
			others = append(others, o.ewma)
		}
	}
	if len(others) == 0 {
		return
	}
	sort.Slice(others, func(i, j int) bool { return others[i] < others[j] })
	median := others[(len(others)-1)/2] // the lower middle when even
	if float64(m.ewma) > c.cfg.lb.ejectFactor*float64(median) && m.ewma-median > c.cfg.lb.ejectFloor {
		c.eject(m)
	}
}

// failed counts a transport failure against m.
func (c *Connector) failed(m *member) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.members[m.endpoint] != m {
		return
	}
	m.errors++
	if m.errors >= c.cfg.lb.ejectErrors && m.ejectedUntil.IsZero() {
		c.eject(m)
	}
}

// -- ejection and re-admission --

// eject takes m out of rotation (caller holds c.mu) — unless it is the last
// usable member: a slow or flaky member beats none.
func (c *Connector) eject(m *member) {
	last := true
	for _, o := range c.members {
		if o != m && o.usable() {
			last = false
			break
		}
	}
	if last {
		return
	}
	now := time.Now()
	if now.Sub(m.readmittedAt) > c.cfg.lb.ejectBackoffMax {
		m.ejections = 0 // healthy for a while: start the backoff over
	}
	m.ejections++
	m.ejectedUntil = now.Add(c.backoffFor(m))
	c.ensureMaintainer()
}

// backoffFor is the length of m's current ejection: the base, doubled per
// repeat, capped.
func (c *Connector) backoffFor(m *member) time.Duration {
	d := c.cfg.lb.ejectBackoff
	for i := 1; i < m.ejections && d < c.cfg.lb.ejectBackoffMax; i++ {
		d *= 2
	}
	return min(d, c.cfg.lb.ejectBackoffMax)
}

// ensureMaintainer starts the background goroutine (caller holds c.mu). It
// exits by itself once no member is ejected and discovery is off, and at
// Close; it never keeps anything alive past either.
func (c *Connector) ensureMaintainer() {
	if c.maintaining || c.closed {
		return
	}
	c.maintaining = true
	go c.maintain()
}

func (c *Connector) maintain() {
	t := time.NewTicker(maintainTick)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			c.mu.Lock()
			c.maintaining = false
			c.mu.Unlock()
			return
		case <-t.C:
		}
		now := time.Now()
		c.mu.Lock()
		var due []*member
		ejected := false
		for _, m := range c.members {
			if m.ejectedUntil.IsZero() {
				continue
			}
			ejected = true
			if !now.Before(m.ejectedUntil) && !m.probing {
				m.probing = true
				due = append(due, m)
			}
		}
		discovering := c.cfg.lb.discover && !c.discoveryOff
		if !ejected && !discovering {
			c.maintaining = false
			c.mu.Unlock()
			return
		}
		refresh := discovering && !now.Before(c.nextRefresh)
		c.mu.Unlock()
		for _, m := range due {
			c.probe(m)
		}
		if refresh {
			c.refresh()
		}
	}
}

// probe dials m alone and runs one statement: success re-admits it with a
// fresh average, failure ejects it again for twice as long. The probe's
// connection is closed — database/sql's idle set is not the driver's to add
// to.
func (c *Connector) probe(m *member) {
	ok := false
	if cn, err := dialOneDeadline(c.cfg, m.endpoint, time.Now().Add(backgroundTimeout)); err == nil {
		_, err = cn.query("SHOW DATABASES")
		ok = err == nil
		cn.Close()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	m.probing = false
	if ok && !c.closed {
		m.ejectedUntil = time.Time{}
		m.freshStart()
		m.readmittedAt = time.Now()
		return
	}
	m.ejections++
	m.ejectedUntil = time.Now().Add(c.backoffFor(m))
}

// -- discovery --

// discoverThrough runs discovery on a freshly dialled connection when the
// member it reached has not named itself yet (once per member, so a seed
// given as a host name is matched to the endpoint the server reports).
func (c *Connector) discoverThrough(cn *conn) {
	if !c.cfg.lb.discover {
		return
	}
	c.mu.Lock()
	m := cn.member
	if c.discoveryOff || m.canonical != "" || c.members[m.endpoint] != m {
		c.mu.Unlock()
		return
	}
	c.ensureMaintainer()
	c.mu.Unlock()
	c.listThrough(cn)
}

// refresh re-reads the members on a connection of its own, closed after.
func (c *Connector) refresh() {
	c.mu.Lock()
	order := c.order(nil)
	// Until a listing lands: a failed dial or listing waits a whole interval.
	c.nextRefresh = time.Now().Add(c.cfg.lb.refreshInterval)
	c.mu.Unlock()
	cn, err := c.dialMember(order, time.Now().Add(backgroundTimeout))
	if err != nil {
		return
	}
	c.listThrough(cn)
	cn.Close()
}

// listedEndpoint is one row of SHOW ENDPOINTS.
type listedEndpoint struct {
	endpoint, state string
	local           bool
}

// listThrough runs SHOW ENDPOINTS on cn and folds the answer in. A
// statement error (a server older than the statement) or no rows (a
// standalone server, a witness) means there are no members to learn, now or
// later: discovery stops and the seeds stay as they are.
func (c *Connector) listThrough(cn *conn) {
	r, err := cn.sendQuery("SHOW ENDPOINTS")
	if err != nil {
		return // transport: cn is broken, and the caller drops it
	}
	if len(r.buf) > 0 && r.buf[0] == 3 {
		c.mu.Lock()
		c.discoveryOff = true
		c.mu.Unlock()
		return
	}
	rs, err := cn.readRows(r)
	if err != nil {
		return
	}
	data := rs.(*rows).data
	if len(data) == 0 {
		c.mu.Lock()
		c.discoveryOff = true
		c.mu.Unlock()
		return
	}
	var listing []listedEndpoint
	for _, row := range data {
		if len(row) < 3 {
			continue
		}
		ep, ok1 := row[0].(string)
		state, ok2 := row[1].(string)
		local, _ := row[2].(bool)
		if ok1 && ok2 && ep != "" {
			listing = append(listing, listedEndpoint{ep, state, local})
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.applyListing(cn.member, listing)
	interval := c.cfg.lb.refreshInterval
	for _, m := range c.members {
		if m.listed == "resyncing" || m.listed == "unreachable" {
			interval = min(interval, refreshNotReady)
			break
		}
	}
	c.nextRefresh = time.Now().Add(interval)
}

// applyListing folds a SHOW ENDPOINTS answer into the member table (caller
// holds c.mu).
func (c *Connector) applyListing(dialed *member, listing []listedEndpoint) {
	if len(listing) == 0 {
		return
	}
	var names []string
	states := map[string]string{}
	for _, row := range listing {
		if _, dup := states[row.endpoint]; !dup {
			names = append(names, row.endpoint)
		}
		states[row.endpoint] = row.state
		if row.local && c.members[dialed.endpoint] == dialed {
			dialed.canonical = row.endpoint
		}
	}
	// A member reported under the name of one already known (a seed given
	// as a host name) is that member, not a new one.
	byName := map[string]*member{}
	for _, m := range c.sorted() {
		if m.canonical == "" {
			continue
		}
		if byName[m.canonical] == nil || m.seed {
			byName[m.canonical] = m
		}
	}
	for key, m := range c.members {
		if m.canonical != "" && byName[m.canonical] != m {
			delete(c.members, key) // a duplicate of a seed
		}
	}
	seen := map[*member]bool{}
	for _, name := range names {
		m := byName[name]
		if m == nil {
			addr, err := withDefaultPort(name)
			if err != nil {
				continue
			}
			if m = c.members[addr]; m == nil {
				m = &member{endpoint: addr, listed: "ready"}
				c.members[addr] = m
			}
			m.canonical = name
		}
		state := states[name]
		switch state {
		case "ready", "resyncing", "unreachable":
		default:
			state = "ready"
		}
		if state == "ready" && m.listed != "ready" {
			m.freshStart() // back in rotation: measured afresh, like a re-admission
		}
		m.listed = state
		seen[m] = true
	}
	for key, m := range c.members {
		if seen[m] || (m.seed && m.canonical == "") {
			continue // listed, or a seed not identified yet
		}
		if m.seed {
			m.listed = "unlisted" // configured, but no longer a member
		} else {
			delete(c.members, key)
		}
	}
}

// -- what counts against a member --

// callerFault marks a dial failure that is this side's problem rather than
// the member's: an unreadable TLS file, a session database the server
// refuses. Its message is the wrapped error's, unchanged.
type callerFault struct{ err error }

func (e callerFault) Error() string { return e.err.Error() }
func (e callerFault) Unwrap() error { return e.err }

// memberFault reports whether a failed dial says something about the
// member's health: any I/O error or handshake failure, a capacity refusal
// included — but not a login refused for its credentials, a server
// signature that does not verify, or a problem on this side.
func memberFault(err error) bool {
	var cf callerFault
	if errors.As(err, &cf) {
		return false
	}
	msg := strings.TrimPrefix(err.Error(), "skaidb: ")
	if strings.HasPrefix(msg, "authentication denied") {
		return strings.Contains(msg, "too many connections:")
	}
	return !strings.HasPrefix(msg, "server signature mismatch")
}

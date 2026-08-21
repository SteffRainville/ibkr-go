// Package mdlines tracks active broker market-data lines against the
// account's simultaneous-line cap, so background/discretionary subscriptions
// (e.g. rotating an at-the-money option strike) can't starve the lines that
// open positions or core signals depend on.
//
// A fresh Ledger should be created per broker session (per TCP connection),
// matching most brokers' behavior of dropping every subscription on
// disconnect.
package mdlines

import (
	"log"
	"sync"
	"time"
)

// Category classifies a persistent market-data line by priority. Two
// categories are PERSISTENT — a line held for as long as the thing it serves
// exists — and two are TRANSIENT, released as soon as the request they were
// opened for completes.
//
// There were formerly two further persistent tiers, CategoryDiscretionaryNew
// and CategoryDiscretionaryChurn, holding a streaming line per watchlist
// option row so a dashboard could display a strike/bid/ask for a contract
// nobody held. They were the whole reason this package needed reserves: they
// consumed ~60 of a 100-line account and were refused thousands of times a
// day. Nothing ever traded on them — an entry resolves its own contract via
// CategoryProbe — so they were removed along with the reserve machinery that
// existed to ration them.
type Category int

const (
	// CategoryStock — streaming market data for an underlying. Guaranteed.
	// Persistent: one per distinct base symbol, for as long as it is watched.
	CategoryStock Category = iota
	// CategoryPosition — market data pinned to an open position's contract.
	// Guaranteed and highest priority: it is what keeps a live position's
	// stop-loss/trailing-stop evaluation alive. Persistent: released only
	// when the last holder of that contract exits.
	CategoryPosition
	// CategorySnapshot — transient snapshot requests. They consume a line
	// only until the broker delivers the snapshot and auto-cancels it, so
	// they are counted while live but released quickly.
	CategorySnapshot
	// CategoryProbe — a high-priority, short-lived probe opened while
	// actively trying to resolve a real, fresh quote for an imminent action
	// (e.g. a buy). Transient: granted in batches of a few candidates,
	// released the moment the resolution picks a winner.
	CategoryProbe

	numCategories = 4
)

// Buffer is the default headroom cushion kept below the hard line cap so
// grants never actually reach the broker's simultaneous-line limit (which
// typically rejects the subscription outright). Probe grants may dip into it
// when the rest of the pool is full, since a probe is short-lived and an
// entry is waiting on it.
const Buffer = 10

// SnapshotMaxAge is how old a transient snapshot line may get before the
// reaper frees it.
const SnapshotMaxAge = 60 * time.Second

// ProbeMaxAge is how old a CategoryProbe line may get before the reaper frees
// it. A healthy probe resolves within its caller's own timeout (entry-side,
// currently 10s) and is released by resolveDeltaCandidates the moment that
// caller's poll loop exits — this is a generous multiple of that, so it only
// fires when the owning resolution goroutine itself got stuck (e.g. blocked
// elsewhere) and never reached its own release path. Without this, such a
// line has no other way back into the pool for the rest of the TCP session,
// unlike CategorySnapshot which has always had ReapSnapshots as a backstop.
const ProbeMaxAge = 30 * time.Second

// Ledger tracks active market-data lines against the account's simultaneous-
// line cap. It is a leaf lock: its methods never call back into the broker
// client, so callers may hold their own locks across a Ledger call without
// risk of deadlock — the actual subscribe/cancel broker calls stay at the
// call sites.
type Ledger struct {
	mu    sync.Mutex
	max   int
	lines map[int64]Category
	byCat [numCategories]int

	// snapAt records when each CategorySnapshot line was granted, so
	// ReapSnapshots can free a transient snapshot whose terminal callback
	// never arrived.
	snapAt map[int64]time.Time

	// probeAt records when each CategoryProbe line was granted, so ReapProbes
	// can free one whose owning resolution never released it.
	probeAt map[int64]time.Time

	// histLines tracks keep-up-to-date historical bar subscriptions, which
	// IB-style brokers govern under a SEPARATE hard ceiling independent of
	// the line cap above (a streaming line and a keep-up-to-date request for
	// the same underlying share one market-data line, so these are NOT also
	// counted in `lines`).
	histLines map[int64]struct{}
	histMax   int

	onChange func(used, max int)
}

// NewLedger returns a Ledger with the given line cap and historical-stream
// ceiling. max defaults to 100 and histMax to 50 when <= 0.
func NewLedger(max, histMax int) *Ledger {
	if max <= 0 {
		max = 100
	}
	if histMax <= 0 {
		histMax = 50
	}
	return &Ledger{
		max:       max,
		lines:     make(map[int64]Category),
		snapAt:    make(map[int64]time.Time),
		probeAt:   make(map[int64]time.Time),
		histLines: make(map[int64]struct{}),
		histMax:   histMax,
	}
}

// SetOnChange registers a callback fired (outside the lock) whenever the
// line count changes, carrying the fresh (used, max) snapshot.
func (l *Ledger) SetOnChange(fn func(used, max int)) {
	l.mu.Lock()
	l.onChange = fn
	l.mu.Unlock()
}

func (l *Ledger) notify() {
	l.mu.Lock()
	fn := l.onChange
	used, max := len(l.lines), l.max
	l.mu.Unlock()
	if fn != nil {
		fn(used, max)
	}
}

// GrantGuaranteed records a stock- or position-category line. It is never
// refused — if honouring it pushes usage past the cap, that is logged
// loudly so the operator can raise the cap (or trim the watchlist), but the
// line is still placed. Returns true if this reqID was newly recorded.
func (l *Ledger) GrantGuaranteed(reqID int64, cat Category) bool {
	l.mu.Lock()
	if _, exists := l.lines[reqID]; exists {
		l.mu.Unlock()
		return false
	}
	l.lines[reqID] = cat
	l.byCat[cat]++
	over := len(l.lines) > l.max
	used, max := len(l.lines), l.max
	l.mu.Unlock()
	if over {
		log.Printf("mdlines: WARNING over cap — %d/%d lines in use after guaranteed subscription", used, max)
	}
	l.notify()
	return true
}

// GrantSnapshot records a transient snapshot line, granted only while
// strictly under the cap so a burst of snapshot requests can never push
// total usage past it. Released on the snapshot's terminal callback via
// Release().
func (l *Ledger) GrantSnapshot(reqID int64) bool {
	l.mu.Lock()
	if _, exists := l.lines[reqID]; exists {
		l.mu.Unlock()
		return true
	}
	if len(l.lines) >= l.max {
		l.mu.Unlock()
		return false
	}
	l.lines[reqID] = CategorySnapshot
	l.byCat[CategorySnapshot]++
	l.snapAt[reqID] = time.Now()
	l.mu.Unlock()
	l.notify()
	return true
}

// GrantProbe records a high-priority, short-lived probe line. It:
//  1. grants directly while there is room outside the buffer (used < cap-Buffer);
//  2. otherwise dips into the buffer if still strictly under the hard cap,
//     since a probe is short-lived and an entry decision is blocked on it;
//  3. only refuses at the hard cap.
//
// It used to preempt a background line at step 2, returning the evicted reqID
// for the caller to cancel. With the discretionary categories gone there is
// nothing left to preempt — the pool is stocks, open positions and other
// in-flight probes, none of which may be dropped for this one. Released via
// Release() like any line.
func (l *Ledger) GrantProbe(reqID int64) bool {
	l.mu.Lock()
	if _, exists := l.lines[reqID]; exists {
		l.mu.Unlock()
		return true
	}
	place := func() {
		l.lines[reqID] = CategoryProbe
		l.byCat[CategoryProbe]++
		l.probeAt[reqID] = time.Now()
	}

	if len(l.lines) < l.max-Buffer {
		place()
		l.mu.Unlock()
		l.notify()
		return true
	}
	if len(l.lines) < l.max {
		place()
		used, max := len(l.lines), l.max
		l.mu.Unlock()
		log.Printf("mdlines: probe granted into the %d-line buffer — %d/%d in use", Buffer, used, max)
		l.notify()
		return true
	}
	used, max := len(l.lines), l.max
	l.mu.Unlock()
	log.Printf("mdlines: WARNING probe refused — %d/%d lines in use, at the hard cap", used, max)
	return false
}

// TrackSnapshot records a transient snapshot line unconditionally (never
// refused). Released on the terminal callback via Release().
func (l *Ledger) TrackSnapshot(reqID int64) {
	l.mu.Lock()
	if _, exists := l.lines[reqID]; exists {
		l.mu.Unlock()
		return
	}
	l.lines[reqID] = CategorySnapshot
	l.byCat[CategorySnapshot]++
	l.snapAt[reqID] = time.Now()
	l.mu.Unlock()
	l.notify()
}

// GrantHist records a keep-up-to-date historical bar subscription against
// the separate historical-stream ceiling. Returns false when the ceiling is
// reached — the caller must NOT subscribe and should warn instead.
func (l *Ledger) GrantHist(reqID int64) bool {
	l.mu.Lock()
	if _, exists := l.histLines[reqID]; exists {
		l.mu.Unlock()
		return true
	}
	if len(l.histLines) >= l.histMax {
		l.mu.Unlock()
		return false
	}
	l.histLines[reqID] = struct{}{}
	l.mu.Unlock()
	l.notify()
	return true
}

// ReleaseHist removes a keep-up-to-date historical subscription from the ledger.
func (l *Ledger) ReleaseHist(reqID int64) {
	l.mu.Lock()
	if _, ok := l.histLines[reqID]; !ok {
		l.mu.Unlock()
		return
	}
	delete(l.histLines, reqID)
	l.mu.Unlock()
	l.notify()
}

// Release removes a reqID from the ledger (whether cancelled locally or
// dropped by the broker on error). Safe to call for an unknown reqID.
func (l *Ledger) Release(reqID int64) {
	l.mu.Lock()
	cat, ok := l.lines[reqID]
	if !ok {
		l.mu.Unlock()
		return
	}
	delete(l.lines, reqID)
	l.byCat[cat]--
	delete(l.snapAt, reqID)  // no-op unless reqID was a snapshot
	delete(l.probeAt, reqID) // no-op unless reqID was a probe
	l.mu.Unlock()
	l.notify()
}

// Reclassify moves an already-recorded line to a different category,
// keeping the same reqID and slot. No-op if reqID is unknown or already in cat.
func (l *Ledger) Reclassify(reqID int64, cat Category) {
	l.mu.Lock()
	old, ok := l.lines[reqID]
	if !ok || old == cat {
		l.mu.Unlock()
		return
	}
	l.byCat[old]--
	l.lines[reqID] = cat
	l.byCat[cat]++
	if old == CategoryProbe {
		delete(l.probeAt, reqID) // graduated to a tracked background line — no longer probe-aged
	}
	l.mu.Unlock()
	l.notify()
}

// ReapSnapshots releases any CategorySnapshot line older than maxAge and
// returns the reaped reqIDs so the caller can cancel each — a defensive
// backstop against a snapshot whose terminal callback never arrived.
func (l *Ledger) ReapSnapshots(maxAge time.Duration) []int64 {
	now := time.Now()
	l.mu.Lock()
	var reaped []int64
	for reqID, at := range l.snapAt {
		if now.Sub(at) < maxAge {
			continue
		}
		if cat, ok := l.lines[reqID]; ok {
			delete(l.lines, reqID)
			l.byCat[cat]--
		}
		delete(l.snapAt, reqID)
		reaped = append(reaped, reqID)
	}
	l.mu.Unlock()
	if len(reaped) > 0 {
		log.Printf("mdlines: reaped %d orphaned snapshot line(s) older than %s", len(reaped), maxAge)
		l.notify()
	}
	return reaped
}

// ReapProbes releases any CategoryProbe line older than maxAge and returns
// the reaped reqIDs so the caller can cancel each — a defensive backstop
// against a probe whose owning resolution goroutine got stuck and never
// reached its own release path. Unlike an orphaned snapshot (an expected,
// occasional broker-side miss), a reaped probe indicates a bug elsewhere;
// callers should log it loudly.
func (l *Ledger) ReapProbes(maxAge time.Duration) []int64 {
	now := time.Now()
	l.mu.Lock()
	var reaped []int64
	for reqID, at := range l.probeAt {
		if now.Sub(at) < maxAge {
			continue
		}
		// Reclassify (a resolved winner graduating to a background line)
		// always clears probeAt in the same critical section it changes
		// category in, so this mismatch should never happen — checked anyway
		// so a stale probeAt entry can never reap an unrelated live line.
		cat, ok := l.lines[reqID]
		delete(l.probeAt, reqID)
		if !ok || cat != CategoryProbe {
			continue
		}
		delete(l.lines, reqID)
		l.byCat[cat]--
		reaped = append(reaped, reqID)
	}
	l.mu.Unlock()
	if len(reaped) > 0 {
		log.Printf("mdlines: WARNING reaped %d stuck probe line(s) older than %s — a resolution never released its probe", len(reaped), maxAge)
		l.notify()
	}
	return reaped
}

// AllReqIDs returns every active market-data line reqID (all categories)
// for a teardown cancel-sweep.
func (l *Ledger) AllReqIDs() []int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	ids := make([]int64, 0, len(l.lines))
	for reqID := range l.lines {
		ids = append(ids, reqID)
	}
	return ids
}

// AllHistReqIDs returns every active keep-up-to-date historical-bar reqID,
// the companion of AllReqIDs for the separate historical-stream pool.
func (l *Ledger) AllHistReqIDs() []int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	ids := make([]int64, 0, len(l.histLines))
	for reqID := range l.histLines {
		ids = append(ids, reqID)
	}
	return ids
}

// Status returns the current (used, max) line counts.
func (l *Ledger) Status() (int, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.lines), l.max
}

// StatusAll returns both pools plus a stock/option breakdown of the line
// pool: (used, max, histUsed, histMax, stockUsed, optionUsed). stockUsed is
// CategoryStock; optionUsed is every category that exists to serve an
// option-style contract (Position + Probe). CategorySnapshot is excluded from
// both (it's transient and small) and folds only into the overall used total.
func (l *Ledger) StatusAll() (used, max, histUsed, histMax, stockUsed, optionUsed int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	stockUsed = l.byCat[CategoryStock]
	optionUsed = l.byCat[CategoryPosition] + l.byCat[CategoryProbe]
	return len(l.lines), l.max, len(l.histLines), l.histMax, stockUsed, optionUsed
}

// CategoryCounts returns the raw per-category line counts behind StatusAll's
// collapsed stockUsed/optionUsed pair, for diagnostics UIs that want the
// priority tiers visible separately.
func (l *Ledger) CategoryCounts() (stock, position, snapshot, probe int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.byCat[CategoryStock], l.byCat[CategoryPosition], l.byCat[CategorySnapshot], l.byCat[CategoryProbe]
}

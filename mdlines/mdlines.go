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

// Category classifies a persistent market-data line by priority. The zero
// value and the five categories below are the library's default priority
// scheme — guaranteed lines are never refused, discretionary lines are
// granted only while usage stays below a per-category reserve threshold.
type Category int

const (
	// CategoryStock — streaming market data for an underlying. Guaranteed.
	CategoryStock Category = iota
	// CategoryPosition — market data pinned to an open position's contract.
	// Guaranteed and highest priority: it is what keeps a live position's
	// stop-loss/trailing-stop evaluation alive once a discretionary/display
	// selection (e.g. an ATM strike) rolls away from the entry contract.
	CategoryPosition
	// CategoryDiscretionaryNew — a background line for something that has NO
	// active, quoted line yet this session. Discretionary, but the
	// higher-priority half of it: getting a watched symbol its first real
	// quote is worth more of the account's headroom than refreshing one that
	// already has data.
	CategoryDiscretionaryNew
	// CategoryDiscretionaryChurn — a background line REPLACING an already-
	// active, already-quoted line purely to refresh a caller-side estimate
	// (e.g. nudging an ATM strike as the underlying moves). Lowest priority:
	// it stops being granted first, at a tighter reserve than
	// CategoryDiscretionaryNew, since an already-working line losing its
	// freshest guess costs nothing real, while a New request losing the race
	// means the underlying never shows data at all.
	CategoryDiscretionaryChurn
	// CategorySnapshot — transient snapshot requests. They consume a line
	// only until the broker delivers the snapshot and auto-cancels it, so
	// they are counted while live but released quickly.
	CategorySnapshot
	// CategoryProbe — a high-priority, short-lived probe opened while
	// actively trying to resolve a real, fresh quote for an imminent action
	// (e.g. a buy). Highest-priority discretionary tier: grantProbe grants
	// into buffer headroom and, when the pool is otherwise full, preempts a
	// background line to make room, since these must never be starved by
	// background churn.
	CategoryProbe

	numCategories = 6
)

// Buffer is the default headroom cushion kept below the hard line cap so
// guaranteed and snapshot grants never actually reach the broker's
// simultaneous-line limit (which typically rejects the subscription
// outright). Probe grants use non-buffer space first and preempt background
// lines before ever touching it.
const Buffer = 10

// ReserveNew is the default number of lines below the cap at which
// CategoryDiscretionaryNew (first-quote) grants stop.
const ReserveNew = 15

// ReserveChurn is the default number of lines below the cap at which
// CategoryDiscretionaryChurn (background refresh) grants stop — tighter
// than ReserveNew, so an account under pressure spends its remaining
// discretionary headroom on things that have never gotten a quote at all.
const ReserveChurn = 25

// SnapshotMaxAge is how old a transient snapshot line may get before the
// reaper frees it.
const SnapshotMaxAge = 60 * time.Second

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

// grantDiscretionary records a discretionary line in category cat, only if
// usage stays at least reserve lines below the cap. Returns false when the
// caller must skip the subscription. label names the tier in the skip log.
func (l *Ledger) grantDiscretionary(reqID int64, cat Category, reserve int, label string) bool {
	l.mu.Lock()
	if _, exists := l.lines[reqID]; exists {
		l.mu.Unlock()
		return true
	}
	if len(l.lines) >= l.max-reserve {
		used, max := len(l.lines), l.max
		l.mu.Unlock()
		log.Printf("mdlines: skipping %s subscription — %d/%d lines in use (reserving %d)", label, used, max, reserve)
		return false
	}
	l.lines[reqID] = cat
	l.byCat[cat]++
	l.mu.Unlock()
	l.notify()
	return true
}

// GrantDiscretionaryNew records a first-quote background line — something
// with no active, quoted line yet this session. Highest-priority
// discretionary tier: grants until usage reaches ReserveNew below the cap.
func (l *Ledger) GrantDiscretionaryNew(reqID int64) bool {
	return l.grantDiscretionary(reqID, CategoryDiscretionaryNew, ReserveNew, "background first-quote")
}

// GrantDiscretionaryChurn records a background REFRESH of an already-active,
// already-quoted line. Lowest-priority discretionary tier: stops being
// granted at the tighter ReserveChurn threshold, well before
// GrantDiscretionaryNew does.
func (l *Ledger) GrantDiscretionaryChurn(reqID int64) bool {
	return l.grantDiscretionary(reqID, CategoryDiscretionaryChurn, ReserveChurn, "background refresh")
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
//  2. otherwise EVICTS one background line (churn first, then new) and grants
//     in its place, returning the evicted reqID so the caller cancels it;
//  3. otherwise, with no background line to evict, dips into the buffer if
//     still strictly under the hard cap;
//  4. only refuses (ok=false) at the hard cap with nothing to preempt.
//
// evicted is 0 when no line was displaced. Released via Release() like any line.
func (l *Ledger) GrantProbe(reqID int64) (evicted int64, ok bool) {
	l.mu.Lock()
	if _, exists := l.lines[reqID]; exists {
		l.mu.Unlock()
		return 0, true
	}
	place := func() {
		l.lines[reqID] = CategoryProbe
		l.byCat[CategoryProbe]++
	}

	if len(l.lines) < l.max-Buffer {
		place()
		l.mu.Unlock()
		l.notify()
		return 0, true
	}
	if victim, cat, found := l.pickBackgroundVictimLocked(); found {
		delete(l.lines, victim)
		l.byCat[cat]--
		place()
		l.mu.Unlock()
		l.notify()
		return victim, true
	}
	if len(l.lines) < l.max {
		place()
		used, max := len(l.lines), l.max
		l.mu.Unlock()
		log.Printf("mdlines: probe granted into the %d-line buffer — %d/%d in use, no background line to preempt", Buffer, used, max)
		l.notify()
		return 0, true
	}
	used, max := len(l.lines), l.max
	l.mu.Unlock()
	log.Printf("mdlines: WARNING probe refused — %d/%d lines in use, at the hard cap with no background to preempt", used, max)
	return 0, false
}

// pickBackgroundVictimLocked returns a background line to preempt for a
// probe — churn (a refresh, costs nothing to drop) before new (a first-quote
// attempt, more valuable). Caller holds l.mu.
func (l *Ledger) pickBackgroundVictimLocked() (reqID int64, cat Category, found bool) {
	for _, want := range [...]Category{CategoryDiscretionaryChurn, CategoryDiscretionaryNew} {
		for id, c := range l.lines {
			if c == want {
				return id, c, true
			}
		}
	}
	return 0, 0, false
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
	delete(l.snapAt, reqID) // no-op unless reqID was a snapshot
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
// option-style contract (Position + DiscretionaryNew + DiscretionaryChurn +
// Probe). CategorySnapshot is excluded from both (it's transient and small)
// and folds only into the overall used total.
func (l *Ledger) StatusAll() (used, max, histUsed, histMax, stockUsed, optionUsed int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	stockUsed = l.byCat[CategoryStock]
	optionUsed = l.byCat[CategoryPosition] + l.byCat[CategoryDiscretionaryNew] + l.byCat[CategoryDiscretionaryChurn] + l.byCat[CategoryProbe]
	return len(l.lines), l.max, len(l.histLines), l.histMax, stockUsed, optionUsed
}

// CategoryCounts returns the raw per-category line counts behind StatusAll's
// collapsed stockUsed/optionUsed pair, for diagnostics UIs that want the
// priority tiers visible separately.
func (l *Ledger) CategoryCounts() (stock, position, discretionaryNew, discretionaryChurn, snapshot, probe int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.byCat[CategoryStock], l.byCat[CategoryPosition], l.byCat[CategoryDiscretionaryNew], l.byCat[CategoryDiscretionaryChurn], l.byCat[CategorySnapshot], l.byCat[CategoryProbe]
}

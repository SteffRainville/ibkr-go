package ibkr

import (
	"testing"
	"time"

	"github.com/SteffRainville/ibkr-go/quotes"
)

// newRotationTestSession builds a Session (via NewSession, so every logger
// field is a real io.Discard-backed *log.Logger rather than a nil one that
// would panic on the first Printf) with the given per-subscriber symbol
// lists — enough to exercise buildSelectors / selectorResolvingLocked without
// an IB client.
func newRotationTestSession(subSymbols [][]SymbolSpec) *Session {
	s := NewSession(Options{}, nil, nil)
	s.subSymbols = subSymbols
	return s
}

// TestBuildSelectors_DedupAndStableIDs verifies selectors are deduped by
// (symbol, right, delay, δ), non-option rows are ignored, both subscribers
// that share a configuration are merged into one selector with both bus
// indices, and each selector gets a distinct stable id.
func TestBuildSelectors_DedupAndStableIDs(t *testing.T) {
	subSymbols := [][]SymbolSpec{
		{ // bus 0
			{Symbol: "TQQQ", Tag: "long"}, // ignored (not call/put)
			{Symbol: "QQQ", Tag: "call", OptionDelay: 0, TargetDelta: 0.50},
			{Symbol: "QQQ", Tag: "put", OptionDelay: 0, TargetDelta: 0.50},
			{Symbol: "SPY", Tag: "call", OptionDelay: 1, TargetDelta: 0.65},
		},
		{ // bus 1 — QQQ with identical params → same selectors as bus 0's
			{Symbol: "QQQ", Tag: "call", OptionDelay: 0, TargetDelta: 0.50},
			{Symbol: "QQQ", Tag: "put", OptionDelay: 0, TargetDelta: 0.50},
		},
	}
	s := newRotationTestSession(subSymbols)
	s.buildSelectors()

	if got := len(s.optChain.rotation); got != 3 {
		t.Fatalf("rotation = %d, want 3 (QQQ call, QQQ put, SPY call)", got)
	}

	byKey := map[string]selector{}
	seenID := map[int]bool{}
	for _, sel := range s.optChain.rotation {
		byKey[sel.symbol+"|"+sel.right] = sel
		if seenID[sel.id] {
			t.Fatalf("duplicate selector id %d", sel.id)
		}
		seenID[sel.id] = true
	}

	qqqCall, ok := byKey["QQQ|call"]
	if !ok {
		t.Fatal("QQQ call selector missing")
	}
	if len(qqqCall.busIdxs) != 2 {
		t.Fatalf("QQQ call busIdxs = %v, want both subscribers", qqqCall.busIdxs)
	}
	spy, ok := byKey["SPY|call"]
	if !ok {
		t.Fatal("SPY call selector missing")
	}
	if spy.optionDelay != 1 || spy.targetDelta != 0.65 {
		t.Fatalf("SPY params wrong: delay=%d δ=%.2f", spy.optionDelay, spy.targetDelta)
	}
	if len(spy.busIdxs) != 1 {
		t.Fatalf("SPY busIdxs = %v, want only bus 0", spy.busIdxs)
	}
	if _, exists := byKey["SPY|put"]; exists {
		t.Fatal("a SPY put selector was invented from a watchlist that has no SPY put row")
	}
}

// TestBuildSelectors_AbsentRightMakesNoSelector is the trigger for the
// 2026-08-13 QQQ blackout. Commenting out VWmacdFilteredRobot's QQQ put row
// made the old builder default that side to δ0.50 — which both subscribed an
// ATM put leg nobody was watching AND, because the default was part of the
// group key, moved the still-watched CALL into a group of its own, away from
// the sibling it had been sharing with.
//
// A call-only watchlist must produce exactly one selector, for the call.
func TestBuildSelectors_AbsentRightMakesNoSelector(t *testing.T) {
	s := newRotationTestSession([][]SymbolSpec{{
		{Symbol: "QQQ", Tag: "call", OptionDelay: 2, TargetDelta: 0.55},
	}})
	s.buildSelectors()

	if got := len(s.optChain.rotation); got != 1 {
		t.Fatalf("rotation = %d, want exactly 1 (the call); a put selector must not be invented", got)
	}
	sel := s.optChain.rotation[0]
	if sel.right != "call" || sel.targetDelta != 0.55 {
		t.Fatalf("selector = %s δ%.2f, want call δ0.55", sel.right, sel.targetDelta)
	}
}

// TestBuildSelectors_CommentingOutAPutLeavesTheCallAlone is the same trigger
// stated as the operation that caused it: editing one right's row must not
// change the other right's identity, since a stable id is what keeps a
// selector's leg, rotation score and share caches across a resync.
func TestBuildSelectors_CommentingOutAPutLeavesTheCallAlone(t *testing.T) {
	both := [][]SymbolSpec{{
		{Symbol: "QQQ", Tag: "call", OptionDelay: 2, TargetDelta: 0.55},
		{Symbol: "QQQ", Tag: "put", OptionDelay: 2, TargetDelta: 0.55},
	}}
	s := newRotationTestSession(both)
	s.buildSelectors()

	var callID int
	for _, sel := range s.optChain.rotation {
		if sel.right == "call" {
			callID = sel.id
		}
	}

	// The put row is commented out and the symbols re-read.
	s.subSymbols = [][]SymbolSpec{{
		{Symbol: "QQQ", Tag: "call", OptionDelay: 2, TargetDelta: 0.55},
	}}
	s.buildSelectors()

	if len(s.optChain.rotation) != 1 {
		t.Fatalf("rotation = %d, want 1", len(s.optChain.rotation))
	}
	if got := s.optChain.rotation[0].id; got != callID {
		t.Fatalf("call selector id changed %d → %d when the PUT row was removed — the call must be untouched by an edit to a different instrument", callID, got)
	}
}

// TestBuildSelectors_Deterministic verifies the rotation order is stable across
// rebuilds (map iteration randomness must not leak into the rotation).
func TestBuildSelectors_Deterministic(t *testing.T) {
	subSymbols := [][]SymbolSpec{{
		{Symbol: "AAA", Tag: "call"},
		{Symbol: "BBB", Tag: "put"},
		{Symbol: "CCC", Tag: "call"},
		{Symbol: "DDD", Tag: "put"},
	}}
	s := newRotationTestSession(subSymbols)
	s.buildSelectors()
	first := make([]string, len(s.optChain.rotation))
	for i, sel := range s.optChain.rotation {
		first[i] = sel.symbol + "|" + sel.right
	}
	for iter := 0; iter < 20; iter++ {
		s.buildSelectors()
		for i, sel := range s.optChain.rotation {
			if got := sel.symbol + "|" + sel.right; got != first[i] {
				t.Fatalf("rotation order changed on rebuild %d: %v vs %v", iter, got, first[i])
			}
		}
	}
}

// TestSelectorResolvingLocked verifies the in-flight guard sees a pending
// conId, chain-params, or delta resolution for a selector and is false once
// cleared.
func TestSelectorResolvingLocked(t *testing.T) {
	s := newRotationTestSession(nil)

	if s.selectorResolvingLocked(7) {
		t.Fatal("empty tracker should report not resolving")
	}

	s.optChain.conIDReqs[100] = &optConIDReq{chain: chainKey{"QQQ", 0}, waiters: []int{7}}
	if !s.selectorResolvingLocked(7) {
		t.Fatal("pending conId lookup not detected")
	}
	delete(s.optChain.conIDReqs, 100)

	s.optChain.chainReqs[200] = &optChainReq{chain: chainKey{"QQQ", 0}, waiters: []int{7}}
	if !s.selectorResolvingLocked(7) {
		t.Fatal("pending chain-params request not detected")
	}
	delete(s.optChain.chainReqs, 200)

	s.optChain.deltaRes[7] = &deltaResolution{selectorID: 7}
	if !s.selectorResolvingLocked(7) {
		t.Fatal("pending delta resolution not detected")
	}
	if s.selectorResolvingLocked(8) {
		t.Fatal("selector 8 falsely reported resolving")
	}
}

// TestPickRotationSelectorLocked_TiedRotateFairly pins down the 2026-07-28
// stuck-first-quote bug: the score is the zero time.Time for anything never
// resolved, so a large batch of them — the normal state right after startup —
// all tie. The old picker always started scanning at index 0 and never replaced
// a tie (score.Before is strict), so entry 0 won every single tick forever and
// nothing else ever got its first resolution attempt. The fix must cycle
// through tied entries instead of freezing on the first one.
func TestPickRotationSelectorLocked_TiedRotateFairly(t *testing.T) {
	s := newRotationTestSession(nil)
	s.optChain.rotation = []selector{
		{id: 0, symbol: "AAA", right: "call"},
		{id: 1, symbol: "BBB", right: "call"},
		{id: 2, symbol: "CCC", right: "call"},
	}

	seen := map[string]int{}
	for i := 0; i < 6; i++ {
		s.optChain.mu.Lock()
		sel, ok := s.pickChainRefreshSelectorLocked()
		s.optChain.mu.Unlock()
		if !ok {
			t.Fatalf("pick %d: expected a selector, got none", i)
		}
		seen[sel.symbol]++
	}

	for _, sym := range []string{"AAA", "BBB", "CCC"} {
		if seen[sym] != 2 {
			t.Errorf("symbol %s picked %d times over 6 ticks, want 2 (fair rotation through 3 tied entries)", sym, seen[sym])
		}
	}
}

// TestPickRotationSelectorLocked_SkipsResolving verifies a selector with an
// in-flight conId/chain/delta resolution is never returned, even when it would
// otherwise tie for staleness.
func TestPickRotationSelectorLocked_SkipsResolving(t *testing.T) {
	s := newRotationTestSession(nil)
	s.optChain.rotation = []selector{
		{id: 0, symbol: "AAA", right: "call"},
		{id: 1, symbol: "BBB", right: "call"},
	}
	s.optChain.conIDReqs[500] = &optConIDReq{chain: chainKey{"AAA", 0}, waiters: []int{0}}

	for i := 0; i < 4; i++ {
		s.optChain.mu.Lock()
		sel, ok := s.pickChainRefreshSelectorLocked()
		s.optChain.mu.Unlock()
		if !ok {
			t.Fatalf("pick %d: expected BBB, got none", i)
		}
		if sel.symbol != "BBB" {
			t.Fatalf("pick %d: got %s, want BBB — AAA is still resolving", i, sel.symbol)
		}
	}
}

// TestPickRotationSelectorLocked_EmptyWhenAllResolving verifies the picker
// returns ok=false (rather than forcing a still-resolving entry through, as the
// old !found fallback did) when nothing is eligible.
func TestPickRotationSelectorLocked_EmptyWhenAllResolving(t *testing.T) {
	s := newRotationTestSession(nil)
	s.optChain.rotation = []selector{{id: 0, symbol: "AAA", right: "call"}}
	s.optChain.conIDReqs[500] = &optConIDReq{chain: chainKey{"AAA", 0}, waiters: []int{0}}

	s.optChain.mu.Lock()
	_, ok := s.pickChainRefreshSelectorLocked()
	s.optChain.mu.Unlock()
	if ok {
		t.Fatal("expected no selector when the only one is still resolving")
	}
}

// TestPickRotationSelectorLocked_UnquotedLegDoesNotStarveForever pins down the
// 2026-07-31 stuck-at-31 "Background ATM — first quote" bug, a sequel to the
// 2026-07-28 tied bug above. This one isn't a tie: an entry with a leg that
// never receives a book quote (e.g. IB never sends ticks for a thin/illiquid
// strike) scored the zero time.Time forever, which beats every real, recent
// quote timestamp unconditionally — not just on ties. That let a single
// chronically-unquoted leg monopolize every rotation tick permanently (in
// production: GLD/HOOD/INTC alone took 96/50/39 of the picks in a 14-minute
// window while 12 of 21 configured groups got none). The fix records when an
// entry was last actually attempted (lastAttempt) and uses that as the score,
// so it ages forward after its shot even if it never gets a quote.
func TestPickRotationSelectorLocked_UnquotedLegDoesNotStarveForever(t *testing.T) {
	s := newRotationTestSession(nil)
	s.book = quotes.NewBook()
	s.optChain.rotation = []selector{
		{id: 0, symbol: "STUCK", right: "call"},
		{id: 1, symbol: "GOOD", right: "call"},
	}

	seedLeg(s, lk("GOOD", "call", 100, ""), legOpts{reqID: 10})
	s.book.SetOptionBid(quotes.ContractKey{Symbol: "GOOD", Right: "call", Strike: 100}, 1.0)
	goodQuotedAt := time.Now()

	// STUCK has a leg (already attempted once, hence it exists) but has never
	// received a book tick — the illiquid-strike case.
	seedLeg(s, lk("STUCK", "call", 50, ""), legOpts{reqID: 20})

	time.Sleep(2 * time.Millisecond) // ensure strictly-ordered timestamps below

	// First pick: STUCK has never been recorded in lastAttempt, so it still
	// correctly scores as maximally stale and goes first.
	s.optChain.mu.Lock()
	sel, ok := s.pickChainRefreshSelectorLocked()
	s.optChain.mu.Unlock()
	if !ok || sel.symbol != "STUCK" {
		t.Fatalf("first pick = %v (ok=%v), want STUCK", sel.symbol, ok)
	}

	s.optChain.mu.Lock()
	s.optChain.lastAttempt[0] = time.Now()
	s.optChain.mu.Unlock()

	if !s.optChain.lastAttempt[0].After(goodQuotedAt) {
		t.Fatal("test setup: STUCK's attempt must be after GOOD's quote for this to prove anything")
	}

	// Second pick: STUCK already had its attempt, so GOOD — the one actually
	// due — must win. Before the fix STUCK would still score zero and win again,
	// forever.
	s.optChain.mu.Lock()
	sel, ok = s.pickChainRefreshSelectorLocked()
	s.optChain.mu.Unlock()
	if !ok || sel.symbol != "GOOD" {
		t.Fatalf("second pick = %v (ok=%v), want GOOD — STUCK must not win indefinitely after its own attempt", sel.symbol, ok)
	}
}

// TestPickRotationSelectorLocked_OverdueLiquidWins pins down the 2026-08-03
// scoring inversion.
//
// The picker used to score by how fresh the option QUOTES were, which made
// "this has good data" count as "this is up to date". Those are different
// claims, and conflating them starved exactly the wrong entries: a
// continuously-quoting underlying scored ~now on every tick and therefore lost
// to anything serviced even fractionally earlier, forever. In production that
// gave SPY, QQQ and IWM — the three most liquid underlyings, whose ATM strike
// moves fastest and matters most — zero rotation picks in two hours, while NVDA
// (the one least able to obtain a quote at all, 532 delta misses) consumed 794
// log lines re-estimating futilely.
//
// Timestamps here are explicit rather than wall-clock. An earlier version drove
// real picks in a loop and passed against the buggy scorer, because the whole
// loop completed inside one clock granule and every comparison collapsed into a
// tie the round-robin cursor then resolved fairly. The bug is a ranking between
// two specific instants, so the test has to state those instants.
func TestPickRotationSelectorLocked_OverdueLiquidWins(t *testing.T) {
	s := newRotationTestSession(nil)
	s.book = quotes.NewBook()
	now := time.Now()

	s.optChain.rotation = []selector{
		{id: 0, symbol: "LIQUID", right: "call"},
		{id: 1, symbol: "THIN", right: "call"},
	}
	seedLeg(s, lk("LIQUID", "call", 100, ""), legOpts{reqID: 10})
	seedLeg(s, lk("THIN", "call", 50, ""), legOpts{reqID: 20})

	// LIQUID is badly overdue for a strike re-estimate; THIN was serviced a
	// moment ago. On need alone LIQUID must win.
	s.optChain.lastAttempt[0] = now.Add(-10 * time.Second)
	s.optChain.lastAttempt[1] = now.Add(-1 * time.Second)

	// ...but LIQUID is quoting continuously, which is what used to disqualify
	// it: max(lastAttempt, bookTime) made its score ~now, later than THIN's
	// one-second-old service time.
	s.book.SetOptionBid(quotes.ContractKey{Symbol: "LIQUID", Right: "call", Strike: 100}, 1.25)

	s.optChain.mu.Lock()
	sel, ok := s.pickChainRefreshSelectorLocked()
	s.optChain.mu.Unlock()

	if !ok {
		t.Fatal("expected a selector, got none")
	}
	if sel.symbol != "LIQUID" {
		t.Fatalf("picked %s, want LIQUID — something 10s overdue must not lose to one serviced 1s ago just because its quotes are fresh", sel.symbol)
	}
}

// TestPickRotationSelectorLocked_LeglessDoesNotMonopolize covers the opposite
// starvation the old quote-based scorer had: it skipped every right with no
// active leg, so an entry with NO legs never entered the comparison at all and
// returned the zero time.Time — beating every real timestamp, every tick,
// forever. That state is reachable in production, since handleOptionMktError
// drops a leg on error 200 and can leave nothing behind.
func TestPickRotationSelectorLocked_LeglessDoesNotMonopolize(t *testing.T) {
	s := newRotationTestSession(nil)
	now := time.Now()

	s.optChain.rotation = []selector{
		{id: 0, symbol: "LEGLESS", right: "call"},
		{id: 1, symbol: "NORMAL", right: "call"},
	}
	seedLeg(s, lk("NORMAL", "call", 50, ""), legOpts{reqID: 20})

	// LEGLESS was serviced most recently, so NORMAL is the one due.
	s.optChain.lastAttempt[0] = now.Add(-1 * time.Second)
	s.optChain.lastAttempt[1] = now.Add(-10 * time.Second)

	s.optChain.mu.Lock()
	sel, ok := s.pickChainRefreshSelectorLocked()
	s.optChain.mu.Unlock()

	if !ok {
		t.Fatal("expected a selector, got none")
	}
	if sel.symbol != "NORMAL" {
		t.Fatalf("picked %s, want NORMAL — an entry with zero legs must age like any other, not score as infinitely stale", sel.symbol)
	}
}

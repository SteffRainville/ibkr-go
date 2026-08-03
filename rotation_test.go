package ibkr

import (
	"testing"
	"time"

	"github.com/SteffRainville/ibkr-go/quotes"
)

// newRotationTestSession builds a Session (via NewSession, so every logger
// field is a real io.Discard-backed *log.Logger rather than a nil one that
// would panic on the first Printf) with the given per-subscriber symbol
// lists — enough to exercise buildOptionGroups / groupResolvingLocked
// without an IB client.
func newRotationTestSession(subSymbols [][]SymbolSpec) *Session {
	s := NewSession(Options{}, nil, nil)
	s.subSymbols = subSymbols
	return s
}

// TestBuildOptionGroups_DedupAndStableIDs verifies option groups are deduped by
// (symbol, delay, deltas), non-option rows are ignored, both subscribers that
// share a symbol are merged into one group with both bus indices, and each
// group gets a distinct stable groupID.
func TestBuildOptionGroups_DedupAndStableIDs(t *testing.T) {
	subSymbols := [][]SymbolSpec{
		{ // bus 0
			{Symbol: "TQQQ", Tag: "long"}, // ignored (not call/put)
			{Symbol: "QQQ", Tag: "call", OptionDelay: 0, TargetDelta: 0.50},
			{Symbol: "QQQ", Tag: "put", OptionDelay: 0, TargetDelta: 0.50},
			{Symbol: "SPY", Tag: "call", OptionDelay: 1, TargetDelta: 0.65},
		},
		{ // bus 1 — QQQ with identical params → same group as bus 0's QQQ
			{Symbol: "QQQ", Tag: "call", OptionDelay: 0, TargetDelta: 0.50},
			{Symbol: "QQQ", Tag: "put", OptionDelay: 0, TargetDelta: 0.50},
		},
	}
	s := newRotationTestSession(subSymbols)
	s.buildOptionGroups()

	if got := len(s.optChain.rotation); got != 2 {
		t.Fatalf("rotation groups = %d, want 2 (QQQ, SPY)", got)
	}

	bySym := map[string]optResGroup{}
	seenID := map[int]bool{}
	for _, g := range s.optChain.rotation {
		bySym[g.symbol] = g
		if seenID[g.groupID] {
			t.Fatalf("duplicate groupID %d", g.groupID)
		}
		seenID[g.groupID] = true
	}

	qqq, ok := bySym["QQQ"]
	if !ok {
		t.Fatal("QQQ group missing")
	}
	if len(qqq.busIdxs) != 2 {
		t.Fatalf("QQQ busIdxs = %v, want both subscribers", qqq.busIdxs)
	}
	spy, ok := bySym["SPY"]
	if !ok {
		t.Fatal("SPY group missing")
	}
	if spy.optionDelay != 1 || spy.targetDeltaCall != 0.65 {
		t.Fatalf("SPY params wrong: delay=%d callδ=%.2f", spy.optionDelay, spy.targetDeltaCall)
	}
	if len(spy.busIdxs) != 1 {
		t.Fatalf("SPY busIdxs = %v, want only bus 0", spy.busIdxs)
	}
}

// TestBuildOptionGroups_Deterministic verifies the rotation order is stable across
// rebuilds (map iteration randomness must not leak into the rotation).
func TestBuildOptionGroups_Deterministic(t *testing.T) {
	subSymbols := [][]SymbolSpec{{
		{Symbol: "AAA", Tag: "call"},
		{Symbol: "BBB", Tag: "put"},
		{Symbol: "CCC", Tag: "call"},
		{Symbol: "DDD", Tag: "put"},
	}}
	s := newRotationTestSession(subSymbols)
	s.buildOptionGroups()
	first := make([]string, len(s.optChain.rotation))
	for i, g := range s.optChain.rotation {
		first[i] = g.symbol
	}
	// Rebuild several times; order must not change.
	for iter := 0; iter < 20; iter++ {
		s.buildOptionGroups()
		for i, g := range s.optChain.rotation {
			if g.symbol != first[i] {
				t.Fatalf("rotation order changed on rebuild %d: %v vs %v", iter, g.symbol, first[i])
			}
		}
	}
}

// TestGroupResolvingLocked verifies the in-flight guard sees a pending conId,
// chain-params, or delta resolution for a group and is false once cleared.
func TestGroupResolvingLocked(t *testing.T) {
	s := newRotationTestSession(nil)

	if s.groupResolvingLocked(7) {
		t.Fatal("empty tracker should report not resolving")
	}

	s.optChain.conIDReqs[100] = &optConIDReq{groupID: 7}
	if !s.groupResolvingLocked(7) {
		t.Fatal("pending conId lookup not detected")
	}
	delete(s.optChain.conIDReqs, 100)

	s.optChain.chainReqs[200] = &optChainReq{groupID: 7}
	if !s.groupResolvingLocked(7) {
		t.Fatal("pending chain-params request not detected")
	}
	delete(s.optChain.chainReqs, 200)

	s.optChain.deltaRes[retryKeyLeg(7, "put")] = &deltaResolution{groupID: 7}
	if !s.groupResolvingLocked(7) {
		t.Fatal("pending delta resolution not detected")
	}
	// A different group must not be considered resolving.
	if s.groupResolvingLocked(8) {
		t.Fatal("group 8 falsely reported resolving")
	}
}

// TestPickRotationGroupLocked_TiedGroupsRotateFairly pins down the
// 2026-07-28 stuck-first-quote bug: groupStalenessLocked returns the zero
// time.Time for any group with no active leg yet, so a large batch of
// never-quoted groups (the normal state right after startup, or any time no
// ticks are flowing, e.g. outside RTH) all tie at the same score. The old
// picker always started scanning at index 0 and never replaced a tie
// (score.Before is strict), so group 0 won every single tick forever and no
// other group ever got its first resolution attempt. The fix must cycle
// through tied groups instead of freezing on the first one.
func TestPickRotationGroupLocked_TiedGroupsRotateFairly(t *testing.T) {
	s := newRotationTestSession(nil)
	s.optChain.rotation = []optResGroup{
		{groupID: 0, symbol: "AAA"},
		{groupID: 1, symbol: "BBB"},
		{groupID: 2, symbol: "CCC"},
	}
	// No active legs and no book entries for any of them — every group's
	// groupStalenessLocked score is the tied zero value.

	seen := map[string]int{}
	for i := 0; i < 6; i++ {
		s.optChain.mu.Lock()
		g, ok := s.pickRotationGroupLocked()
		s.optChain.mu.Unlock()
		if !ok {
			t.Fatalf("pick %d: expected a group, got none", i)
		}
		seen[g.symbol]++
	}

	for _, sym := range []string{"AAA", "BBB", "CCC"} {
		if seen[sym] != 2 {
			t.Errorf("symbol %s picked %d times over 6 ticks, want 2 (fair rotation through 3 tied groups)", sym, seen[sym])
		}
	}
}

// TestPickRotationGroupLocked_SkipsResolvingGroups verifies a group with an
// in-flight conId/chain/delta resolution is never returned, even when it
// would otherwise tie for staleness.
func TestPickRotationGroupLocked_SkipsResolvingGroups(t *testing.T) {
	s := newRotationTestSession(nil)
	s.optChain.rotation = []optResGroup{
		{groupID: 0, symbol: "AAA"},
		{groupID: 1, symbol: "BBB"},
	}
	s.optChain.conIDReqs[500] = &optConIDReq{groupID: 0}

	for i := 0; i < 4; i++ {
		s.optChain.mu.Lock()
		g, ok := s.pickRotationGroupLocked()
		s.optChain.mu.Unlock()
		if !ok {
			t.Fatalf("pick %d: expected BBB, got none", i)
		}
		if g.symbol != "BBB" {
			t.Fatalf("pick %d: got %s, want BBB — AAA is still resolving", i, g.symbol)
		}
	}
}

// TestPickRotationGroupLocked_EmptyWhenAllResolving verifies the picker
// returns ok=false (rather than forcing a still-resolving group through,
// as the old !found fallback did) when nothing is eligible.
func TestPickRotationGroupLocked_EmptyWhenAllResolving(t *testing.T) {
	s := newRotationTestSession(nil)
	s.optChain.rotation = []optResGroup{{groupID: 0, symbol: "AAA"}}
	s.optChain.conIDReqs[500] = &optConIDReq{groupID: 0}

	s.optChain.mu.Lock()
	_, ok := s.pickRotationGroupLocked()
	s.optChain.mu.Unlock()
	if ok {
		t.Fatal("expected no group when the only group is still resolving")
	}
}

// TestPickRotationGroupLocked_UnquotedLegDoesNotStarveForever pins down the
// 2026-07-31 stuck-at-31 "Background ATM — first quote" bug, a sequel to the
// 2026-07-28 tied-groups bug above. This one isn't a tie: a group with an
// active leg that never receives a book quote (e.g. IB never sends ticks for
// a thin/illiquid strike) scored the zero time.Time forever, which beats
// every group with a real, recent quote timestamp unconditionally — not just
// on ties. That let a single chronically-unquoted leg monopolize every
// rotation tick permanently, starving every other group's churn (in
// production: GLD/HOOD/INTC alone took 96/50/39 of the rotation picks in a
// 14-minute window while 12 of 21 configured groups got none). The fix
// records when a group was last actually attempted (lastAttempt) and uses
// that as the fallback score instead of the zero value, so a group ages
// forward after its shot even if it never gets a quote — a genuinely never-
// touched group still wins first (matching the tied-groups test above), but
// a group that already had its turn and failed can't freeze at "more stale
// than the dawn of time" and lock out everyone else.
func TestPickRotationGroupLocked_UnquotedLegDoesNotStarveForever(t *testing.T) {
	s := newRotationTestSession(nil)
	s.book = quotes.NewBook()
	s.optChain.rotation = []optResGroup{
		{groupID: 0, symbol: "STUCK"},
		{groupID: 1, symbol: "GOOD"},
	}

	// GOOD has a real, recent book quote.
	s.optChain.mktReqs[10] = &optMktReq{groupID: 1, symbol: "GOOD", right: "call", strike: 100}
	s.book.SetOptionBid(quotes.ContractKey{Symbol: "GOOD", Right: "call", Strike: 100}, 1.0)
	goodQuotedAt := time.Now()

	// STUCK has an active leg (already attempted once, hence it exists) but
	// has never received a book tick — the illiquid-strike case.
	s.optChain.mktReqs[20] = &optMktReq{groupID: 0, symbol: "STUCK", right: "call", strike: 50}

	time.Sleep(2 * time.Millisecond) // ensure strictly-ordered timestamps below

	// First pick: STUCK has never been recorded in lastAttempt, so it still
	// correctly scores as maximally stale and goes first — a never-quoted
	// leg should get its shot before an already-flowing one is re-churned.
	s.optChain.mu.Lock()
	g, ok := s.pickRotationGroupLocked()
	s.optChain.mu.Unlock()
	if !ok || g.symbol != "STUCK" {
		t.Fatalf("first pick = %v (ok=%v), want STUCK", g.symbol, ok)
	}

	// Simulate resolveOptionGroup committing to that attempt (it still gets
	// no quote — the strike stays illiquid).
	s.optChain.mu.Lock()
	s.optChain.lastAttempt[0] = time.Now()
	s.optChain.mu.Unlock()

	if !s.optChain.lastAttempt[0].After(goodQuotedAt) {
		t.Fatal("test setup: STUCK's attempt must be after GOOD's quote for this to prove anything")
	}

	// Second pick: STUCK already had its attempt and is now "fresher" than
	// GOOD's now-older quote, so GOOD — the one actually due for a refresh —
	// must win. Before the fix, STUCK would still score zero and win again,
	// forever.
	s.optChain.mu.Lock()
	g, ok = s.pickRotationGroupLocked()
	s.optChain.mu.Unlock()
	if !ok || g.symbol != "GOOD" {
		t.Fatalf("second pick = %v (ok=%v), want GOOD — STUCK must not win indefinitely after its own attempt", g.symbol, ok)
	}
}

// TestPickRotationGroupLocked_OverdueLiquidGroupWins pins down the 2026-08-03
// scoring inversion.
//
// The picker used to score a group by how fresh its option QUOTES were, which
// made "this group has good data" count as "this group is up to date". Those
// are different claims, and conflating them starved exactly the wrong groups:
// a continuously-quoting underlying scored ~now on every tick and therefore
// lost to any group serviced even fractionally earlier, forever. In production
// that gave SPY, QQQ and IWM — the three most liquid underlyings, whose ATM
// strike moves fastest and matters most — zero rotation picks in two hours,
// while NVDA (the group least able to obtain a quote at all, 532 delta misses)
// consumed 794 log lines re-estimating futilely.
//
// Timestamps here are explicit rather than wall-clock. An earlier version of
// this test drove real picks in a loop and passed against the buggy scorer,
// because the whole loop completed inside one clock granule and every
// comparison collapsed into a tie that the round-robin cursor then resolved
// fairly. The bug is a ranking between two specific instants, so the test has
// to state those instants.
func TestPickRotationGroupLocked_OverdueLiquidGroupWins(t *testing.T) {
	s := newRotationTestSession(nil)
	s.book = quotes.NewBook()
	now := time.Now()

	s.optChain.rotation = []optResGroup{
		{groupID: 0, symbol: "LIQUID"},
		{groupID: 1, symbol: "THIN"},
	}
	s.optChain.mktReqs[10] = &optMktReq{groupID: 0, symbol: "LIQUID", right: "call", strike: 100}
	s.optChain.mktReqs[20] = &optMktReq{groupID: 1, symbol: "THIN", right: "call", strike: 50}

	// LIQUID is badly overdue for a strike re-estimate; THIN was serviced a
	// moment ago. On need alone LIQUID must win.
	s.optChain.lastAttempt[0] = now.Add(-10 * time.Second)
	s.optChain.lastAttempt[1] = now.Add(-1 * time.Second)

	// ...but LIQUID is quoting continuously, which is what used to disqualify
	// it: max(lastAttempt, bookTime) made its score ~now, later than THIN's
	// one-second-old service time, so THIN won despite being ten times more
	// recently serviced.
	s.book.SetOptionBid(quotes.ContractKey{Symbol: "LIQUID", Right: "call", Strike: 100}, 1.25)

	s.optChain.mu.Lock()
	g, ok := s.pickRotationGroupLocked()
	s.optChain.mu.Unlock()

	if !ok {
		t.Fatal("expected a group, got none")
	}
	if g.symbol != "LIQUID" {
		t.Fatalf("picked %s, want LIQUID — a group 10s overdue must not lose to one serviced 1s ago just because its quotes are fresh", g.symbol)
	}
}

// TestPickRotationGroupLocked_LeglessGroupDoesNotMonopolize covers the
// opposite starvation the old quote-based scorer had: it skipped every right
// with no active leg, so a group with NO legs never entered the comparison at
// all and returned the zero time.Time — beating every real timestamp, every
// tick, forever. That state is reachable in production, since
// handleOptionMktError drops a leg on error 200 and returns without
// resubscribing when no ATM retry record exists.
func TestPickRotationGroupLocked_LeglessGroupDoesNotMonopolize(t *testing.T) {
	s := newRotationTestSession(nil)
	now := time.Now()

	s.optChain.rotation = []optResGroup{
		{groupID: 0, symbol: "LEGLESS"},
		{groupID: 1, symbol: "NORMAL"},
	}
	// LEGLESS has no mktReqs entry at all; NORMAL has one.
	s.optChain.mktReqs[20] = &optMktReq{groupID: 1, symbol: "NORMAL", right: "call", strike: 50}

	// LEGLESS was serviced most recently, so NORMAL is the one due.
	s.optChain.lastAttempt[0] = now.Add(-1 * time.Second)
	s.optChain.lastAttempt[1] = now.Add(-10 * time.Second)

	s.optChain.mu.Lock()
	g, ok := s.pickRotationGroupLocked()
	s.optChain.mu.Unlock()

	if !ok {
		t.Fatal("expected a group, got none")
	}
	if g.symbol != "NORMAL" {
		t.Fatalf("picked %s, want NORMAL — a group with zero legs must age like any other, not score as infinitely stale", g.symbol)
	}
}

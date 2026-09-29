package ibkr

import "testing"

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

	if got := len(s.optChain.selectors); got != 3 {
		t.Fatalf("selectors = %d, want 3 (QQQ call, QQQ put, SPY call)", got)
	}

	byKey := map[string]selector{}
	seenID := map[int]bool{}
	for _, sel := range s.optChain.selectors {
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

	if got := len(s.optChain.selectors); got != 1 {
		t.Fatalf("selectors = %d, want exactly 1 (the call); a put selector must not be invented", got)
	}
	sel := s.optChain.selectors[0]
	if sel.right != "call" || sel.targetDelta != 0.55 {
		t.Fatalf("selector = %s δ%.2f, want call δ0.55", sel.right, sel.targetDelta)
	}
}

// TestBuildSelectors_CommentingOutAPutLeavesTheCallAlone is the same trigger
// stated as the operation that caused it: editing one right's row must not
// change the other right's identity, since a stable id is what keeps a
// selector's in-flight probe across a resync.
func TestBuildSelectors_CommentingOutAPutLeavesTheCallAlone(t *testing.T) {
	both := [][]SymbolSpec{{
		{Symbol: "QQQ", Tag: "call", OptionDelay: 2, TargetDelta: 0.55},
		{Symbol: "QQQ", Tag: "put", OptionDelay: 2, TargetDelta: 0.55},
	}}
	s := newRotationTestSession(both)
	s.buildSelectors()

	var callID int
	for _, sel := range s.optChain.selectors {
		if sel.right == "call" {
			callID = sel.id
		}
	}

	// The put row is commented out and the symbols re-read.
	s.subSymbols = [][]SymbolSpec{{
		{Symbol: "QQQ", Tag: "call", OptionDelay: 2, TargetDelta: 0.55},
	}}
	s.buildSelectors()

	if len(s.optChain.selectors) != 1 {
		t.Fatalf("selectors = %d, want 1", len(s.optChain.selectors))
	}
	if got := s.optChain.selectors[0].id; got != callID {
		t.Fatalf("call selector id changed %d → %d when the PUT row was removed — the call must be untouched by an edit to a different instrument", callID, got)
	}
}

// TestBuildSelectors_Deterministic verifies the selector order is stable across
// rebuilds (map iteration randomness must not leak into the selector list).
func TestBuildSelectors_Deterministic(t *testing.T) {
	subSymbols := [][]SymbolSpec{{
		{Symbol: "AAA", Tag: "call"},
		{Symbol: "BBB", Tag: "put"},
		{Symbol: "CCC", Tag: "call"},
		{Symbol: "DDD", Tag: "put"},
	}}
	s := newRotationTestSession(subSymbols)
	s.buildSelectors()
	first := make([]string, len(s.optChain.selectors))
	for i, sel := range s.optChain.selectors {
		first[i] = sel.symbol + "|" + sel.right
	}
	for iter := 0; iter < 20; iter++ {
		s.buildSelectors()
		for i, sel := range s.optChain.selectors {
			if got := sel.symbol + "|" + sel.right; got != first[i] {
				t.Fatalf("selector order changed on rebuild %d: %v vs %v", iter, got, first[i])
			}
		}
	}
}

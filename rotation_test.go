package ibkr

import "testing"

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

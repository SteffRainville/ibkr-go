package ibkr

import "testing"

// TestBuildSelectors_IDsSurviveRebuild is the invariant ResyncSymbols depends
// on. Every watchlist edit rebuilds the rotation, and almost every edit leaves
// most selectors untouched. If a rebuild renumbered them, each edit would
// invalidate everything keyed by selector id — the resolvedEntry share cache,
// lastAttempt rotation scores, the in-flight-resolution guard, the legs each
// one holds — and silently provoke a fresh conId + chain round trip for
// everything at once, which is exactly the connection-pool burst a delta
// resync exists to avoid.
func TestBuildSelectors_IDsSurviveRebuild(t *testing.T) {
	s := newRotationTestSession([][]SymbolSpec{{
		{Symbol: "QQQ", Tag: "call", TargetDelta: 0.50},
		{Symbol: "SPY", Tag: "put", TargetDelta: 0.60},
	}})

	first := s.buildSelectors()
	if len(first) != 2 {
		t.Fatalf("first build returned %d fresh selectors, want 2", len(first))
	}
	before := map[string]int{}
	for _, sel := range s.optChain.rotation {
		before[sel.symbol] = sel.id
	}

	// Add a third symbol; the two originals are unchanged.
	s.subSymbols = [][]SymbolSpec{{
		{Symbol: "QQQ", Tag: "call", TargetDelta: 0.50},
		{Symbol: "SPY", Tag: "put", TargetDelta: 0.60},
		{Symbol: "IWM", Tag: "call", TargetDelta: 0.55},
	}}
	fresh := s.buildSelectors()

	if len(fresh) != 1 || fresh[0].symbol != "IWM" {
		t.Fatalf("second build fresh selectors = %+v, want only IWM — an unchanged selector must not be re-resolved", fresh)
	}
	for _, sel := range s.optChain.rotation {
		if was, existed := before[sel.symbol]; existed && was != sel.id {
			t.Fatalf("%s selector id changed across rebuild: %d -> %d", sel.symbol, was, sel.id)
		}
	}
}

// TestBuildSelectors_ParameterChangeIsANewSelector verifies the selector key
// really is the configuration tuple: editing target_delta on a row must produce
// a new selector (and so a fresh resolution), not silently reuse the old one's
// strike.
func TestBuildSelectors_ParameterChangeIsANewSelector(t *testing.T) {
	s := newRotationTestSession([][]SymbolSpec{{
		{Symbol: "QQQ", Tag: "call", TargetDelta: 0.50},
	}})
	s.buildSelectors()
	origID := s.optChain.rotation[0].id

	s.subSymbols = [][]SymbolSpec{{
		{Symbol: "QQQ", Tag: "call", TargetDelta: 0.70},
	}}
	fresh := s.buildSelectors()

	if len(fresh) != 1 {
		t.Fatalf("changing target_delta produced %d fresh selectors, want 1", len(fresh))
	}
	if fresh[0].id == origID {
		t.Fatal("a re-parameterized selector reused the old id — it would inherit the previous target's cached strike")
	}
	if len(s.optChain.rotation) != 1 {
		t.Fatalf("rotation holds %d selectors, want 1 — the superseded one must not linger", len(s.optChain.rotation))
	}
}

// TestReleaseDepartedSelectors_FreesTheirLines and
// ..._KeepsAPinnedContract stood here. Both exercised releaseDepartedSelectors,
// which walked selCurrent/selPending after a rebuild and cancelled the lines of
// selectors that no longer existed — necessary when editing a row's target_delta
// would otherwise strand its old leg's subscription for the session.
//
// A selector holds no market-data line now, so dropping it from the rotation IS
// releasing it and there is nothing left to walk. The half that still matters —
// a watchlist edit must never cancel a contract an open position is pricing its
// stops against — is unconditional rather than guarded: only a pin holds a leg,
// and only the exit path releases one. possub_refcount_test.go covers it.

// TestSymbolRegistry_IDsAreNeverReissued covers the reqID allocation rule
// ResyncSymbols relies on. Index-derived IDs are unique only while the
// watchlist is immutable; once a symbol can be removed and another added,
// reusing its slot would point a newcomer at reqIDs TWS may still have
// callbacks in flight against.
func TestSymbolRegistry_IDsAreNeverReissued(t *testing.T) {
	s := NewSession(Options{}, nil, nil)

	aHist, aMkt := s.reserveSymbolIDsLocked("TQQQ")
	bHist, bMkt := s.reserveSymbolIDsLocked("SQQQ")

	if got := s.histSymbol(aHist); got != "TQQQ" {
		t.Fatalf("histSymbol(%d) = %q, want TQQQ", aHist, got)
	}
	if sym, ok := s.streamSymbol(bMkt); !ok || sym != "SQQQ" {
		t.Fatalf("streamSymbol(%d) = %q,%v, want SQQQ,true", bMkt, sym, ok)
	}

	gotHist, gotMkt, ok := s.releaseSymbolIDsLocked("TQQQ")
	if !ok || gotHist != aHist || gotMkt != aMkt {
		t.Fatalf("release returned (%d,%d,%v), want (%d,%d,true)", gotHist, gotMkt, ok, aHist, aMkt)
	}
	if got := s.histSymbol(aHist); got != "" {
		t.Fatalf("released reqID %d still resolves to %q", aHist, got)
	}

	cHist, cMkt := s.reserveSymbolIDsLocked("GLD")
	if cHist == aHist || cHist == bHist || cMkt == aMkt || cMkt == bMkt {
		t.Fatalf("reissued a retired reqID: GLD got (%d,%d), already used (%d,%d) and (%d,%d)",
			cHist, cMkt, aHist, aMkt, bHist, bMkt)
	}

	if _, _, ok := s.releaseSymbolIDsLocked("NVDA"); ok {
		t.Fatal("releasing an unsubscribed symbol reported success")
	}
}

// TestResyncSymbols_NoClientIsNoOp guards the offline / pre-Connect path:
// the reload button must be harmless when there is no IB session behind it.
func TestResyncSymbols_NoClientIsNoOp(t *testing.T) {
	s := NewSession(Options{}, nil, nil)
	if d := s.ResyncSymbols(); d.Changed() {
		t.Fatalf("ResyncSymbols on an unconnected session reported %+v, want no change", d)
	}
}

package ibkr

import "testing"

// TestBuildOptionGroups_IDsSurviveRebuild is the invariant ResyncSymbols
// depends on. Every watchlist edit rebuilds the rotation, and almost every
// edit leaves most groups untouched. If a rebuild renumbered them, each edit
// would invalidate everything keyed by groupID — the resolvedEntry share
// cache, lastAttempt rotation scores, the in-flight-resolution guard — and
// silently provoke a fresh conId + chain round trip for every group at once,
// which is exactly the connection-pool burst a delta resync exists to avoid.
func TestBuildOptionGroups_IDsSurviveRebuild(t *testing.T) {
	s := newRotationTestSession([][]SymbolSpec{{
		{Symbol: "QQQ", Tag: "call", TargetDelta: 0.50},
		{Symbol: "SPY", Tag: "put", TargetDelta: 0.60},
	}})

	first := s.buildOptionGroups()
	if len(first) != 2 {
		t.Fatalf("first build returned %d fresh groups, want 2", len(first))
	}
	before := map[string]int{}
	for _, g := range s.optChain.rotation {
		before[g.symbol] = g.groupID
	}

	// Add a third symbol; the two originals are unchanged.
	s.subSymbols = [][]SymbolSpec{{
		{Symbol: "QQQ", Tag: "call", TargetDelta: 0.50},
		{Symbol: "SPY", Tag: "put", TargetDelta: 0.60},
		{Symbol: "IWM", Tag: "call", TargetDelta: 0.55},
	}}
	fresh := s.buildOptionGroups()

	if len(fresh) != 1 || fresh[0].symbol != "IWM" {
		t.Fatalf("second build fresh groups = %+v, want only IWM — an unchanged group must not be re-resolved", fresh)
	}
	for _, g := range s.optChain.rotation {
		if was, existed := before[g.symbol]; existed && was != g.groupID {
			t.Fatalf("%s groupID changed across rebuild: %d → %d", g.symbol, was, g.groupID)
		}
	}
}

// TestBuildOptionGroups_ParameterChangeIsANewGroup verifies the group key
// really is the configuration tuple: editing target_delta on a row must
// produce a new group (and so a fresh resolution), not silently reuse the
// old group's strike.
func TestBuildOptionGroups_ParameterChangeIsANewGroup(t *testing.T) {
	s := newRotationTestSession([][]SymbolSpec{{
		{Symbol: "QQQ", Tag: "call", TargetDelta: 0.50},
	}})
	s.buildOptionGroups()
	origID := s.optChain.rotation[0].groupID

	s.subSymbols = [][]SymbolSpec{{
		{Symbol: "QQQ", Tag: "call", TargetDelta: 0.70},
	}}
	fresh := s.buildOptionGroups()

	if len(fresh) != 1 {
		t.Fatalf("changing target_delta produced %d fresh groups, want 1", len(fresh))
	}
	if fresh[0].groupID == origID {
		t.Fatal("a re-parameterized group reused the old groupID — it would inherit the previous target's cached strike")
	}
	if len(s.optChain.rotation) != 1 {
		t.Fatalf("rotation holds %d groups, want 1 — the superseded group must not linger", len(s.optChain.rotation))
	}
}

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

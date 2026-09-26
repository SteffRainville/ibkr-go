// Tests that an entry probe's OUTCOME is published in the same critical section
// that ends its ownership.
//
// The 2026-09-22 AMD incident: OrbOptionData owned the AMD call probe and
// resolved strike 622.50; OrbOptionFiltered had joined it and was told
// "option_sibling_failed: another robot's delta probe ... finished without a
// usable quote". Both entries that morning went the same way. The owner
// deleted deltaRes under the lock, unlocked, logged and cancelled its candidate
// lines, and only then re-locked to write resolvedEntry. A joined caller
// polling in that gap saw "no longer owned", found neither resolvedEntry nor
// lastEntryFailure, and fell through to the generic failure — the robot that
// happened to be waiting lost a trade its sibling took.
//
// The fix mirrors reserveEntryProbe: whatever a waiter will read must already
// be in place when the lock that ends ownership is released. These tests call
// resolveDeltaCandidates directly and require the outcome to be published by
// the time it returns, i.e. not by a later step in ResolveEntryStrike.
package ibkr

import "testing"

// A ready candidate with dupTicker set: the release path skips CancelMktData
// for it, which matters because s.client is nil in this harness.
func readyCandidate(sel selector, strike, delta, bid, ask float64) *deltaCandidate {
	return &deltaCandidate{selectorID: sel.id, symbol: sel.symbol, right: sel.right,
		strike: strike, expiry: "20260731", delta: delta, bid: bid, ask: ask, ready: true, dupTicker: true}
}

func TestResolveDeltaCandidates_PublishesSuccessBeforeReleasingOwnership(t *testing.T) {
	sub := newTestSubscriber()
	s := newResolveEntryTestSession(sub)
	sel := s.optChain.rotation[0]

	// A failure left over from an earlier probe of this selector must not
	// survive a success — a waiter reading it would get a stale diagnosis.
	s.optChain.lastEntryFailure = map[int]EntryStrikeResult{sel.id: {Reason: entryFailQuoteTimeout}}

	mine, _, _ := s.reserveEntryProbe(sel)
	if mine == nil {
		t.Fatal("failed to become the owner")
	}
	mine.candidates = []*deltaCandidate{readyCandidate(sel, 735, -0.65, 6.50, 6.60)}

	q, res := s.resolveDeltaCandidates(sel, mine)
	if !res.OK {
		t.Fatalf("expected a resolved quote, got %+v", res)
	}

	s.optChain.mu.Lock()
	leg, published := s.optChain.resolvedEntry[sel.id]
	_, staleFailure := s.optChain.lastEntryFailure[sel.id]
	s.optChain.mu.Unlock()
	if !published {
		t.Fatal("ownership released without publishing resolvedEntry — a joined sibling polling now reads option_sibling_failed")
	}
	if leg.strike != q.Strike || leg.bid != q.Bid || leg.ask != q.Ask {
		t.Errorf("published %+v, want the returned quote %+v", leg, q)
	}
	if staleFailure {
		t.Error("a stale lastEntryFailure survived a successful probe")
	}
	if shared, ok := s.sharedResolvedEntry(sel.id); !ok || shared.Strike != 735 {
		t.Errorf("sibling fast path returned (%+v, %v), want strike 735", shared, ok)
	}
}

func TestResolveDeltaCandidates_PublishesFailureBeforeReleasingOwnership(t *testing.T) {
	sub := newTestSubscriber()
	s := newResolveEntryTestSession(sub)
	sel := s.optChain.rotation[0]

	mine, _, _ := s.reserveEntryProbe(sel)
	if mine == nil {
		t.Fatal("failed to become the owner")
	}

	_, res := s.resolveDeltaCandidates(sel, mine)
	if res.OK {
		t.Fatal("no candidate reported, yet the probe succeeded")
	}

	s.optChain.mu.Lock()
	fail, published := s.optChain.lastEntryFailure[sel.id]
	s.optChain.mu.Unlock()
	if !published {
		t.Fatal("ownership released without publishing the cause — a joined sibling reads the generic option_sibling_failed")
	}
	if fail.Reason != res.Reason {
		t.Errorf("published reason %q, owner got %q — the sibling must get the owner's own diagnosis", fail.Reason, res.Reason)
	}
}

// A winner on delta with no two-sided price is a failure too, and the sibling
// must learn that exact cause.
func TestResolveDeltaCandidates_PublishesDeltaNoPrice(t *testing.T) {
	sub := newTestSubscriber()
	s := newResolveEntryTestSession(sub)
	sel := s.optChain.rotation[0]

	mine, _, _ := s.reserveEntryProbe(sel)
	mine.candidates = []*deltaCandidate{readyCandidate(sel, 735, -0.65, 0, 0)}

	_, res := s.resolveDeltaCandidates(sel, mine)
	if res.Reason != entryFailDeltaNoPrice {
		t.Fatalf("reason = %q, want %q", res.Reason, entryFailDeltaNoPrice)
	}
	s.optChain.mu.Lock()
	fail, published := s.optChain.lastEntryFailure[sel.id]
	_, resolved := s.optChain.resolvedEntry[sel.id]
	s.optChain.mu.Unlock()
	if !published || fail.Reason != entryFailDeltaNoPrice {
		t.Errorf("lastEntryFailure = (%+v, %v), want %q", fail, published, entryFailDeltaNoPrice)
	}
	if resolved {
		t.Error("an unpriced winner was shared with siblings as a resolved entry")
	}
}

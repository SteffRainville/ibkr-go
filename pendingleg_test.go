package ibkr

import (
	"testing"
	"time"
)

// TestActiveLegLocked verifies an active leg is detected (and its strike
// returned) and a pending-only state is not treated as active.
func TestActiveLegLocked(t *testing.T) {
	s := newRotationTestSession(nil)
	s.optChain.mktReqs[1] = &optMktReq{symbol: "QQQ", right: "call", strike: 99, pending: false}
	s.optChain.mktReqs[2] = &optMktReq{symbol: "QQQ", right: "call", strike: 100, pending: true}

	active, ok := s.activeLegLocked("QQQ", "call")
	if !ok {
		t.Fatal("active leg should be detected")
	}
	if active.strike != 99 {
		t.Fatalf("active leg strike = %.0f, want 99 (the non-pending leg, not the pending 100)", active.strike)
	}
	if _, ok := s.activeLegLocked("QQQ", "put"); ok {
		t.Fatal("no put leg exists")
	}
	// Drop the active leg — only the pending remains.
	delete(s.optChain.mktReqs, 1)
	if _, ok := s.activeLegLocked("QQQ", "call"); ok {
		t.Fatal("a pending-only state must not count as active")
	}
}

// TestCurrentOptionContract_PrefersActive verifies a buy resolves against the live
// active leg during a swap, never the not-yet-ready pending replacement — and falls
// back to pending only when no active leg exists.
func TestCurrentOptionContract_PrefersActive(t *testing.T) {
	s := newRotationTestSession(nil)
	// Active leg (old strike 99) has a live quote; pending replacement (strike 100)
	// has none yet.
	s.optChain.mktReqs[1] = &optMktReq{symbol: "QQQ", right: "call", strike: 99, bid: 5.0, ask: 5.2, pending: false, busIdxs: []int{0}}
	s.optChain.mktReqs[2] = &optMktReq{symbol: "QQQ", right: "call", strike: 100, pending: true, busIdxs: []int{0}}

	opt, ok := s.currentOptionContract("QQQ", "call", 0)
	if !ok {
		t.Fatal("expected a resolved contract")
	}
	if opt.strike != 99 {
		t.Fatalf("resolved strike = %.0f, want 99 (the active leg, not the pending 100)", opt.strike)
	}
	if opt.mid != 5.1 {
		t.Fatalf("mid = %.2f, want 5.10 (active leg's live quote)", opt.mid)
	}

	// Once the active leg is gone, the pending leg is the only fallback.
	delete(s.optChain.mktReqs, 1)
	opt, ok = s.currentOptionContract("QQQ", "call", 0)
	if !ok || opt.strike != 100 {
		t.Fatalf("fallback to pending failed: ok=%v strike=%.0f", ok, opt.strike)
	}
}

// TestPromoteIfReadyLocked verifies a pending leg stays pending until it has a
// complete quote (or the grace elapses), then flips to active.
func TestPromoteIfReadyLocked(t *testing.T) {
	s := newRotationTestSession(nil)

	// Not pending → always "ready" (already active).
	active := &optMktReq{symbol: "QQQ", right: "call", strike: 99, pending: false}
	s.optChain.mktReqs[1] = active
	if !s.promoteIfReadyLocked(active, 1) {
		t.Fatal("a non-pending leg must report ready")
	}

	// Pending with no quote → not ready.
	pend := &optMktReq{symbol: "QQQ", right: "put", strike: 50, pending: true, pendingSince: time.Now()}
	s.optChain.mktReqs[2] = pend
	if s.promoteIfReadyLocked(pend, 2) {
		t.Fatal("pending leg with no quote must not promote")
	}
	if !pend.pending {
		t.Fatal("leg should still be pending")
	}

	// Complete two-sided quote → promotes.
	pend.bid, pend.ask = 3.0, 3.2
	if !s.promoteIfReadyLocked(pend, 2) {
		t.Fatal("pending leg with a complete quote must promote")
	}
	if pend.pending {
		t.Fatal("leg should now be active")
	}

	// Grace fallback: one-sided quote, but pendingSince older than the grace window.
	slow := &optMktReq{symbol: "SPY", right: "call", strike: 500, pending: true, price: 2.5,
		pendingSince: time.Now().Add(-pendingPromoteGrace - time.Second)}
	s.optChain.mktReqs[3] = slow
	if !s.promoteIfReadyLocked(slow, 3) {
		t.Fatal("a one-sided quote past the grace window must promote")
	}
	if slow.pending {
		t.Fatal("slow leg should have been promoted by the grace fallback")
	}
}

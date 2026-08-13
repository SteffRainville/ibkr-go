package ibkr

import (
	"testing"
	"time"
)

// TestSelectorLegLocked verifies a selector's displayed contract is found, and
// that a contract it is only WARMING into does not count as displayed.
func TestSelectorLegLocked(t *testing.T) {
	s := newRotationTestSession(nil)
	shown := lk("QQQ", "call", 99, "20260727")
	warming := lk("QQQ", "call", 100, "20260727")
	seedLeg(s, shown, legOpts{reqID: 1, selectors: []int{5}})
	seedLeg(s, warming, legOpts{reqID: 2, warming: []int{5}})

	leg, ok := s.selectorLegLocked(5)
	if !ok {
		t.Fatal("the selector's displayed leg should be found")
	}
	if leg.strike != 99 {
		t.Fatalf("displayed strike = %.0f, want 99 (not the warming 100)", leg.strike)
	}
	if _, ok := s.selectorLegLocked(6); ok {
		t.Fatal("selector 6 holds nothing")
	}
}

// TestCurrentOptionContract_PrefersDisplayed verifies a buy resolves against
// the contract the selector is actually showing during a strike swap, never
// the not-yet-ready replacement — and falls back to the replacement only when
// nothing is displayed.
func TestCurrentOptionContract_PrefersDisplayed(t *testing.T) {
	s := newRotationTestSession(nil)
	sel := seedSelector(s, selector{id: 5, symbol: "QQQ", right: "call", targetDelta: 0.60, busIdxs: []int{0}})

	shown := lk("QQQ", "call", 99, "20260727")
	warming := lk("QQQ", "call", 100, "20260727")
	seedLeg(s, shown, legOpts{reqID: 1, selectors: []int{sel.id}, bid: 5.0, ask: 5.2})
	seedLeg(s, warming, legOpts{reqID: 2, warming: []int{sel.id}})

	opt, ok := s.currentOptionContract("QQQ", "call", 0)
	if !ok {
		t.Fatal("expected a resolved contract")
	}
	if opt.strike != 99 {
		t.Fatalf("resolved strike = %.0f, want 99 (the displayed leg, not the warming 100)", opt.strike)
	}
	if opt.mid != 5.1 {
		t.Fatalf("mid = %.2f, want 5.10 (displayed leg's live quote)", opt.mid)
	}

	// Once the displayed leg is gone, the warming one is the only thing this
	// selector means.
	s.optChain.mu.Lock()
	delete(s.optChain.selCurrent, sel.id)
	delete(s.optChain.legs, shown)
	s.optChain.mu.Unlock()

	opt, ok = s.currentOptionContract("QQQ", "call", 0)
	if !ok || opt.strike != 100 {
		t.Fatalf("fallback to the warming contract failed: ok=%v strike=%.0f", ok, opt.strike)
	}
}

// TestPromotePendingLocked verifies a warming selector stays on its old
// contract until the new one has a complete quote (or the grace elapses), then
// switches — and that switching releases the contract it left.
func TestPromotePendingLocked(t *testing.T) {
	t.Run("no pending swap is not a promotion", func(t *testing.T) {
		s := newRotationTestSession(nil)
		seedLeg(s, lk("QQQ", "call", 99, "20260727"), legOpts{reqID: 1, selectors: []int{5}})
		if _, promoted := s.promotePendingLocked(5, time.Now()); promoted {
			t.Fatal("a selector with no in-flight swap must not report a promotion")
		}
	})

	t.Run("warming with no quote does not promote", func(t *testing.T) {
		s := newRotationTestSession(nil)
		seedLeg(s, lk("QQQ", "put", 60, "20260727"), legOpts{reqID: 1, selectors: []int{5}})
		to := lk("QQQ", "put", 50, "20260727")
		seedLeg(s, to, legOpts{reqID: 2, warming: []int{5}})

		if _, promoted := s.promotePendingLocked(5, time.Now()); promoted {
			t.Fatal("promoted onto a contract with no quote — the row would blank")
		}
		if k, _ := displayedKey(s, 5); k.strike != 60 {
			t.Fatalf("displayed strike = %.0f, want the old 60", k.strike)
		}
	})

	t.Run("complete quote promotes and releases the old contract", func(t *testing.T) {
		s := withOfflineClient(newRotationTestSession(nil))
		old := lk("QQQ", "put", 60, "20260727")
		to := lk("QQQ", "put", 50, "20260727")
		seedLeg(s, old, legOpts{reqID: 1, selectors: []int{5}})
		leg := seedLeg(s, to, legOpts{reqID: 2, warming: []int{5}})

		s.optChain.mu.Lock()
		leg.bid, leg.ask = 3.0, 3.2
		s.optChain.mu.Unlock()

		cancel, promoted := s.promotePendingLocked(5, time.Now())
		if !promoted {
			t.Fatal("a complete two-sided quote must promote")
		}
		if cancel != 1 {
			t.Fatalf("cancel = %d, want reqID 1 — the abandoned contract had no other holder", cancel)
		}
		if k, _ := displayedKey(s, 5); k.strike != 50 {
			t.Fatalf("displayed strike = %.0f, want 50", k.strike)
		}
	})

	t.Run("one-sided quote promotes past the grace window", func(t *testing.T) {
		s := newRotationTestSession(nil)
		seedLeg(s, lk("SPY", "call", 510, "20260727"), legOpts{reqID: 1, selectors: []int{5}})
		to := lk("SPY", "call", 500, "20260727")
		seedLeg(s, to, legOpts{reqID: 2, warming: []int{5}, price: 2.5,
			pendingSince: time.Now().Add(-pendingPromoteGrace - time.Second)})

		if _, promoted := s.promotePendingLocked(5, time.Now()); !promoted {
			t.Fatal("a one-sided quote past the grace window must promote")
		}
	})

	t.Run("abandoning a contract another selector holds keeps its line", func(t *testing.T) {
		s := newRotationTestSession(nil)
		shared := lk("QQQ", "call", 480, "20260727")
		seedLeg(s, shared, legOpts{reqID: 1, selectors: []int{5, 6}})
		to := lk("QQQ", "call", 485, "20260727")
		leg := seedLeg(s, to, legOpts{reqID: 2, warming: []int{5}})

		s.optChain.mu.Lock()
		leg.bid, leg.ask = 1.0, 1.1
		s.optChain.mu.Unlock()

		cancel, promoted := s.promotePendingLocked(5, time.Now())
		if !promoted {
			t.Fatal("expected promotion")
		}
		if cancel != 0 {
			t.Fatalf("cancel = %d, want 0 — selector 6 still holds the 480 contract", cancel)
		}
		if _, _, ok := legHolders(s, shared); !ok {
			t.Fatal("the shared contract was torn down while another selector still displayed it")
		}
	})
}

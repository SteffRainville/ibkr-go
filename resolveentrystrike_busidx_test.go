// Tests for the 2026-08-04 cross-robot mismatch: ResolveEntryStrike used to
// resolve "the" group for a symbol, ignoring which subscriber was actually
// calling AND which right it was asking about. Since selectors are sorted by
// target_delta ascending before ids are assigned, two robots tracking the same
// underlying+right with different target_delta always resolved against
// whichever had the SMALLER one, regardless of the caller's own config.
// Confirmed in production: VWmacdOptionRobot (IWM call, target_delta 0.60)
// entered against VWmacdOptionDataRobot's 0.55 instead of its own. These tests
// use the incident's own selector/busIdx numbers.
package ibkr

import (
	"testing"
	"time"

	"github.com/SteffRainville/ibkr-go/eventbus"
	"github.com/SteffRainville/ibkr-go/quotes"
)

// newIWMCrossRobotTestSession seeds two IWM-call selectors mirroring the
// 2026-08-04 incident: selector 10 (VWmacdOptionDataRobot, target_delta 0.55,
// busIdx 4) and selector 11 (OrbOptionRobot + VWmacdOptionRobot, target_delta
// 0.60, busIdxs 1 and 3). Returns the session plus the two subscribers used as
// stand-ins for VWmacdOptionDataRobot (busIdx 4) and VWmacdOptionRobot
// (busIdx 3).
func newIWMCrossRobotTestSession() (s *Session, subData *testSubscriber, subVWmacd *testSubscriber) {
	subData = newTestSubscriber()
	subVWmacd = newTestSubscriber()

	s = NewSession(Options{}, nil, nil)
	s.buses = make([]*eventbus.Bus, 5)
	s.buses[4] = subData.Bus()
	s.buses[3] = subVWmacd.Bus()

	s.optChain.rotation = []selector{
		{id: 10, symbol: "IWM", right: "call", targetDelta: 0.55, busIdxs: []int{4}},
		{id: 11, symbol: "IWM", right: "call", targetDelta: 0.60, busIdxs: []int{1, 3}},
	}
	s.optChain.lastChainInfo = map[chainKey]chainSnapshot{
		{symbol: "IWM"}: {expiry: "20260806", strikes: []float64{293, 294, 295, 296, 297}, at: time.Now()},
	}
	return s, subData, subVWmacd
}

// TestResolveEntryStrike_DifferentTargetDeltaGroupsResolveIndependently is
// the direct regression test for the 2026-08-04 incident. Before the fix,
// both subscribers below would resolve to group 10 (the smaller target_delta,
// sorted first) regardless of which one actually called in.
func TestResolveEntryStrike_DifferentTargetDeltaGroupsResolveIndependently(t *testing.T) {
	s, subData, subVWmacd := newIWMCrossRobotTestSession()

	// selectorForLocked alone: each busIdx must find its OWN selector.
	sel, ok := s.selectorForLocked("IWM", "call", 4)
	if !ok || sel.id != 10 || sel.targetDelta != 0.55 {
		t.Fatalf("busIdx=4: got selector=%+v ok=%v, want id=10 delta=0.55", sel, ok)
	}
	sel, ok = s.selectorForLocked("IWM", "call", 3)
	if !ok || sel.id != 11 || sel.targetDelta != 0.60 {
		t.Fatalf("busIdx=3: got selector=%+v ok=%v, want id=11 delta=0.60", sel, ok)
	}

	// End-to-end: seed each selector's own resolved contract at a different
	// strike, plus Book prices, and confirm each subscriber gets its OWN
	// strike back — not whichever selector sorts first.
	s.optChain.mu.Lock()
	s.optChain.resolvedEntry[10] = resolvedEntryLeg{strike: 296, expiry: "20260806", delta: 0.55, bid: 1.60, ask: 1.70, at: time.Now()}
	s.optChain.resolvedEntry[11] = resolvedEntryLeg{strike: 297, expiry: "20260806", delta: 0.6083, bid: 2.11, ask: 2.20, at: time.Now()}
	s.optChain.mu.Unlock()

	key10 := quotes.ContractKey{Symbol: "IWM", Right: "call", Strike: 296, Expiry: "20260806"}
	s.book.SetOptionBid(key10, 1.60)
	s.book.SetOptionAsk(key10, 1.70)
	key11 := quotes.ContractKey{Symbol: "IWM", Right: "call", Strike: 297, Expiry: "20260806"}
	s.book.SetOptionBid(key11, 2.11)
	s.book.SetOptionAsk(key11, 2.20)

	q, res := s.ResolveEntryStrike(subData, "IWM", "call", 2*time.Second)
	if !res.OK || q.Strike != 296 {
		t.Fatalf("VWmacdOptionDataRobot (busIdx 4): got strike=%.0f res=%+v, want strike=296 (its own selector 10)", q.Strike, res)
	}

	q, res = s.ResolveEntryStrike(subVWmacd, "IWM", "call", 2*time.Second)
	if !res.OK || q.Strike != 297 {
		t.Fatalf("VWmacdOptionRobot (busIdx 3): got strike=%.0f res=%+v, want strike=297 (its own selector 11) — this is the 2026-08-04 bug if it instead got 296", q.Strike, res)
	}
}

// TestResolveEntryStrike_SameTargetDeltaSharedAcrossBusIdxs proves the fix
// does not break the legitimate case: two subscribers configured identically
// (same target_delta, same selector) must still join/share one probe via
// waitForEntryResolution rather than each launching their own — exactly like
// the real selector 11 (OrbOptionRobot busIdx 1 + VWmacdOptionRobot busIdx 3,
// both target_delta 0.60).
func TestResolveEntryStrike_SameTargetDeltaSharedAcrossBusIdxs(t *testing.T) {
	s, _, subVWmacd := newIWMCrossRobotTestSession()

	subOrbOption := newTestSubscriber()
	s.buses[1] = subOrbOption.Bus()

	// subOrbOption (busIdx 1) is the "owner" of an in-flight probe for selector 11.
	s.optChain.mu.Lock()
	s.optChain.deltaRes[11] = &deltaResolution{selectorID: 11, symbol: "IWM", right: "call", targetDelta: 0.60}
	s.optChain.mu.Unlock()

	const simulatedProbeDelay = 100 * time.Millisecond
	go func() {
		time.Sleep(simulatedProbeDelay)
		key := quotes.ContractKey{Symbol: "IWM", Right: "call", Strike: 297, Expiry: "20260806"}
		s.book.SetOptionBid(key, 2.11)
		s.book.SetOptionAsk(key, 2.20)
		s.optChain.mu.Lock()
		delete(s.optChain.deltaRes, 11)
		s.optChain.resolvedEntry[11] = resolvedEntryLeg{strike: 297, expiry: "20260806", delta: 0.6083, bid: 2.11, ask: 2.20, at: time.Now()}
		s.optChain.mu.Unlock()
	}()

	// subVWmacd (busIdx 3) calls in while busIdx 1 owns the resolution for
	// the SAME selector (11) — it must join, not launch its own probe (s.client
	// is nil here; launching a real probe would panic).
	q, res := s.ResolveEntryStrike(subVWmacd, "IWM", "call", 2*time.Second)
	if !res.OK || q.Strike != 297 {
		t.Fatalf("expected busIdx 3 to join busIdx 1's in-flight resolution for the shared selector 11, got strike=%.0f res=%+v", q.Strike, res)
	}
}

// TestSelectorForLocked_FiltersByBusIdx table-drives selectorForLocked
// directly: a busIdx exclusive to one selector, a busIdx exclusive to the
// other, a busIdx in neither, and the -1 (bus not found) fallback.
func TestSelectorForLocked_FiltersByBusIdx(t *testing.T) {
	s, _, _ := newIWMCrossRobotTestSession()

	tests := []struct {
		name   string
		busIdx int
		wantOK bool
		wantID int
	}{
		{"busIdx in selector 10 only", 4, true, 10},
		{"busIdx in selector 11 only", 3, true, 11},
		{"busIdx in selector 11 (other member)", 1, true, 11},
		{"busIdx in neither", 2, false, 0},
		{"busIdx -1 falls back to first match", -1, true, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sel, ok := s.selectorForLocked("IWM", "call", tt.busIdx)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v (selector=%+v)", ok, tt.wantOK, sel)
			}
			if ok && sel.id != tt.wantID {
				t.Fatalf("selector id = %d, want %d", sel.id, tt.wantID)
			}
		})
	}
}

// TestSelectorForLocked_RightIsPartOfTheKey is the structural half of the same
// fix: a call and a put on one underlying are unrelated instruments, so asking
// for one must never hand back the other. Under the group model they shared a
// key and the caller had to disambiguate afterwards.
func TestSelectorForLocked_RightIsPartOfTheKey(t *testing.T) {
	s, _, _ := newIWMCrossRobotTestSession()
	if sel, ok := s.selectorForLocked("IWM", "put", -1); ok {
		t.Fatalf("found a put selector %+v in a rotation that only has calls", sel)
	}
}

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
)

// newIWMCrossRobotTestSession seeds two IWM-call selectors mirroring the
// 2026-08-04 incident: selector 10 (VWmacdOptionDataRobot, target_delta 0.55,
// busIdx 4) and selector 11 (OrbOptionRobot + VWmacdOptionRobot, target_delta
// 0.60, busIdxs 1 and 3). Returns the session plus the two subscribers used as
// stand-ins for VWmacdOptionDataRobot (busIdx 4) and VWmacdOptionRobot
// (busIdx 3). s.client is nil: any probe launch would panic.
func newIWMCrossRobotTestSession() (s *Session, subData *testSubscriber, subVWmacd *testSubscriber) {
	subData = newTestSubscriber()
	subVWmacd = newTestSubscriber()

	s = NewSession(Options{}, nil, nil)
	s.buses = make([]*eventbus.Bus, 5)
	s.buses[4] = subData.Bus()
	s.buses[3] = subVWmacd.Bus()

	s.optChain.selectors = []selector{
		{id: 10, symbol: "IWM", right: "call", targetDelta: 0.55, busIdxs: []int{4}},
		{id: 11, symbol: "IWM", right: "call", targetDelta: 0.60, busIdxs: []int{1, 3}},
	}
	s.optChain.chains[chainKey{symbol: "IWM"}] = &chain{
		day: tradingDay(time.Now()), expiry: "20260806",
		strikes: map[string][]float64{"call": {293, 294, 295, 296, 297}},
	}
	return s, subData, subVWmacd
}

// seedFinishedProbe records a just-finished successful probe for selID.
func seedFinishedProbe(s *Session, selID int, strike float64) {
	call := &probeCall{done: make(chan struct{}), finishedAt: time.Now(),
		q:   OptionQuote{Strike: strike, Expiry: "20260806", Bid: 2.11, Ask: 2.20},
		res: EntryStrikeResult{OK: true}}
	close(call.done)
	s.optChain.mu.Lock()
	s.optChain.probes[selID] = call
	s.optChain.mu.Unlock()
}

// TestResolveEntryStrike_DifferentTargetDeltaGroupsResolveIndependently is
// the direct regression test for the 2026-08-04 incident: each subscriber must
// get its OWN selector's contract, not whichever selector sorts first.
func TestResolveEntryStrike_DifferentTargetDeltaGroupsResolveIndependently(t *testing.T) {
	s, subData, subVWmacd := newIWMCrossRobotTestSession()
	seedFinishedProbe(s, 10, 296)
	seedFinishedProbe(s, 11, 297)

	q, res := s.ResolveEntryStrike(subData, "IWM", "call", 2*time.Second)
	if !res.OK || q.Strike != 296 {
		t.Fatalf("VWmacdOptionDataRobot (busIdx 4): got strike=%.0f res=%+v, want strike=296 (its own selector 10)", q.Strike, res)
	}
	q, res = s.ResolveEntryStrike(subVWmacd, "IWM", "call", 2*time.Second)
	if !res.OK || q.Strike != 297 {
		t.Fatalf("VWmacdOptionRobot (busIdx 3): got strike=%.0f res=%+v, want strike=297 (its own selector 11) — this is the 2026-08-04 bug if it instead got 296", q.Strike, res)
	}
}

// TestResolveEntryStrike_SameTargetDeltaSharedAcrossBusIdxs: two subscribers
// configured identically (selector 11: busIdx 1 and busIdx 3) share one probe.
// busIdx 3 calls while busIdx 1's probe is in flight and must join it.
func TestResolveEntryStrike_SameTargetDeltaSharedAcrossBusIdxs(t *testing.T) {
	s, _, subVWmacd := newIWMCrossRobotTestSession()

	call := &probeCall{done: make(chan struct{}), deadline: time.Now().Add(time.Second)}
	s.optChain.mu.Lock()
	s.optChain.probes[11] = call
	s.optChain.mu.Unlock()
	go func() {
		time.Sleep(100 * time.Millisecond)
		s.optChain.mu.Lock()
		call.q = OptionQuote{Strike: 297, Expiry: "20260806", Bid: 2.11, Ask: 2.20}
		call.res, call.finishedAt = EntryStrikeResult{OK: true}, time.Now()
		close(call.done)
		s.optChain.mu.Unlock()
	}()

	q, res := s.ResolveEntryStrike(subVWmacd, "IWM", "call", 2*time.Second)
	if !res.OK || q.Strike != 297 {
		t.Fatalf("expected busIdx 3 to join busIdx 1's in-flight probe for the shared selector 11, got strike=%.0f res=%+v", q.Strike, res)
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

// TestSelectorForLocked_RightIsPartOfTheKey: a call and a put on one
// underlying are unrelated instruments, so asking for one must never hand back
// the other.
func TestSelectorForLocked_RightIsPartOfTheKey(t *testing.T) {
	s, _, _ := newIWMCrossRobotTestSession()
	if sel, ok := s.selectorForLocked("IWM", "put", -1); ok {
		t.Fatalf("found a put selector %+v among selectors that only have calls", sel)
	}
}

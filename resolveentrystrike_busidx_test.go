// Tests for the 2026-08-04 cross-robot group mismatch: ResolveEntryStrike
// used to resolve "the" group for a symbol via groupForSymbolLocked(symbol),
// which ignored which subscriber was actually calling. Since
// buildOptionGroups sorts groups by (symbol, optionDelay, targetDeltaCall,
// targetDeltaPut) ascending before assigning groupIDs, two robots tracking
// the same underlying+right with different target_delta always resolved
// against whichever group had the SMALLER target_delta, regardless of the
// caller's own config. Confirmed in production: VWmacdOptionRobot (IWM call,
// target_delta 0.60) entered against VWmacdOptionDataRobot's group (target
// 0.55) instead of its own. These tests use the incident's own group/busIdx
// numbers.
package ibkr

import (
	"testing"
	"time"

	"github.com/SteffRainville/ibkr-go/eventbus"
	"github.com/SteffRainville/ibkr-go/quotes"
)

// newIWMCrossRobotTestSession seeds two IWM-call resolution groups mirroring
// the 2026-08-04 incident: group 10 (VWmacdOptionDataRobot, target_delta
// 0.55, busIdx 4) and group 11 (OrbOptionRobot + VWmacdOptionRobot, target_delta
// 0.60, busIdxs 1 and 3). Returns the session plus the two subscribers used
// as stand-ins for VWmacdOptionDataRobot (busIdx 4) and VWmacdOptionRobot
// (busIdx 3).
func newIWMCrossRobotTestSession() (s *Session, subData *testSubscriber, subVWmacd *testSubscriber) {
	subData = newTestSubscriber()
	subVWmacd = newTestSubscriber()

	s = NewSession(Options{}, nil, nil)
	s.buses = make([]*eventbus.Bus, 5)
	s.buses[4] = subData.Bus()
	s.buses[3] = subVWmacd.Bus()

	s.optChain.rotation = []optResGroup{
		{groupID: 10, symbol: "IWM", targetDeltaCall: 0.55, targetDeltaPut: 0.55, busIdxs: []int{4}},
		{groupID: 11, symbol: "IWM", targetDeltaCall: 0.60, targetDeltaPut: 0.60, busIdxs: []int{1, 3}},
	}
	s.optChain.lastChainInfo = map[string]chainSnapshot{
		"IWM": {expiry: "20260806", strikes: []float64{293, 294, 295, 296, 297}},
	}
	return s, subData, subVWmacd
}

// TestResolveEntryStrike_DifferentTargetDeltaGroupsResolveIndependently is
// the direct regression test for the 2026-08-04 incident. Before the fix,
// both subscribers below would resolve to group 10 (the smaller target_delta,
// sorted first) regardless of which one actually called in.
func TestResolveEntryStrike_DifferentTargetDeltaGroupsResolveIndependently(t *testing.T) {
	s, subData, subVWmacd := newIWMCrossRobotTestSession()

	// groupForSymbolLocked alone: each busIdx must find its OWN group.
	g, ok := s.groupForSymbolLocked("IWM", 4)
	if !ok || g.groupID != 10 || g.targetDeltaCall != 0.55 {
		t.Fatalf("busIdx=4: got group=%+v ok=%v, want groupID=10 targetDeltaCall=0.55", g, ok)
	}
	g, ok = s.groupForSymbolLocked("IWM", 3)
	if !ok || g.groupID != 11 || g.targetDeltaCall != 0.60 {
		t.Fatalf("busIdx=3: got group=%+v ok=%v, want groupID=11 targetDeltaCall=0.60", g, ok)
	}

	// End-to-end: seed each group's own resolved contract at a different
	// strike, plus Book prices, and confirm each subscriber gets its OWN
	// group's strike back — not whichever group sorts first.
	s.optChain.mu.Lock()
	s.optChain.resolvedEntry[retryKeyLeg(10, "call")] = resolvedEntryLeg{strike: 296, expiry: "20260806", delta: 0.55, at: time.Now()}
	s.optChain.resolvedEntry[retryKeyLeg(11, "call")] = resolvedEntryLeg{strike: 297, expiry: "20260806", delta: 0.6083, at: time.Now()}
	s.optChain.mu.Unlock()

	key10 := quotes.ContractKey{Symbol: "IWM", Right: "call", Strike: 296, Expiry: "20260806"}
	s.book.SetOptionBid(key10, 1.60)
	s.book.SetOptionAsk(key10, 1.70)
	key11 := quotes.ContractKey{Symbol: "IWM", Right: "call", Strike: 297, Expiry: "20260806"}
	s.book.SetOptionBid(key11, 2.11)
	s.book.SetOptionAsk(key11, 2.20)

	q, res := s.ResolveEntryStrike(subData, "IWM", "call", 2*time.Second)
	if !res.OK || q.Strike != 296 {
		t.Fatalf("VWmacdOptionDataRobot (busIdx 4): got strike=%.0f res=%+v, want strike=296 (its own group 10)", q.Strike, res)
	}

	q, res = s.ResolveEntryStrike(subVWmacd, "IWM", "call", 2*time.Second)
	if !res.OK || q.Strike != 297 {
		t.Fatalf("VWmacdOptionRobot (busIdx 3): got strike=%.0f res=%+v, want strike=297 (its own group 11) — this is the 2026-08-04 bug if it instead got 296", q.Strike, res)
	}
}

// TestResolveEntryStrike_SameTargetDeltaGroupSharedAcrossBusIdxs proves the
// fix does not break the legitimate case: two subscribers configured
// identically (same target_delta, same group) must still join/share one
// probe via waitForEntryResolution rather than each launching their own —
// exactly like the real group 11 (OrbOptionRobot busIdx 1 + VWmacdOptionRobot
// busIdx 3, both target_delta 0.60).
func TestResolveEntryStrike_SameTargetDeltaGroupSharedAcrossBusIdxs(t *testing.T) {
	s, _, subVWmacd := newIWMCrossRobotTestSession()

	subOrbOption := newTestSubscriber()
	s.buses[1] = subOrbOption.Bus()

	// subOrbOption (busIdx 1) is the "owner" of an in-flight probe for group 11.
	s.optChain.mu.Lock()
	s.optChain.deltaRes[retryKeyLeg(11, "call")] = &deltaResolution{groupID: 11, symbol: "IWM", right: "call", targetDelta: 0.60}
	s.optChain.mu.Unlock()

	const simulatedProbeDelay = 100 * time.Millisecond
	go func() {
		time.Sleep(simulatedProbeDelay)
		key := quotes.ContractKey{Symbol: "IWM", Right: "call", Strike: 297, Expiry: "20260806"}
		s.book.SetOptionBid(key, 2.11)
		s.book.SetOptionAsk(key, 2.20)
		s.optChain.mu.Lock()
		delete(s.optChain.deltaRes, retryKeyLeg(11, "call"))
		s.optChain.resolvedEntry[retryKeyLeg(11, "call")] = resolvedEntryLeg{strike: 297, expiry: "20260806", delta: 0.6083, at: time.Now()}
		s.optChain.mu.Unlock()
	}()

	// subVWmacd (busIdx 3) calls in while busIdx 1 owns the resolution for
	// the SAME group (11) — it must join, not launch its own probe (s.client
	// is nil here; launching a real probe would panic).
	q, res := s.ResolveEntryStrike(subVWmacd, "IWM", "call", 2*time.Second)
	if !res.OK || q.Strike != 297 {
		t.Fatalf("expected busIdx 3 to join busIdx 1's in-flight resolution for the shared group 11, got strike=%.0f res=%+v", q.Strike, res)
	}
}

// TestGroupForSymbolLocked_FiltersByBusIdx table-drives groupForSymbolLocked
// directly: a busIdx exclusive to one group, a busIdx exclusive to the
// other, a busIdx in neither, and the -1 (bus not found) fallback.
func TestGroupForSymbolLocked_FiltersByBusIdx(t *testing.T) {
	s, _, _ := newIWMCrossRobotTestSession()

	tests := []struct {
		name        string
		busIdx      int
		wantOK      bool
		wantGroupID int
	}{
		{"busIdx in group 10 only", 4, true, 10},
		{"busIdx in group 11 only", 3, true, 11},
		{"busIdx in group 11 (other member)", 1, true, 11},
		{"busIdx in neither group", 2, false, 0},
		{"busIdx -1 falls back to first symbol match", -1, true, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, ok := s.groupForSymbolLocked("IWM", tt.busIdx)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v (group=%+v)", ok, tt.wantOK, g)
			}
			if ok && g.groupID != tt.wantGroupID {
				t.Fatalf("groupID = %d, want %d", g.groupID, tt.wantGroupID)
			}
		})
	}
}

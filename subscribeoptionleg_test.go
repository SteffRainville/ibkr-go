// Tests for subscribeOptionLeg's two behaviors: skipping a background
// refresh entirely when the estimate strike hasn't changed (avoiding
// pointless churn), and routing to the correct priority tier
// (DiscretionaryNew vs DiscretionaryChurn) based on whether an active leg
// already exists.
//
// Every test here exercises only paths that return before reaching
// s.client.ReqMktData — s.client stays nil throughout (a nil client would
// panic if a call ever reached it, which is itself the proof these paths
// return early; ibapi.EClient.ReqMktData dereferences its receiver on the
// very first line, so this is a real constraint, not just caution). The "a
// grant succeeds and the subscription proceeds" side of subscribeOptionLeg
// is therefore only exercised indirectly, via the mdlines-level priority
// tests (mdlines.TestLedger_DiscretionaryNewOutranksChurn), which test the
// same threshold logic with no Session/client involved at all.
package ibkr

import (
	"testing"

	"github.com/SteffRainville/ibkr-go/eventbus"
	"github.com/SteffRainville/ibkr-go/mdlines"
)

// TestSubscribeOptionLeg_SkipsChurnWhenStrikeUnchanged verifies that
// resubscribing an already-active leg with the SAME strike is a no-op: no new
// mktReqs entry, no ledger line spent. s.mdLines is deliberately left nil —
// the skip must happen before the function ever touches it.
func TestSubscribeOptionLeg_SkipsChurnWhenStrikeUnchanged(t *testing.T) {
	s := newRotationTestSession(nil)
	s.optChain.mktReqs[1] = &optMktReq{
		groupID: 5, symbol: "QQQ", right: "call", strike: 480, expiry: "20260727", pending: false,
	}

	s.subscribeOptionLeg(5, []int{0}, "QQQ", "call", 480, "20260727", true)

	if len(s.optChain.mktReqs) != 1 {
		t.Fatalf("mktReqs has %d entries, want 1 (no new entry for an unchanged strike)", len(s.optChain.mktReqs))
	}
	if _, ok := s.optChain.mktReqs[1]; !ok {
		t.Fatal("the original active leg was removed — it must be left untouched")
	}
}

// TestSubscribeOptionLeg_ChurnRefusedAtChurnThreshold verifies two things at
// once: (1) the skip from the test above is specifically keyed on the strike
// matching, not merely "an active leg exists" — a genuine strike change is a
// real refresh attempt, so it must still try to grant a line; and (2) that
// attempt is routed to GrantDiscretionaryChurn, not GrantDiscretionaryNew —
// saturating the ledger to exactly the churn threshold via
// GrantDiscretionaryChurn causes the request to be refused right there.
func TestSubscribeOptionLeg_ChurnRefusedAtChurnThreshold(t *testing.T) {
	s := newRotationTestSession(nil)
	s.mdLines = mdlines.NewLedger(100, 50)
	for i := int64(0); i < int64(100-mdlines.ReserveChurn); i++ {
		if !s.mdLines.GrantDiscretionaryChurn(i) {
			t.Fatalf("setup: churn grant %d refused before reaching the churn threshold", i)
		}
	}
	usedBefore, _ := s.mdLines.Status()

	s.optChain.mktReqs[1] = &optMktReq{
		groupID: 5, symbol: "QQQ", right: "call", strike: 480, expiry: "20260727", pending: false,
	}
	s.subscribeOptionLeg(5, []int{0}, "QQQ", "call", 485, "20260727", true)

	if len(s.optChain.mktReqs) != 1 {
		t.Fatalf("mktReqs has %d entries, want 1 (refused churn request must not add an entry)", len(s.optChain.mktReqs))
	}
	usedAfter, _ := s.mdLines.Status()
	if usedAfter != usedBefore {
		t.Fatalf("ledger usage changed from %d to %d — a refused grant must not touch the ledger", usedBefore, usedAfter)
	}
}

// TestSubscribeOptionLeg_JoiningGroupGetsAddedToActiveLeg is the regression
// test for the 2026-07-28 IBIT-call incident: a second resolution group
// (e.g. a second robot whose independent delta estimate lands on the same
// strike) that hits the "strike unchanged" skip must still be added to the
// active leg's busIdxs and receive an immediate snapshot of the leg's
// current data — otherwise its hub never learns the strike at all, even
// though the underlying market data subscription already exists.
func TestSubscribeOptionLeg_JoiningGroupGetsAddedToActiveLeg(t *testing.T) {
	s := newRotationTestSession(nil)
	bus0 := eventbus.New()
	bus1 := eventbus.New()
	s.buses = []*eventbus.Bus{bus0, bus1}
	ch1 := bus1.Subscribe(eventbus.KindOptionData)

	s.optChain.mktReqs[1] = &optMktReq{
		groupID: 5, symbol: "QQQ", right: "call", strike: 480, expiry: "20260727",
		pending: false, busIdxs: []int{0}, price: 8.5, bid: 8.4, ask: 8.6, delta: 0.62,
		deltaSource: "matched",
	}

	// A different group (bus 1), whose own estimate independently landed on
	// the same strike, "subscribes" — it must join the existing leg rather
	// than being silently dropped.
	s.subscribeOptionLeg(6, []int{1}, "QQQ", "call", 480, "20260727", true)

	req := s.optChain.mktReqs[1]
	if len(req.busIdxs) != 2 || req.busIdxs[0] != 0 || req.busIdxs[1] != 1 {
		t.Fatalf("active leg busIdxs = %v, want [0 1] — the joining group's bus must be merged in", req.busIdxs)
	}

	select {
	case evt := <-ch1:
		od, ok := evt.Payload.(eventbus.OptionData)
		if !ok {
			t.Fatalf("payload type = %T, want eventbus.OptionData", evt.Payload)
		}
		if od.Strike != 480 || od.Bid != 8.4 || od.Ask != 8.6 || od.DeltaSource != "matched" {
			t.Fatalf("snapshot event = %+v, want the active leg's current strike/bid/ask/deltaSource", od)
		}
	default:
		t.Fatal("joining bus never received a KindOptionData snapshot of the already-active leg")
	}
}

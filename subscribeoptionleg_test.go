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

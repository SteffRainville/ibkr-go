// Tests for pointSelectorAt — how a selector acquires the contract it should
// be tracking, and what happens when the market-data line budget says no.
//
// The tests that reach an actual ReqMktData are the ones that install a fake
// client; the rest exercise paths that return before touching s.client, which
// stays nil (ibapi.EClient.ReqMktData dereferences its receiver on the very
// first line, so a nil client is a real assertion, not just caution).
package ibkr

import (
	"testing"
	"time"

	"github.com/SteffRainville/ibkr-go/eventbus"
	"github.com/SteffRainville/ibkr-go/mdlines"
)

// TestPointSelectorAt_SkipsWhenStrikeUnchanged verifies that re-pointing a
// selector at the contract it already displays is a no-op: no second leg, no
// ledger line spent. s.mdLines is deliberately left nil — the skip must happen
// before the function ever touches it.
func TestPointSelectorAt_SkipsWhenStrikeUnchanged(t *testing.T) {
	s := newRotationTestSession(nil)
	sel := seedSelector(s, selector{id: 5, symbol: "QQQ", right: "call", targetDelta: 0.60, busIdxs: []int{0}})
	key := lk("QQQ", "call", 480, "20260727")
	seedLeg(s, key, legOpts{reqID: 1, selectors: []int{sel.id}})

	s.pointSelectorAt(sel, key, true)

	if got := legCount(s); got != 1 {
		t.Fatalf("legs = %d, want 1 (no new subscription for an unchanged strike)", got)
	}
	if _, _, ok := legHolders(s, key); !ok {
		t.Fatal("the original leg was removed — it must be left untouched")
	}
}

// TestPointSelectorAt_ChurnRefusedAtChurnThreshold verifies two things at once:
// (1) the skip above is keyed on the CONTRACT matching, not merely "this
// selector has some leg" — a genuine strike change is a real refresh attempt,
// so it must still try to grant a line; and (2) that attempt is routed to
// GrantDiscretionaryChurn, not GrantDiscretionaryNew, so saturating the ledger
// to exactly the churn threshold refuses it right there.
//
// The seeded leg carries a two-sided quote deliberately: churn means "refresh
// something that is already working", and a leg with no bid and no ask is not
// working. See TestPointSelectorAt_UnquotedLegTakesNewTier.
func TestPointSelectorAt_ChurnRefusedAtChurnThreshold(t *testing.T) {
	s := newRotationTestSession(nil)
	s.mdLines = mdlines.NewLedger(100, 50)
	for i := int64(0); i < int64(100-mdlines.ReserveChurnFor(100)); i++ {
		if !s.mdLines.GrantDiscretionaryChurn(i) {
			t.Fatalf("setup: churn grant %d refused before reaching the churn threshold", i)
		}
	}
	usedBefore, _ := s.mdLines.Status()

	sel := seedSelector(s, selector{id: 5, symbol: "QQQ", right: "call", targetDelta: 0.60, busIdxs: []int{0}})
	seedLeg(s, lk("QQQ", "call", 480, "20260727"), legOpts{reqID: 1, selectors: []int{sel.id},
		price: 8.5, bid: 8.4, ask: 8.6})

	s.pointSelectorAt(sel, lk("QQQ", "call", 485, "20260727"), true)

	if got := legCount(s); got != 1 {
		t.Fatalf("legs = %d, want 1 (a refused churn request must not open a subscription)", got)
	}
	usedAfter, _ := s.mdLines.Status()
	if usedAfter != usedBefore {
		t.Fatalf("ledger usage changed from %d to %d — a refused grant must not touch the ledger", usedBefore, usedAfter)
	}
}

// TestPointSelectorAt_UnquotedLegTakesNewTier is the regression for the second
// half of the 2026-08-17 MSFT incident — the half that made it permanent.
//
// A bad strike guess left MSFT displaying a contract IB never quoted: no bid,
// no ask, δ≈1.00 against a target of 0.55. Because the tier was chosen on "does
// this selector have a leg", every attempt to move back to a real strike asked
// for the churn tier, which is refused from the churn reserve upward. The
// account sat just past that line all day, so the row could not be repaired for
// the rest of the session.
//
// A leg that has never carried a two-sided quote is precisely what
// CategoryDiscretionaryNew is defined to mean, so it must take that tier and
// keep working in the band between the two reserves.
func TestPointSelectorAt_UnquotedLegTakesNewTier(t *testing.T) {
	s := withOfflineClient(newRotationTestSession(nil))
	s.buses = []*eventbus.Bus{eventbus.New()}
	s.mdLines = mdlines.NewLedger(100, 50)
	// Saturate exactly to the churn threshold: churn is now refused, first-quote
	// still has the band up to the new threshold.
	for i := int64(0); i < int64(100-mdlines.ReserveChurnFor(100)); i++ {
		if !s.mdLines.GrantDiscretionaryChurn(i) {
			t.Fatalf("setup: churn grant %d refused before reaching the churn threshold", i)
		}
	}
	if s.mdLines.GrantDiscretionaryChurn(999) {
		t.Fatal("setup: churn is still being granted — the ledger is not at the churn threshold")
	}

	sel := seedSelector(s, selector{id: 5, symbol: "MSFT", right: "call", targetDelta: 0.55, busIdxs: []int{0}})
	// The stuck leg: subscribed, ticking a price, but never a two-sided quote.
	seedLeg(s, lk("MSFT", "call", 400, "20260820"), legOpts{
		reqID: 1, selectors: []int{sel.id}, price: 94.42, delta: 0.9999,
	})

	want := lk("MSFT", "call", 485, "20260820")
	s.pointSelectorAt(sel, want, true)

	if _, _, ok := legHolders(s, want); !ok {
		t.Fatal("the selector could not escape its unquoted leg — a row with no quote must not be " +
			"starved by the tier that protects rows which already have one")
	}
	// The old leg stays until the replacement quotes (pendingSwap), so the
	// selector still displays it — the escape is the new subscription existing.
	if got := legCount(s); got != 2 {
		t.Fatalf("legs = %d, want 2 (the stuck leg plus the replacement warming into place)", got)
	}
}

// TestPointSelectorAt_RefusedLineSharesExistingLeg is the core regression for
// the 2026-08-13 QQQ blackout. VWmacdFilteredRobot's QQQ call selector wanted a
// strike a sibling did not hold; every attempt took the churn tier, which was
// refused (82/100 lines used against a churn reserve of 25); and the refusal
// path was a bare `return`. The row therefore showed a 41-minute-old strike
// with no bid and no ask while the contract's market data was flowing the whole
// time — to a different robot.
//
// A selector that cannot get its own line must share whatever leg exists rather
// than displaying nothing.
func TestPointSelectorAt_RefusedLineSharesExistingLeg(t *testing.T) {
	s := newRotationTestSession(nil)
	bus0, bus1 := eventbus.New(), eventbus.New()
	s.buses = []*eventbus.Bus{bus0, bus1}
	ch1 := bus1.Subscribe(eventbus.KindOptionData)

	s.mdLines = mdlines.NewLedger(100, 50)
	for i := int64(0); i < int64(100-mdlines.ReserveNewFor(100)); i++ {
		s.mdLines.GrantDiscretionaryNew(i)
	}

	// A sibling robot (bus 0) already holds QQQ 480; ours (bus 1) has nothing.
	sibling := seedSelector(s, selector{id: 5, symbol: "QQQ", right: "call", targetDelta: 0.55, busIdxs: []int{0}})
	mine := seedSelector(s, selector{id: 6, symbol: "QQQ", right: "call", targetDelta: 0.60, busIdxs: []int{1}})
	held := lk("QQQ", "call", 480, "20260727")
	seedLeg(s, held, legOpts{reqID: 1, selectors: []int{sibling.id}, bid: 8.4, ask: 8.6, delta: 0.62})

	s.pointSelectorAt(mine, lk("QQQ", "call", 476, "20260727"), true)

	if got := legCount(s); got != 1 {
		t.Fatalf("legs = %d, want 1 — the line was refused, nothing new should be subscribed", got)
	}
	k, ok := displayedKey(s, mine.id)
	if !ok {
		t.Fatal("the refused selector displays NOTHING — this is the blank-row bug the fallback exists to prevent")
	}
	if k != held {
		t.Fatalf("displayed %v, want the sibling's %v", k, held)
	}

	select {
	case evt := <-ch1:
		od := evt.Payload.(eventbus.OptionData)
		if od.Strike != 480 || od.Bid != 8.4 || od.Ask != 8.6 {
			t.Fatalf("snapshot = %+v, want the shared leg's live strike/bid/ask", od)
		}
	default:
		t.Fatal("the joining bus never received a KindOptionData snapshot of the leg it now shares")
	}
}

// TestPointSelectorAt_JoiningSelectorSharesOneLine is the regression for the
// 2026-07-28 IBIT-call incident, restated in the registry model: a second
// selector whose own estimate lands on a contract someone already holds must
// attach to it — receiving an immediate snapshot — and must NOT open a second
// IB subscription for the same contract.
func TestPointSelectorAt_JoiningSelectorSharesOneLine(t *testing.T) {
	s := newRotationTestSession(nil)
	bus0, bus1 := eventbus.New(), eventbus.New()
	s.buses = []*eventbus.Bus{bus0, bus1}
	ch1 := bus1.Subscribe(eventbus.KindOptionData)
	s.mdLines = mdlines.NewLedger(100, 50)

	first := seedSelector(s, selector{id: 5, symbol: "QQQ", right: "call", targetDelta: 0.55, busIdxs: []int{0}})
	second := seedSelector(s, selector{id: 6, symbol: "QQQ", right: "call", targetDelta: 0.60, busIdxs: []int{1}})
	key := lk("QQQ", "call", 480, "20260727")
	seedLeg(s, key, legOpts{reqID: 1, selectors: []int{first.id},
		price: 8.5, bid: 8.4, ask: 8.6, delta: 0.62})

	usedBefore, _ := s.mdLines.Status()
	s.pointSelectorAt(second, key, true)
	usedAfter, _ := s.mdLines.Status()

	if usedAfter != usedBefore {
		t.Fatalf("ledger went %d → %d — joining an existing contract must cost no line", usedBefore, usedAfter)
	}
	sels, _, ok := legHolders(s, key)
	if !ok || len(sels) != 2 {
		t.Fatalf("leg holders = %v, want both selectors sharing one subscription", sels)
	}
	if k, _ := displayedKey(s, second.id); k != key {
		t.Fatalf("joining selector displays %v, want %v", k, key)
	}

	select {
	case evt := <-ch1:
		od := evt.Payload.(eventbus.OptionData)
		if od.Strike != 480 || od.Bid != 8.4 || od.Ask != 8.6 || od.DeltaSource != "matched" {
			t.Fatalf("snapshot event = %+v, want the leg's current strike/bid/ask/deltaSource", od)
		}
	default:
		t.Fatal("joining bus never received a KindOptionData snapshot of the leg it now shares")
	}
}

// TestPointSelectorAt_TwoSelectorsDoNotEvictEachOther is the other half of the
// 2026-08-13 regression. Under the old (symbol, right) ownership every
// subscribe cancelled whatever leg existed for that pair, whoever owned it, so
// three QQQ groups took turns blanking each other. Distinct target deltas
// resolving to distinct strikes must now coexist.
func TestPointSelectorAt_TwoSelectorsDoNotEvictEachOther(t *testing.T) {
	s := newRotationTestSession(nil)
	s.client = nil
	s.mdLines = mdlines.NewLedger(100, 50)
	s.buses = []*eventbus.Bus{eventbus.New(), eventbus.New()}

	a := seedSelector(s, selector{id: 5, symbol: "QQQ", right: "call", targetDelta: 0.55, busIdxs: []int{0}})
	b := seedSelector(s, selector{id: 6, symbol: "QQQ", right: "call", targetDelta: 0.60, busIdxs: []int{1}})

	keyA := lk("QQQ", "call", 727, "20260817")
	keyB := lk("QQQ", "call", 726, "20260817")
	seedLeg(s, keyA, legOpts{reqID: 1, selectors: []int{a.id}, bid: 5.0, ask: 5.2})
	seedLeg(s, keyB, legOpts{reqID: 2, selectors: []int{b.id}, bid: 5.6, ask: 5.8})

	// A rotation pass over each: neither may disturb the other.
	s.pointSelectorAt(a, keyA, true)
	s.pointSelectorAt(b, keyB, true)

	if got := legCount(s); got != 2 {
		t.Fatalf("legs = %d, want 2 — one per distinct contract", got)
	}
	if k, _ := displayedKey(s, a.id); k != keyA {
		t.Fatalf("selector A displays %v, want %v — it was evicted by its sibling", k, keyA)
	}
	if k, _ := displayedKey(s, b.id); k != keyB {
		t.Fatalf("selector B displays %v, want %v — it was evicted by its sibling", k, keyB)
	}
}

// TestPointSelectorAt_SharedLegSurvivesOneSelectorMovingOn verifies the
// refcount: when two selectors share a contract and one rolls its strike, the
// other keeps its feed.
func TestPointSelectorAt_SharedLegSurvivesOneSelectorMovingOn(t *testing.T) {
	s := withOfflineClient(newRotationTestSession(nil))
	s.mdLines = mdlines.NewLedger(100, 50)
	s.buses = []*eventbus.Bus{eventbus.New(), eventbus.New()}

	stay := seedSelector(s, selector{id: 5, symbol: "IWM", right: "put", targetDelta: 0.55, busIdxs: []int{0}})
	move := seedSelector(s, selector{id: 6, symbol: "IWM", right: "put", targetDelta: 0.60, busIdxs: []int{1}})
	shared := lk("IWM", "put", 298, "20260817")
	seedLeg(s, shared, legOpts{reqID: 1, selectors: []int{stay.id, move.id}, bid: 1.8, ask: 1.9})

	// `move` rolls onto a different strike, which for it is a live swap.
	next := lk("IWM", "put", 299, "20260817")
	seedLeg(s, next, legOpts{reqID: 2, bid: 2.1, ask: 2.2})
	s.pointSelectorAt(move, next, true)

	if _, _, ok := legHolders(s, shared); !ok {
		t.Fatal("the shared contract was torn down — this is the 2026-08-04 frozen-IWM-298-PUT failure")
	}
	if k, _ := displayedKey(s, stay.id); k != shared {
		t.Fatalf("the staying selector now displays %v, want %v", k, shared)
	}
	if k, _ := displayedKey(s, move.id); k != next {
		t.Fatalf("the moving selector displays %v, want %v (the new leg already quotes)", k, next)
	}
}

// TestPointSelectorAt_PinnedLegIsSharedNotDuplicated verifies a watchlist
// selector landing on a contract an open position already pins reuses that one
// subscription. They used to be separate registries, so the same contract
// burned two market-data lines.
func TestPointSelectorAt_PinnedLegIsSharedNotDuplicated(t *testing.T) {
	s := newRotationTestSession(nil)
	s.mdLines = mdlines.NewLedger(100, 50)
	s.buses = []*eventbus.Bus{eventbus.New()}

	sel := seedSelector(s, selector{id: 5, symbol: "SPY", right: "call", targetDelta: 0.60, busIdxs: []int{0}})
	key := lk("SPY", "call", 640, "20260817")
	seedLeg(s, key, legOpts{reqID: 900, pins: 1, bid: 3.1, ask: 3.2})

	usedBefore, _ := s.mdLines.Status()
	s.pointSelectorAt(sel, key, false)
	usedAfter, _ := s.mdLines.Status()

	if usedAfter != usedBefore {
		t.Fatalf("ledger went %d → %d — a selector joining a pinned contract must cost no extra line", usedBefore, usedAfter)
	}
	sels, pins, ok := legHolders(s, key)
	if !ok || len(sels) != 1 || pins != 1 {
		t.Fatalf("holders = selectors %v / pins %d, want one of each on a single leg", sels, pins)
	}
}

// TestShouldSkipReEstimate_OtherSelectorsLegIsNotMine is the third mechanism
// behind the 2026-08-13 blackout. The old guard inspected whatever leg existed
// for the (symbol, right); QQQ group 32 therefore skipped re-estimating its
// call on pass after pass because group 33's leg looked healthy — while its own
// buses were attached to nothing at all. Skipping actively guaranteed the row
// would stay blank.
func TestShouldSkipReEstimate_OtherSelectorsLegIsNotMine(t *testing.T) {
	s := newRotationTestSession(nil)
	now := time.Now()

	sibling := seedSelector(s, selector{id: 5, symbol: "QQQ", right: "call", targetDelta: 0.55, busIdxs: []int{0}})
	mine := seedSelector(s, selector{id: 6, symbol: "QQQ", right: "call", targetDelta: 0.55, busIdxs: []int{1}})

	// A perfectly healthy leg at exactly the target delta — but the sibling's.
	seedLeg(s, lk("QQQ", "call", 727, "20260817"), legOpts{
		reqID: 1, selectors: []int{sibling.id}, delta: 0.55, deltaSource: "matched",
		subscribedAt: now.Add(-time.Hour), lastTickAt: now.Add(-time.Second),
	})
	s.optChain.lastAnyOptionTick = now.Add(-time.Second)

	if skip, reason := s.shouldSkipReEstimateLocked(sibling, now); !skip {
		t.Fatalf("the leg's OWN selector should skip (reason: %s)", reason)
	}
	if skip, _ := s.shouldSkipReEstimateLocked(mine, now); skip {
		t.Fatal("a selector holding NOTHING skipped because a sibling's leg looked healthy — its row stays blank forever")
	}
}

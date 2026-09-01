package ibkr

import (
	"testing"

	"github.com/SteffRainville/ibkr-go/mdlines"
	"github.com/scmhub/ibapi"
)

// legReqID reads the id a leg is currently subscribed under.
func legReqID(s *Session, key legKey) (int64, bool) {
	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()
	leg, ok := s.optChain.legs[key]
	if !ok {
		return 0, false
	}
	return leg.reqID, true
}

// legBid reads a leg's cached bid.
func legBid(s *Session, key legKey) (float64, bool) {
	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()
	leg, ok := s.optChain.legs[key]
	if !ok {
		return 0, false
	}
	return leg.bid, true
}

// TestDuplicateTickerID_ForeignTicksStopReachingTheLeg is the 2026-09-01
// regression, reduced to its mechanism.
//
// HOOD's held 103 call was re-subscribed under reqID 10194 — an id MU's live
// 925 call was already streaming on. IB refused the request (322), but
// legByReqID had already been repointed, so every MU tick arrived as HOOD's:
// a $2.91 call marked at $21.80, the whole take-profit ladder fired, +639%
// booked on an underlying that had not moved.
//
// The refusal must therefore detach the leg from that id before the next tick
// under it is decoded.
func TestDuplicateTickerID_ForeignTicksStopReachingTheLeg(t *testing.T) {
	s := withOfflineClient(newRotationTestSession(nil))
	hood := lk("HOOD", "call", 103, "20260904")

	const stolenID = 10194
	seedLeg(s, hood, legOpts{reqID: stolenID, pins: 1, bid: 2.88, ask: 2.92})

	s.handleDuplicateTickerID(stolenID)

	// MU's 925 call, still streaming on the id HOOD was refused.
	s.handleOptionTick(stolenID, ibapi.BID, 21.80)

	bid, ok := legBid(s, hood)
	if !ok {
		t.Fatal("HOOD leg vanished on a duplicate-id refusal — it holds an open position and must survive")
	}
	if bid != 2.88 {
		t.Fatalf("HOOD bid = %.2f after a foreign tick on the refused id, want 2.88 unchanged — this is the +639%% mis-mark", bid)
	}
}

// TestDuplicateTickerID_ResubscribesUnderAFreshID checks the other half: the
// contract still needs a feed, so the leg is re-requested under an id that is
// actually ours. Without this the position keeps its row and loses its quote,
// which silently disarms stop-loss and trailing-stop evaluation.
func TestDuplicateTickerID_ResubscribesUnderAFreshID(t *testing.T) {
	s := withOfflineClient(newRotationTestSession(nil))
	hood := lk("HOOD", "call", 103, "20260904")

	const stolenID = 10194
	seedLeg(s, hood, legOpts{reqID: stolenID, pins: 1})

	if !s.handleDuplicateTickerID(stolenID) {
		t.Fatal("handleDuplicateTickerID returned false for a known option leg")
	}

	newID, ok := legReqID(s, hood)
	if !ok {
		t.Fatal("leg dropped instead of re-subscribed on the first duplicate-id refusal")
	}
	if newID == stolenID {
		t.Fatal("leg is still subscribed under the refused id — that id belongs to another live request")
	}

	s.optChain.mu.Lock()
	_, stale := s.optChain.legByReqID[stolenID]
	key, mapped := s.optChain.legByReqID[newID]
	s.optChain.mu.Unlock()

	if stale {
		t.Error("legByReqID still maps the refused id — foreign ticks would keep arriving as this contract")
	}
	if !mapped || key != hood {
		t.Errorf("legByReqID[%d] = %v (mapped=%v), want the HOOD leg", newID, key, mapped)
	}
}

// TestDuplicateTickerID_DoesNotCancelTheRealOwnersFeed pins the subtler half.
// The refused id is another request's, so neither the cancel nor the
// market-data-line release may run against it: cancelling is what killed
// HOOD's own subscription 90 seconds before the mis-pricing, and releasing
// frees a ledger slot for a line IB is still serving.
func TestDuplicateTickerID_DoesNotCancelTheRealOwnersFeed(t *testing.T) {
	s := withOfflineClient(newRotationTestSession(nil))
	hood := lk("HOOD", "call", 103, "20260904")

	// The real owner's line, granted under the id our leg is about to be
	// refused on.
	const stolenID = 10194
	s.mdLines.GrantGuaranteed(stolenID, mdlines.CategoryPosition)

	seedLeg(s, hood, legOpts{reqID: stolenID, pins: 1})
	s.handleDuplicateTickerID(stolenID)

	// Two lines now: the real owner's, untouched, plus the fresh one our leg
	// was re-subscribed under. If the repair had released the refused id, the
	// ledger would show one and IB would still be serving a line nobody counts.
	if used, _, _, _, _, _ := s.mdLines.StatusAll(); used != 2 {
		t.Fatalf("market-data lines in use = %d after the repair, want 2 (the real owner's + the replacement) — the refused id was released out from under its owner", used)
	}

	// And the owner's own release still works, leaving only the replacement.
	s.mdLines.Release(stolenID)
	if used, _, _, _, _, _ := s.mdLines.StatusAll(); used != 1 {
		t.Fatalf("market-data lines in use = %d after the real owner released, want 1", used)
	}
}

// TestDuplicateTickerID_RepairsAreBounded keeps the error path from becoming a
// request loop if some unknown second source of collisions ever reappears.
// After the budget, the leg is dropped rather than re-requested forever — with
// an operator alert, since a held contract with no quote is the dangerous end
// state.
func TestDuplicateTickerID_RepairsAreBounded(t *testing.T) {
	var alerts []ErrorEvent
	s := withOfflineClient(NewSession(Options{
		OnError: func(e ErrorEvent) { alerts = append(alerts, e) },
	}, nil, nil))
	hood := lk("HOOD", "call", 103, "20260904")

	seedLeg(s, hood, legOpts{reqID: 10194, pins: 1})

	for i := 0; i <= maxDupTickerRepairs; i++ {
		id, ok := legReqID(s, hood)
		if !ok {
			break
		}
		s.handleDuplicateTickerID(id)
	}

	if _, ok := legReqID(s, hood); ok {
		t.Fatalf("leg survived more than %d duplicate-id repairs — the error path is looping", maxDupTickerRepairs)
	}
	if len(alerts) == 0 {
		t.Error("no operator alert when a held contract was left without market data")
	}
}

// TestDuplicateTickerID_ProbeCandidateIsLeftAlone covers the probe side. A
// candidate refused as a duplicate must be flagged so the resolution cleanup
// skips both the cancel and the release — that cancel is what tore down the
// contract genuinely streaming on the id.
func TestDuplicateTickerID_ProbeCandidateIsLeftAlone(t *testing.T) {
	s := withOfflineClient(newRotationTestSession(nil))

	const stolenID = 10185
	cand := &deltaCandidate{symbol: "HLIT", right: "call", strike: 25, expiry: "20260918", reqID: stolenID}
	s.optChain.mu.Lock()
	s.optChain.deltaCands[stolenID] = cand
	s.optChain.mu.Unlock()

	if !s.handleDuplicateTickerID(stolenID) {
		t.Fatal("handleDuplicateTickerID returned false for a known delta candidate")
	}
	if !cand.dupTicker {
		t.Fatal("candidate not flagged — resolveDeltaCandidates would cancel an id owned by another live request")
	}

	// The candidate stays discoverable so classifyCandidateErrors can still
	// read the cause off it.
	s.optChain.mu.Lock()
	_, still := s.optChain.deltaCands[stolenID]
	s.optChain.mu.Unlock()
	if !still {
		t.Error("candidate dropped from deltaCands — the probe can no longer report why it failed")
	}
}

// TestDuplicateTickerID_IgnoresUnknownReqIDs keeps the handler from claiming
// errors that belong to other subsystems' requests.
func TestDuplicateTickerID_IgnoresUnknownReqIDs(t *testing.T) {
	s := withOfflineClient(newRotationTestSession(nil))
	if s.handleDuplicateTickerID(999999) {
		t.Error("claimed a reqID that is neither a leg nor a delta candidate")
	}
}

// Tests for the single-flight entry probe (entryprobe.go). The scenarios are
// the incidents the old hand-built coordination produced:
//
//	2026-07-27 SPY  two robots, one selector: the loser skipped the entry
//	2026-09-02 ORCL simultaneous callers each became the owner
//	2026-09-22 AMD  a joiner read "failed" for a probe that resolved
//	2026-09-28 AGQ  joiners timed out a moment before the owner published
package ibkr

import (
	"sync"
	"testing"
	"time"

	"github.com/SteffRainville/ibkr-go/eventbus"
)

// newProbeTestSession: one SPY put selector (id 1, busIdx 0, δ 0.40), a chain
// loaded for today, SPY at 735, and an offline client so a real launch runs.
func newProbeTestSession(sub Subscriber) *Session {
	s := withOfflineClient(NewSession(Options{}, nil, nil))
	s.buses = []*eventbus.Bus{sub.Bus()}
	s.optChain.selectors = []selector{{id: 1, symbol: "SPY", right: "put", targetDelta: 0.40, busIdxs: []int{0}}}
	s.optChain.chains[chainKey{symbol: "SPY"}] = &chain{
		day: tradingDay(time.Now()), expiry: tradingDay(time.Now()),
		strikes: map[string][]float64{"put": {725, 730, 735, 740, 745}},
	}
	s.mktData.bid["SPY"] = quote{Price: 734.9}
	s.mktData.ask["SPY"] = quote{Price: 735.1}
	return s
}

var spyQuote = OptionQuote{Strike: 735, Expiry: "20260928", Bid: 6.50, Ask: 6.60, Delta: -0.41}

// inFlight registers a probe on selector 1 as if a sibling owned it.
func inFlight(s *Session, deadline time.Time) *probeCall {
	call := &probeCall{done: make(chan struct{}), deadline: deadline}
	s.optChain.mu.Lock()
	s.optChain.probes[1] = call
	s.optChain.mu.Unlock()
	return call
}

func finish(s *Session, call *probeCall, q OptionQuote, res EntryStrikeResult) {
	s.optChain.mu.Lock()
	call.q, call.res, call.finishedAt = q, res, time.Now()
	close(call.done)
	s.optChain.mu.Unlock()
}

// AGQ: the owner runs its full window and publishes after the joiner's own
// timeout. The joiner must still get the answer.
func TestResolveEntryStrike_JoinerWaitsForOwnerNotItsOwnClock(t *testing.T) {
	sub := newTestSubscriber()
	s := newProbeTestSession(sub)
	call := inFlight(s, time.Now().Add(300*time.Millisecond))
	go func() {
		time.Sleep(350 * time.Millisecond) // past the owner's deadline: the resolve step
		finish(s, call, spyQuote, EntryStrikeResult{OK: true})
	}()

	q, res := s.ResolveEntryStrike(sub, "SPY", "put", 50*time.Millisecond)
	if !res.OK || q.Strike != 735 {
		t.Fatalf("joiner got (%+v, %+v), want the owner's strike 735", q, res)
	}
}

// AMD / SPY: a joiner receives exactly the owner's outcome, failure included,
// with the owner's own cause rather than a generic one.
func TestResolveEntryStrike_JoinerSharesOwnersFailureCause(t *testing.T) {
	sub := newTestSubscriber()
	s := newProbeTestSession(sub)
	call := inFlight(s, time.Now().Add(time.Second))
	cause := EntryStrikeResult{Reason: entryFailNoSubscription, Detail: "IB 354", IBCode: 354}
	go func() {
		time.Sleep(50 * time.Millisecond)
		finish(s, call, OptionQuote{}, cause)
	}()

	_, res := s.ResolveEntryStrike(sub, "SPY", "put", time.Second)
	if res != cause {
		t.Fatalf("joiner got %+v, want the owner's cause %+v", res, cause)
	}
}

// ORCL: simultaneous callers on one selector launch exactly one probe and all
// receive the same answer.
func TestResolveEntryStrike_ConcurrentCallersShareOneProbe(t *testing.T) {
	sub := newTestSubscriber()
	s := newProbeTestSession(sub)

	const callers = 5
	var wg sync.WaitGroup
	results := make([]EntryStrikeResult, callers)
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, results[i] = s.ResolveEntryStrike(sub, "SPY", "put", 200*time.Millisecond)
		}()
	}
	// Let the candidates go out, then count them: one probe's worth.
	time.Sleep(100 * time.Millisecond)
	s.optChain.mu.Lock()
	launched := len(s.optChain.deltaCands)
	s.optChain.mu.Unlock()
	wg.Wait()

	if launched == 0 || launched > deltaProbeITMCandidates+deltaProbeOTMCandidates {
		t.Fatalf("%d candidates in flight, want one probe's worth", launched)
	}
	for i, r := range results {
		if r != results[0] {
			t.Fatalf("caller %d got %+v, caller 0 got %+v — one probe, one answer", i, r, results[0])
		}
	}
	if results[0].Reason != entryFailQuoteTimeout {
		t.Errorf("reason = %q, want %q (nothing ticked)", results[0].Reason, entryFailQuoteTimeout)
	}
	if len(s.optChain.deltaCands) != 0 {
		t.Errorf("%d candidate lines left open", len(s.optChain.deltaCands))
	}
}

// The owner path end to end: deltas arrive, the nearest to the target wins,
// every line is released.
func TestResolveEntryStrike_OwnerPicksNearestDelta(t *testing.T) {
	sub := newTestSubscriber()
	s := newProbeTestSession(sub)

	go func() {
		deltas := map[float64]float64{725: -0.22, 730: -0.31, 735: -0.39, 740: -0.52, 745: -0.63}
		for {
			time.Sleep(20 * time.Millisecond)
			s.optChain.mu.Lock()
			n := len(s.optChain.deltaCands)
			var ids []int64
			var strikes []float64
			for id, c := range s.optChain.deltaCands {
				ids, strikes = append(ids, id), append(strikes, c.strike)
			}
			s.optChain.mu.Unlock()
			if n == 0 {
				continue
			}
			for i, id := range ids {
				s.handleOptionTick(id, 1, 6.50) // bid
				s.handleOptionTick(id, 2, 6.60) // ask
				s.TickOptionComputation(id, 13, 0, 0.2, deltas[strikes[i]], 0, 0, 0, 0, 0, 735)
			}
			return
		}
	}()

	q, res := s.ResolveEntryStrike(sub, "SPY", "put", 2*time.Second)
	if !res.OK || q.Strike != 735 {
		t.Fatalf("got (%+v, %+v), want strike 735 (δ -0.39, nearest 0.40)", q, res)
	}
	if len(s.optChain.deltaCands) != 0 {
		t.Errorf("%d candidate lines left open", len(s.optChain.deltaCands))
	}
}

// A success is reused for resolvedEntryTTL without asking IB again. s.client
// is nil, so any launch would panic.
func TestResolveEntryStrike_RecentSuccessReused(t *testing.T) {
	sub := newTestSubscriber()
	s := newProbeTestSession(sub)
	s.client = nil
	finish(s, inFlight(s, time.Now()), spyQuote, EntryStrikeResult{OK: true})

	q, res := s.ResolveEntryStrike(sub, "SPY", "put", time.Second)
	if !res.OK || q != spyQuote {
		t.Fatalf("got (%+v, %+v), want the shared quote", q, res)
	}
}

// A failure is returned for entryProbeFailCooldown, then IB is asked again.
func TestResolveEntryStrike_FailureCooldown(t *testing.T) {
	sub := newTestSubscriber()
	s := newProbeTestSession(sub)
	cause := EntryStrikeResult{Reason: entryFailContractInvalid, Detail: "IB 200"}
	old := inFlight(s, time.Now())
	finish(s, old, OptionQuote{}, cause)

	if _, res := s.ResolveEntryStrike(sub, "SPY", "put", 50*time.Millisecond); res != cause {
		t.Fatalf("inside the cooldown got %+v, want the previous cause %+v", res, cause)
	}

	old.finishedAt = time.Now().Add(-entryProbeFailCooldown - time.Second)
	_, res := s.ResolveEntryStrike(sub, "SPY", "put", 50*time.Millisecond)
	if res == cause || s.optChain.probes[1] == old {
		t.Fatal("after the cooldown no new probe was launched")
	}
}

func TestResolveEntryStrike_NoChainNoEntry(t *testing.T) {
	sub := newTestSubscriber()
	s := newProbeTestSession(sub)
	s.client = nil
	s.optChain.chains[chainKey{symbol: "SPY"}].day = "20000101"

	if _, res := s.ResolveEntryStrike(sub, "SPY", "put", time.Second); res.Reason != entryFailNoChain {
		t.Fatalf("reason = %q, want %q for a chain not loaded today", res.Reason, entryFailNoChain)
	}
	if _, res := s.ResolveEntryStrike(sub, "SPY", "call", time.Second); res.Reason != entryFailNoChain {
		t.Fatalf("reason = %q, want %q for a right with no selector", res.Reason, entryFailNoChain)
	}
}

func TestResolveEntryStrike_ATMTargetRefused(t *testing.T) {
	sub := newTestSubscriber()
	s := newProbeTestSession(sub)
	s.client = nil
	s.optChain.selectors[0].targetDelta = 0.50
	if _, res := s.ResolveEntryStrike(sub, "SPY", "put", time.Second); res.Reason != entryFailDeltaTargetATM {
		t.Fatalf("reason = %q, want %q", res.Reason, entryFailDeltaTargetATM)
	}
}

// The join bound exists only for a defect: an owner that never finishes.
func TestResolveEntryStrike_JoinBoundedIfOwnerNeverFinishes(t *testing.T) {
	sub := newTestSubscriber()
	s := newProbeTestSession(sub)
	inFlight(s, time.Now())

	start := time.Now()
	_, res := s.ResolveEntryStrike(sub, "SPY", "put", time.Second)
	if res.Reason != entryFailSiblingFailed {
		t.Fatalf("reason = %q, want %q", res.Reason, entryFailSiblingFailed)
	}
	if el := time.Since(start); el < probeJoinSafety || el > probeJoinSafety+time.Second {
		t.Fatalf("gave up after %s, want ≈ probeJoinSafety (%s) past the owner's deadline", el, probeJoinSafety)
	}
}

// ── resolveDeltaCandidates ────────────────────────────────────────────────

func readyCandidate(strike, delta, bid, ask float64) *deltaCandidate {
	// dupTicker: the release path then skips CancelMktData.
	return &deltaCandidate{symbol: "SPY", right: "put", strike: strike, expiry: "20260928",
		delta: delta, bid: bid, ask: ask, ready: true, dupTicker: true}
}

func TestResolveDeltaCandidates(t *testing.T) {
	sel := selector{id: 1, symbol: "SPY", right: "put", targetDelta: 0.40}
	s := NewSession(Options{}, nil, nil)

	q, res := s.resolveDeltaCandidates(sel, []*deltaCandidate{
		readyCandidate(730, -0.30, 4, 4.1), readyCandidate(735, -0.41, 6.5, 6.6), readyCandidate(740, -0.52, 9, 9.2)})
	if !res.OK || q.Strike != 735 {
		t.Errorf("nearest delta: got (%+v, %+v), want strike 735", q, res)
	}

	_, res = s.resolveDeltaCandidates(sel, []*deltaCandidate{readyCandidate(735, -0.41, 0, 0)})
	if res.Reason != entryFailDeltaNoPrice {
		t.Errorf("unpriced winner: reason = %q, want %q", res.Reason, entryFailDeltaNoPrice)
	}

	_, res = s.resolveDeltaCandidates(sel, []*deltaCandidate{{strike: 735, dupTicker: true, errCode: 354, errMsg: "not subscribed"}})
	if res.Reason != entryFailNoSubscription {
		t.Errorf("no ready candidate: reason = %q, want %q", res.Reason, entryFailNoSubscription)
	}
}

// A candidate IB rejected outright (error 200) will never report, so it must
// not hold the probe open.
func TestDeltaCandidatesSettled_RejectedCountsAsReported(t *testing.T) {
	cands := []*deltaCandidate{{ready: true, bid: 1, ask: 1.1, delta: 0.44}, {ready: true, bid: 1, ask: 1.1, delta: 0.31}, {rejected: true}}
	if !deltaCandidatesSettled(cands, 0.40) {
		t.Fatal("a rejected candidate kept the probe waiting out its full timeout")
	}
	cands = append(cands, &deltaCandidate{})
	if deltaCandidatesSettled(cands, 0.40) {
		t.Fatal("settled while a live candidate had not yet reported")
	}
}

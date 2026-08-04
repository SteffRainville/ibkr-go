// Tests for ResolveEntryStrike's handling of a concurrent sibling call
// resolving the exact same (groupID, right) — the 2026-07-27 SPY put
// incident where two robots watching the same underlying both received the
// identical crossover, but each independently probed IB for a strike; the
// loser of that race came back with no quote and silently skipped the
// entry it should have taken alongside its sibling. See CLAUDE.md for the
// deltaRes[groupID|right] "in flight" convention these tests exercise.
package ibkr

import (
	"testing"
	"time"

	"github.com/SteffRainville/ibkr-go/eventbus"
	"github.com/SteffRainville/ibkr-go/quotes"
)

// newResolveEntryTestSession builds a session with a resolution group and
// chain info already in place (bypassing buildOptionGroups/the conId+chain
// round trip, neither of which is under test here) so ResolveEntryStrike's
// early guards pass without a real IB connection. sub is wired into s.buses
// at busIdx 0, and the seeded group carries busIdxs:[0], mirroring what a
// real Run() would set up — so groupForSymbolLocked matches sub for real
// rather than falling back on the busIdx<0 bypass.
func newResolveEntryTestSession(sub Subscriber) *Session {
	s := NewSession(Options{}, nil, nil)
	s.buses = []*eventbus.Bus{sub.Bus()}
	s.optChain.rotation = []optResGroup{
		{groupID: 1, symbol: "SPY", targetDeltaCall: 0.65, targetDeltaPut: 0.65, busIdxs: []int{0}},
	}
	s.optChain.lastChainInfo = map[string]chainSnapshot{
		"SPY": {expiry: "20260731", strikes: []float64{725, 730, 735, 740, 745}},
	}
	return s
}

// TestResolveEntryStrike_SiblingInFlightWaitsInsteadOfDuplicateProbe pins
// down the fix: when deltaRes["g1|put"] already belongs to a concurrent
// sibling call, ResolveEntryStrike must not build its own candidate set and
// issue its own ReqMktData probes (s.client is nil here — doing so would
// panic) — it must wait for the sibling's result and return the identical
// resolved contract once the sibling publishes it.
func TestResolveEntryStrike_SiblingInFlightWaitsInsteadOfDuplicateProbe(t *testing.T) {
	sub := newTestSubscriber()
	s := newResolveEntryTestSession(sub)

	// Simulate a sibling call already owning the resolution for SPY|put.
	s.optChain.mu.Lock()
	s.optChain.deltaRes[retryKeyLeg(1, "put")] = &deltaResolution{groupID: 1, symbol: "SPY", right: "put", targetDelta: 0.65}
	s.optChain.mu.Unlock()

	const simulatedProbeDelay = 150 * time.Millisecond
	go func() {
		time.Sleep(simulatedProbeDelay)
		key := quotes.ContractKey{Symbol: "SPY", Right: "put", Strike: 735, Expiry: "20260731"}
		s.book.SetOptionBid(key, 6.50)
		s.book.SetOptionAsk(key, 6.60)
		s.optChain.mu.Lock()
		delete(s.optChain.deltaRes, retryKeyLeg(1, "put"))
		if s.optChain.resolvedEntry == nil {
			s.optChain.resolvedEntry = make(map[string]resolvedEntryLeg)
		}
		s.optChain.resolvedEntry[retryKeyLeg(1, "put")] = resolvedEntryLeg{strike: 735, expiry: "20260731", delta: -0.65, at: time.Now()}
		s.optChain.mu.Unlock()
	}()

	start := time.Now()
	q, ok := s.ResolveEntryStrike(sub, "SPY", "put", 2*time.Second)
	elapsed := time.Since(start)

	if !ok {
		t.Fatal("expected ResolveEntryStrike to return the sibling's resolved contract, got ok=false")
	}
	if q.Strike != 735 || q.Expiry != "20260731" {
		t.Fatalf("got strike=%.0f expiry=%s, want the sibling's contract strike=735 expiry=20260731", q.Strike, q.Expiry)
	}
	if elapsed < simulatedProbeDelay {
		t.Fatalf("returned in %s, before the sibling's simulated probe delay of %s — did it actually wait?", elapsed, simulatedProbeDelay)
	}
	if elapsed > time.Second {
		t.Fatalf("took %s to notice the sibling's result — poll loop may be broken", elapsed)
	}
}

// TestResolveEntryStrike_SiblingInFlightNeverResolves verifies a waiting
// caller gives up at its own timeout (rather than hanging) when the owning
// sibling's probe never produces a result — e.g. the sibling's own probe
// also failed and found nothing to publish.
func TestResolveEntryStrike_SiblingInFlightNeverResolves(t *testing.T) {
	sub := newTestSubscriber()
	s := newResolveEntryTestSession(sub)

	s.optChain.mu.Lock()
	s.optChain.deltaRes[retryKeyLeg(1, "put")] = &deltaResolution{groupID: 1, symbol: "SPY", right: "put", targetDelta: 0.65}
	s.optChain.mu.Unlock()

	const timeout = 300 * time.Millisecond
	start := time.Now()
	q, ok := s.ResolveEntryStrike(sub, "SPY", "put", timeout)
	elapsed := time.Since(start)

	if ok {
		t.Fatalf("expected no quote when the owning sibling never resolves, got %+v", q)
	}
	if elapsed < timeout {
		t.Fatalf("returned after %s, before its own timeout of %s", elapsed, timeout)
	}
	if elapsed > timeout+500*time.Millisecond {
		t.Fatalf("took %s to give up — well past its %s timeout", elapsed, timeout)
	}
}

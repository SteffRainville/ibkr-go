// Tests for entryProbeLaunchCooldown — the 2026-07-31 IWM divergence fix.
// Before this fix, ORBtrader gated the entire call to ResolveEntryStrike
// behind a per-robot, per-symbol|direction cooldown, so a robot whose own
// cooldown hadn't cleared never reached this function at all — not even the
// free sharedResolvedEntry read or joining an in-flight sibling probe. These
// tests pin down the fix: the cooldown now lives here, scoped to
// groupID|right (shared across every caller), and only gates becoming the
// OWNER of a fresh batch of ReqMktData candidate probes — never the free
// reads/joins. See resolveentrystrike_concurrent_test.go for the sibling-
// wait tests this complements.
package ibkr

import (
	"testing"
	"time"

	"github.com/SteffRainville/ibkr-go/quotes"
)

// TestResolveEntryStrike_LaunchCooldownBlocksRepeatedOwnerAttempt proves a
// recent owner-probe launch for groupID|right blocks a new one, returning
// immediately rather than blocking or (with s.client nil in this harness)
// panicking on a real ReqMktData call — i.e. no new probe was launched.
func TestResolveEntryStrike_LaunchCooldownBlocksRepeatedOwnerAttempt(t *testing.T) {
	sub := newTestSubscriber()
	s := newResolveEntryTestSession(sub)

	s.optChain.mu.Lock()
	s.optChain.lastProbeLaunch[retryKeyLeg(1, "put")] = time.Now()
	s.optChain.mu.Unlock()

	start := time.Now()
	q, ok := s.ResolveEntryStrike(sub, "SPY", "put", 2*time.Second)
	elapsed := time.Since(start)

	if ok {
		t.Fatalf("expected no quote while the launch cooldown is active, got %+v", q)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("took %s to return — should fail fast on a cooled-down key, not block", elapsed)
	}
}

// TestResolveEntryStrike_LaunchCooldownDoesNotBlockSharedRead proves the free
// sharedResolvedEntry path — a sibling's recently resolved contract, Book-
// priced — is reachable even while this exact key is inside the launch
// cooldown. This is the actual bug: previously ORBtrader's own cooldown
// gate sat in front of this call and blocked it from ever running, so a
// robot could never see what a sibling had already resolved.
func TestResolveEntryStrike_LaunchCooldownDoesNotBlockSharedRead(t *testing.T) {
	sub := newTestSubscriber()
	s := newResolveEntryTestSession(sub)

	key := quotes.ContractKey{Symbol: "SPY", Right: "put", Strike: 735, Expiry: "20260731"}
	s.book.SetOptionBid(key, 6.50)
	s.book.SetOptionAsk(key, 6.60)

	s.optChain.mu.Lock()
	s.optChain.resolvedEntry[retryKeyLeg(1, "put")] = resolvedEntryLeg{strike: 735, expiry: "20260731", delta: -0.65, at: time.Now()}
	// A launch cooldown is simultaneously active for this exact key — proves
	// it doesn't gate the shared-cache read, only becoming a fresh owner.
	s.optChain.lastProbeLaunch[retryKeyLeg(1, "put")] = time.Now()
	s.optChain.mu.Unlock()

	start := time.Now()
	q, ok := s.ResolveEntryStrike(sub, "SPY", "put", 2*time.Second)
	elapsed := time.Since(start)

	if !ok {
		t.Fatal("expected the shared resolved contract despite an active launch cooldown, got ok=false")
	}
	if q.Strike != 735 || q.Expiry != "20260731" {
		t.Fatalf("got strike=%.0f expiry=%s, want strike=735 expiry=20260731", q.Strike, q.Expiry)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("took %s — the shared-cache read should be instant", elapsed)
	}
}

// TestResolveEntryStrike_LaunchCooldownDoesNotBlockJoiningInFlightSibling
// proves the join-an-in-flight-sibling path is likewise unaffected by an
// active launch cooldown for the same key.
func TestResolveEntryStrike_LaunchCooldownDoesNotBlockJoiningInFlightSibling(t *testing.T) {
	sub := newTestSubscriber()
	s := newResolveEntryTestSession(sub)

	s.optChain.mu.Lock()
	s.optChain.deltaRes[retryKeyLeg(1, "put")] = &deltaResolution{groupID: 1, symbol: "SPY", right: "put", targetDelta: 0.65}
	s.optChain.lastProbeLaunch[retryKeyLeg(1, "put")] = time.Now()
	s.optChain.mu.Unlock()

	const simulatedProbeDelay = 100 * time.Millisecond
	go func() {
		time.Sleep(simulatedProbeDelay)
		key := quotes.ContractKey{Symbol: "SPY", Right: "put", Strike: 735, Expiry: "20260731"}
		s.book.SetOptionBid(key, 6.50)
		s.book.SetOptionAsk(key, 6.60)
		s.optChain.mu.Lock()
		delete(s.optChain.deltaRes, retryKeyLeg(1, "put"))
		s.optChain.resolvedEntry[retryKeyLeg(1, "put")] = resolvedEntryLeg{strike: 735, expiry: "20260731", delta: -0.65, at: time.Now()}
		s.optChain.mu.Unlock()
	}()

	q, ok := s.ResolveEntryStrike(sub, "SPY", "put", 2*time.Second)

	if !ok {
		t.Fatal("expected to join and receive the sibling's resolved contract despite an active launch cooldown")
	}
	if q.Strike != 735 {
		t.Fatalf("got strike=%.0f, want 735", q.Strike)
	}
}

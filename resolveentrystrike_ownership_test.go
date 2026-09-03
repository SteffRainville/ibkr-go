// Tests for entry-probe OWNERSHIP — which call is allowed to launch a fresh
// batch of delta-candidate probes for a selector, and which resolution that
// call is then allowed to read back.
//
// The 2026-09-03 MU incident: ResolveEntryStrike read "is a sibling already
// probing?" at the top of the function and wrote deltaRes ninety lines and
// three lock cycles later. Three robots share MU|call at option_delay=3 /
// target_delta=0.55 — one selector id, three event-loop goroutines, all
// reacting to the same VW-MACD crossover — so all three read false, all three
// launched six probes, and the last write silently replaced the others. The
// survivor resolved a candidate set it had never polled; the displaced callers
// reported "option_quote_timeout: delta resolution for MU call vanished before
// it could be read", a diagnosis IB had no part in. It fired 15-38 times a day
// for months.
//
// The launch loop itself is not exercised here: s.client is a concrete
// *ibapi.EClient, nil in this harness, and ReqMktData dereferences it
// immediately. reserveEntryProbe is the seam that makes the decision testable
// without a broker — the same split deadleg_test.go relies on for
// planDeadLegRepairsLocked.
package ibkr

import (
	"sync"
	"testing"
	"time"
)

// TestReserveEntryProbe_ExactlyOneOwnerUnderConcurrency is the direct
// regression test. Against the pre-fix code every goroutine here became the
// owner; the invariant is that exactly one does and the rest are told to join.
func TestReserveEntryProbe_ExactlyOneOwnerUnderConcurrency(t *testing.T) {
	sub := newTestSubscriber()
	s := newResolveEntryTestSession(sub)
	sel := s.optChain.rotation[0]

	const callers = 8
	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	done.Add(callers)

	owners := make([]*deltaResolution, callers)
	joins := make([]bool, callers)
	fails := make([]EntryStrikeResult, callers)

	for i := range callers {
		go func() {
			defer done.Done()
			start.Wait()
			owners[i], joins[i], fails[i] = s.reserveEntryProbe(sel)
		}()
	}
	start.Done()
	done.Wait()

	var owned, joined int
	var firstOwner *deltaResolution
	for i := range callers {
		switch {
		case owners[i] != nil:
			owned++
			if firstOwner == nil {
				firstOwner = owners[i]
			} else if owners[i] == firstOwner {
				t.Fatalf("caller %d returned the SAME resolution as an earlier owner", i)
			}
			if joins[i] {
				t.Errorf("caller %d: got both a resolution and join=true", i)
			}
		case joins[i]:
			joined++
			if fails[i].Reason != "" {
				t.Errorf("caller %d: join=true carries reason %q, want none", i, fails[i].Reason)
			}
		default:
			t.Errorf("caller %d: neither owner nor join — failed with %q: %s", i, fails[i].Reason, fails[i].Detail)
		}
	}
	if owned != 1 {
		t.Fatalf("owners = %d, want exactly 1 — %d concurrent callers each launching their own probe batch is the MU bug", owned, owned)
	}
	if joined != callers-1 {
		t.Fatalf("joiners = %d, want %d", joined, callers-1)
	}

	// The one owner must be the resolution actually published for the
	// selector, or resolveDeltaCandidates would refuse to read it back.
	s.optChain.mu.Lock()
	published := s.optChain.deltaRes[sel.id]
	s.optChain.mu.Unlock()
	if published != firstOwner {
		t.Fatalf("deltaRes[%d] = %p, want the owner's own resolution %p", sel.id, published, firstOwner)
	}
	if published.expiry != "20260731" || len(published.allStrikes) == 0 {
		t.Errorf("published resolution = %+v, want the chain snapshot's expiry and strikes copied in", published)
	}
}

// TestReserveEntryProbe_StampsLaunchAtReservation proves lastProbeLaunch is
// written by the reservation itself, not after the IB round trip. Stamping it
// late left a second window in which two callers both passed the cooldown
// check and both went on to launch.
func TestReserveEntryProbe_StampsLaunchAtReservation(t *testing.T) {
	sub := newTestSubscriber()
	s := newResolveEntryTestSession(sub)
	sel := s.optChain.rotation[0]

	res, join, fail := s.reserveEntryProbe(sel)
	if res == nil || join {
		t.Fatalf("first call: res=%v join=%v fail=%q, want to become the owner", res, join, fail.Reason)
	}

	s.optChain.mu.Lock()
	stamped := s.optChain.lastProbeLaunch[sel.id]
	s.optChain.mu.Unlock()
	if stamped.IsZero() {
		t.Fatal("lastProbeLaunch not stamped at reservation — a second caller could still pass the cooldown and launch its own batch")
	}

	// Release the reservation as a completed probe would, leaving only the
	// cooldown standing. The next caller must be refused, not made an owner.
	s.optChain.mu.Lock()
	delete(s.optChain.deltaRes, sel.id)
	s.optChain.mu.Unlock()

	res2, join2, fail2 := s.reserveEntryProbe(sel)
	if res2 != nil || join2 {
		t.Fatalf("second call: res=%v join=%v, want refusal on the launch cooldown", res2, join2)
	}
	if fail2.Reason != entryFailProbeCooldown {
		t.Errorf("reason = %q, want %q", fail2.Reason, entryFailProbeCooldown)
	}
}

// TestAbandonEntryProbe_ReleasesReservationAndPublishesCause covers the two
// paths that now fail AFTER reserving (no candidates, no md lines). Leaving
// the reservation published would park every later caller in
// waitForEntryResolution on a probe that will never answer; publishing no
// cause would make a sibling already parked there wait out its full timeout to
// learn nothing.
func TestAbandonEntryProbe_ReleasesReservationAndPublishesCause(t *testing.T) {
	sub := newTestSubscriber()
	s := newResolveEntryTestSession(sub)
	sel := s.optChain.rotation[0]

	res, _, _ := s.reserveEntryProbe(sel)
	if res == nil {
		t.Fatal("failed to become the owner")
	}

	want := EntryStrikeResult{Reason: entryFailNoCandidates, Detail: "no put strike for SPY near underlying 0.00"}
	_, got := s.abandonEntryProbe(sel, res, want)
	if got.Reason != want.Reason || got.Detail != want.Detail {
		t.Errorf("returned %+v, want %+v", got, want)
	}

	s.optChain.mu.Lock()
	_, stillOwned := s.optChain.deltaRes[sel.id]
	cause, published := s.optChain.lastEntryFailure[sel.id]
	s.optChain.mu.Unlock()
	if stillOwned {
		t.Error("reservation still published after abandon — the selector is now permanently in flight")
	}
	if !published || cause.Reason != want.Reason {
		t.Errorf("lastEntryFailure = %+v (published=%v), want the real cause %q", cause, published, want.Reason)
	}

	// A sibling arriving now must get that real cause back promptly, not sit
	// out its own timeout.
	start := time.Now()
	_, joined := s.waitForEntryResolution(sel, 2*time.Second)
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("sibling waited %v, want a prompt answer", elapsed)
	}
	if joined.Reason != want.Reason {
		t.Errorf("sibling got %q, want the owner's real cause %q", joined.Reason, want.Reason)
	}
}

// TestAbandonEntryProbe_LeavesAnotherCallersReservationAlone: a caller only
// ever releases its own. Deleting whatever happens to be under the key is the
// same class of mistake as resolving whatever happens to be under it.
func TestAbandonEntryProbe_LeavesAnotherCallersReservationAlone(t *testing.T) {
	sub := newTestSubscriber()
	s := newResolveEntryTestSession(sub)
	sel := s.optChain.rotation[0]

	stranger := &deltaResolution{selectorID: sel.id, symbol: "SPY", right: "put", targetDelta: 0.65}
	s.optChain.mu.Lock()
	s.optChain.deltaRes[sel.id] = stranger
	s.optChain.mu.Unlock()

	mine := &deltaResolution{selectorID: sel.id, symbol: "SPY", right: "put", targetDelta: 0.65}
	s.abandonEntryProbe(sel, mine, EntryStrikeResult{Reason: entryFailNoMDLines})

	s.optChain.mu.Lock()
	cur := s.optChain.deltaRes[sel.id]
	s.optChain.mu.Unlock()
	if cur != stranger {
		t.Fatalf("deltaRes[%d] = %p, want the stranger's reservation %p left untouched", sel.id, cur, stranger)
	}
}

// TestResolveDeltaCandidates_RefusesAForeignResolution pins the second half of
// the fix. Before it, this function re-read deltaRes and resolved whatever it
// found — so the caller whose write landed last had its candidate set consumed
// by a caller that never polled it, which on 2026-09-02 (ORCL sel=54) meant
// resolving against a batch carrying a different EXPIRY than the one selected.
// A resolution that is not this call's own must be reported as the internal
// defect it is, never as an IB quote timeout.
func TestResolveDeltaCandidates_RefusesAForeignResolution(t *testing.T) {
	sub := newTestSubscriber()
	s := newResolveEntryTestSession(sub)
	sel := s.optChain.rotation[0]

	stranger := &deltaResolution{selectorID: sel.id, symbol: "SPY", right: "put", targetDelta: 0.65, expiry: "20260807"}
	s.optChain.mu.Lock()
	s.optChain.deltaRes[sel.id] = stranger
	s.optChain.mu.Unlock()

	// No candidates on `mine`, so the release path reaches no CancelMktData —
	// s.client is nil in this harness.
	mine := &deltaResolution{selectorID: sel.id, symbol: "SPY", right: "put", targetDelta: 0.65, expiry: "20260731"}
	q, res := s.resolveDeltaCandidates(sel, mine)

	if res.OK {
		t.Fatalf("resolved a foreign resolution to %+v — this is how a wrong strike gets traded", q)
	}
	if res.Reason != entryFailProbeOwnershipLost {
		t.Errorf("reason = %q, want %q — an internal defect must not be reported as an IB condition", res.Reason, entryFailProbeOwnershipLost)
	}
	if res.Reason == entryFailQuoteTimeout {
		t.Error("still reporting the fabricated quote timeout")
	}

	s.optChain.mu.Lock()
	cur := s.optChain.deltaRes[sel.id]
	s.optChain.mu.Unlock()
	if cur != stranger {
		t.Fatalf("deltaRes[%d] = %p, want the stranger's resolution %p left for its own owner to read", sel.id, cur, stranger)
	}
}

// TestResolveDeltaCandidates_ConsumesItsOwnResolution is the positive case:
// the ownership check must not have made the ordinary path unreachable.
func TestResolveDeltaCandidates_ConsumesItsOwnResolution(t *testing.T) {
	sub := newTestSubscriber()
	s := newResolveEntryTestSession(sub)
	sel := s.optChain.rotation[0]

	mine, _, _ := s.reserveEntryProbe(sel)
	if mine == nil {
		t.Fatal("failed to become the owner")
	}

	// No candidates reported, so this is the genuine "IB never answered" case
	// entryFailQuoteTimeout exclusively describes now.
	_, res := s.resolveDeltaCandidates(sel, mine)
	if res.Reason != entryFailQuoteTimeout {
		t.Errorf("reason = %q, want %q", res.Reason, entryFailQuoteTimeout)
	}

	s.optChain.mu.Lock()
	_, stillOwned := s.optChain.deltaRes[sel.id]
	s.optChain.mu.Unlock()
	if stillOwned {
		t.Error("own resolution not consumed — a sibling would join a probe that already finished")
	}
}

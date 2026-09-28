// Tests that a strike IB refuses as a delta candidate (error 200, "no security
// definition") is not chosen again for the same expiry.
//
// reqSecDefOptParams returns the union of strikes across every expiry, so on
// 2026-09-28 TQQQ 77.50, IWM 282.50, AGQ 71.50 and others — listed only on
// later expiries — were probed and refused 276 times, each refusal costing a
// probe slot (and, before the settle fix, the whole 10s window).
package ibkr

import (
	"slices"
	"testing"
	"time"
)

func TestHandleOptionMktError_RefusedCandidateNotProbedAgain(t *testing.T) {
	sub := newTestSubscriber()
	s := newResolveEntryTestSession(sub)
	sel := s.optChain.rotation[0]
	expiry := time.Now().AddDate(0, 0, 3).Format("20060102")
	s.optChain.lastChainInfo[sel.chainKey()] = chainSnapshot{expiry: expiry, strikes: []float64{725, 730, 732.5, 735, 740}, at: time.Now()}

	const reqID = 9001
	s.optChain.deltaCands[reqID] = &deltaCandidate{selectorID: sel.id, symbol: "SPY", right: "put", strike: 732.5, expiry: expiry, reqID: reqID}
	if !s.handleOptionMktError(reqID, "No security definition has been found for the request") {
		t.Fatal("error 200 on a delta candidate was not handled")
	}

	res, _, fail := s.reserveEntryProbe(sel)
	if res == nil {
		t.Fatalf("failed to reserve a probe: %+v", fail)
	}
	if slices.Contains(res.allStrikes, 732.5) {
		t.Errorf("allStrikes = %v still offers the strike IB refused for %s", res.allStrikes, expiry)
	}
	if len(res.allStrikes) != 4 {
		t.Errorf("allStrikes = %v, want the other four strikes kept", res.allStrikes)
	}

	// The shared chain snapshot itself must be untouched.
	if got := s.optChain.lastChainInfo[sel.chainKey()].strikes; len(got) != 5 {
		t.Errorf("chain snapshot mutated to %v", got)
	}
}

// The refusal is specific to one contract: the same strike on another expiry
// or the other right stays probeable.
func TestListedStrikes_RefusalScopedToContract(t *testing.T) {
	s := NewSession(Options{}, nil, nil)
	exp := time.Now().Format("20060102")
	later := time.Now().AddDate(0, 0, 7).Format("20060102")

	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()
	s.markUnlistedLocked(legKey{symbol: "TQQQ", right: "put", strike: 77.5, expiry: exp})

	strikes := []float64{77, 77.5, 78}
	if got := s.listedStrikesLocked("TQQQ", "put", exp, strikes); slices.Contains(got, 77.5) {
		t.Errorf("refused contract still listed: %v", got)
	}
	if got := s.listedStrikesLocked("TQQQ", "put", later, strikes); !slices.Contains(got, 77.5) {
		t.Errorf("refusal leaked to expiry %s: %v", later, got)
	}
	if got := s.listedStrikesLocked("TQQQ", "call", exp, strikes); !slices.Contains(got, 77.5) {
		t.Errorf("refusal leaked to the call side: %v", got)
	}
}

// Entries for expiries already past are dropped on the next refusal, so the
// set cannot grow across trading days.
func TestMarkUnlisted_PrunesPastExpiries(t *testing.T) {
	s := NewSession(Options{}, nil, nil)
	past := time.Now().AddDate(0, 0, -1).Format("20060102")
	today := time.Now().Format("20060102")

	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()
	s.optChain.unlisted[legKey{symbol: "IWM", right: "call", strike: 282.5, expiry: past}] = struct{}{}
	s.markUnlistedLocked(legKey{symbol: "IWM", right: "call", strike: 277.5, expiry: today})

	if _, ok := s.optChain.unlisted[legKey{symbol: "IWM", right: "call", strike: 282.5, expiry: past}]; ok {
		t.Error("a past expiry's refusal was not pruned")
	}
	if len(s.optChain.unlisted) != 1 {
		t.Errorf("unlisted has %d entries, want just today's", len(s.optChain.unlisted))
	}
}

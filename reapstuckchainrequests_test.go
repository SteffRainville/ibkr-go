// Tests for reapStuckChainRequests — the backstop that frees a conId-lookup
// or chain-params request whose IB response never arrived (most likely a
// pacing rejection from firing every configured group's initial resolution
// nearly simultaneously at startup, which produces neither a success nor an
// error callback). Without this, the affected group's "resolving" flag
// never clears, permanently excluding it from pickRotationGroupLocked and
// leaving it stuck at zero background lines for the rest of the session —
// see CLAUDE.md's mdlines section and the 2026-07-28 slow-first-quote
// investigation this fix came out of.
package ibkr

import (
	"testing"
	"time"
)

// TestReapStuckChainRequests_FreesOnlyStaleEntries verifies the reaper
// leaves fresh conId/chain requests alone but frees ones older than
// chainResolutionMaxAge, unblocking selectorResolvingLocked for its waiters.
func TestReapStuckChainRequests_FreesOnlyStaleEntries(t *testing.T) {
	s := NewSession(Options{}, nil, nil)

	s.optChain.mu.Lock()
	s.optChain.conIDReqs[100] = &optConIDReq{chain: chainKey{"FRESH_CONID", 0}, waiters: []int{1}, requestedAt: time.Now()}
	s.optChain.conIDReqs[101] = &optConIDReq{chain: chainKey{"STUCK_CONID", 0}, waiters: []int{2}, requestedAt: time.Now().Add(-time.Minute)}
	s.optChain.chainReqs[200] = &optChainReq{chain: chainKey{"FRESH_CHAIN", 0}, waiters: []int{3}, requestedAt: time.Now()}
	s.optChain.chainReqs[201] = &optChainReq{chain: chainKey{"STUCK_CHAIN", 0}, waiters: []int{4}, requestedAt: time.Now().Add(-time.Minute)}
	s.optChain.mu.Unlock()

	s.reapStuckChainRequests()

	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()

	if _, ok := s.optChain.conIDReqs[100]; !ok {
		t.Error("fresh conId request was reaped")
	}
	if _, ok := s.optChain.conIDReqs[101]; ok {
		t.Error("stale conId request was not reaped")
	}
	if _, ok := s.optChain.chainReqs[200]; !ok {
		t.Error("fresh chain request was reaped")
	}
	if _, ok := s.optChain.chainReqs[201]; ok {
		t.Error("stale chain request was not reaped")
	}

	if s.selectorResolvingLocked(2) {
		t.Error("selector 2 still reports resolving after its stuck conId request was reaped")
	}
	if s.selectorResolvingLocked(4) {
		t.Error("selector 4 still reports resolving after its stuck chain request was reaped")
	}
	if !s.selectorResolvingLocked(1) {
		t.Error("selector 1 should still report resolving — its conId request is fresh, not reaped")
	}
}

// TestHandleOptionMktError_ClearsConIDAndChainRequests verifies an explicit
// IB rejection (error 200) of a conId lookup or chain-params request is
// cleaned up immediately, rather than waiting for reapStuckChainRequests'
// timeout — that timeout exists for the silent-drop case (no callback at
// all), not an explicit "no security definition" rejection.
func TestHandleOptionMktError_ClearsConIDAndChainRequests(t *testing.T) {
	s := NewSession(Options{}, nil, nil)

	s.optChain.mu.Lock()
	s.optChain.conIDReqs[100] = &optConIDReq{chain: chainKey{"BADSYM", 0}, waiters: []int{1}, requestedAt: time.Now()}
	s.optChain.chainReqs[200] = &optChainReq{chain: chainKey{"BADSYM2", 0}, waiters: []int{2}, requestedAt: time.Now()}
	s.optChain.mu.Unlock()

	if handled := s.handleOptionMktError(100, "No security definition has been found for the request"); !handled {
		t.Error("handleOptionMktError did not recognize the conId-lookup reqID")
	}
	if handled := s.handleOptionMktError(200, "No security definition has been found for the request"); !handled {
		t.Error("handleOptionMktError did not recognize the chain-params reqID")
	}

	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()
	if _, ok := s.optChain.conIDReqs[100]; ok {
		t.Error("conId request still present after handleOptionMktError")
	}
	if _, ok := s.optChain.chainReqs[200]; ok {
		t.Error("chain request still present after handleOptionMktError")
	}
	if s.selectorResolvingLocked(1) {
		t.Error("selector 1 still reports resolving after its rejected conId request was cleared")
	}
	if s.selectorResolvingLocked(2) {
		t.Error("selector 2 still reports resolving after its rejected chain request was cleared")
	}
}

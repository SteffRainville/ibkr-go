// Tests for deltaCandidatesSettled, ResolveEntryStrike's poll-loop exit
// condition. Before this existed, the loop only exited early once every one
// of the (up to 5) probed candidates had reported — a single slow strike
// forced the full entry-probe timeout even when the strike actually wanted
// had already answered on the first poll. These tests pin down that a
// near-exact match now short-circuits the wait regardless of the others'
// state, while a genuinely unresolved set still requires every candidate in.
package ibkr

import "testing"

func TestDeltaCandidatesSettled_GoodEnoughMatchShortCircuits(t *testing.T) {
	candidates := []*deltaCandidate{
		{strike: 480, ready: true, delta: 0.651}, // within tolerance of target 0.65
		{strike: 475, ready: false},               // never answered
		{strike: 470, ready: false},               // never answered
	}
	if !deltaCandidatesSettled(candidates, 0.65) {
		t.Error("expected settled=true — a near-exact ready match must not wait on unanswered stragglers")
	}
}

func TestDeltaCandidatesSettled_NoGoodMatchWaitsForAll(t *testing.T) {
	candidates := []*deltaCandidate{
		{strike: 480, ready: true, delta: 0.80}, // far from target, not good enough
		{strike: 475, ready: false},
	}
	if deltaCandidatesSettled(candidates, 0.65) {
		t.Error("expected settled=false — no ready candidate is close enough, and one is still pending")
	}
}

func TestDeltaCandidatesSettled_AllReadyNoneCloseStillSettles(t *testing.T) {
	// No candidate is within tolerance, but every one has reported — the
	// timeout-free "everyone answered" path must still terminate the loop so
	// resolveDeltaCandidates can pick the closest of what's available.
	candidates := []*deltaCandidate{
		{strike: 480, ready: true, delta: 0.80},
		{strike: 475, ready: true, delta: 0.85},
	}
	if !deltaCandidatesSettled(candidates, 0.65) {
		t.Error("expected settled=true — every candidate has reported, even though none is a good match")
	}
}

func TestDeltaCandidatesSettled_EmptyIsSettled(t *testing.T) {
	if !deltaCandidatesSettled(nil, 0.65) {
		t.Error("expected settled=true for an empty candidate set (vacuously all-ready)")
	}
}

func TestDeltaCandidatesSettled_ToleranceIsOnAbsoluteDelta(t *testing.T) {
	// Put deltas are reported negative by IB; the comparison must use |delta|
	// against targetDelta (which is always stored positive), matching
	// resolveDeltaCandidates' own distance calculation.
	candidates := []*deltaCandidate{
		{strike: 480, ready: true, delta: -0.651},
		{strike: 475, ready: false},
	}
	if !deltaCandidatesSettled(candidates, 0.65) {
		t.Error("expected settled=true — a negative (put) delta near -targetDelta must still count as a good match")
	}
}

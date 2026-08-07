// Tests for the entry-probe failure classifier — the thing that lets a caller
// tell "this account has no option market-data subscription" apart from "IB
// accepted the request and never answered". Before EntryStrikeResult existed
// both arrived as a bare false and were reported to the operator as one
// indistinguishable "option quote unavailable".
package ibkr

import (
	"strings"
	"testing"
)

func TestClassifyCandidateErrors(t *testing.T) {
	// silent is a candidate IB never said anything about — no delta, no price,
	// no error. A probe made entirely of these is the genuine timeout case.
	silent := func(strike float64) *deltaCandidate {
		return &deltaCandidate{symbol: "QQQ", right: "call", strike: strike}
	}
	failed := func(strike float64, code int64, msg string) *deltaCandidate {
		return &deltaCandidate{symbol: "QQQ", right: "call", strike: strike, errCode: code, errMsg: msg}
	}

	tests := []struct {
		name       string
		candidates []*deltaCandidate
		wantReason string
		wantCode   int64
	}{
		{
			name:       "no errors at all is a timeout, not a failure",
			candidates: []*deltaCandidate{silent(600), silent(601), silent(602)},
			wantReason: entryFailQuoteTimeout,
			wantCode:   0,
		},
		{
			name:       "no candidates at all still reports a timeout",
			candidates: nil,
			wantReason: entryFailQuoteTimeout,
			wantCode:   0,
		},
		{
			name: "one not-subscribed among four silent candidates wins",
			candidates: []*deltaCandidate{
				silent(600), silent(601),
				failed(602, 354, "Requested market data is not subscribed."),
				silent(603),
			},
			wantReason: entryFailNoSubscription,
			wantCode:   354,
		},
		{
			name:       "IB 200 alone is a contract problem",
			candidates: []*deltaCandidate{failed(600, 200, "No security definition has been found")},
			wantReason: entryFailContractInvalid,
			wantCode:   200,
		},
		{
			// The precedence rule that matters: an entitlement gap is the whole
			// story and must not be masked by a co-occurring bad contract.
			name: "not-subscribed outranks a bad contract",
			candidates: []*deltaCandidate{
				failed(600, 200, "No security definition has been found"),
				failed(601, 10168, "Requested market data is not subscribed. Delayed market data is not enabled."),
			},
			wantReason: entryFailNoSubscription,
			wantCode:   10168,
		},
		{
			name:       "an unrecognised code is visibly unclassified, never filed as a timeout",
			candidates: []*deltaCandidate{failed(600, 420, "Invalid Real-time Query")},
			wantReason: entryFailMDError,
			wantCode:   420,
		},
		{
			name: "a bad contract outranks a pacing error",
			candidates: []*deltaCandidate{
				failed(600, 420, "Invalid Real-time Query"),
				failed(601, 200, "No security definition has been found"),
			},
			wantReason: entryFailContractInvalid,
			wantCode:   200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyCandidateErrors(tt.candidates)
			if got.OK {
				t.Error("OK = true; a classified failure is never a success")
			}
			if got.Reason != tt.wantReason {
				t.Errorf("Reason = %q, want %q", got.Reason, tt.wantReason)
			}
			if got.IBCode != tt.wantCode {
				t.Errorf("IBCode = %d, want %d", got.IBCode, tt.wantCode)
			}
			if got.Detail == "" {
				t.Error("Detail is empty — the detail is the whole point; it carries IB's own words to the operator")
			}
		})
	}
}

// TestClassifyCandidateErrors_DetailCarriesIBMessage: the operator's actual
// question is "which contract, and what did IB say?". Detail must answer both
// without them having to go grep the raw log.
func TestClassifyCandidateErrors_DetailCarriesIBMessage(t *testing.T) {
	const msg = "Requested market data is not subscribed. Displaying delayed market data."
	got := classifyCandidateErrors([]*deltaCandidate{
		{symbol: "SPY", right: "put", strike: 735, errCode: 10167, errMsg: msg},
	})

	for _, want := range []string{"10167", "SPY", "put", "735", msg} {
		if !strings.Contains(got.Detail, want) {
			t.Errorf("Detail = %q, missing %q", got.Detail, want)
		}
	}
}

func TestClassifyEntryIBCode(t *testing.T) {
	// Every member of the not-subscribed family maps to one reason: the
	// distinction between them does not change what the operator must do.
	for _, code := range []int64{354, 10089, 10090, 10167, 10168, 10197} {
		if got := classifyEntryIBCode(code); got != entryFailNoSubscription {
			t.Errorf("classifyEntryIBCode(%d) = %q, want %q", code, got, entryFailNoSubscription)
		}
	}
	if got := classifyEntryIBCode(200); got != entryFailContractInvalid {
		t.Errorf("classifyEntryIBCode(200) = %q, want %q", got, entryFailContractInvalid)
	}
	for _, code := range []int64{100, 162, 321, 322, 420} {
		if got := classifyEntryIBCode(code); got != entryFailMDError {
			t.Errorf("classifyEntryIBCode(%d) = %q, want %q", code, got, entryFailMDError)
		}
	}
}

// TestNoteCandidateError_StampsInFlightProbe proves the Error callback's
// attribution actually lands on the candidate, which is the link that was
// missing entirely: IB reported these codes against the probe's own reqID and
// the library dropped them on the floor.
func TestNoteCandidateError_StampsInFlightProbe(t *testing.T) {
	sub := newTestSubscriber()
	s := newResolveEntryTestSession(sub)

	cand := &deltaCandidate{symbol: "QQQ", right: "call", strike: 600, reqID: 7042}
	s.optChain.mu.Lock()
	s.optChain.deltaCands[7042] = cand
	s.optChain.mu.Unlock()

	s.noteCandidateError(7042, 354, "Requested market data is not subscribed.")

	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()
	if cand.errCode != 354 {
		t.Errorf("errCode = %d, want 354", cand.errCode)
	}
	if cand.errMsg == "" {
		t.Error("errMsg is empty — IB's own wording is what makes the row actionable")
	}
	// Observational only: recording a cause must not change probe lifecycle or
	// timing, so the candidate stays registered and stays not-ready.
	if _, ok := s.optChain.deltaCands[7042]; !ok {
		t.Error("noteCandidateError removed the candidate — it must only observe, never alter the probe")
	}
	if cand.ready {
		t.Error("noteCandidateError marked the candidate ready — that would change when the probe gives up")
	}
}

// TestNoteCandidateError_UnknownReqIDIsNoop: the Error callback fires for every
// reqID in the session, so the overwhelming majority of calls are for something
// that is not an entry probe.
func TestNoteCandidateError_UnknownReqIDIsNoop(t *testing.T) {
	sub := newTestSubscriber()
	s := newResolveEntryTestSession(sub)
	s.noteCandidateError(999999, 354, "not subscribed") // must not panic

	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()
	if len(s.optChain.deltaCands) != 0 {
		t.Errorf("deltaCands = %d entries, want 0 — an unrelated reqID must not create one", len(s.optChain.deltaCands))
	}
}

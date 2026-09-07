package ibkr

import (
	"strings"
	"testing"
)

// Code 162 ("Historical Market Data Service error message") is a grab-bag —
// it also covers a cancelled scanner subscription and "no data for the
// range" (see the real 2026-09-04 log line quoted in the non-pacing case
// below), so the pacing-violation branch in Error() must not fire on every
// 162, only ones that actually say "pacing" AND name a symbol this session
// is tracking bars for. Before this, a real pacing rejection had ZERO
// handling anywhere in the library: it fell into the generic log line like
// any other notice, and the affected symbol silently got no further bars for
// the rest of the session with no retry and nothing visible on the dashboard.
func TestError_HistoricalDataPacingViolation(t *testing.T) {
	const reqID = 501

	t.Run("pacing message for a tracked symbol fires OnError", func(t *testing.T) {
		var got *ErrorEvent
		s := NewSession(Options{TradingAccount: "DU12345", OnError: func(e ErrorEvent) {
			got = &e
		}}, nil, nil)
		s.reqSymbol[reqID] = "MU"

		const ibMsg = "Historical Market Data Service error message:Historical data request pacing violation"
		s.Error(reqID, 0, errCodeHistoricalDataPacing, ibMsg, "")

		if got == nil {
			t.Fatal("OnError was not called for a pacing-violation message on a tracked historical-data reqID")
		}
		if got.Type != "market_data" {
			t.Errorf("Type = %q, want %q", got.Type, "market_data")
		}
		if !strings.Contains(got.Message, "MU") || !strings.Contains(got.Message, ibMsg) {
			t.Errorf("Message = %q, want it to name the symbol (MU) and include IB's text verbatim (%q)", got.Message, ibMsg)
		}
	})

	t.Run("non-pacing code-162 message does not fire OnError", func(t *testing.T) {
		var fired bool
		s := NewSession(Options{TradingAccount: "DU12345", OnError: func(ErrorEvent) {
			fired = true
		}}, nil, nil)
		s.reqSymbol[reqID] = "ULTY"

		// The real code-162 text observed on 2026-09-04 — not a pacing
		// rejection, and must not be misclassified as one.
		s.Error(reqID, 0, errCodeHistoricalDataPacing, "Historical Market Data Service error message:API scanner subscription cancelled: 1156", "")

		if fired {
			t.Error("OnError fired for a non-pacing code-162 message — this would misreport a routine scanner cancellation as a pacing violation")
		}
	})

	t.Run("pacing message for an unknown reqID does not fire OnError", func(t *testing.T) {
		var fired bool
		s := NewSession(Options{TradingAccount: "DU12345", OnError: func(ErrorEvent) {
			fired = true
		}}, nil, nil)
		// reqSymbol deliberately left empty for reqID — not a historical-data
		// request this session issued.

		s.Error(reqID, 0, errCodeHistoricalDataPacing, "Historical Market Data Service error message:Historical data request pacing violation", "")

		if fired {
			t.Error("OnError fired for a pacing message on a reqID with no known historical-data symbol")
		}
	})
}

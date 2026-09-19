package ibkr

import "testing"

// TestOptionBarsStatus pins what the option chart page relies on to explain
// an empty chart: a refused stream reports IB's error, a delivered backfill
// is flagged, and an unsubscribed handle is unknown. Before this, IB's
// refusal (e.g. error 200 for Friday's expired 0DTE on a Saturday) reached
// only the log and the page polled an empty candle key forever.
func TestOptionBarsStatus(t *testing.T) {
	s := withOfflineClient(NewSession(Options{}, nil, nil))

	refused, err := s.SubscribeOptionBars("SPY", "put", 660, "20260918", "OPT:SPY:PUT:660:20260918")
	if err != nil {
		t.Fatal(err)
	}
	quiet, _ := s.SubscribeOptionBars("SPY", "call", 670, "20261016", "OPT:SPY:CALL:670:20261016")

	if st, ok := s.OptionBarsStatus(refused); !ok || st != (OptionBarsStatus{}) {
		t.Fatalf("fresh stream = %+v, %v; want zero value, true", st, ok)
	}

	s.Error(refused, 0, 200, "No security definition has been found for the request", "")
	st, _ := s.OptionBarsStatus(refused)
	if st.ErrCode != 200 || st.ErrMsg == "" {
		t.Errorf("refused stream = %+v; want error 200 recorded", st)
	}

	// A notice (2000..9999 band) is not a refusal.
	s.Error(quiet, 0, 2174, "some farm notice", "")
	s.HistoricalDataEnd(quiet, "", "")
	st, _ = s.OptionBarsStatus(quiet)
	if st.ErrCode != 0 || !st.Backfilled {
		t.Errorf("quiet stream = %+v; want backfilled, no error", st)
	}

	s.UnsubscribeOptionBars(refused)
	if _, ok := s.OptionBarsStatus(refused); ok {
		t.Error("unsubscribed stream still reported as known")
	}
}

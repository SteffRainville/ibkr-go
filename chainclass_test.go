package ibkr

import (
	"testing"
)

// TestSecDefOptParams_KeepsOneTradingClass is the regression for the 2026-08-17
// MSFT incident.
//
// IB delivers one SecurityDefinitionOptionParameter callback per (exchange,
// trading class). One underlying routinely has several SMART classes — the
// standard one plus adjusted/mini classes from corporate actions — and each has
// its OWN expiry calendar and its OWN strike ladder. The old code merged them
// into a single expirations+strikes pair, which meant the selection could pair
// an expiry from one class with a strike from another. That contract does not
// exist, so IB answered error 200 to every near-money strike and the retry walk
// eventually settled on an untradable deep-ITM leg with no bid and no ask.
//
// The chosen class here is the second callback (named after the symbol,
// multiplier 100) even though the phantom class arrives first and offers more
// expirations — identity beats size, or the same failure returns wearing a
// different label.
func TestSecDefOptParams_KeepsOneTradingClass(t *testing.T) {
	s := newRotationTestSession(nil)
	const reqID = int64(7)
	s.optChain.chainReqs[reqID] = &optChainReq{chain: chainKey{symbol: "MSFT", optionDelay: 3}}

	// The phantom class: a Thursday expiry and a coarse ladder.
	s.SecurityDefinitionOptionParameter(reqID, "SMART", 1, "MSFT9", "100",
		[]string{"20260820", "20260827", "20260903"}, []float64{300, 350, 400})
	// The standard class: Friday expiries and the real $5 ladder.
	s.SecurityDefinitionOptionParameter(reqID, "SMART", 1, "MSFT", "100",
		[]string{"20260821", "20260828"}, []float64{480, 485, 490, 495})
	// A non-SMART callback must still be ignored outright.
	s.SecurityDefinitionOptionParameter(reqID, "CBOE", 1, "MSFT", "100",
		[]string{"20260819"}, []float64{1})

	req := s.optChain.chainReqs[reqID]
	if got := len(req.classes); got != 2 {
		t.Fatalf("SMART trading classes recorded = %d, want 2 (the CBOE callback must be ignored)", got)
	}

	chosen, ignored := pickChainClass("MSFT", req.classes)
	if chosen == nil {
		t.Fatal("pickChainClass returned nothing for a chain with two usable SMART classes")
	}
	if chosen.tradingClass != "MSFT" {
		t.Fatalf("chosen trading class = %q, want %q — the class named after the underlying wins",
			chosen.tradingClass, "MSFT")
	}
	if len(ignored) != 1 || ignored[0].tradingClass != "MSFT9" {
		t.Fatalf("ignored classes = %v, want exactly MSFT9 reported", ignored)
	}

	// The whole point: neither the phantom expiry nor the phantom strikes may
	// leak into the selection the chosen class drives.
	for _, e := range chosen.expirations {
		if e == "20260820" {
			t.Fatal("expiry 20260820 came from MSFT9 but survived into the chosen class — " +
				"this is the cross-class pairing that produced 35 error-200s")
		}
	}
	for _, st := range chosen.strikes {
		if st < 480 {
			t.Fatalf("strike %.0f came from MSFT9's ladder but survived into the chosen class", st)
		}
	}
	if got := nearestExpiry(chosen.expirations, 0); got != "20260821" {
		t.Fatalf("nearestExpiry = %q, want the standard class's own 20260821", got)
	}
}

// TestPickChainClass_Preferences pins the fallback order for the cases where no
// class is named after the underlying — a symbol whose standard class carries a
// different name, and an underlying that only has non-standard deliverables.
func TestPickChainClass_Preferences(t *testing.T) {
	tests := []struct {
		name    string
		symbol  string
		classes map[string]*chainClass
		want    string
	}{
		{
			name:   "standard multiplier beats a richer non-standard class",
			symbol: "ABC",
			classes: map[string]*chainClass{
				"ABC1": {tradingClass: "ABC1", multiplier: "10",
					expirations: []string{"1", "2", "3", "4"}, strikes: []float64{1}},
				"XYZ": {tradingClass: "XYZ", multiplier: "100",
					expirations: []string{"1"}, strikes: []float64{1}},
			},
			want: "XYZ",
		},
		{
			name:   "among standard classes, the richest calendar wins",
			symbol: "ABC",
			classes: map[string]*chainClass{
				"P": {tradingClass: "P", multiplier: "100",
					expirations: []string{"1"}, strikes: []float64{1}},
				"Q": {tradingClass: "Q", multiplier: "100",
					expirations: []string{"1", "2"}, strikes: []float64{1}},
			},
			want: "Q",
		},
		{
			name:   "a non-standard deliverable beats nothing at all",
			symbol: "ABC",
			classes: map[string]*chainClass{
				"ABC1": {tradingClass: "ABC1", multiplier: "10",
					expirations: []string{"1"}, strikes: []float64{1}},
			},
			want: "ABC1",
		},
		{
			name:   "a class with no strikes is not usable",
			symbol: "ABC",
			classes: map[string]*chainClass{
				"ABC": {tradingClass: "ABC", multiplier: "100",
					expirations: []string{"1"}},
				"ABC1": {tradingClass: "ABC1", multiplier: "100",
					expirations: []string{"1"}, strikes: []float64{1}},
			},
			want: "ABC1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chosen, _ := pickChainClass(tc.symbol, tc.classes)
			if chosen == nil {
				t.Fatalf("pickChainClass returned nothing, want %q", tc.want)
			}
			if chosen.tradingClass != tc.want {
				t.Fatalf("chosen = %q, want %q", chosen.tradingClass, tc.want)
			}
		})
	}

	if chosen, _ := pickChainClass("ABC", nil); chosen != nil {
		t.Fatalf("pickChainClass over no classes = %v, want nil", chosen)
	}
}

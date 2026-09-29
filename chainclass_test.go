package ibkr

import "testing"

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

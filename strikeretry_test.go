package ibkr

import (
	"math"
	"testing"
)

// sortedByDistance returns strikes ordered the way selectStrike stores them on
// an optStrikeRetry: nearest to the underlying first.
func sortedByDistance(undPrice float64, strikes ...float64) []float64 {
	out := append([]float64(nil), strikes...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && math.Abs(out[j]-undPrice) < math.Abs(out[j-1]-undPrice); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// drain walks a retry to exhaustion the way handleOptionMarketDataError does,
// one candidate per error-200 callback.
func drain(r *optStrikeRetry) (tried []float64, reason string) {
	for {
		strike, why, ok := r.nextCandidate()
		if !ok {
			return tried, why
		}
		tried = append(tried, strike)
	}
}

// TestStrikeRetry_StopsAtTheAttemptCap pins the first bound. The walk runs
// inside the error callback, firing the next ReqMktData synchronously on each
// failure, so an expiry whose strikes are simply not listed used to produce an
// unbounded burst — 35 subscribe attempts in 8 seconds for MSFT on 2026-08-17.
func TestStrikeRetry_StopsAtTheAttemptCap(t *testing.T) {
	// A dense ladder, every strike well inside the distance band, so the
	// attempt cap is unambiguously the thing that stops the walk.
	r := &optStrikeRetry{
		symbol: "MSFT", right: "call", expiry: "20260820", undPrice: 485,
		strikes: sortedByDistance(485, 485, 490, 480, 495, 475, 500, 470, 505, 465, 510),
		nextIdx: 1,
	}

	tried, reason := drain(r)
	if len(tried) != maxStrikeRetries {
		t.Fatalf("retries attempted = %d (%v), want %d", len(tried), tried, maxStrikeRetries)
	}
	if reason == "" {
		t.Fatal("giving up produced no reason — the log line is the only way this is ever diagnosed")
	}
}

// TestStrikeRetry_RefusesStrikesFarFromTheUnderlying pins the second and more
// important bound. MSFT's walk ran from 490 down to 400 against a $485
// underlying and "succeeded" on a δ≈1.00 contract with no bid and no ask —
// nominally a subscription, in practice an untradable row that then counted as
// an existing leg and so could no longer be corrected.
//
// Only 400 is listed here besides the estimate, i.e. exactly the MSFT shape.
// The walk must refuse it and give up rather than take it.
func TestStrikeRetry_RefusesStrikesFarFromTheUnderlying(t *testing.T) {
	r := &optStrikeRetry{
		symbol: "MSFT", right: "call", expiry: "20260820", undPrice: 485,
		strikes: sortedByDistance(485, 490, 400),
		nextIdx: 1,
	}

	tried, reason := drain(r)
	for _, st := range tried {
		if math.Abs(st-485)/485*100 > maxStrikeRetryDistancePct {
			t.Fatalf("attempted strike %.2f is more than %.0f%% from the underlying 485 — "+
				"this is the deep-ITM leg the bound exists to refuse", st, maxStrikeRetryDistancePct)
		}
	}
	if len(tried) != 0 {
		t.Fatalf("attempted %v, want none — 400 is the only untried strike and it is out of band", tried)
	}
	if reason == "" {
		t.Fatal("giving up produced no reason")
	}
}

// TestStrikeRetry_StillStepsOverAGapInTheLadder guards against over-correcting:
// the walk exists to step past a strike IB does not list, and a nearby
// alternative must still be taken.
func TestStrikeRetry_StillStepsOverAGapInTheLadder(t *testing.T) {
	r := &optStrikeRetry{
		symbol: "QQQ", right: "put", expiry: "20260819", undPrice: 480,
		strikes: sortedByDistance(480, 480, 481, 482),
		nextIdx: 1,
	}

	strike, _, ok := r.nextCandidate()
	if !ok {
		t.Fatal("the first next-nearest strike was refused — the walk must still step over a gap")
	}
	if strike != 481 {
		t.Fatalf("next candidate = %.2f, want 481 (nearest untried)", strike)
	}
}

// TestStrikeRetry_UnknownUnderlyingPriceStillBounded covers the degenerate case
// where no underlying price was available when the retry was built: the
// distance bound cannot apply, so the attempt cap must carry it alone.
func TestStrikeRetry_UnknownUnderlyingPriceStillBounded(t *testing.T) {
	r := &optStrikeRetry{
		symbol: "ABC", right: "call", expiry: "20260821", undPrice: 0,
		strikes: []float64{10, 20, 30, 40, 50, 60, 70, 80},
		nextIdx: 1,
	}

	tried, _ := drain(r)
	if len(tried) != maxStrikeRetries {
		t.Fatalf("retries attempted = %d (%v), want %d — the attempt cap must bound the walk "+
			"even with no underlying price to measure distance against", len(tried), tried, maxStrikeRetries)
	}
}

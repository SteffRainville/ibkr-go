package ibkr

import (
	"math"
	"testing"
)

// The entry delta probe must reach the strikes that actually carry the target
// delta. A ~0.55 target sits AT or just OUTSIDE the money; the probe used to
// look only strictly INSIDE it, so those strikes were never subscribed.
//
// Delta is monotonic in strike, so the rung nearest the money always carried
// the lowest delta of the old set and every further candidate was one rung
// deeper ITM -- widening an inside-only window moved strictly away from the
// target. These tests pin the direction, not the count.

// ladder builds an evenly spaced strike ladder, as IB reports one.
func ladder(lo, hi, step float64) []float64 {
	var out []float64
	for k := lo; k <= hi+1e-9; k += step {
		out = append(out, math.Round(k*100)/100)
	}
	return out
}

func contains(strikes []float64, want float64) bool {
	for _, s := range strikes {
		if math.Abs(s-want) < 1e-9 {
			return true
		}
	}
	return false
}

// The regression, taken from real 2026-08-21..26 entries. Each of these bought
// the strike named in `got` at the delta named beside it, when the strike one
// rung the other way was sitting at roughly the configured target.
func TestSelectStrikeCandidates_ReachesTheTargetStrike(t *testing.T) {
	for _, tc := range []struct {
		name        string
		right       string
		strikes     []float64
		undPrice    float64
		chosenWas   float64 // what the ITM-only probe resolved to
		chosenDelta float64 // ...at this delta
		wantProbed  float64 // the rung that carries the target, one step away
	}{
		{"AAPL call 3DTE", "call", ladder(280, 340, 5), 309.80, 305, 0.8409, 310},
		{"AMZN call 3DTE", "call", ladder(230, 290, 5), 259.84, 255, 0.8527, 260},
		{"TSLA call 0DTE", "call", ladder(320, 380, 5), 346.80, 345, 0.8249, 350},
		{"NVDA put 3DTE", "put", ladder(190, 250, 5), 215.56, 220, -0.8097, 215},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := selectStrikeCandidates(tc.strikes, tc.undPrice, tc.right,
				deltaProbeITMCandidates, deltaProbeOTMCandidates)

			if !contains(got, tc.wantProbed) {
				t.Errorf("strike %.2f not probed: candidates %v\n"+
					"  it is one rung from spot %.2f and carries roughly the target delta;\n"+
					"  the inside-only probe resolved to %.2f at delta %.4f instead",
					tc.wantProbed, got, tc.undPrice, tc.chosenWas, tc.chosenDelta)
			}
			// The formerly chosen strike must still be reachable -- this widens
			// the window, it does not move it.
			if !contains(got, tc.chosenWas) {
				t.Errorf("strike %.2f no longer probed: candidates %v — the window moved instead of widening", tc.chosenWas, got)
			}
		})
	}
}

// Both sides of the money, and the two rights exact mirrors of one another.
func TestSelectStrikeCandidates_StraddlesBothSides(t *testing.T) {
	strikes := ladder(280, 340, 5)
	const spot = 309.80

	for _, right := range []string{"call", "put"} {
		got := selectStrikeCandidates(strikes, spot, right,
			deltaProbeITMCandidates, deltaProbeOTMCandidates)

		var below, above int
		for _, s := range got {
			if s <= spot {
				below++
			} else {
				above++
			}
		}
		if below == 0 || above == 0 {
			t.Errorf("%s: candidates %v sit entirely on one side of spot %.2f (below=%d above=%d)",
				right, got, spot, below, above)
		}
		if want := deltaProbeITMCandidates + deltaProbeOTMCandidates; len(got) != want {
			t.Errorf("%s: got %d candidates, want %d", right, len(got), want)
		}
	}

	// A call and a put on the same ladder and spot probe the same strikes --
	// which side is "inside" flips, but the window is centred either way.
	call := selectStrikeCandidates(strikes, spot, "call", 3, 3)
	put := selectStrikeCandidates(strikes, spot, "put", 3, 3)
	if len(call) != len(put) {
		t.Fatalf("call probed %v, put probed %v — asymmetric window", call, put)
	}
	for i := range call {
		if call[i] != put[i] {
			t.Errorf("call %v != put %v at %d — the window must be symmetric about spot", call, put, i)
		}
	}
}

// A strike sitting exactly at the underlying is the single rung most likely to
// carry delta 0.5. The old strict `<` dropped it.
func TestSelectStrikeCandidates_IncludesTheAtTheMoneyStrike(t *testing.T) {
	strikes := ladder(280, 340, 5)
	for _, right := range []string{"call", "put"} {
		got := selectStrikeCandidates(strikes, 310.00, right, 3, 3)
		if !contains(got, 310) {
			t.Errorf("%s: exactly-at-the-money strike 310 not probed: %v", right, got)
		}
	}
}

// GrantProbe refusals skip candidates in loop order, so the farthest-from-target
// strikes must be last.
func TestSelectStrikeCandidates_NearestTheMoneyFirst(t *testing.T) {
	got := selectStrikeCandidates(ladder(280, 340, 5), 309.80, "call", 3, 3)
	if len(got) < 2 {
		t.Fatalf("got %v", got)
	}
	for i := 1; i < len(got); i++ {
		prev := math.Abs(got[i-1] - 309.80)
		cur := math.Abs(got[i] - 309.80)
		if cur < prev {
			t.Errorf("candidates %v not ordered nearest-the-money first: %.2f (dist %.2f) after %.2f (dist %.2f)",
				got, got[i], cur, got[i-1], prev)
		}
	}
}

// The underlying sitting near the end of the ladder must still get a full-width
// probe, borrowing the short side's budget.
func TestSelectStrikeCandidates_BackfillsFromTheOtherSide(t *testing.T) {
	strikes := ladder(280, 340, 5)

	// Spot near the top: only one strike above it.
	got := selectStrikeCandidates(strikes, 336.00, "call", 3, 3)
	if want := 6; len(got) != want {
		t.Errorf("spot above the ladder top: got %d candidates %v, want %d (the inside side must lend its budget)", len(got), got, want)
	}

	// Spot near the bottom: only one strike at-or-below it.
	got = selectStrikeCandidates(strikes, 281.00, "call", 3, 3)
	if want := 6; len(got) != want {
		t.Errorf("spot below the ladder bottom: got %d candidates %v, want %d", len(got), got, want)
	}

	// No duplicates in either case.
	seen := map[float64]bool{}
	for _, s := range got {
		if seen[s] {
			t.Errorf("duplicate strike %.2f in %v — backfill re-took a strike it had already picked", s, got)
		}
		seen[s] = true
	}
}

// A chain thinner than the window, and the degenerate inputs that feed
// entryFailNoCandidates, must return what exists without panicking.
func TestSelectStrikeCandidates_ThinLadder(t *testing.T) {
	// LNSR at 8.13 after wholeDollarStrikes: only 5 and 10 survive.
	got := selectStrikeCandidates([]float64{5, 10}, 8.13, "call", 3, 3)
	if len(got) != 2 {
		t.Errorf("got %v, want both strikes", got)
	}
	if got[0] != 10 {
		t.Errorf("got %v, want 10 first — it is 1.87 from spot, 5 is 3.13", got)
	}

	for _, tc := range []struct {
		name     string
		strikes  []float64
		undPrice float64
	}{
		{"empty ladder", nil, 309.80},
		{"no underlying price", ladder(280, 340, 5), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := selectStrikeCandidates(tc.strikes, tc.undPrice, "call", 3, 3); len(got) != 0 {
				t.Errorf("got %v, want none — this feeds entryFailNoCandidates", got)
			}
		})
	}
}

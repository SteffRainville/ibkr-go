package ibkr

import (
	"fmt"
	"testing"
	"time"
)

// Every fixture here uses one explicit base instant plus offsets, never
// time.Now() in a loop. A loop that stamps time.Now() completes inside a
// single clock granule, so every comparison ties and a broken timestamp
// classifier passes — that exact mistake produced a green test against a
// known-buggy rotation scorer once already.
//
// rthBase is deliberately inside RTH so legSilenceRTH (90s) applies;
// offRTHBase exercises the legSilenceOffRTH (15m) branch of legSilenceLimit.
func rthBase() time.Time    { return time.Date(2026, 8, 4, 14, 30, 0, 0, time.Local) }
func offRTHBase() time.Time { return time.Date(2026, 8, 4, 3, 30, 0, 0, time.Local) }

// TestLegHealthStateCoversEveryLegHealth guards the projection between the
// unexported classifier and the exported strings. LegHealthState is meant to
// be legHealth.String() verbatim; if someone adds a legHealth value without
// an exported constant, callers would silently start seeing "unknown" and
// treat a real condition as unclassifiable.
func TestLegHealthStateCoversEveryLegHealth(t *testing.T) {
	declared := map[LegHealthState]bool{
		LegHealthHealthy:   true,
		LegHealthWarming:   true,
		LegHealthSilent:    true,
		LegHealthStale:     true,
		LegHealthFeedQuiet: true,
	}
	for _, h := range []legHealth{legHealthy, legWarming, legSilent, legStale, legFeedQuiet} {
		got := LegHealthState(h.String())
		if got == "unknown" {
			t.Errorf("legHealth(%d).String() = %q — every classifier value needs an exported constant", h, got)
		}
		if !declared[got] {
			t.Errorf("legHealth(%d) maps to %q, which is not a declared LegHealthState constant", h, got)
		}
	}
}

// TestPositionLegsAt_ClassifiesEveryState walks the full liveness matrix. The
// feed_quiet row is the load-bearing one: it is what stops a market-wide lull
// (halt, weekend, IB outage) from condemning every held position at once.
func TestPositionLegsAt_ClassifiesEveryState(t *testing.T) {
	cases := []struct {
		name         string
		base         time.Time
		subscribedAt time.Duration // offset from base (negative = in the past)
		lastTickAt   time.Duration // 0 means "never ticked"
		lastAnyTick  time.Duration
		want         LegHealthState
	}{
		{"ticking recently is healthy", rthBase(), -30 * time.Minute, -10 * time.Second, -time.Second, LegHealthHealthy},
		{"just subscribed is warming", rthBase(), -5 * time.Second, 0, -time.Second, LegHealthWarming},
		{"past warmup, never ticked, peers alive is silent", rthBase(), -10 * time.Minute, 0, -time.Second, LegHealthSilent},
		{"was ticking then stopped while peers alive is stale", rthBase(), -30 * time.Minute, -5 * time.Minute, -time.Second, LegHealthStale},
		{"quiet but so is the whole feed is feed_quiet", rthBase(), -30 * time.Minute, -5 * time.Minute, -30 * time.Minute, LegHealthFeedQuiet},
		// Off-RTH the silence threshold widens to 15m, so a 5-minute gap that
		// reads "stale" during RTH is still healthy here.
		{"off-RTH tolerates a longer gap", offRTHBase(), -30 * time.Minute, -5 * time.Minute, -time.Second, LegHealthHealthy},
		{"off-RTH still flags a gap past 15m", offRTHBase(), -60 * time.Minute, -20 * time.Minute, -time.Second, LegHealthStale},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newRotationTestSession(nil)
			var lastTick time.Time
			if tc.lastTickAt != 0 {
				lastTick = tc.base.Add(tc.lastTickAt)
			}
			s.optChain.lastAnyOptionTick = tc.base.Add(tc.lastAnyTick)
			seedLeg(s, lk("IWM", "put", 298, "20260806"), legOpts{
				reqID: 10011, pins: 1,
				subscribedAt: tc.base.Add(tc.subscribedAt), lastTickAt: lastTick,
			})

			snap := s.positionLegsAt(tc.base)
			if len(snap.Legs) != 1 {
				t.Fatalf("got %d legs, want 1", len(snap.Legs))
			}
			if got := snap.Legs[0].Health; got != tc.want {
				t.Errorf("Health = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestPositionLegsAt_CarriesContractIdentityAndRefCount verifies the snapshot
// carries everything a caller needs to join a row back onto its own held
// position and to reason about shared feeds — without which the caller would
// have to re-derive contract identity and could pair a leg with the wrong
// position.
func TestPositionLegsAt_CarriesContractIdentityAndRefCount(t *testing.T) {
	base := rthBase()
	s := newRotationTestSession(nil)
	s.optChain.lastAnyOptionTick = base.Add(-time.Second)
	seedLeg(s, lk("IWM", "put", 298, "20260806"), legOpts{
		reqID: 10011, pins: 2,
		subscribedAt: base.Add(-30 * time.Minute), lastTickAt: base.Add(-10 * time.Second),
	})

	snap := s.positionLegsAt(base)
	leg, ok := snap.Leg("IWM", "put", 298)
	if !ok {
		t.Fatal("Leg() did not find the subscribed contract")
	}
	if leg.Expiry != "20260806" || leg.ReqID != 10011 {
		t.Errorf("identity wrong: expiry=%q reqID=%d", leg.Expiry, leg.ReqID)
	}
	if leg.RefCount != 2 {
		t.Errorf("RefCount = %d, want 2 (two robots sharing one feed)", leg.RefCount)
	}
	if leg.SilentFor != 10*time.Second {
		t.Errorf("SilentFor = %v, want 10s", leg.SilentFor)
	}
	if !snap.FeedAlive {
		t.Error("FeedAlive = false, want true — a peer ticked 1s ago")
	}

	// A contract nobody subscribed must report absent, not a zero-valued row:
	// for a caller holding that contract, absence IS the alarm.
	if _, ok := snap.Leg("IWM", "put", 297); ok {
		t.Error("Leg() reported a contract that was never subscribed")
	}
}

// TestPositionLegsAt_NeverTickedReportsSilentForNegative pins the encoding
// that distinguishes "never ticked" from "ticked just now" — both would read
// as a near-zero duration if never-ticked were reported as 0.
func TestPositionLegsAt_NeverTickedReportsSilentForNegative(t *testing.T) {
	base := rthBase()
	s := newRotationTestSession(nil)
	s.optChain.lastAnyOptionTick = base.Add(-time.Second)
	seedLeg(s, lk("QQQ", "call", 711, "20260806"), legOpts{
		reqID: 10011, pins: 1,
		subscribedAt: base.Add(-10 * time.Minute),
	})

	snap := s.positionLegsAt(base)
	if got := snap.Legs[0].SilentFor; got != -1 {
		t.Errorf("SilentFor = %v for a leg that never ticked, want -1", got)
	}
}

// TestPositionLegsAt_FeedAliveFalseWhenWholeFeedQuiet verifies the
// session-wide caveat is surfaced, so a caller can tell "this position is in
// trouble" apart from "nothing is quotable right now".
func TestPositionLegsAt_FeedAliveFalseWhenWholeFeedQuiet(t *testing.T) {
	base := rthBase()
	s := newRotationTestSession(nil)
	s.optChain.lastAnyOptionTick = base.Add(-30 * time.Minute)
	seedLeg(s, lk("SPY", "put", 770, "20260806"), legOpts{
		reqID: 10011, pins: 1,
		subscribedAt: base.Add(-40 * time.Minute), lastTickAt: base.Add(-30 * time.Minute),
	})

	snap := s.positionLegsAt(base)
	if snap.FeedAlive {
		t.Error("FeedAlive = true while the whole option feed has been quiet for 30m")
	}
	if got := snap.Legs[0].Health; got != LegHealthFeedQuiet {
		t.Errorf("Health = %q, want %q — a quiet feed must condemn nothing", got, LegHealthFeedQuiet)
	}
}

// TestPositionLegsAt_SortedForStableReading — map iteration is random, so
// successive snapshots would reshuffle and make logs undiffable.
func TestPositionLegsAt_SortedForStableReading(t *testing.T) {
	base := rthBase()
	s := newRotationTestSession(nil)
	s.optChain.lastAnyOptionTick = base.Add(-time.Second)
	for i, spec := range []struct {
		sym, right string
		strike     float64
	}{
		{"SPY", "put", 770}, {"IWM", "call", 302}, {"IWM", "put", 298}, {"IWM", "put", 297},
	} {
		reqID := int64(10000 + i)
		seedLeg(s, lk(spec.sym, spec.right, spec.strike, "20260806"), legOpts{
			reqID: reqID, pins: 1,
			subscribedAt: base.Add(-30 * time.Minute), lastTickAt: base.Add(-time.Second),
		})
	}

	snap := s.positionLegsAt(base)
	want := []string{"IWM|call|302", "IWM|put|297", "IWM|put|298", "SPY|put|770"}
	if len(snap.Legs) != len(want) {
		t.Fatalf("got %d legs, want %d", len(snap.Legs), len(want))
	}
	for i, l := range snap.Legs {
		got := fmt.Sprintf("%s|%s|%.0f", l.Symbol, l.Right, l.Strike)
		if got != want[i] {
			t.Errorf("Legs[%d] = %s, want %s", i, got, want[i])
		}
	}
}

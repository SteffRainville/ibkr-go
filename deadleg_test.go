// Tests for detection and repair of option market-data legs IB has silently
// stopped serving — the 2026-08-03 QQQ/SPY/IWM incident, where three legs sat
// frozen for two hours showing prices ~35% away from the real market while
// every guard in the system concluded they were fine.
//
// The repair EXECUTION path is not exercised here: s.client is nil in these
// tests and ibapi.EClient.ReqMktData dereferences its receiver immediately.
// That is why planDeadLegRepairsLocked is split out from reapDeadOptionLegs —
// the whole decision matrix (health classification, feed-alive gate, backoff,
// per-tick cap) is testable with no broker involved, and the execution half
// is a thin loop over its output.
package ibkr

import (
	"testing"
	"time"

	"github.com/scmhub/ibapi"

	"github.com/SteffRainville/ibkr-go/quotes"
)

// rthNoon is inside RTH on a Tuesday, so legSilenceRTH applies.
func rthNoon() time.Time { return atHHMM(12, 0) }

// TestLegHealthAt covers the full classification matrix. The cases that matter
// most are the two that must NOT be actioned: a leg still warming up, and a
// leg that is silent only because the entire option feed is silent.
func TestLegHealthAt(t *testing.T) {
	now := rthNoon()
	ago := func(d time.Duration) time.Time { return now.Add(-d) }

	cases := []struct {
		name                              string
		subscribedAt, lastTickAt, lastAny time.Time
		want                              legHealth
	}{
		{
			name:         "ticking normally",
			subscribedAt: ago(time.Hour), lastTickAt: ago(2 * time.Second), lastAny: ago(time.Second),
			want: legHealthy,
		},
		{
			name:         "just subscribed, no tick yet",
			subscribedAt: ago(3 * time.Second), lastTickAt: time.Time{}, lastAny: ago(time.Second),
			want: legWarming,
		},
		{
			name:         "zero subscribedAt is never condemned",
			subscribedAt: time.Time{}, lastTickAt: time.Time{}, lastAny: ago(time.Second),
			want: legWarming,
		},
		{
			name:         "past warmup, never ticked, feed alive",
			subscribedAt: ago(5 * time.Minute), lastTickAt: time.Time{}, lastAny: ago(time.Second),
			want: legSilent,
		},
		{
			// The incident: ticked for a while, then IB stopped, while every
			// other leg kept flowing.
			name:         "ticked then went silent, feed alive",
			subscribedAt: ago(3 * time.Hour), lastTickAt: ago(2 * time.Hour), lastAny: ago(time.Second),
			want: legStale,
		},
		{
			// The false-positive guard. Identical to the case above except the
			// whole feed is quiet — a halt, an outage, a weekend. Nothing can
			// be concluded about this leg specifically.
			name:         "silent but the whole feed is silent",
			subscribedAt: ago(3 * time.Hour), lastTickAt: ago(2 * time.Hour), lastAny: ago(2 * time.Hour),
			want: legFeedQuiet,
		},
		{
			name:         "no peer ticks recorded at all",
			subscribedAt: ago(3 * time.Hour), lastTickAt: ago(2 * time.Hour), lastAny: time.Time{},
			want: legFeedQuiet,
		},
		{
			// Silence that would be fatal during RTH is normal at 03:00.
			name:         "off-RTH uses the wider threshold",
			subscribedAt: ago(time.Hour), lastTickAt: ago(5 * time.Minute), lastAny: ago(time.Second),
			want: legHealthy,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			at := now
			if c.name == "off-RTH uses the wider threshold" {
				at = atHHMM(3, 0)
				c.subscribedAt = at.Add(-time.Hour)
				c.lastTickAt = at.Add(-5 * time.Minute)
				c.lastAny = at.Add(-time.Second)
			}
			if got := legHealthAt(c.subscribedAt, c.lastTickAt, c.lastAny, at); got != c.want {
				t.Errorf("legHealthAt = %v, want %v", got, c.want)
			}
		})
	}
}

// TestShouldSkipReEstimate_StaleLegIsNotTrusted stood here. It was the crux
// regression for the 2026-08-03 incident: QQQ's put froze at delta -0.5672
// against a 0.60 target — drift 0.033, inside the 0.05 tolerance — so the old
// guard skipped re-estimating it on every rotation pass for the rest of the
// session, the frozen delta itself being the reason the dead leg was never
// repaired.
//
// There is no re-estimate to skip any more. The rotation it guarded selected a
// strike for a watchlist row; rows hold no contract now. The 2026-08-03 failure
// mode is addressed at its root rather than by a better guard — a leg only
// exists while a position holds it, and legHealthAt (tested above) plus
// reapDeadOptionLegs repair it on liveness alone, never on what its cached
// delta claims.

// seedStaleLeg installs a leg that has been silent for 2h.
func seedStaleLeg(s *Session, now time.Time, reqID int64, symbol, right string, strike float64) {
	seedLeg(s, lk(symbol, right, strike, "20260805"), legOpts{
		reqID: reqID, pins: 1,
		subscribedAt: now.Add(-3 * time.Hour), lastTickAt: now.Add(-2 * time.Hour),
	})
}

// TestPlanDeadLegRepairs_QuietFeedActsOnNothing is the anti-storm guard. If
// every leg is silent the correct conclusion is that the FEED is down, not
// that ten contracts simultaneously died — and firing a re-subscribe burst at
// a broker that is not answering is the worst possible response.
func TestPlanDeadLegRepairs_QuietFeedActsOnNothing(t *testing.T) {
	now := rthNoon()
	s := newRotationTestSession(nil)
	for i := range 10 {
		seedStaleLeg(s, now, int64(i), "SYM", "call", float64(100+i))
	}
	s.optChain.lastAnyOptionTick = now.Add(-2 * time.Hour) // whole feed quiet

	repair, silent := s.planDeadLegRepairsLocked(now)
	if len(repair) != 0 || len(silent) != 0 {
		t.Fatalf("planned %d repairs / %d silent with a dead feed, want 0/0", len(repair), len(silent))
	}
}

// TestPlanDeadLegRepairs_CapsPerTick bounds the blast radius if the feed-alive
// gate is ever wrong, so a bad assumption cannot become an IB pacing violation.
func TestPlanDeadLegRepairs_CapsPerTick(t *testing.T) {
	now := rthNoon()
	s := newRotationTestSession(nil)
	for i := range 10 {
		seedStaleLeg(s, now, int64(i), "SYM", "call", float64(100+i))
	}
	s.optChain.lastAnyOptionTick = now.Add(-time.Second)

	repair, _ := s.planDeadLegRepairsLocked(now)
	if len(repair) != maxForcedResubsPerTick {
		t.Fatalf("planned %d repairs, want the per-tick cap of %d", len(repair), maxForcedResubsPerTick)
	}
}

// TestPlanDeadLegRepairs_CooldownPreventsStorm verifies a contract IB will
// never quote cannot burn a market-data line on every rotation tick forever.
func TestPlanDeadLegRepairs_CooldownPreventsStorm(t *testing.T) {
	now := rthNoon()
	s := newRotationTestSession(nil)
	seedStaleLeg(s, now, 1, "QQQ", "put", 693)
	s.optChain.lastAnyOptionTick = now.Add(-time.Second)

	if repair, _ := s.planDeadLegRepairsLocked(now); len(repair) != 1 {
		t.Fatalf("first pass planned %d repairs, want 1", len(repair))
	}
	// A tick later the leg is still dead, but it is inside its backoff.
	later := now.Add(3 * time.Second)
	s.optChain.lastAnyOptionTick = later.Add(-time.Second)
	if repair, _ := s.planDeadLegRepairsLocked(later); len(repair) != 0 {
		t.Fatalf("second pass planned %d repairs inside the cooldown, want 0", len(repair))
	}
	// Past the backoff it may try again.
	muchLater := now.Add(forcedResubBaseCooldown + time.Second)
	s.optChain.lastAnyOptionTick = muchLater.Add(-time.Second)
	if repair, _ := s.planDeadLegRepairsLocked(muchLater); len(repair) != 1 {
		t.Fatalf("third pass planned %d repairs after the cooldown elapsed, want 1", len(repair))
	}
}

// TestPlanDeadLegRepairs_NeverTickedIsNotForceResubscribed separates the two
// failure modes: a leg that ticked and died is repaired at the same strike,
// but one that never ticked at all may be sitting on an unquotable strike, so
// re-requesting it would fail identically. Those are reported and left to the
// rotation to re-estimate onto a different strike.
func TestPlanDeadLegRepairs_NeverTickedIsNotForceResubscribed(t *testing.T) {
	now := rthNoon()
	s := newRotationTestSession(nil)
	seedLeg(s, lk("THIN", "call", 50, "20260805"), legOpts{
		reqID:        1,
		subscribedAt: now.Add(-5 * time.Minute), // past warmup, never ticked
	})
	s.optChain.lastAnyOptionTick = now.Add(-time.Second)

	repair, silent := s.planDeadLegRepairsLocked(now)
	if len(repair) != 0 {
		t.Fatalf("planned %d forced re-subscribes for a never-ticked leg, want 0", len(repair))
	}
	if len(silent) != 1 || silent[0].health != legSilent {
		t.Fatalf("silent = %+v, want one legSilent entry", silent)
	}
}

// TestPlanDeadLegRepairs_PinnedLegIsFlagged verifies a position-pinned leg is
// picked up and marked, since that is the stop-loss-critical case: a frozen
// pinned quote silently disarms the exit logic.
func TestPlanDeadLegRepairs_PinnedLegIsFlagged(t *testing.T) {
	now := rthNoon()
	s := newRotationTestSession(nil)
	seedLeg(s, lk("QQQ", "put", 693, "20260805"), legOpts{
		reqID: 900, pins: 1,
		subscribedAt: now.Add(-3 * time.Hour), lastTickAt: now.Add(-2 * time.Hour),
	})
	s.optChain.lastAnyOptionTick = now.Add(-time.Second)

	repair, _ := s.planDeadLegRepairsLocked(now)
	if len(repair) != 1 {
		t.Fatalf("planned %d repairs for a frozen pinned leg, want 1", len(repair))
	}
	if !repair[0].pinned {
		t.Error("pinned flag not set — the recovery path and the alert both branch on it")
	}
}

// TestTickSizeStampsLegLiveness verifies size ticks count as liveness. This is
// the signal that separates a flat-but-live quote from a dead one, which the
// Book's advance-on-change timestamps cannot do. It also guards the lock
// ordering against resolveReqID, which takes optChain.mu itself — a
// regression there deadlocks rather than fails, so this test hanging is the
// symptom to look for.
func TestTickSizeStampsLegLiveness(t *testing.T) {
	s := newRotationTestSession(nil)
	key := lk("QQQ", "call", 480, "20260805")
	seedLeg(s, key, legOpts{reqID: 1, subscribedAt: time.Now().Add(-time.Hour)})

	s.TickSize(1, 0, ibapi.StringToDecimal("100"))

	s.optChain.mu.Lock()
	if s.optChain.legs[key].lastTickAt.IsZero() {
		t.Fatal("lastTickAt still zero after a size tick — a liquid option with a flat quote would be judged dead")
	}
	if s.optChain.lastAnyOptionTick.IsZero() {
		t.Fatal("lastAnyOptionTick still zero — the feed-alive gate depends on this being advanced")
	}
	s.optChain.mu.Unlock()

	// The shared Book must agree with the tracker above — the ORBtrader
	// dashboard/position liveness coloring reads the Book, not deadleg.go's
	// internal state, so a size tick that only touched the tracker would leave
	// the UI showing this leg as dead while deadleg.go considers it alive.
	oq, ok := s.book.Option(quotes.ContractKey{Symbol: "QQQ", Right: "call", Strike: 480, Expiry: "20260805"})
	if !ok || oq.LastTickTime.IsZero() {
		t.Fatal("Book's OptionQuote.LastTickTime not stamped by a size tick")
	}
}

// TestTickSizeStampsStockLiveness is the stock analogue: a stock's size ticks
// (bid/ask size changing with the price flat, common outside RTH) must reach
// the Book too, not just the option-only tracker above.
func TestTickSizeStampsStockLiveness(t *testing.T) {
	s := newRotationTestSession(nil)
	s.mktData.mktDataSymbol[7] = "TQQQ"

	s.TickSize(7, 0, ibapi.StringToDecimal("500"))

	sq, ok := s.book.Stock("TQQQ")
	if !ok || sq.LastTickTime.IsZero() {
		t.Fatal("Book's StockQuote.LastTickTime not stamped by a size tick")
	}
}

// TestResubCooldownBacksOff verifies the backoff grows and is capped.
func TestResubCooldownBacksOff(t *testing.T) {
	if got := resubCooldown(1); got != forcedResubBaseCooldown {
		t.Errorf("resubCooldown(1) = %s, want %s", got, forcedResubBaseCooldown)
	}
	if got := resubCooldown(2); got != 2*forcedResubBaseCooldown {
		t.Errorf("resubCooldown(2) = %s, want %s", got, 2*forcedResubBaseCooldown)
	}
	if got := resubCooldown(50); got != forcedResubMaxCooldown {
		t.Errorf("resubCooldown(50) = %s, want the %s cap", got, forcedResubMaxCooldown)
	}
}

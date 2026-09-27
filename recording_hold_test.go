package ibkr

import (
	"testing"
	"time"

	"github.com/SteffRainville/ibkr-go/mdlines"
)

// A recording hold keeps a contract streaming after its last position exits —
// so an exit replay can price a wider take-profit than the one taken — at the
// lowest line priority, and lets the feed go once the hold is released.
func TestRecordingHold_OutlivesTheLastPosition(t *testing.T) {
	s := withOfflineClient(newRotationTestSession(nil))
	key := lk("IWM", "put", 298, "20260806")
	seedLeg(s, key, legOpts{reqID: 10011, pins: 1})
	s.mdLines.GrantGuaranteed(10011, mdlines.CategoryPosition)

	s.HoldForRecording("IWM", "put", 298, "20260806")
	s.UnsubscribePositionStrike("IWM", "put", 298, "20260806")

	pins, ok := legPins(s, key)
	if !ok || pins != 0 {
		t.Fatalf("leg after the position exits: ok=%v pins=%d, want kept with no pins", ok, pins)
	}
	if got := s.mdLines.RecordingCount(); got != 1 {
		t.Errorf("recording lines = %d, want 1 (the line demoted, not cancelled)", got)
	}

	s.ReleaseRecording("IWM", "put", 298, "20260806")
	if _, ok := legPins(s, key); ok {
		t.Error("leg survived its last recording hold — its line is leaked")
	}
	if used, _ := s.mdLines.Status(); used != 0 {
		t.Errorf("ledger lines = %d after the last holder left, want 0", used)
	}
}

// A contract nothing holds is subscribed afresh for recording only from free
// headroom, as a recording line.
func TestRecordingHold_NewSubscriptionIsARecordingLine(t *testing.T) {
	s := withOfflineClient(newRotationTestSession(nil))
	s.HoldForRecording("QQQ", "call", 492.5, "20260928")

	if _, ok := legPins(s, lk("QQQ", "call", 492.5, "20260928")); !ok {
		t.Fatal("no leg opened for the recording hold")
	}
	if got := s.mdLines.RecordingCount(); got != 1 {
		t.Errorf("recording lines = %d, want 1", got)
	}
}

// Eviction drops a recording-only leg, but a leg a position re-pinned in the
// meantime keeps its feed and gets its line back as a position line.
func TestEvictRecordingLines(t *testing.T) {
	s := withOfflineClient(newRotationTestSession(nil))
	gone := lk("SPY", "put", 640, "20260807")
	kept := lk("SPY", "call", 650, "20260807")
	l := seedLeg(s, gone, legOpts{reqID: 20001})
	l.tails = 1
	l = seedLeg(s, kept, legOpts{reqID: 20002, pins: 1})
	l.tails = 1

	s.evictRecordingLines([]int64{20001, 20002})

	if _, ok := legPins(s, gone); ok {
		t.Error("recording-only leg survived its eviction")
	}
	if pins, ok := legPins(s, kept); !ok || pins != 1 {
		t.Error("a position-pinned leg was dropped by a recording eviction")
	}
	if _, pos, _, _ := s.mdLines.CategoryCounts(); pos != 1 {
		t.Errorf("position lines = %d, want the re-pinned leg's line re-granted", pos)
	}
}

// A leg kept only for recording is never force-resubscribed: its position is
// gone, and a contract going quiet after its own close is expected.
func TestPlanDeadLegRepairs_SkipsRecordingOnlyLegs(t *testing.T) {
	now := rthNoon()
	s := newRotationTestSession(nil)
	leg := seedLeg(s, lk("QQQ", "put", 693, "20260805"), legOpts{
		reqID: 1, subscribedAt: now.Add(-time.Hour), lastTickAt: now.Add(-10 * time.Minute),
	})
	leg.tails = 1
	s.optChain.lastAnyOptionTick = now.Add(-time.Second)

	repair, silent := s.planDeadLegRepairsLocked(now)
	if len(repair) != 0 || len(silent) != 0 {
		t.Fatalf("repair=%v silent=%v for a recording-only leg, want none", repair, silent)
	}
}

package ibkr

import (
	"testing"
	"time"

	"github.com/scmhub/ibapi"
)

// sessionWithTimeout builds a minimal Session whose bar-stall timeout is
// the given duration.
func sessionWithTimeout(d time.Duration) *Session {
	return &Session{opts: Options{BarStallTimeout: d}}
}

func atHHMM(hh, mm int) time.Time {
	return time.Date(2026, 7, 14, hh, mm, 0, 0, time.Local)
}

func TestIsRTHAt(t *testing.T) {
	// atHHMM builds times on 2026-07-14, a Tuesday, so weekday checks pass.
	cases := []struct {
		h, m int
		want bool
	}{
		{9, 29, false}, // just before the open
		{9, 30, true},  // the open boundary is inclusive
		{12, 0, true},  // midday
		{15, 59, true}, // last RTH minute
		{16, 0, false}, // the close boundary is exclusive
		{8, 0, false},  // pre-market
		{18, 0, false}, // after-hours
	}
	for _, c := range cases {
		if got := isRTHAt(atHHMM(c.h, c.m)); got != c.want {
			t.Errorf("isRTHAt(%02d:%02d) = %v, want %v", c.h, c.m, got, c.want)
		}
	}
	// Weekend is never RTH, even midday. 2026-07-18 is a Saturday.
	sat := time.Date(2026, 7, 18, 12, 0, 0, 0, time.Local)
	if isRTHAt(sat) {
		t.Error("isRTHAt(Saturday 12:00) = true, want false")
	}
}

func TestBarStallTimeoutTimeOfDay(t *testing.T) {
	s := sessionWithTimeout(2 * time.Minute)

	// During RTH the configured timeout is used verbatim.
	if got := s.barStallTimeout(atHHMM(11, 0)); got != 2*time.Minute {
		t.Errorf("RTH timeout = %s, want 2m", got)
	}
	// Outside RTH the looser floor (5 min) wins over the smaller configured value.
	if got := s.barStallTimeout(atHHMM(17, 0)); got != barStallTimeoutOffRTH {
		t.Errorf("off-RTH timeout = %s, want %s", got, barStallTimeoutOffRTH)
	}

	// A configured timeout larger than the off-RTH floor is respected off-RTH.
	s2 := sessionWithTimeout(10 * time.Minute)
	if got := s2.barStallTimeout(atHHMM(17, 0)); got != 10*time.Minute {
		t.Errorf("off-RTH timeout with large config = %s, want 10m", got)
	}
}

func TestBarFeedStalled(t *testing.T) {
	s := sessionWithTimeout(2 * time.Minute)

	t.Run("no bar seen yet is never stalled", func(t *testing.T) {
		if s.barFeedStalled(atHHMM(11, 0)) {
			t.Error("barFeedStalled = true with lastBarNano unset, want false")
		}
	})

	now := atHHMM(11, 0) // RTH → 2 min threshold

	t.Run("recent bar is not stalled", func(t *testing.T) {
		s.lastBarNano.Store(now.Add(-30 * time.Second).UnixNano())
		if s.barFeedStalled(now) {
			t.Error("barFeedStalled = true 30s after last bar, want false")
		}
	})

	t.Run("stale bar past threshold is stalled", func(t *testing.T) {
		s.lastBarNano.Store(now.Add(-3 * time.Minute).UnixNano())
		if !s.barFeedStalled(now) {
			t.Error("barFeedStalled = false 3m after last bar (RTH), want true")
		}
	})

	t.Run("off-RTH uses the looser threshold", func(t *testing.T) {
		off := atHHMM(17, 0) // off-RTH → 5 min threshold
		s.lastBarNano.Store(off.Add(-3 * time.Minute).UnixNano())
		if s.barFeedStalled(off) {
			t.Error("barFeedStalled = true 3m after last bar off-RTH (5m threshold), want false")
		}
		s.lastBarNano.Store(off.Add(-6 * time.Minute).UnixNano())
		if !s.barFeedStalled(off) {
			t.Error("barFeedStalled = false 6m after last bar off-RTH, want true")
		}
	})
}

// TestHistoricalDataUpdateStampsLastBar verifies the watchdog liveness signal is
// advanced by every live bar update, since that stamp is what the watchdog reads.
func TestHistoricalDataUpdateStampsLastBar(t *testing.T) {
	s := NewSession(Options{}, nil, nil)
	s.reqSymbol = map[int64]string{reqIDHistBase: "TEST"}
	// buses left nil — publish/updateBookPrice range over them safely.
	if got := s.lastBarNano.Load(); got != 0 {
		t.Fatalf("lastBarNano = %d before any bar, want 0", got)
	}

	before := time.Now().UnixNano()
	bar := &ibapi.Bar{Date: "20260714 11:00:00", Open: 1, High: 1, Low: 1, Close: 1}
	s.HistoricalDataUpdate(reqIDHistBase, bar)

	if got := s.lastBarNano.Load(); got < before {
		t.Errorf("lastBarNano = %d after HistoricalDataUpdate, want >= %d", got, before)
	}
}

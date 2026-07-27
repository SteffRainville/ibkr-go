package ibkr

import (
	"testing"
	"time"
)

// TestInLiveWarmup covers the three states of the post-connect warm-up gate.
func TestInLiveWarmup(t *testing.T) {
	t.Run("zero liveStart disables the gate", func(t *testing.T) {
		s := &Session{}
		if s.inLiveWarmup() {
			t.Error("inLiveWarmup() = true with unset liveStart, want false")
		}
	})

	t.Run("just connected is in warm-up", func(t *testing.T) {
		s := &Session{liveStart: time.Now()}
		if !s.inLiveWarmup() {
			t.Error("inLiveWarmup() = false immediately after connect, want true")
		}
	})

	t.Run("past the window trades normally", func(t *testing.T) {
		s := &Session{liveStart: time.Now().Add(-liveTradingWarmup - time.Second)}
		if s.inLiveWarmup() {
			t.Error("inLiveWarmup() = true after the warm-up window elapsed, want false")
		}
	})
}

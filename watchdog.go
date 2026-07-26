package ibkr

import "time"

// Bar-feed watchdog.
//
// Live bars stream through IB's Historical Market Data Service (HMDS) data
// farm via reqHistoricalData(keepUpToDate=true). When that farm drops but
// the TCP socket to TWS stays open — account and tick data keep flowing —
// IB terminates the bar streams and does NOT resume them on a "data
// maintained" restore (error code 1102). The client's normal reconnect
// never fires, because it is only triggered when the connection's context
// is cancelled, which happens on a socket close. The result is a silent
// freeze: bar timestamps stop advancing while the console still scrolls IB
// data.
//
// The watchdog observes lastBarNano — stamped on every HistoricalDataUpdate
// — and, once bars have been flowing, forces a clean reconnect if no bar
// arrives within the stall timeout, by disconnecting the client (which
// cancels its context, so Run returns and the caller can reconnect with
// fresh subscriptions).
const (
	// barStallTimeoutOffRTH is the looser stall threshold used outside
	// regular trading hours, where per-symbol bar cadence can be less
	// uniform. It is a floor: the configured Options.BarStallTimeout is
	// used when it is larger.
	barStallTimeoutOffRTH = 5 * time.Minute

	// barWatchdogInterval is how often the watchdog checks for a stall.
	barWatchdogInterval = 30 * time.Second
)

// barStallTimeout returns the stall threshold appropriate for the time of
// day: the configured Options.BarStallTimeout during RTH, and the looser of
// that value or barStallTimeoutOffRTH outside RTH.
func (s *Session) barStallTimeout(now time.Time) time.Duration {
	base := s.opts.BarStallTimeout
	if isRTHAt(now) {
		return base
	}
	if base > barStallTimeoutOffRTH {
		return base
	}
	return barStallTimeoutOffRTH
}

// barFeedStalled reports whether the live bar feed has gone silent for
// longer than the time-of-day threshold. Returns false until the first live
// bar of the session has been seen.
func (s *Session) barFeedStalled(now time.Time) bool {
	last := s.lastBarNano.Load()
	if last == 0 {
		return false
	}
	return now.Sub(time.Unix(0, last)) > s.barStallTimeout(now)
}

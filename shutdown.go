package ibkr

import "time"

// Orderly disconnect.
//
// A session that simply drops its socket (process exit, Ctrl-C) leaves the
// cancelling to TWS, and TWS does not reliably do it: keepUpToDate historical
// streams in particular can outlive the client and keep counting against IB's
// ~50 concurrent-stream limit until TWS is restarted. So a session cancels
// every standing request itself before it disconnects.
//
// Cancelling is not enough on its own. ibapi queues every request on a
// channel that a writer goroutine drains, and EClient.Disconnect stops that
// goroutine at once — the cancels just issued would mostly still be sitting
// in the queue, never sent. teardown therefore follows them with a
// ReqCurrentTime and waits for the reply: the writer sends in order and TWS
// answers in order, so the reply proves every cancel before it was delivered.

// teardownTimeout bounds how long a teardown waits for TWS to confirm its
// cancels before disconnecting anyway.
const teardownTimeout = 3 * time.Second

// Close cancels every subscription this session holds, waits (up to timeout)
// for TWS to have received the cancels, and disconnects. Safe to call on a
// session that never connected or has already disconnected.
func (s *Session) Close(timeout time.Duration) {
	if s.client == nil || !s.client.IsConnected() {
		return
	}
	s.teardown(timeout)
}

// teardown is Close without the connected check, for callers inside Run that
// already know the client is live.
func (s *Session) teardown(timeout time.Duration) {
	s.cancelAllSubscriptions()
	if s.awaitRequestsDelivered(timeout) {
		s.logger.Println("Teardown: TWS confirmed the cancels — disconnecting")
	} else {
		s.logger.Printf("Teardown: no confirmation from TWS within %s — disconnecting anyway", timeout)
	}
	s.client.Disconnect()
}

// awaitRequestsDelivered sends a ReqCurrentTime behind everything already
// queued and reports whether TWS answered it within timeout.
func (s *Session) awaitRequestsDelivered(timeout time.Duration) bool {
	select {
	case <-s.currentTime: // a stale reply must not stand in for this one
	default:
	}
	s.client.ReqCurrentTime()
	select {
	case <-s.currentTime:
		return true
	case <-s.client.Ctx().Done():
		return false
	case <-time.After(timeout):
		return false
	}
}

// CurrentTime is IB's reply to ReqCurrentTime — used only as teardown's barrier.
func (s *Session) CurrentTime(int64) {
	select {
	case s.currentTime <- struct{}{}:
	default:
	}
}

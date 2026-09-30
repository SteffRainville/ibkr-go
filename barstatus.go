package ibkr

import (
	"sync"
	"time"
)

// BarStreamStatus is what one trading symbol's 30-second bar stream (the
// keepUpToDate historical request every indicator and VWAP/ATR is built on)
// has done this session — the answer to "why does this symbol have no bars?".
// A request IB neither answers nor refuses leaves no log line at all; this is
// where that silence becomes visible.
type BarStreamStatus struct {
	Requested  time.Time // when the stream was requested; zero = never
	Skipped    bool      // not requested: over the MaxHistoricalStreams limit
	HistoryEnd time.Time // IB finished delivering the day's backfill; zero = not yet
	FirstBar   time.Time // first bar (backfill or live) received; zero = none
	LastBar    time.Time // latest bar or live update received
	ErrCode    int64     // last IB error on the request; 0 = none
	ErrMsg     string
	ErrTime    time.Time
}

// barStatusTable holds BarStreamStatus per symbol for the current session.
type barStatusTable struct {
	mu sync.Mutex
	m  map[string]*BarStreamStatus
}

func (t *barStatusTable) reset() {
	t.mu.Lock()
	t.m = make(map[string]*BarStreamStatus)
	t.mu.Unlock()
}

// update applies fn to symbol's status, creating it on first use.
func (t *barStatusTable) update(symbol string, fn func(*BarStreamStatus)) {
	if symbol == "" {
		return
	}
	t.mu.Lock()
	if t.m == nil {
		t.m = make(map[string]*BarStreamStatus)
	}
	st := t.m[symbol]
	if st == nil {
		st = &BarStreamStatus{}
		t.m[symbol] = st
	}
	fn(st)
	t.mu.Unlock()
}

func (t *barStatusTable) get(symbol string) (BarStreamStatus, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st, ok := t.m[symbol]
	if !ok {
		return BarStreamStatus{}, false
	}
	return *st, true
}

// noteBar records a bar or live update for symbol.
func (t *barStatusTable) noteBar(symbol string) {
	now := time.Now()
	t.update(symbol, func(st *BarStreamStatus) {
		if st.FirstBar.IsZero() {
			st.FirstBar = now
		}
		st.LastBar = now
	})
}

// BarStreamStatus returns symbol's bar-stream status for this session, and
// false when the session has never subscribed it.
func (s *Session) BarStreamStatus(symbol string) (BarStreamStatus, bool) {
	return s.barStatus.get(symbol)
}

// Package candlestore accumulates completed OHLCV candles for every tracked
// symbol, at whatever bar size the IB subscription requests (currently
// 30 seconds). It is the in-process historical data source used by indicators
// (MACD, EMA, RSI, …) that need a lookback window of past bars.
//
// Two bar types are handled:
//
//   - Historical bars (from ReqHistoricalData) arrive as complete bars and are
//     appended directly via AddHistorical.
//
//   - Live bars (from HistoricalDataUpdate) arrive as 5-second partial updates
//     sharing the same bar-start timestamp. UpdateLive reconstructs the bar
//     incrementally:
//       · first tick  → Open
//       · every tick  → High / Low (running min/max)
//       · latest tick → Close
//       · volume      → cumulative within the bar; use last value as total
//       · WAP         → sum(wap_5s × ΔVol) / totalVol  (volume-weighted)
//       · count       → cumulated across all ticks
//
//     The pending candle is finalised (moved to history) when a tick with a new
//     bar-start timestamp arrives.
package candlestore

import (
	"sync"
	"time"
)

// Candle holds OHLCV data for one completed bar.
type Candle struct {
	Symbol    string
	Timestamp time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64 // total shares traded in the bar
	Wap       float64 // volume-weighted average price for the bar
	Count     int64   // total trade count for the bar
}

// inProgress accumulates 5-second HistoricalDataUpdate ticks into a single
// bar. It is keyed by the bar's formatted start timestamp string so that all
// ticks sharing that key are merged.
type inProgress struct {
	ts      string  // bar-start key — "2026-04-09 09:45:00"
	open    float64
	high    float64
	low     float64
	close_  float64
	lastVol float64 // last cumulative volume seen (5-sec vol resets each bar)
	wapNum  float64 // running sum(wap_5s × ΔVol) for final WAP calculation
	count   int64   // cumulated trade count across all ticks
}

// Store is a thread-safe in-memory store of completed candles.
// Candles are appended in chronological order as they arrive; no sorting is
// performed. The store is never persisted — it resets on every IB reconnect.
type Store struct {
	mu      sync.RWMutex
	candles map[string][]Candle    // symbol → completed candles (oldest first)
	pending map[string]*inProgress // symbol → candle under construction
}

// New returns an empty, ready-to-use Store.
func New() *Store {
	return &Store{
		candles: make(map[string][]Candle),
		pending: make(map[string]*inProgress),
	}
}

// AddHistorical appends a complete bar received from ReqHistoricalData.
// The WAP and Volume are used as-is — IB already computes them correctly for
// historical bars.
func (s *Store) AddHistorical(symbol, dateStr string, open, high, low, close_, volume, wap float64, count int64) {
	c := Candle{
		Symbol:    symbol,
		Timestamp: parseTS(dateStr),
		Open:      open,
		High:      high,
		Low:       low,
		Close:     close_,
		Volume:    volume,
		Wap:       wap,
		Count:     count,
	}
	s.mu.Lock()
	s.candles[symbol] = append(s.candles[symbol], c)
	s.mu.Unlock()
}

// UpdateLive incorporates one 5-second HistoricalDataUpdate tick into the
// pending candle for symbol.
//
// When a tick with a new dateStr arrives (bar boundary crossed), the previous
// pending candle is finalised and appended to history before the new one begins.
//
// Returns true when a complete bar was just finalised (bar boundary crossed).
// The caller can use this signal to publish a live KindCandle event for the
// completed bar — ensuring indicators compute on whole bars, not partials.
func (s *Store) UpdateLive(symbol, dateStr string, open, high, low, close_, volume, wap float64, count int64) (finalized bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	prev := s.pending[symbol]

	// Bar boundary — finalise the previous pending candle.
	if prev != nil && prev.ts != dateStr {
		s.candles[symbol] = append(s.candles[symbol], finalize(symbol, prev))
		prev = nil
		finalized = true
	}

	if prev == nil {
		// First tick for this bar: volume is cumulative from 0, so the first
		// incremental volume equals the raw volume field.
		s.pending[symbol] = &inProgress{
			ts:      dateStr,
			open:    open,
			high:    high,
			low:     low,
			close_:  close_,
			lastVol: volume,
			wapNum:  wap * volume,
			count:   count,
		}
		return
	}

	// Subsequent tick within the same bar.
	incrVol := volume - prev.lastVol
	if incrVol < 0 {
		incrVol = 0 // guard against unexpected IB volume resets
	}
	if high > prev.high {
		prev.high = high
	}
	if low < prev.low {
		prev.low = low
	}
	prev.close_ = close_
	prev.lastVol = volume
	prev.wapNum += wap * incrVol
	prev.count += count
	return
}

// Candles returns a snapshot of all completed candles for symbol, oldest first.
// The returned slice is a copy — safe to read without holding the store lock.
func (s *Store) Candles(symbol string) []Candle {
	s.mu.RLock()
	defer s.mu.RUnlock()
	src := s.candles[symbol]
	out := make([]Candle, len(src))
	copy(out, src)
	return out
}

// Last returns the n most-recent completed candles for symbol (oldest first).
// If fewer than n candles are available, all available candles are returned.
func (s *Store) Last(symbol string, n int) []Candle {
	s.mu.RLock()
	defer s.mu.RUnlock()
	src := s.candles[symbol]
	if n >= len(src) {
		out := make([]Candle, len(src))
		copy(out, src)
		return out
	}
	out := make([]Candle, n)
	copy(out, src[len(src)-n:])
	return out
}

// Pending returns a snapshot of the in-progress bar for symbol if one exists.
// The bar is built from 5-second live ticks and has not yet been finalized.
// The second return value is false when no live bar is currently accumulating.
func (s *Store) Pending(symbol string) (Candle, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p := s.pending[symbol]
	if p == nil {
		return Candle{}, false
	}
	return finalize(symbol, p), true
}

// Reset clears all accumulated candles and pending bars for a new IB session.
func (s *Store) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.candles = make(map[string][]Candle)
	s.pending = make(map[string]*inProgress)
}

// Symbols returns the base symbols that have at least one completed candle,
// in no guaranteed order.
func (s *Store) Symbols() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.candles))
	for sym := range s.candles {
		out = append(out, sym)
	}
	return out
}

// Len returns the number of completed candles stored for symbol.
func (s *Store) Len(symbol string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.candles[symbol])
}

// finalize converts an inProgress into a completed Candle.
// The final WAP = total WAP numerator / total cumulative volume.
func finalize(symbol string, p *inProgress) Candle {
	wap := p.wapNum
	if p.lastVol > 0 {
		wap = p.wapNum / p.lastVol
	}
	return Candle{
		Symbol:    symbol,
		Timestamp: parseTS(p.ts),
		Open:      p.open,
		High:      p.high,
		Low:       p.low,
		Close:     p.close_,
		Volume:    p.lastVol,
		Wap:       wap,
		Count:     p.count,
	}
}

// parseTS parses the formatted date string produced by FormatBarDate
// ("2026-04-09 09:45:00") into a time.Time in the local timezone.
// Returns zero time on parse failure.
func parseTS(formatted string) time.Time {
	t, _ := time.ParseInLocation("2006-01-02 15:04:05", formatted, time.Local)
	return t
}

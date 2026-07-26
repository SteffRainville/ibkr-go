package ibkr

import (
	"strings"
	"time"

	"github.com/scmhub/ibapi"

	"github.com/SteffRainville/ibkr-go/eventbus"
)

// liveTradingWarmup is how long after a session begins subscribing to live
// bars the caller should ignore trade signals. It must comfortably exceed
// the near-instant catch-up burst IB delivers on (re)connect while staying
// well under the live 30-second bar cadence.
const liveTradingWarmup = 8 * time.Second

// inLiveWarmup reports whether the session is still within the post-connect
// warm-up window during which catch-up bars must not trigger trades.
func (s *Session) inLiveWarmup() bool {
	return !s.liveStart.IsZero() && time.Since(s.liveStart) < liveTradingWarmup
}

// FormatBarDate converts IB's raw date string into a clean, readable form.
// Input:  "20260409 09:45:00 US/Eastern"
// Output: "2026-04-09 09:45:00"
func FormatBarDate(date string) string {
	parts := strings.Fields(date)
	if len(parts) < 2 {
		return date
	}
	d := parts[0]
	if len(d) == 8 {
		d = d[:4] + "-" + d[4:6] + "-" + d[6:]
	}
	return d + " " + parts[1]
}

// HistoricalData is called once per historical bar during backfill.
//
// On-demand requests (reqID in the 4001+ range) are routed differently:
// bars are stored in the candle store but no KindCandle event is published,
// since on-demand symbols are not part of the live trading universe.
func (s *Session) HistoricalData(reqId int64, bar *ibapi.Bar) {
	s.onDemand.mu.Lock()
	odSym, isOD := s.onDemand.reqSymbol[reqId]
	s.onDemand.mu.Unlock()
	if isOD {
		date := FormatBarDate(bar.Date)
		s.Candles.AddHistorical(odSym, date, bar.Open, bar.High, bar.Low, bar.Close, bar.Volume.Float(), bar.Wap.Float(), bar.BarCount)
		return
	}

	baseSym := s.reqSymbol[reqId]
	date := FormatBarDate(bar.Date)
	s.logger.Printf("Historical Bar %-5s %s O:%.2f H:%.2f L:%.2f C:%.2f", baseSym, date, bar.Open, bar.High, bar.Low, bar.Close)
	s.Candles.AddHistorical(baseSym, date, bar.Open, bar.High, bar.Low, bar.Close, bar.Volume.Float(), bar.Wap.Float(), bar.BarCount)
	s.processBar(baseSym, bar, false)
}

// HistoricalDataEnd is called by IB when all historical bars for a non-
// keepUpToDate request have been delivered.
func (s *Session) HistoricalDataEnd(reqID int64, startDateStr string, endDateStr string) {
	s.onDemand.mu.Lock()
	ch, ok := s.onDemand.done[reqID]
	s.onDemand.mu.Unlock()
	if !ok {
		return
	}
	s.logger.Printf("OnDemand hist end: reqID=%d start=%s end=%s", reqID, startDateStr, endDateStr)
	select {
	case ch <- nil:
	default:
	}
}

// HistoricalDataUpdate is called repeatedly while a live bar is forming.
// Each call is a partial update; the candle store assembles them into
// completed 30-second bars as bar boundaries are crossed.
func (s *Session) HistoricalDataUpdate(reqId int64, bar *ibapi.Bar) {
	// Stamp the bar-feed watchdog liveness signal.
	s.lastBarNano.Store(time.Now().UnixNano())

	baseSym := s.reqSymbol[reqId]
	date := FormatBarDate(bar.Date)
	s.logger.Printf(">>> LIVE      %-5s %s O:%.2f H:%.2f L:%.2f C:%.2f", baseSym, date, bar.Open, bar.High, bar.Low, bar.Close)

	complete := s.Candles.UpdateLive(baseSym, date, bar.Open, bar.High, bar.Low, bar.Close, bar.Volume.Float(), bar.Wap.Float(), bar.BarCount)

	s.updateBookPrice(baseSym, bar.Close, date)
	if s.opts.OnPositionPriceUpdate != nil {
		s.opts.OnPositionPriceUpdate(baseSym, bar.Close)
	}

	live := !s.inLiveWarmup()

	if complete {
		completed := s.Candles.Last(baseSym, 1)
		if len(completed) > 0 {
			c := completed[0]
			prevDate := c.Timestamp.Format("2006-01-02 15:04:05")
			s.publish(eventbus.Event{
				Kind: eventbus.KindCandle,
				Payload: eventbus.Candle{
					Symbol: baseSym,
					Date:   prevDate,
					Open:   c.Open,
					High:   c.High,
					Low:    c.Low,
					Close:  c.Close,
					Volume: c.Volume,
					WAP:    c.Wap,
					IsLive: live,
				},
			})
		}
	}

	if live {
		s.publish(eventbus.Event{
			Kind: eventbus.KindLiveTick,
			Payload: eventbus.LiveTick{
				Symbol: baseSym,
				Price:  bar.Close,
				Date:   date,
			},
		})
	}
}

// processBar publishes the raw candle for a non-live (historical) bar.
func (s *Session) processBar(baseSym string, bar *ibapi.Bar, isLive bool) {
	date := FormatBarDate(bar.Date)
	s.updateBookPrice(baseSym, bar.Close, date)
	if s.opts.OnPositionPriceUpdate != nil {
		s.opts.OnPositionPriceUpdate(baseSym, bar.Close)
	}
	s.publish(eventbus.Event{
		Kind: eventbus.KindCandle,
		Payload: eventbus.Candle{
			Symbol: baseSym,
			Date:   date,
			Open:   bar.Open,
			High:   bar.High,
			Low:    bar.Low,
			Close:  bar.Close,
			Volume: bar.Volume.Float(),
			WAP:    bar.Wap.Float(),
			IsLive: isLive,
		},
	})
}

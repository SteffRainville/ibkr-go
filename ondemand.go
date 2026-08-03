package ibkr

import (
	"context"
	"fmt"
	"strings"

	"github.com/scmhub/ibapi"
)

// FetchHistorical requests 1 day of 30-second historical bars for symbol and
// blocks until all bars have been received (HistoricalDataEnd callback) or
// ctx is cancelled/times out.
//
// endDateTime is the IB endDateTime parameter: "" means "now"; a
// "YYYYMMDD HH:MM:SS" value fetches bars up to and including that time.
//
// storeKey is the candle store key to store bars under. When empty,
// defaults to the normalised symbol. Bars are stored in s.Candles as they
// arrive; the caller reads them after this returns nil.
func (s *Session) FetchHistorical(ctx context.Context, symbol, endDateTime, storeKey, exchange string) error {
	return s.fetchHistoricalRange(ctx, symbol, endDateTime, storeKey, exchange, "1 D", "30 secs")
}

// FetchHistoricalRange is FetchHistorical with a caller-supplied duration
// and bar size (IB's durationStr/barSizeSetting, e.g. "1 M"/"1 hour")
// instead of the fixed "1 D"/"30 secs" FetchHistorical always uses.
func (s *Session) FetchHistoricalRange(ctx context.Context, symbol, endDateTime, storeKey, exchange, duration, barSize string) error {
	return s.fetchHistoricalRange(ctx, symbol, endDateTime, storeKey, exchange, duration, barSize)
}

func (s *Session) fetchHistoricalRange(ctx context.Context, symbol, endDateTime, storeKey, exchange, duration, barSize string) error {
	sym := strings.ToUpper(strings.TrimSpace(symbol))
	if sym == "" {
		return fmt.Errorf("empty symbol")
	}
	if storeKey == "" {
		storeKey = sym
	}
	exchange = strings.ToUpper(strings.TrimSpace(exchange))
	if exchange == "" {
		exchange = "SMART"
	}

	s.onDemand.mu.Lock()
	reqID := s.onDemand.nextID
	s.onDemand.nextID++
	ch := make(chan error, 1)
	s.onDemand.reqSymbol[reqID] = storeKey
	s.onDemand.done[reqID] = ch
	s.onDemand.mu.Unlock()

	secType := "STK"
	if exchange == "CBOE" {
		secType = "IND"
	}

	contract := ibapi.Contract{
		Symbol:   sym,
		SecType:  secType,
		Exchange: exchange,
		Currency: "USD",
	}

	s.logger.Printf("OnDemand hist fetch: reqID=%d symbol=%s storeKey=%s endDateTime=%q duration=%q barSize=%q",
		reqID, sym, storeKey, endDateTime, duration, barSize)
	s.client.ReqHistoricalData(reqID, &contract, endDateTime, duration, barSize, "TRADES", false, 1, false, nil)

	select {
	case err := <-ch:
		s.onDemand.mu.Lock()
		delete(s.onDemand.reqSymbol, reqID)
		delete(s.onDemand.done, reqID)
		s.onDemand.mu.Unlock()
		if err != nil {
			return fmt.Errorf("IB historical data error for %s: %w", sym, err)
		}
		s.logger.Printf("OnDemand hist fetch done: reqID=%d symbol=%s storeKey=%s bars=%d", reqID, sym, storeKey, s.Candles.Len(storeKey))
		return nil

	case <-ctx.Done():
		s.client.CancelHistoricalData(reqID)
		s.onDemand.mu.Lock()
		delete(s.onDemand.reqSymbol, reqID)
		delete(s.onDemand.done, reqID)
		s.onDemand.mu.Unlock()
		s.logger.Printf("OnDemand hist fetch cancelled: reqID=%d symbol=%s reason=%v", reqID, sym, ctx.Err())
		return ctx.Err()
	}
}

// SubscribeOptionBars opens a continuous (keepUpToDate) historical-bar
// stream for one option contract, storing bars in the candle store under
// storeKey (30-second bars, same as a live-tracked symbol). Used for ad-hoc
// requests — e.g. a chart page letting the user pick any strike/expiry — so
// it is deliberately routed through the onDemand tracker rather than
// s.reqSymbol: HistoricalDataUpdate's onDemand branch stores bars but never
// publishes KindCandle to a robot bus or writes the shared quote Book,
// keeping an arbitrary contract's bars from ever reaching indicator/bot
// logic. Call UnsubscribeOptionBars when the chart is no longer displayed —
// IB caps the number of concurrent keepUpToDate streams per session.
func (s *Session) SubscribeOptionBars(symbol, right string, strike float64, expiry, storeKey string) (int64, error) {
	if right != "call" && right != "put" {
		return 0, fmt.Errorf("invalid right %q (want \"call\" or \"put\")", right)
	}
	sym := strings.ToUpper(strings.TrimSpace(symbol))
	if sym == "" {
		return 0, fmt.Errorf("empty symbol")
	}
	if storeKey == "" {
		return 0, fmt.Errorf("empty storeKey")
	}

	ibRight := "C"
	if right == "put" {
		ibRight = "P"
	}
	contract := makeOptionContract(sym, ibRight, strike, expiry)

	s.onDemand.mu.Lock()
	reqID := s.onDemand.nextID
	s.onDemand.nextID++
	s.onDemand.reqSymbol[reqID] = storeKey
	s.onDemand.mu.Unlock()

	s.logger.Printf("OnDemand option bars: reqID=%d symbol=%s right=%s strike=%.2f expiry=%s storeKey=%s",
		reqID, sym, right, strike, expiry, storeKey)
	// "TRADES" (used everywhere else in this file/package) requires an actual
	// trade print in each bar window — most option strikes go many bars
	// without one, so a TRADES request often backfills zero bars and then
	// sits silently waiting for a trade that may never come, with no error
	// on either side. "MIDPOINT" is computed from the live bid/ask instead,
	// so it updates continuously regardless of trade activity — consistent
	// with how the rest of the app already values options (bid/ask mid, see
	// CLAUDE.md's "Booked fill prices" section).
	s.client.ReqHistoricalData(reqID, contract, "", "2 D", "30 secs", "MIDPOINT", false, 1, true, nil)
	return reqID, nil
}

// UnsubscribeOptionBars cancels a stream started by SubscribeOptionBars and
// releases its onDemand tracking entry.
func (s *Session) UnsubscribeOptionBars(reqID int64) {
	s.client.CancelHistoricalData(reqID)
	s.onDemand.mu.Lock()
	delete(s.onDemand.reqSymbol, reqID)
	delete(s.onDemand.done, reqID)
	s.onDemand.mu.Unlock()
}

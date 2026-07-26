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

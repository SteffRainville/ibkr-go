package ibkr

import (
	"time"

	"github.com/scmhub/ibapi"

	"github.com/SteffRainville/ibkr-go/eventbus"
)

// TickPrice handles price ticks from snapshot and streaming market data subscriptions.
//
//   - Snapshot ticks (snapReqSymbol): update market price on a positions display.
//   - Scanner enrichment ticks: accumulate bid/ask/last/close/open for scan rows.
//   - Streaming ticks: update the shared quote book and the internal bid/ask
//     cache used for outside-RTH order pricing.
func (s *Session) TickPrice(reqID int64, tickType int64, price float64, attrib ibapi.TickAttrib) {
	if price <= 0 {
		return
	}

	if sym, ok := s.mktData.snapReqSymbol[reqID]; ok {
		switch tickType {
		case ibapi.LAST, ibapi.DELAYED_LAST, ibapi.CLOSE, ibapi.DELAYED_CLOSE:
			s.logger.Printf("TickPrice snapshot: symbol=%s tickType=%d price=%.2f", sym, tickType, price)
			if s.opts.OnPositionPriceUpdate != nil {
				s.opts.OnPositionPriceUpdate(sym, price)
			}
		}
		return
	}

	s.scanner.mu.Lock()
	entry, isScanSnap := s.scanner.snapData[reqID]
	if isScanSnap {
		switch tickType {
		case ibapi.BID, ibapi.DELAYED_BID:
			entry.bid = price
		case ibapi.ASK, ibapi.DELAYED_ASK:
			entry.ask = price
		case ibapi.LAST, ibapi.DELAYED_LAST:
			entry.last = price
		case ibapi.CLOSE, ibapi.DELAYED_CLOSE:
			entry.prevClose = price
		case ibapi.OPEN, ibapi.DELAYED_OPEN:
			entry.open = price
		}
	}
	s.scanner.mu.Unlock()
	if isScanSnap {
		return
	}

	if s.handleOptionTick(reqID, tickType, price) {
		return
	}

	if sym, ok := s.mktData.mktDataSymbol[reqID]; ok {
		now := time.Now()
		switch tickType {
		case ibapi.BID, ibapi.DELAYED_BID:
			s.mktData.bidAskMu.Lock()
			s.mktData.bid[sym] = quote{Price: price, Time: now}
			s.mktData.bidAskMu.Unlock()
			if s.book != nil {
				s.book.SetStockBid(sym, price)
			}
		case ibapi.ASK, ibapi.DELAYED_ASK:
			s.mktData.bidAskMu.Lock()
			s.mktData.ask[sym] = quote{Price: price, Time: now}
			s.mktData.bidAskMu.Unlock()
			if s.book != nil {
				s.book.SetStockAsk(sym, price)
			}
		case ibapi.CLOSE, ibapi.DELAYED_CLOSE:
			s.mktData.bidAskMu.Lock()
			s.mktData.prevClose[sym] = price
			s.mktData.bidAskMu.Unlock()
		}
	}
}

// PrevClose returns the prior session's close for symbol, as delivered by
// IB's CLOSE tick on the streaming subscription. Returns 0 if not yet received.
func (s *Session) PrevClose(symbol string) float64 {
	s.mktData.bidAskMu.RLock()
	defer s.mktData.bidAskMu.RUnlock()
	return s.mktData.prevClose[symbol]
}

// TickSnapshotEnd is called when a market data snapshot request completes.
func (s *Session) TickSnapshotEnd(reqID int64) {
	s.mdLines.Release(reqID)

	if sym, ok := s.mktData.snapReqSymbol[reqID]; ok {
		s.logger.Printf("TickSnapshotEnd: symbol=%s reqID=%d", sym, reqID)
		delete(s.mktData.snapReqSymbol, reqID)
		return
	}

	s.scanner.mu.Lock()
	entry, ok := s.scanner.snapData[reqID]
	if !ok {
		s.scanner.mu.Unlock()
		return
	}

	parentReqID := entry.parentReqID
	idx := entry.resultIdx

	rows := s.scanner.enriched[parentReqID]
	if idx < len(rows) {
		r := &rows[idx]
		r.Bid = entry.bid
		r.Ask = entry.ask
		r.Last = entry.last
		r.PrevClose = entry.prevClose
		r.Open = entry.open
		r.Volume = entry.volume
		r.Halted = int(entry.haltStatus)
		if entry.prevClose > 0 {
			ref := entry.last
			if ref == 0 {
				ref = entry.bid
			}
			if ref > 0 {
				r.ChangePercent = (ref - entry.prevClose) / entry.prevClose * 100
			}
			if entry.open > 0 {
				r.GapPercent = (entry.open - entry.prevClose) / entry.prevClose * 100
			}
		}
	}

	delete(s.scanner.snapData, reqID)
	s.scanner.snapCount[parentReqID]--

	finalResults, ch := s.checkAndFinalize(parentReqID)
	s.scanner.mu.Unlock()

	if ch != nil {
		s.scanLog.Printf("Scanner: reqID=%d all enrichment done, signalling %d results", parentReqID, len(finalResults))
		select {
		case ch <- finalResults:
		default:
		}
	}
}

// TickString overrides the default ibapi.Wrapper log to include the symbol name.
func (s *Session) TickString(reqID int64, tickType int64, value string) {
	s.logger.Printf("TickString: symbol=%s reqID=%d tickType=%d value=%s", s.resolveReqID(reqID), reqID, tickType, value)
}

// TickSize handles size ticks. Captures daily volume for scanner enrichment snapshots.
func (s *Session) TickSize(reqID int64, tickType int64, size ibapi.Decimal) {
	s.logger.Printf("TickSize: symbol=%s reqID=%d tickType=%d size=%s", s.resolveReqID(reqID), reqID, tickType, ibapi.DecimalMaxString(size))

	switch tickType {
	case ibapi.VOLUME, ibapi.DELAYED_VOLUME:
		s.scanner.mu.Lock()
		if entry, ok := s.scanner.snapData[reqID]; ok {
			entry.volume = size.Int()
		}
		s.scanner.mu.Unlock()
	}
}

// TickOptionComputation receives Greeks from IB for option market data
// subscriptions. Captures delta and publishes it via KindOptionData (ATM)
// or KindPositionOptionData (position-pinned) so quote consumers stay current.
func (s *Session) TickOptionComputation(reqID int64, tickType int64, tickAttrib int64, impliedVol, delta, optPrice, pvDividend, gamma, vega, theta, undPrice float64) {
	if delta <= -2 || delta >= 2 {
		return
	}

	s.optChain.mu.Lock()

	if impliedVol > 0 {
		if s.optChain.lastIV == nil {
			s.optChain.lastIV = make(map[string]float64)
		}
		symbolKey := ""
		if req, ok := s.optChain.mktReqs[reqID]; ok {
			symbolKey = req.symbol
		} else if sub, ok := s.optChain.posSubs[reqID]; ok {
			symbolKey = sub.symbol
		} else if cand, ok := s.optChain.deltaCands[reqID]; ok {
			symbolKey = cand.symbol
		}
		if symbolKey != "" {
			s.optChain.lastIV[symbolKey] = impliedVol
		}
	}

	if req, ok := s.optChain.mktReqs[reqID]; ok {
		req.delta = delta
		req.deltaSource = "matched"
		od := eventbus.OptionData{
			Symbol: req.symbol, Right: req.right, Strike: req.strike, Expiry: req.expiry,
			Price: req.price, Bid: req.bid, Ask: req.ask, Delta: req.delta, IV: impliedVol, DeltaSource: req.deltaSource,
		}
		busIdxs := req.busIdxs
		s.optChain.mu.Unlock()
		s.bookOption(od)
		s.publishTo(busIdxs, eventbus.Event{Kind: eventbus.KindOptionData, Payload: od})
		return
	}

	if sub, ok := s.optChain.posSubs[reqID]; ok {
		sub.delta = delta
		sub.deltaSource = "matched"
		od := eventbus.OptionData{
			Symbol: sub.symbol, Right: sub.right, Strike: sub.strike, Expiry: sub.expiry,
			Price: sub.price, Bid: sub.bid, Ask: sub.ask, Delta: sub.delta, IV: impliedVol, DeltaSource: sub.deltaSource,
		}
		s.optChain.mu.Unlock()
		s.bookOption(od)
		s.publish(eventbus.Event{Kind: eventbus.KindPositionOptionData, Payload: od})
		return
	}

	if cand, ok := s.optChain.deltaCands[reqID]; ok {
		cand.delta = delta
		cand.ready = true
		s.optChain.mu.Unlock()
		return
	}

	s.optChain.mu.Unlock()
}

// TickGeneric overrides the default ibapi.Wrapper log to include the symbol
// name. Tick type 49 is the halt status: 0=not halted, 1=general halt,
// 2=volatility halt.
func (s *Session) TickGeneric(reqID int64, tickType int64, value float64) {
	s.logger.Printf("TickGeneric: symbol=%s reqID=%d tickType=%d value=%g", s.resolveReqID(reqID), reqID, tickType, value)
	if tickType == 49 {
		s.scanner.mu.Lock()
		if entry, ok := s.scanner.snapData[reqID]; ok {
			entry.haltStatus = value
		}
		s.scanner.mu.Unlock()
	}
}

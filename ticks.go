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
		// CLOSE/DELAYED_CLOSE are deliberately absent: they carry the PRIOR
		// session's close, and OnPositionPriceUpdate marks a position with
		// whatever it is handed. The streaming branch below has always routed
		// them to mktData.prevClose instead; this branch used to mark a
		// position at yesterday's price whenever a snapshot answered before any
		// live trade did.
		case ibapi.LAST, ibapi.DELAYED_LAST:
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

	if sym, ok := s.streamSymbol(reqID); ok {
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

// TickString overrides the default ibapi.Wrapper handler to SUPPRESS it.
//
// Nothing in the library consumes a string tick — the override existed only
// to log one line per tick (exchange-code strings for tick types 32/33/45/84),
// which was 30% of every byte written to error.log. Liveness is already
// stamped by the callbacks that carry an actual value, so dropping these
// costs no signal. Kept as an explicit no-op rather than deleted, because
// removing the override would restore the embedded Wrapper's own logging.
func (s *Session) TickString(reqID int64, tickType int64, value string) {}

// TickSize handles size ticks: stamps option-leg liveness (both the deadleg
// tracker and the shared Book, for options AND stocks) and captures daily
// volume for scanner enrichment snapshots.
func (s *Session) TickSize(reqID int64, tickType int64, size ibapi.Decimal) {
	// Size ticks are the most valuable liveness signal available — they keep
	// arriving on a liquid line whose price is flat, which is precisely the
	// case the Book's advance-on-change timestamps (BidTime/AskTime) cannot
	// distinguish from a dead line. Stamp the Book's LastTickTime here too —
	// not just deadleg.go's option-only tracker — so a size-only tick keeps a
	// stock's dashboard/position liveness coloring fresh as well.
	s.optChain.mu.Lock()
	s.touchOptionLegLocked(reqID, time.Now())
	optKey, isOption := s.optionKeyForReqIDLocked(reqID)
	s.optChain.mu.Unlock()

	if s.book != nil {
		if isOption {
			s.book.TouchOptionTick(optKey)
		} else if sym, ok := s.streamSymbol(reqID); ok {
			s.book.TouchStockTick(sym)
		}
	}

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

	// A leg can receive greeks without a price tick; without this stamp such
	// a leg would look dead to the reaper while IB is actively serving it.
	s.touchOptionLegLocked(reqID, time.Now())

	if impliedVol > 0 {
		if s.optChain.lastIV == nil {
			s.optChain.lastIV = make(map[string]float64)
		}
		symbolKey := ""
		if leg, ok := s.legByReqIDLocked(reqID); ok {
			symbolKey = leg.symbol
		} else if cand, ok := s.optChain.deltaCands[reqID]; ok {
			symbolKey = cand.symbol
		}
		if symbolKey != "" {
			s.optChain.lastIV[symbolKey] = impliedVol
		}
	}

	if leg, ok := s.legByReqIDLocked(reqID); ok {
		leg.delta = delta
		leg.deltaSource = "matched"
		od := eventbus.OptionData{
			Symbol: leg.symbol, Right: leg.right, Strike: leg.strike, Expiry: leg.expiry,
			Price: leg.price, Bid: leg.bid, Ask: leg.ask, Delta: leg.delta, IV: impliedVol, DeltaSource: leg.deltaSource,
			// The greeks changed; the PRICE did not. Carrying the leg's
			// unchanged quoteSeq is what stops this re-emission being counted
			// as a second price observation by whoever is watching for one.
			QuoteSeq: leg.quoteSeq,
		}
		pinned := leg.pins > 0
		s.optChain.mu.Unlock()
		s.bookOption(od)
		if pinned {
			s.publish(eventbus.Event{Kind: eventbus.KindPositionOptionData, Payload: od})
		}
		return
	}

	if cand, ok := s.optChain.deltaCands[reqID]; ok {
		cand.delta = delta
		cand.iv = impliedVol
		cand.ready = true
		s.optChain.mu.Unlock()
		return
	}

	s.optChain.mu.Unlock()
}

// TickGeneric stamps option-leg liveness and captures the halt status for
// scanner snapshots. Tick type 49 is the halt status: 0=not halted,
// 1=general halt, 2=volatility halt.
func (s *Session) TickGeneric(reqID int64, tickType int64, value float64) {
	s.optChain.mu.Lock()
	s.touchOptionLegLocked(reqID, time.Now())
	s.optChain.mu.Unlock()

	if tickType == 49 {
		s.scanner.mu.Lock()
		if entry, ok := s.scanner.snapData[reqID]; ok {
			entry.haltStatus = value
		}
		s.scanner.mu.Unlock()
	}
}

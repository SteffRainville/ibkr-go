package ibkr

import (
	"context"
	"strings"

	"github.com/scmhub/ibapi"
)

// RunScanner fires an IB market scanner subscription and collects up to 30
// rows, then enriches each with a market data snapshot AND a
// ReqContractDetails call in parallel (bid/ask/price/change%/gap% and
// L/NT flags). Blocks until all enrichment completes or ctx is cancelled.
func (s *Session) RunScanner(ctx context.Context, params ScanParams) ([]ScanResult, error) {
	done := make(chan []ScanResult, 1)

	s.scanner.mu.Lock()
	if len(s.scanner.pending) == 0 && len(s.scanner.snapData) == 0 && len(s.scanner.cdData) == 0 {
		s.scanner.nextID = reqIDScannerBase
	}
	reqID := s.scanner.nextID
	s.scanner.nextID++
	s.scanner.results[reqID] = nil
	s.scanner.pending[reqID] = done
	s.scanner.mu.Unlock()

	sub := ibapi.NewScannerSubscription()
	sub.Instrument = params.Instrument
	sub.LocationCode = params.LocationCode
	sub.ScanCode = params.ScanCode
	sub.NumberOfRows = 30
	if params.AbovePrice > 0 {
		sub.AbovePrice = params.AbovePrice
	}
	if params.BelowPrice > 0 {
		sub.BelowPrice = params.BelowPrice
	}
	if params.AboveVolume > 0 {
		sub.AboveVolume = params.AboveVolume
	}

	s.scanLog.Printf("--- new scan reqID=%d instrument=%s location=%s code=%s abovePrice=%.2f belowPrice=%.2f aboveVolume=%d",
		reqID, params.Instrument, params.LocationCode, params.ScanCode, params.AbovePrice, params.BelowPrice, params.AboveVolume)
	s.client.ReqScannerSubscription(reqID, sub, nil, nil)

	select {
	case results := <-done:
		s.scanLog.Printf("scan done reqID=%d results=%d", reqID, len(results))
		return results, nil
	case <-ctx.Done():
		s.scanLog.Printf("scan cancelled reqID=%d: %v", reqID, ctx.Err())
		s.client.CancelScannerSubscription(reqID)

		s.scanner.mu.Lock()
		snapIDs := s.scanner.snapReqIDs[reqID]
		cdIDs := s.scanner.cdReqIDs[reqID]
		for _, snapID := range snapIDs {
			delete(s.scanner.snapData, snapID)
		}
		for _, cdID := range cdIDs {
			delete(s.scanner.cdData, cdID)
		}
		delete(s.scanner.results, reqID)
		delete(s.scanner.enriched, reqID)
		delete(s.scanner.pending, reqID)
		delete(s.scanner.snapCount, reqID)
		delete(s.scanner.snapReqIDs, reqID)
		delete(s.scanner.cdCount, reqID)
		delete(s.scanner.cdReqIDs, reqID)
		s.scanner.mu.Unlock()

		for _, snapID := range snapIDs {
			s.mdLines.Release(snapID)
			s.client.CancelMktData(snapID)
		}
		return nil, ctx.Err()
	}
}

// checkAndFinalize checks whether both the snapshot and contract-details
// enrichment phases are complete for parentReqID. Must be called with
// s.scanner.mu held.
func (s *Session) checkAndFinalize(parentReqID int64) ([]ScanResult, chan []ScanResult) {
	if s.scanner.snapCount[parentReqID] != 0 || s.scanner.cdCount[parentReqID] != 0 {
		return nil, nil
	}
	results := s.scanner.enriched[parentReqID]
	ch := s.scanner.pending[parentReqID]
	delete(s.scanner.enriched, parentReqID)
	delete(s.scanner.snapCount, parentReqID)
	delete(s.scanner.snapReqIDs, parentReqID)
	delete(s.scanner.cdCount, parentReqID)
	delete(s.scanner.cdReqIDs, parentReqID)
	delete(s.scanner.pending, parentReqID)
	return results, ch
}

// ScannerData is called by the IB library for each result row.
func (s *Session) ScannerData(reqID int64, rank int64, contractDetails *ibapi.ContractDetails, distance, benchmark, projection, legsStr string) {
	row := ScanResult{
		Rank:         int(rank),
		Symbol:       contractDetails.Contract.Symbol,
		SecType:      string(contractDetails.Contract.SecType),
		Exchange:     contractDetails.Contract.Exchange,
		Currency:     contractDetails.Contract.Currency,
		LocalSymbol:  contractDetails.Contract.LocalSymbol,
		TradingClass: contractDetails.Contract.TradingClass,
		MarketName:   contractDetails.MarketName,
		ConID:        contractDetails.Contract.ConID,
		Strike:       contractDetails.Contract.Strike,
		Right:        contractDetails.Contract.Right,
		Expiry:       contractDetails.Contract.LastTradeDateOrContractMonth,
		Distance:     distance,
		Benchmark:    benchmark,
		Projection:   projection,
		LegsStr:      legsStr,
		LongName:     contractDetails.LongName,
		StockType:    contractDetails.StockType,
		LowLiquidity: isLowLiquidity(contractDetails.StockType),
	}
	s.scanLog.Printf("row rank=%d symbol=%s secType=%s exchange=%s currency=%s "+
		"localSymbol=%s tradingClass=%s marketName=%s conID=%d "+
		"longName=%q stockType=%q distance=%q benchmark=%q projection=%q legsStr=%q "+
		"ineligibleCount=%d",
		row.Rank, row.Symbol, row.SecType, row.Exchange, row.Currency,
		row.LocalSymbol, row.TradingClass, row.MarketName, row.ConID,
		row.LongName, row.StockType, distance, benchmark, projection, legsStr,
		len(contractDetails.IneligibilityReasonList))
	for i, r := range contractDetails.IneligibilityReasonList {
		s.scanLog.Printf("  ineligible[%d] id=%q description=%q", i, r.ID, r.Description)
	}

	if len(contractDetails.IneligibilityReasonList) > 0 {
		ids := make([]string, 0, len(contractDetails.IneligibilityReasonList))
		for _, r := range contractDetails.IneligibilityReasonList {
			ids = append(ids, r.ID)
		}
		row.IneligibleReasons = strings.Join(ids, ",")
		for _, r := range contractDetails.IneligibilityReasonList {
			switch r.ID {
			case "CLOSE_ONLY", "CFD_NOT_AVAILABLE":
				row.CloseOnly = true
			case "LOW_LIQUIDITY", "INACCESSIBLE", "NOT_LIQUID":
				row.LowLiquidity = true
			case "SHORT_SELL_NOT_AVAILABLE", "NOT_SHORTABLE", "NO_SHORT_SALE", "SHORT_EXEMPT":
				row.NoShorts = true
			}
		}
	}
	s.scanner.mu.Lock()
	s.scanner.results[reqID] = append(s.scanner.results[reqID], row)
	s.scanner.mu.Unlock()
}

// ScannerDataEnd is called when all rows for a scan request have been
// delivered. Cancels the scanner subscription, then fires one market data
// snapshot AND one ReqContractDetails per row in parallel.
func (s *Session) ScannerDataEnd(reqID int64) {
	s.scanner.mu.Lock()
	results := s.scanner.results[reqID]
	delete(s.scanner.results, reqID)
	ch := s.scanner.pending[reqID]
	s.scanner.mu.Unlock()

	s.client.CancelScannerSubscription(reqID)

	if ch == nil || len(results) == 0 {
		if ch != nil {
			select {
			case ch <- results:
			default:
			}
			s.scanner.mu.Lock()
			delete(s.scanner.pending, reqID)
			s.scanner.mu.Unlock()
		}
		return
	}

	s.scanLog.Printf("ScannerDataEnd reqID=%d rows=%d — firing snapshots + contract details", reqID, len(results))

	s.client.ReqMarketDataType(int64(ibapi.DELAYED_FROZEN))

	snapIDs := make([]int64, len(results))
	cdIDs := make([]int64, len(results))
	var sentSnapIDs []int64

	s.scanner.mu.Lock()
	s.scanner.enriched[reqID] = append([]ScanResult(nil), results...)

	capped := 0
	for i := range results {
		snapID := s.scanner.nextID
		s.scanner.nextID++
		if s.mdLines.GrantSnapshot(snapID) {
			s.scanner.snapData[snapID] = &scanSnapEntry{parentReqID: reqID, resultIdx: i}
			snapIDs[i] = snapID
			sentSnapIDs = append(sentSnapIDs, snapID)
		} else {
			capped++
		}

		cdID := s.scanner.nextID
		s.scanner.nextID++
		s.scanner.cdData[cdID] = &scanCDEntry{parentReqID: reqID, resultIdx: i}
		cdIDs[i] = cdID
	}

	s.scanner.snapCount[reqID] = len(sentSnapIDs)
	s.scanner.snapReqIDs[reqID] = sentSnapIDs
	s.scanner.cdCount[reqID] = len(results)
	s.scanner.cdReqIDs[reqID] = cdIDs
	s.scanner.mu.Unlock()

	if capped > 0 {
		s.scanLog.Printf("Scanner: capped %d/%d snapshot quotes (market-data line pool full) — those rows get contract-details only", capped, len(results))
	}

	for i, r := range results {
		contract := ibapi.NewContract()
		if r.ConID > 0 {
			contract.ConID = r.ConID
			contract.Exchange = "SMART"
		} else {
			contract.Symbol = r.Symbol
			contract.SecType = r.SecType
			contract.Exchange = r.Exchange
			contract.Currency = r.Currency
		}
		if snapIDs[i] != 0 {
			s.client.ReqMktData(snapIDs[i], contract, "", true, false, nil)
		}
		s.client.ReqContractDetails(cdIDs[i], contract)
	}
	s.client.ReqMarketDataType(int64(ibapi.REALTIME))
}

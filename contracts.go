package ibkr

import (
	"math"
	"strings"

	"github.com/scmhub/ibapi"
)

// isLowLiquidity reports whether a StockType string indicates low-liquidity
// securities (PINK sheets, OTC Bulletin Board, grey market, etc.).
func isLowLiquidity(stockType string) bool {
	switch strings.ToUpper(stockType) {
	case "PINK", "OTC", "OTCBB", "GREY", "GRAY":
		return true
	}
	return false
}

// ContractDetails is called by the IB library for each contract that
// matches a ReqContractDetails request. Routes to option conId lookups or
// scanner enrichment.
func (s *Session) ContractDetails(reqID int64, contractDetails *ibapi.ContractDetails) {
	if s.handleConIDContractDetails(reqID, contractDetails) {
		return
	}

	s.scanner.mu.Lock()
	entry, ok := s.scanner.cdData[reqID]
	if !ok {
		s.scanner.mu.Unlock()
		return
	}
	if contractDetails == nil {
		s.scanner.mu.Unlock()
		return
	}

	parentReqID := entry.parentReqID
	idx := entry.resultIdx

	rows := s.scanner.enriched[parentReqID]
	if idx < len(rows) {
		r := &rows[idx]
		s.scanLog.Printf("CD reqID=%d symbol=%s longName=%q stockType=%q ineligibleCount=%d "+
			"validExchanges=%q liquidHours=%q notes=%q",
			reqID, r.Symbol, contractDetails.LongName, contractDetails.StockType,
			len(contractDetails.IneligibilityReasonList),
			contractDetails.ValidExchanges, contractDetails.LiquidHours, contractDetails.Notes)
		for i, reason := range contractDetails.IneligibilityReasonList {
			s.scanLog.Printf("  CD ineligible[%d] id=%q description=%q", i, reason.ID, reason.Description)
		}
		r.LongName = contractDetails.LongName
		r.StockType = contractDetails.StockType
		r.LowLiquidity = isLowLiquidity(contractDetails.StockType)

		if len(contractDetails.IneligibilityReasonList) > 0 {
			ids := make([]string, 0, len(contractDetails.IneligibilityReasonList))
			for _, reason := range contractDetails.IneligibilityReasonList {
				ids = append(ids, reason.ID)
			}
			r.IneligibleReasons = strings.Join(ids, ",")
			for _, reason := range contractDetails.IneligibilityReasonList {
				switch reason.ID {
				case "CLOSE_ONLY", "CFD_NOT_AVAILABLE":
					r.CloseOnly = true
				case "LOW_LIQUIDITY", "INACCESSIBLE", "NOT_LIQUID":
					r.LowLiquidity = true
				case "SHORT_SELL_NOT_AVAILABLE", "NOT_SHORTABLE", "NO_SHORT_SALE", "SHORT_EXEMPT":
					r.NoShorts = true
				}
			}
		}

		if contractDetails.MinTick == math.MaxFloat64 {
			contractDetails.MinTick = 0
		}
	}
	s.scanner.mu.Unlock()
}

// ContractDetailsEnd is called when all ContractDetails responses for a
// ReqContractDetails request have been delivered.
func (s *Session) ContractDetailsEnd(reqID int64) {
	if s.handleConIDContractDetailsEnd(reqID) {
		return
	}

	s.scanner.mu.Lock()
	entry, ok := s.scanner.cdData[reqID]
	if !ok {
		s.scanner.mu.Unlock()
		return
	}

	parentReqID := entry.parentReqID
	delete(s.scanner.cdData, reqID)
	s.scanner.cdCount[parentReqID]--

	finalResults, ch := s.checkAndFinalize(parentReqID)
	s.scanner.mu.Unlock()

	if ch != nil {
		s.scanLog.Printf("ContractDetailsEnd reqID=%d parentReqID=%d — CD enrichment done, %d results", reqID, parentReqID, len(finalResults))
		select {
		case ch <- finalResults:
		default:
		}
	}
}

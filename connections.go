package ibkr

import "github.com/SteffRainville/ibkr-go/mdlines"

// MarketDataLineStatus returns (used, max, historical-stream used,
// historical-stream max, stock lines used, option lines used) — the
// collapsed pair behind ConnectionsSnapshot's full per-category breakdown,
// for a lightweight status indicator.
func (s *Session) MarketDataLineStatus() (used, max, histUsed, histMax, stockUsed, optionUsed int) {
	return s.mdLines.StatusAll()
}

// ConnectionsSnapshot reports current IB market-data subscription usage by
// category, plus enough configuration context (rows configured vs.
// resolution groups actually resolved) to explain the gap between
// watchlist size and live line count.
func (s *Session) ConnectionsSnapshot() ConnectionsStatus {
	used, max, histUsed, histMax, _, _ := s.mdLines.StatusAll()
	stock, position, discNew, discChurn, snapshot, probe := s.mdLines.CategoryCounts()

	stockRows, optionRows, uniqueUnderlyings := s.configuredRowCounts()

	s.optChain.mu.Lock()
	groups := len(s.optChain.rotation)
	s.optChain.mu.Unlock()

	return ConnectionsStatus{
		Used:     used,
		Max:      max,
		HistUsed: histUsed,
		HistMax:  histMax,

		StockLines:              stock,
		PositionLines:           position,
		DiscretionaryNewLines:   discNew,
		DiscretionaryChurnLines: discChurn,
		SnapshotLines:           snapshot,
		ProbeLines:              probe,
		BufferLines:             mdlines.Buffer,

		ConfiguredStockRows:  stockRows,
		ConfiguredOptionRows: optionRows,
		UniqueUnderlyings:    uniqueUnderlyings,
		OptionGroups:         groups,

		RotationIntervalSeconds: int(s.opts.OptionRotationInterval.Seconds()),
	}
}

// configuredRowCounts totals stock- and option-tagged rows across every
// subscriber's symbol list (s.subSymbols), plus the count of distinct base
// symbols among them — the dedup unit for underlying market-data lines.
func (s *Session) configuredRowCounts() (stockRows, optionRows, uniqueUnderlyings int) {
	seen := make(map[string]bool)
	for _, syms := range s.subSymbolLists() {
		for _, sy := range syms {
			if isOptionTag(sy.Tag) {
				optionRows++
			} else {
				stockRows++
			}
			if !seen[sy.Symbol] {
				seen[sy.Symbol] = true
				uniqueUnderlyings++
			}
		}
	}
	return stockRows, optionRows, uniqueUnderlyings
}

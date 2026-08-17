package ibkr

import (
	"sort"

	"github.com/SteffRainville/ibkr-go/mdlines"
)

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

// ConnectionLine describes one currently-active market-data line for the
// connections diagnostics page — a subscribed underlying (Right/Strike/Expiry
// zero) or a held option contract. "Held" means a selector is displaying it
// or a position is pinned to it; a leg that is only warming into place
// (pendingSwap) is not yet a connection a user would recognize.
//
// The quote and delta fields exist because "this line is active" and "this line
// is usable" are different claims, and the page previously made only the first.
// On 2026-08-17 MSFT's call and put lines were listed here as tracked while both
// sat on a strike that carried no bid and no ask and a delta of 1.00 against a
// configured target of 0.55 — accurate, and exactly the wrong impression.
type ConnectionLine struct {
	Symbol string
	Right  string  // "" for stock, "call" | "put" for option
	Strike float64 // 0 for stock
	Expiry string  // "" for stock, "YYYYMMDD" for option

	// Live quote on the contract. Both zero on an option line means IB has
	// never delivered a two-sided quote for it: the row can be displayed but
	// not traded.
	Bid, Ask float64

	// Delta as IB reported it, DeltaSource "matched" (a genuine live match or a
	// deliberate ATM pick) or "atm_fallback" (chosen without one), and
	// TargetDelta the largest target among the selectors holding this contract
	// — 0 when only an open position pins it, which has no target.
	Delta       float64
	DeltaSource string
	TargetDelta float64

	// Pinned reports that an open position holds this contract, so it stays
	// subscribed regardless of what the watchlist rotation wants.
	Pinned bool
}

// ConnectionsLines reports every currently-active market-data line: every
// subscribed underlying first, then every held option contract (symbol, then
// right, then strike). Options carry strike/expiry directly from legKey
// rather than re-deriving them, since legKey is already the canonical
// contract identity behind the line.
func (s *Session) ConnectionsLines() []ConnectionLine {
	s.symMu.RLock()
	stockSymbols := make([]string, 0, len(s.streamReqID))
	for sym := range s.streamReqID {
		stockSymbols = append(stockSymbols, sym)
	}
	s.symMu.RUnlock()
	sort.Strings(stockSymbols)

	s.optChain.mu.Lock()
	optLines := make([]ConnectionLine, 0, len(s.optChain.legs))
	for key, leg := range s.optChain.legs {
		if !leg.held() {
			continue
		}
		line := ConnectionLine{
			Symbol: key.symbol, Right: key.right, Strike: key.strike, Expiry: key.expiry,
			Bid: leg.bid, Ask: leg.ask, Delta: leg.delta, DeltaSource: leg.deltaSource,
			Pinned: leg.pins > 0,
		}
		for selID := range leg.selectors {
			if sel, ok := s.selectorByIDLocked(selID); ok && sel.targetDelta > line.TargetDelta {
				line.TargetDelta = sel.targetDelta
			}
		}
		optLines = append(optLines, line)
	}
	s.optChain.mu.Unlock()
	sort.Slice(optLines, func(i, j int) bool {
		if optLines[i].Symbol != optLines[j].Symbol {
			return optLines[i].Symbol < optLines[j].Symbol
		}
		if optLines[i].Right != optLines[j].Right {
			return optLines[i].Right < optLines[j].Right
		}
		return optLines[i].Strike < optLines[j].Strike
	})

	lines := make([]ConnectionLine, 0, len(stockSymbols)+len(optLines))
	for _, sym := range stockSymbols {
		lines = append(lines, ConnectionLine{Symbol: sym})
	}
	return append(lines, optLines...)
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

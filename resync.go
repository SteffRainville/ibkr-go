// resync.go — mid-session symbol subscription deltas.
//
// Session.Run subscribes the whole watchlist once and then never revisits it:
// a symbol added to the caller's config only reaches IB on the next connect.
// Reconnecting to pick up one new ticker is a blunt instrument — it drops and
// re-establishes every bar stream and market-data line the session holds,
// re-runs option-chain resolution for every group at once, and briefly doubles
// the account's market-data line usage while TWS retires the old ones.
//
// ResyncSymbols exists so the caller can apply just the difference. It diffs
// the subscribers' current symbol lists against what this session actually has
// subscribed and touches only the symbols that moved: new ones get a bar
// stream and a market-data line, departed ones give theirs back, and everything
// already subscribed is left strictly alone — same reqIDs, same feeds, same
// option groups, no interruption.
package ibkr

import (
	"github.com/SteffRainville/ibkr-go/mdlines"
)

// SymbolDelta reports what one ResyncSymbols call changed. Both lists hold
// BASE symbols (the market-data dedup unit), so a config change that only
// adds a second direction — a "call" leg alongside an existing "put" on the
// same underlying — reports nothing added: the underlying feed it needs is
// already running. Skipped names hit the concurrent bar-stream cap and have
// no live bars; they are the one outcome a caller should surface.
type SymbolDelta struct {
	Added   []string
	Removed []string
	Skipped []string
}

// Changed reports whether the resync moved anything at all.
func (d SymbolDelta) Changed() bool { return len(d.Added) > 0 || len(d.Removed) > 0 }

// reserveSymbolIDsLocked allocates this symbol's pair of reqIDs — one for its
// keepUpToDate bar stream, one for its streaming market-data line — and
// registers it in every symbol⇄reqID map. It does no I/O; the caller issues
// the actual IB requests after releasing symMu.
//
// IDs come from the session's monotonic allocator rather than the symbol's
// index in the watchlist. Index-derived IDs are only unique while the
// watchlist is immutable: remove the 3rd of 5 symbols and add another, and the
// newcomer would reuse the departed symbol's ID while TWS may still have
// callbacks in flight against it. The allocator never reissues an ID for the
// life of a session.
//
// Must be called with s.symMu held for writing.
func (s *Session) reserveSymbolIDsLocked(symbol string) (histID, mktID int64) {
	histID = s.nextReqID()
	mktID = s.nextReqID()

	s.reqSymbol[histID] = symbol
	s.histReqID[symbol] = histID
	s.mktData.mktDataSymbol[mktID] = symbol
	s.streamReqID[symbol] = mktID
	return histID, mktID
}

// releaseSymbolIDsLocked removes symbol from the registry and returns the
// reqIDs the caller must cancel at IB. ok is false when the symbol was not
// subscribed.
//
// Must be called with s.symMu held for writing.
func (s *Session) releaseSymbolIDsLocked(symbol string) (histID, mktID int64, ok bool) {
	histID, hasHist := s.histReqID[symbol]
	mktID, hasMkt := s.streamReqID[symbol]
	if !hasHist && !hasMkt {
		return 0, 0, false
	}
	delete(s.histReqID, symbol)
	delete(s.streamReqID, symbol)
	delete(s.reqSymbol, histID)
	delete(s.mktData.mktDataSymbol, mktID)
	return histID, mktID, true
}

// histSymbol resolves a keepUpToDate bar-stream reqID to its base symbol.
func (s *Session) histSymbol(reqID int64) string {
	s.symMu.RLock()
	defer s.symMu.RUnlock()
	return s.reqSymbol[reqID]
}

// streamSymbol resolves a streaming market-data reqID to its base symbol.
func (s *Session) streamSymbol(reqID int64) (string, bool) {
	s.symMu.RLock()
	defer s.symMu.RUnlock()
	sym, ok := s.mktData.mktDataSymbol[reqID]
	return sym, ok
}

// symbolSpecs returns a snapshot of every subscriber's symbol specs, flattened.
func (s *Session) symbolSpecs() []SymbolSpec {
	s.symMu.RLock()
	defer s.symMu.RUnlock()
	return append([]SymbolSpec(nil), s.symbols...)
}

// subSymbolLists returns a snapshot of the per-subscriber symbol lists,
// indexed like s.subs / s.buses.
func (s *Session) subSymbolLists() [][]SymbolSpec {
	s.symMu.RLock()
	defer s.symMu.RUnlock()
	out := make([][]SymbolSpec, len(s.subSymbols))
	for i, syms := range s.subSymbols {
		out[i] = append([]SymbolSpec(nil), syms...)
	}
	return out
}

// ResyncSymbols re-reads every subscriber's Symbols() and brings this
// session's IB subscriptions in line with them, touching only what changed.
//
// Safe to call at any time on a connected session, including while bars and
// ticks are arriving. It is a no-op before Connect, and returns an empty
// delta when nothing moved.
//
// What it does NOT do, deliberately:
//   - It never re-subscribes an unchanged symbol. That is the entire point:
//     the alternative (a reconnect) churns every line the account holds.
//   - It never touches position-pinned option subscriptions. Those belong to
//     open positions, not to watchlist rows, and the caller is expected to
//     refuse to drop a row it still holds a position in. A pinned leg whose
//     row genuinely departed is released by the normal exit path.
//   - It does not re-resolve existing option groups. Group IDs survive a
//     rebuild (see buildOptionGroups), so only genuinely new groups pay for a
//     conId + chain round trip; the rest stay in the rotation untouched.
func (s *Session) ResyncSymbols() SymbolDelta {
	if s.client == nil {
		return SymbolDelta{}
	}

	// Snapshot the subscribers under the lock, then call Symbols() outside it:
	// Symbols() runs caller code, which must never be invoked while holding a
	// library mutex.
	s.symMu.RLock()
	subs := append([]Subscriber(nil), s.subs...)
	s.symMu.RUnlock()
	if len(subs) == 0 {
		return SymbolDelta{}
	}

	var allSymbols []SymbolSpec
	subSymbols := make([][]SymbolSpec, len(subs))
	wanted := make(map[string]SymbolSpec)
	var wantOrder []string
	for i, sub := range subs {
		syms := sub.Symbols()
		subSymbols[i] = syms
		allSymbols = append(allSymbols, syms...)
		for _, sy := range syms {
			if _, seen := wanted[sy.Symbol]; !seen {
				wanted[sy.Symbol] = sy
				wantOrder = append(wantOrder, sy.Symbol)
			}
		}
	}

	type addition struct {
		spec   SymbolSpec
		histID int64
		mktID  int64
	}
	type removal struct {
		symbol string
		histID int64
		mktID  int64
	}
	var additions []addition
	var removals []removal

	s.symMu.Lock()
	s.symbols = allSymbols
	s.subSymbols = subSymbols
	for _, symbol := range wantOrder {
		if _, already := s.histReqID[symbol]; already {
			continue
		}
		histID, mktID := s.reserveSymbolIDsLocked(symbol)
		additions = append(additions, addition{spec: wanted[symbol], histID: histID, mktID: mktID})
	}
	for symbol := range s.histReqID {
		if _, keep := wanted[symbol]; keep {
			continue
		}
		removals = append(removals, removal{symbol: symbol})
	}
	for i := range removals {
		histID, mktID, _ := s.releaseSymbolIDsLocked(removals[i].symbol)
		removals[i].histID, removals[i].mktID = histID, mktID
	}
	s.symMu.Unlock()

	delta := SymbolDelta{}

	// Removals first: they hand market-data lines and bar streams back to the
	// ledger, so an add/remove pair in the same resync stays flat against the
	// account's caps instead of transiently exceeding them.
	for _, r := range removals {
		s.client.CancelHistoricalData(r.histID)
		s.mdLines.ReleaseHist(r.histID)
		s.client.CancelMktData(r.mktID)
		s.mdLines.Release(r.mktID)
		s.dropSymbolOptionState(r.symbol)
		delta.Removed = append(delta.Removed, r.symbol)
		s.logger.Printf("Resync: unsubscribed %s (bars reqID=%d, quotes reqID=%d)", r.symbol, r.histID, r.mktID)
	}

	for _, a := range additions {
		if s.mdLines.GrantHist(a.histID) {
			s.client.ReqHistoricalData(a.histID, a.spec.Contract, "", "1 D", "30 secs", "TRADES", false, 1, true, nil)
		} else {
			_, _, _, histMax, _, _ := s.mdLines.StatusAll()
			s.logger.Printf("Resync: live bars for %s SKIPPED — over the %d concurrent keepUpToDate stream limit; this symbol will get NO live bars. Trim the watchlist or raise MaxHistoricalStreams.", a.spec.Symbol, histMax)
			delta.Skipped = append(delta.Skipped, a.spec.Symbol)
		}
		s.mdLines.GrantGuaranteed(a.mktID, mdlines.CategoryStock)
		s.client.ReqMktData(a.mktID, a.spec.Contract, "", false, false, nil)
		delta.Added = append(delta.Added, a.spec.Symbol)
		s.logger.Printf("Resync: subscribed %s (bars reqID=%d, quotes reqID=%d)", a.spec.Symbol, a.histID, a.mktID)
	}

	// Rebuild the option selectors from the new symbol lists and resolve only
	// the ones that did not exist before. Existing selectors keep their stable
	// id and their place in the rotation, so an unrelated edit cannot trigger a
	// chain-resolution burst across the whole watchlist.
	//
	// A departed selector (its row removed, or its target_delta edited) needs
	// no teardown any more: a selector holds no market-data line, only a
	// strike-selection recipe, so dropping it from the rotation IS releasing
	// it. What used to live here — releaseDepartedSelectors, walking selCurrent
	// and selPending to cancel orphaned background lines — has nothing left to
	// walk.
	fresh := s.buildSelectors()
	for _, sel := range fresh {
		s.refreshChainFor(sel)
	}

	return delta
}

// dropSymbolOptionState tears down the cached chain data and in-flight delta
// probes belonging to a symbol that has left the watchlist.
//
// Legs a position still pins are deliberately kept — they are owned by open
// positions and released by the exit path, not by a watchlist edit. Since a
// pin is now the ONLY thing that holds a leg, this function no longer touches
// s.optChain.legs at all.
func (s *Session) dropSymbolOptionState(symbol string) {
	s.optChain.mu.Lock()
	var cands []int64
	for reqID, c := range s.optChain.deltaCands {
		if c.symbol == symbol {
			cands = append(cands, reqID)
		}
	}
	for _, reqID := range cands {
		delete(s.optChain.deltaCands, reqID)
		s.mdLines.Release(reqID)
	}
	for key := range s.optChain.lastChainInfo {
		if key.symbol == symbol {
			delete(s.optChain.lastChainInfo, key)
		}
	}
	delete(s.optChain.lastIV, symbol)
	s.optChain.mu.Unlock()

	s.cancelLines(cands)
	if n := len(cands); n > 0 {
		s.optionLog.Printf("Resync: cancelled %d in-flight delta probe(s) for departed symbol %s", n, symbol)
	}
}

// Ad-hoc option chain queries: expirations + SMART strikes for an
// underlying, for callers that just want to show the user a picker (e.g. an
// option chart page) rather than resolve a strike to trade. Deliberately
// isolated from the trading-group resolution machinery in options.go — a
// query here never subscribes market data, never affects ATM strike
// selection, and shares no state with optionChainTracker.
package ibkr

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/scmhub/ibapi"
)

// ErrNoOptionChain reports that the underlying resolved fine but IB lists no
// SMART option chain for it — a definitive "this instrument has no options",
// as opposed to a lookup that simply failed (unresolvable conId, timeout,
// disconnect). Callers that need to distinguish "no" from "don't know" must
// test with errors.Is; the message alone is not a contract.
var ErrNoOptionChain = errors.New("no SMART option chain")

// OptionChainInfo is the expirations (YYYYMMDD, ascending, today or later)
// and SMART strikes (ascending) available for one underlying's option chain.
type OptionChainInfo struct {
	Expirations []string
	Strikes     []float64
}

// optQueryReq tracks one in-flight RequestOptionChain call across its two
// phases (conId lookup, then reqSecDefOptParams). Stored under a different
// reqID in each phase's map (see handleOptionQueryContractDetailsEnd), so
// the same *optQueryReq is reachable from either map while it is pending.
type optQueryReq struct {
	symbol      string
	conID       int64
	expirations []string
	strikes     []float64
	done        chan optQueryResult
}

type optQueryResult struct {
	info OptionChainInfo
	err  error
}

// optionQueryTracker holds state for RequestOptionChain calls, keyed by
// reqID like every other IB request in this package.
type optionQueryTracker struct {
	mu          sync.Mutex
	nextConID   int64
	nextChainID int64
	conIDReqs   map[int64]*optQueryReq // keyed by conId-lookup reqID
	chainReqs   map[int64]*optQueryReq // keyed by reqSecDefOptParams reqID
}

// RequestOptionChain performs a synchronous, blocking lookup of the SMART
// expirations and strikes available for symbol's option chain: resolves the
// underlying's conId via ReqContractDetails, then calls reqSecDefOptParams.
// Blocks the calling goroutine until both round trips complete or ctx is
// cancelled. Intended for ad-hoc UI queries — not part of any subscriber's
// trading configuration.
func (s *Session) RequestOptionChain(ctx context.Context, symbol string) (OptionChainInfo, error) {
	sym := strings.ToUpper(strings.TrimSpace(symbol))
	if sym == "" {
		return OptionChainInfo{}, fmt.Errorf("empty symbol")
	}

	done := make(chan optQueryResult, 1)

	s.optQuery.mu.Lock()
	// Both counters walk forward one ID per call and never wrap on their own;
	// after 1000 lookups nextConID would cross into the chain range and the
	// two phases would collide. Rewind while fully idle — no request is in
	// flight under either ID then — the same idiom RunScanner uses for its
	// own range.
	if len(s.optQuery.conIDReqs) == 0 && len(s.optQuery.chainReqs) == 0 {
		s.optQuery.nextConID = reqIDOptQueryConIDBase
		s.optQuery.nextChainID = reqIDOptQueryChainBase
	}
	reqID := s.optQuery.nextConID
	s.optQuery.nextConID++
	s.optQuery.conIDReqs[reqID] = &optQueryReq{symbol: sym, done: done}
	s.optQuery.mu.Unlock()

	contract := &ibapi.Contract{Symbol: sym, SecType: "STK", Currency: "USD", Exchange: "SMART"}
	s.client.ReqContractDetails(reqID, contract)

	select {
	case res := <-done:
		return res.info, res.err
	case <-ctx.Done():
		s.optQuery.mu.Lock()
		delete(s.optQuery.conIDReqs, reqID)
		s.optQuery.mu.Unlock()
		return OptionChainInfo{}, ctx.Err()
	}
}

// handleOptionQueryContractDetails stores the first conId received for a
// RequestOptionChain conId-lookup request. Returns true if reqID belongs to
// this phase (caller should not fall through to any other ContractDetails
// handling).
func (s *Session) handleOptionQueryContractDetails(reqID int64, cd *ibapi.ContractDetails) bool {
	s.optQuery.mu.Lock()
	req, ok := s.optQuery.conIDReqs[reqID]
	if !ok {
		s.optQuery.mu.Unlock()
		return false
	}
	if req.conID == 0 && cd != nil {
		req.conID = cd.Contract.ConID
	}
	s.optQuery.mu.Unlock()
	return true
}

// handleOptionQueryContractDetailsEnd fires reqSecDefOptParams with the
// resolved conId, moving req into chainReqs under the new reqID. Delivers a
// failure result immediately if no conId was ever resolved.
func (s *Session) handleOptionQueryContractDetailsEnd(reqID int64) bool {
	s.optQuery.mu.Lock()
	req, ok := s.optQuery.conIDReqs[reqID]
	if !ok {
		s.optQuery.mu.Unlock()
		return false
	}
	delete(s.optQuery.conIDReqs, reqID)

	if req.conID == 0 {
		s.optQuery.mu.Unlock()
		req.done <- optQueryResult{err: fmt.Errorf("could not resolve conId for %s", req.symbol)}
		return true
	}

	chainReqID := s.optQuery.nextChainID
	s.optQuery.nextChainID++
	s.optQuery.chainReqs[chainReqID] = req
	symbol, conID := req.symbol, req.conID
	s.optQuery.mu.Unlock()

	s.client.ReqSecDefOptParams(chainReqID, symbol, "", "STK", conID)
	return true
}

// handleOptionQuerySecDefOptParams accumulates one reqSecDefOptParams
// exchange callback (SMART only — see options.go's identical rationale) for
// a RequestOptionChain request.
func (s *Session) handleOptionQuerySecDefOptParams(reqID int64, exchange string, expirations []string, strikes []float64) bool {
	s.optQuery.mu.Lock()
	req, ok := s.optQuery.chainReqs[reqID]
	if !ok {
		s.optQuery.mu.Unlock()
		return false
	}
	if exchange == "SMART" {
		expSet := make(map[string]bool, len(req.expirations))
		for _, e := range req.expirations {
			expSet[e] = true
		}
		for _, e := range expirations {
			if !expSet[e] {
				expSet[e] = true
				req.expirations = append(req.expirations, e)
			}
		}
		strikeSet := make(map[float64]bool, len(req.strikes))
		for _, st := range req.strikes {
			strikeSet[st] = true
		}
		for _, st := range strikes {
			if !strikeSet[st] {
				strikeSet[st] = true
				req.strikes = append(req.strikes, st)
			}
		}
	}
	s.optQuery.mu.Unlock()
	return true
}

// handleOptionQuerySecDefOptParamsEnd delivers the final result once all
// exchange callbacks for a RequestOptionChain request have arrived —
// expirations filtered to today-or-later and both slices sorted ascending.
func (s *Session) handleOptionQuerySecDefOptParamsEnd(reqID int64) bool {
	s.optQuery.mu.Lock()
	req, ok := s.optQuery.chainReqs[reqID]
	if !ok {
		s.optQuery.mu.Unlock()
		return false
	}
	delete(s.optQuery.chainReqs, reqID)
	expirations := append([]string(nil), req.expirations...)
	strikes := append([]float64(nil), req.strikes...)
	symbol := req.symbol
	s.optQuery.mu.Unlock()

	today := time.Now().Format("20060102")
	future := expirations[:0:0]
	for _, e := range expirations {
		if e >= today {
			future = append(future, e)
		}
	}
	sort.Strings(future)
	sort.Float64s(strikes)

	if len(future) == 0 || len(strikes) == 0 {
		req.done <- optQueryResult{err: fmt.Errorf("%w found for %s", ErrNoOptionChain, symbol)}
		return true
	}
	req.done <- optQueryResult{info: OptionChainInfo{Expirations: future, Strikes: strikes}}
	return true
}

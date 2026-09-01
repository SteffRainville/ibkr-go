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
	symbol  string
	conID   int64
	classes map[string]*chainClass
	done    chan optQueryResult
}

type optQueryResult struct {
	info OptionChainInfo
	err  error
}

// optionQueryTracker holds state for RequestOptionChain calls, keyed by
// reqID like every other IB request in this package.
type optionQueryTracker struct {
	mu          sync.Mutex
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
	reqID := s.nextReqID()
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

	chainReqID := s.nextReqID()
	s.optQuery.chainReqs[chainReqID] = req
	symbol, conID := req.symbol, req.conID
	s.optQuery.mu.Unlock()

	s.client.ReqSecDefOptParams(chainReqID, symbol, "", "STK", conID)
	return true
}

// handleOptionQuerySecDefOptParams accumulates one reqSecDefOptParams
// callback (SMART only — see options.go's identical rationale) for a
// RequestOptionChain request, bucketed by trading class for the same reason
// optChainReq is: this result populates the option chart's expiry and strike
// pickers, and a union across classes offers combinations that do not exist.
func (s *Session) handleOptionQuerySecDefOptParams(reqID int64, exchange, tradingClass, multiplier string, expirations []string, strikes []float64) bool {
	s.optQuery.mu.Lock()
	req, ok := s.optQuery.chainReqs[reqID]
	if !ok {
		s.optQuery.mu.Unlock()
		return false
	}
	if exchange == "SMART" {
		if req.classes == nil {
			req.classes = make(map[string]*chainClass)
		}
		cls, known := req.classes[tradingClass]
		if !known {
			cls = &chainClass{tradingClass: tradingClass, multiplier: multiplier}
			req.classes[tradingClass] = cls
		}
		cls.mergeChainParams(expirations, strikes)
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
	symbol := req.symbol
	chosen, _ := pickChainClass(symbol, req.classes)
	s.optQuery.mu.Unlock()

	if chosen == nil {
		req.done <- optQueryResult{err: fmt.Errorf("%w found for %s", ErrNoOptionChain, symbol)}
		return true
	}
	expirations := append([]string(nil), chosen.expirations...)
	strikes := append([]float64(nil), chosen.strikes...)

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

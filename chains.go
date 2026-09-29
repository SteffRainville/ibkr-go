// Option chains: which strikes exist for the expiry each selector trades.
//
// The model is deliberately the simple one:
//
//	per symbol, once per session:     conId                 (conIDs)
//	per (symbol, delay), once a day:  ReqSecDefOptParams    → pick the trading class and the expiry
//	                                  OPT ReqContractDetails → the strikes listed for THAT expiry
//	at entry:                         ResolveEntryStrike probes deltas over those strikes
//
// A chain is loaded once for the trading day and not touched again until the
// date changes. It used to be re-fetched every 5 minutes by a selector rotation
// with fairness scoring — machinery built for background market-data legs that
// no longer exist — and its strike list was ReqSecDefOptParams' UNION across
// every expiry of the class, so strikes that exist only on later expiries (TQQQ
// 77.50, IWM 282.50, AGQ 71.50 on the 0DTE/weekly) were probed and refused 276
// times on 2026-09-28. The per-expiry listing is the answer to "which strikes
// exist"; a chain without one is not ready, and entries wait for the retry
// rather than guess.
package ibkr

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/scmhub/ibapi"
)

// chainStage is which of a chain's three requests is in flight.
type chainStage int

const (
	stageIdle    chainStage = iota
	stageConID              // ReqContractDetails STK → the underlying's conId
	stageParams             // ReqSecDefOptParams → trading classes + expirations
	stageListing            // ReqContractDetails OPT for one expiry → its strikes
)

func (st chainStage) String() string {
	switch st {
	case stageConID:
		return "conId lookup"
	case stageParams:
		return "chain params"
	case stageListing:
		return "expiry listing"
	default:
		return "idle"
	}
}

// chain is one (symbol, optionDelay)'s option chain for the trading day: the
// expiry the delay selects and the strikes IB lists for it, per right. The
// fetch fields are the one in-flight request, if any.
type chain struct {
	day     string               // local YYYYMMDD the chain was loaded on; "" = never
	expiry  string               // YYYYMMDD
	strikes map[string][]float64 // "call"/"put" → listed strikes, ascending

	currency string

	stage     chainStage
	reqID     int64
	startedAt time.Time // start of the current stage
	failedAt  time.Time // last failed fetch; retried after chainRetryBackoff

	// accumulated while in flight
	classes map[string]*chainClass // stageParams: SMART callbacks per trading class
	class   *chainClass            // chosen class, carried into stageListing
	listed  map[string]map[float64]bool
}

// readyOn reports whether the chain holds strikes loaded on day.
func (c *chain) readyOn(day string) bool { return c.day == day && len(c.strikes) > 0 }

const (
	// chainStageTimeout bounds one stage of a fetch. A healthy round trip takes
	// low single-digit seconds; a request IB silently dropped (pacing) gets
	// neither a result nor an error, so without this the chain would stay in
	// flight forever.
	chainStageTimeout = 20 * time.Second

	// chainRetryBackoff spaces retries of a chain whose fetch failed.
	chainRetryBackoff = 30 * time.Second

	// maxChainFetches caps concurrent chain fetches, so connecting with a long
	// watchlist does not fire every lookup in the same instant.
	maxChainFetches = 4
)

func tradingDay(t time.Time) string { return t.Format("20060102") }

// loadChains is the chain loader's tick: fail stages IB never answered, then
// start the chains that are missing or from a previous day.
func (s *Session) loadChains() {
	s.expireChainFetches()
	s.startChainFetches()
}

// startChainFetches begins a fetch for every chain some selector needs that is
// not ready for today, not in flight, and not inside its retry backoff — up to
// maxChainFetches in flight. Chains no selector needs any more are dropped.
func (s *Session) startChainFetches() {
	if s.client == nil {
		return
	}
	now := time.Now()
	today := tradingDay(now)

	type send struct {
		key      chainKey
		stage    chainStage
		reqID    int64
		currency string
		conID    int64
	}
	var sends []send

	s.optChain.mu.Lock()
	needed := make(map[chainKey]string)
	for _, sel := range s.optChain.selectors {
		needed[sel.chainKey()] = sel.currency
	}
	for key, c := range s.optChain.chains {
		if _, ok := needed[key]; !ok {
			delete(s.optChain.chainByReq, c.reqID)
			delete(s.optChain.chains, key)
		}
	}
	keys := make([]chainKey, 0, len(needed))
	inFlight := 0
	for key, currency := range needed {
		c := s.optChain.chains[key]
		if c == nil {
			c = &chain{currency: currency}
			s.optChain.chains[key] = c
		}
		switch {
		case c.stage != stageIdle:
			inFlight++
		case c.readyOn(today):
		case !c.failedAt.IsZero() && now.Sub(c.failedAt) < chainRetryBackoff:
		default:
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].symbol != keys[j].symbol {
			return keys[i].symbol < keys[j].symbol
		}
		return keys[i].optionDelay < keys[j].optionDelay
	})
	for _, key := range keys {
		if inFlight >= maxChainFetches {
			break
		}
		inFlight++
		c := s.optChain.chains[key]
		st := stageConID
		conID := s.optChain.conIDs[key.symbol]
		if conID != 0 {
			st = stageParams
		}
		reqID := s.beginChainStageLocked(key, c, st, now)
		sends = append(sends, send{key, st, reqID, c.currency, conID})
	}
	s.optChain.mu.Unlock()

	for _, sd := range sends {
		switch sd.stage {
		case stageConID:
			s.optionLog.Printf("Option chain: %s delay=%d — resolving conId (reqID=%d)", sd.key.symbol, sd.key.optionDelay, sd.reqID)
			s.client.ReqContractDetails(sd.reqID, &ibapi.Contract{Symbol: sd.key.symbol, SecType: "STK", Currency: sd.currency, Exchange: "SMART"})
		case stageParams:
			s.optionLog.Printf("Option chain: %s delay=%d — requesting chain params (conId=%d, reqID=%d)", sd.key.symbol, sd.key.optionDelay, sd.conID, sd.reqID)
			s.client.ReqSecDefOptParams(sd.reqID, sd.key.symbol, "", "STK", sd.conID)
		}
	}
}

// beginChainStageLocked moves c to stage under a fresh reqID. Caller holds
// s.optChain.mu and sends the request after unlocking.
func (s *Session) beginChainStageLocked(key chainKey, c *chain, stage chainStage, now time.Time) int64 {
	delete(s.optChain.chainByReq, c.reqID)
	reqID := s.nextReqID()
	c.stage, c.reqID, c.startedAt = stage, reqID, now
	s.optChain.chainByReq[reqID] = key
	return reqID
}

// failChainLocked ends c's fetch as failed; the loader retries it after
// chainRetryBackoff. Caller holds s.optChain.mu.
func (s *Session) failChainLocked(c *chain) {
	delete(s.optChain.chainByReq, c.reqID)
	c.stage, c.reqID = stageIdle, 0
	c.classes, c.class, c.listed = nil, nil, nil
	c.failedAt = time.Now()
}

// chainForReqLocked returns the chain a reqID's stage belongs to, if the
// request is still that chain's current one. Caller holds s.optChain.mu.
func (s *Session) chainForReqLocked(reqID int64) (chainKey, *chain, bool) {
	key, ok := s.optChain.chainByReq[reqID]
	if !ok {
		return chainKey{}, nil, false
	}
	c := s.optChain.chains[key]
	if c == nil || c.reqID != reqID {
		delete(s.optChain.chainByReq, reqID)
		return chainKey{}, nil, false
	}
	return key, c, true
}

// expireChainFetches fails every stage IB has not answered within
// chainStageTimeout.
func (s *Session) expireChainFetches() {
	now := time.Now()
	var stuck []string
	s.optChain.mu.Lock()
	for key, c := range s.optChain.chains {
		if c.stage == stageIdle || now.Sub(c.startedAt) < chainStageTimeout {
			continue
		}
		stuck = append(stuck, fmt.Sprintf("%s delay=%d (%s, reqID=%d)", key.symbol, key.optionDelay, c.stage, c.reqID))
		s.failChainLocked(c)
	}
	s.optChain.mu.Unlock()
	for _, d := range stuck {
		s.optionLog.Printf("Option chain: WARNING %s — IB never answered within %s; retrying in %s", d, chainStageTimeout, chainRetryBackoff)
	}
}

// resetChainFetches forgets every in-flight fetch without penalty — for a new
// connection, on which the previous connection's requests will never be
// answered. Loaded chains and conIds stay: they are still true.
func (s *Session) resetChainFetches() {
	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()
	for _, c := range s.optChain.chains {
		if c.stage != stageIdle {
			delete(s.optChain.chainByReq, c.reqID)
			c.stage, c.reqID = stageIdle, 0
			c.classes, c.class, c.listed = nil, nil, nil
		}
	}
}

// handleChainContractDetails receives one contract for a chain's conId lookup
// or expiry listing. Returns true if reqID belongs to a chain.
func (s *Session) handleChainContractDetails(reqID int64, cd *ibapi.ContractDetails) bool {
	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()
	key, c, ok := s.chainForReqLocked(reqID)
	if !ok {
		return false
	}
	if cd == nil {
		return true
	}
	switch c.stage {
	case stageConID:
		if s.optChain.conIDs[key.symbol] == 0 {
			s.optChain.conIDs[key.symbol] = cd.Contract.ConID
		}
	case stageListing:
		if cd.Contract.LastTradeDateOrContractMonth != c.expiry {
			return true
		}
		right := ""
		switch strings.ToUpper(cd.Contract.Right) {
		case "C", "CALL":
			right = "call"
		case "P", "PUT":
			right = "put"
		default:
			return true
		}
		if c.listed == nil {
			c.listed = make(map[string]map[float64]bool, 2)
		}
		if c.listed[right] == nil {
			c.listed[right] = make(map[float64]bool)
		}
		c.listed[right][cd.Contract.Strike] = true
	}
	return true
}

// handleChainContractDetailsEnd completes a conId lookup (→ chain params) or an
// expiry listing (→ the chain is ready). Returns true if reqID belongs to a chain.
func (s *Session) handleChainContractDetailsEnd(reqID int64) bool {
	s.optChain.mu.Lock()
	key, c, ok := s.chainForReqLocked(reqID)
	if !ok {
		s.optChain.mu.Unlock()
		return false
	}

	switch c.stage {
	case stageConID:
		conID := s.optChain.conIDs[key.symbol]
		if conID == 0 {
			s.failChainLocked(c)
			s.optChain.mu.Unlock()
			s.optionLog.Printf("Option chain: %s — IB returned no conId; retrying in %s", key.symbol, chainRetryBackoff)
			return true
		}
		paramsID := s.beginChainStageLocked(key, c, stageParams, time.Now())
		s.optChain.mu.Unlock()
		s.optionLog.Printf("Option chain: %s delay=%d — conId=%d, requesting chain params (reqID=%d)", key.symbol, key.optionDelay, conID, paramsID)
		s.client.ReqSecDefOptParams(paramsID, key.symbol, "", "STK", conID)
		return true

	case stageListing:
		strikes := make(map[string][]float64, 2)
		for right, set := range c.listed {
			list := make([]float64, 0, len(set))
			for st := range set {
				list = append(list, st)
			}
			sort.Float64s(list)
			strikes[right] = list
		}
		if len(strikes) == 0 {
			expiry := c.expiry
			s.failChainLocked(c)
			s.optChain.mu.Unlock()
			s.optionLog.Printf("Option chain: %s %s — IB listed no contracts for the expiry; retrying in %s", key.symbol, expiry, chainRetryBackoff)
			return true
		}
		delete(s.optChain.chainByReq, reqID)
		c.day, c.strikes = tradingDay(time.Now()), strikes
		c.stage, c.reqID, c.failedAt = stageIdle, 0, time.Time{}
		c.classes, c.class, c.listed = nil, nil, nil
		expiry := c.expiry
		s.optChain.mu.Unlock()
		s.optionLog.Printf("Option chain listed: %s delay=%d expiry=%s — calls=%d puts=%d strikes",
			key.symbol, key.optionDelay, expiry, len(strikes["call"]), len(strikes["put"]))
		s.startChainFetches()
		return true
	}
	s.optChain.mu.Unlock()
	return true
}

// SecurityDefinitionOptionParameter accumulates one (exchange, trading class)
// callback of a chain-params request. Only SMART is kept — it is exactly what
// SMART routing can trade — and each class is kept apart (see chainClass).
func (s *Session) SecurityDefinitionOptionParameter(reqID int64, exchange string, underlyingConID int64, tradingClass string, multiplier string, expirations []string, strikes []float64) {
	if s.handleOptionQuerySecDefOptParams(reqID, exchange, tradingClass, multiplier, expirations, strikes) {
		return
	}
	if exchange != "SMART" {
		return
	}
	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()
	_, c, ok := s.chainForReqLocked(reqID)
	if !ok || c.stage != stageParams {
		return
	}
	if c.classes == nil {
		c.classes = make(map[string]*chainClass)
	}
	cls, ok := c.classes[tradingClass]
	if !ok {
		cls = &chainClass{tradingClass: tradingClass, multiplier: multiplier}
		c.classes[tradingClass] = cls
	}
	cls.mergeChainParams(expirations, strikes)
}

// SecurityDefinitionOptionParameterEnd picks ONE trading class and the expiry
// the chain's optionDelay selects from it, then lists that expiry's contracts.
func (s *Session) SecurityDefinitionOptionParameterEnd(reqID int64) {
	if s.handleOptionQuerySecDefOptParamsEnd(reqID) {
		return
	}
	s.optChain.mu.Lock()
	key, c, ok := s.chainForReqLocked(reqID)
	if !ok || c.stage != stageParams {
		s.optChain.mu.Unlock()
		return
	}
	chosen, ignored := pickChainClass(key.symbol, c.classes)
	if chosen == nil {
		s.failChainLocked(c)
		s.optChain.mu.Unlock()
		s.optionLog.Printf("Option chain: %s delay=%d — no usable SMART trading class (saw: %s); retrying in %s",
			key.symbol, key.optionDelay, describeChainClasses(ignored), chainRetryBackoff)
		return
	}
	expiry := nearestExpiry(chosen.expirations, key.optionDelay)
	if expiry == "" {
		s.failChainLocked(c)
		s.optChain.mu.Unlock()
		s.optionLog.Printf("Option chain: %s — no current or future expiration in class %s; retrying in %s", key.symbol, chosen.tradingClass, chainRetryBackoff)
		return
	}
	c.expiry, c.class, c.classes, c.listed = expiry, chosen, nil, nil
	listID := s.beginChainStageLocked(key, c, stageListing, time.Now())
	currency := c.currency
	s.optChain.mu.Unlock()

	if chosen.multiplier != standardMultiplier {
		s.logger.Printf("Option chain: %s — no standard (multiplier %s) SMART trading class; using %s mult=%s",
			key.symbol, standardMultiplier, chosen.tradingClass, chosen.multiplier)
	}
	s.optionLog.Printf("Option chain: %s delay=%d — class=%s mult=%s expiry=%s (ignored: %s); listing its contracts (reqID=%d)",
		key.symbol, key.optionDelay, chosen.tradingClass, chosen.multiplier, expiry, describeChainClasses(ignored), listID)

	if currency == "" {
		currency = "USD"
	}
	ct := ibapi.NewContract() // Strike stays unset: every strike, both rights
	ct.Symbol = key.symbol
	ct.SecType = "OPT"
	ct.Exchange = "SMART"
	ct.Currency = currency
	ct.LastTradeDateOrContractMonth = expiry
	ct.TradingClass = chosen.tradingClass
	ct.Multiplier = chosen.multiplier
	s.client.ReqContractDetails(listID, ct)
}

// handleChainError fails the chain whose current request IB rejected. Returns
// true if reqID belongs to a chain.
func (s *Session) handleChainError(reqID int64, errStr string) bool {
	s.optChain.mu.Lock()
	key, c, ok := s.chainForReqLocked(reqID)
	if !ok {
		s.optChain.mu.Unlock()
		return false
	}
	stage := c.stage
	s.failChainLocked(c)
	s.optChain.mu.Unlock()
	s.optionLog.Printf("Option chain: %s delay=%d — %s FAILED (reqID=%d): %s; retrying in %s",
		key.symbol, key.optionDelay, stage, reqID, errStr, chainRetryBackoff)
	return true
}

// readyChainsLocked counts chains loaded for today. Caller holds s.optChain.mu.
func (s *Session) readyChainsLocked() int {
	today := tradingDay(time.Now())
	n := 0
	for _, c := range s.optChain.chains {
		if c.readyOn(today) {
			n++
		}
	}
	return n
}

// dropSymbolChainsLocked forgets every chain of symbol. Caller holds
// s.optChain.mu.
func (s *Session) dropSymbolChainsLocked(symbol string) {
	for key, c := range s.optChain.chains {
		if key.symbol == symbol {
			delete(s.optChain.chainByReq, c.reqID)
			delete(s.optChain.chains, key)
		}
	}
}

// chainStrikesLocked returns the expiry and the listed strikes for sel's right,
// or ok=false when the chain is not loaded for today. Caller holds
// s.optChain.mu.
func (s *Session) chainStrikesLocked(sel selector) (expiry string, strikes []float64, ok bool) {
	c := s.optChain.chains[sel.chainKey()]
	if c == nil || !c.readyOn(tradingDay(time.Now())) {
		return "", nil, false
	}
	strikes = c.strikes[sel.right]
	return c.expiry, slices.Clone(strikes), len(strikes) > 0
}

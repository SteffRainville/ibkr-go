// Option chain lookup and market data subscription.
//
// Flow per session:
//  1. requestOptionChains fires ReqContractDetails for each unique option
//     underlying to obtain its conId.
//  2. ContractDetailsEnd for those reqIDs calls ReqSecDefOptParams with the
//     real conId.
//  3. SecurityDefinitionOptionParameter accumulates expirations/strikes from
//     the SMART exchange only (IBKR calls this once per available exchange;
//     non-SMART exchanges may include phantom strikes not routable via SMART).
//  4. SecurityDefinitionOptionParameterEnd fires when all exchanges have
//     responded; picks the nearest expiry + ATM/target-delta strike from the
//     SMART-only strike set, then subscribes to streaming market data.
//  5. If IB returns error 200 for both legs of a strike, handleOptionMktError
//     automatically retries with the next nearest strike.
//  6. TickPrice for option reqIDs updates cached prices and publishes
//     KindOptionData/KindPositionOptionData.
package ibkr

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/scmhub/ibapi"

	"github.com/SteffRainville/ibkr-go/eventbus"
	"github.com/SteffRainville/ibkr-go/mdlines"
	"github.com/SteffRainville/ibkr-go/quotes"
)

// A "group" is one distinct (symbol, optionDelay, callδ, putδ) resolution
// request. Subscribers that configure the same parameters for the same
// underlying share a group (and a single IB feed); subscribers that differ
// get their own group. busIdxs lists the subscriber bus indices (into
// Session.buses) that the group's option data must be routed to.

// optConIDReq tracks one pending ReqContractDetails call used to resolve
// the underlying conId before calling ReqSecDefOptParams.
type optConIDReq struct {
	groupID         int
	symbol          string
	currency        string
	conID           int64
	optionDelay     int
	targetDeltaCall float64
	targetDeltaPut  float64
	busIdxs         []int
}

// optChainReq tracks one pending reqSecDefOptParams call. IBKR may send
// multiple SecurityDefinitionOptionParameter callbacks for the same reqID
// (one per available exchange), so we merge them.
type optChainReq struct {
	groupID         int
	symbol          string
	optionDelay     int
	targetDeltaCall float64
	targetDeltaPut  float64
	busIdxs         []int
	expirations     []string
	strikes         []float64
}

// optMktReq maps one option market data reqID → contract details + live prices.
type optMktReq struct {
	groupID int
	symbol  string
	right   string // "call" or "put"
	strike  float64
	expiry  string
	price   float64
	bid     float64
	ask     float64
	delta   float64
	busIdxs []int

	// deltaSource is "matched" (a deliberate ATM target, or a genuine live
	// IB delta match) or "atm_fallback" (no usable delta, strike picked
	// without one).
	deltaSource string

	// pending marks a freshly-subscribed replacement leg that is NOT yet
	// the active leg for its (symbol, right). While pending it silently
	// accumulates ticks but is neither published nor used for orders — the
	// previous active leg stays live throughout — so an ATM strike swap
	// never blanks the row or leaves a buy without a quote. Promoted to
	// active once it carries a complete quote, or after pendingPromoteGrace.
	pending      bool
	pendingSince time.Time
}

// pendingPromoteGrace bounds how long a replacement option leg may stay
// pending waiting for a complete two-sided quote before it is promoted
// anyway (on any usable price).
const pendingPromoteGrace = 4 * time.Second

// optStrikeRetry holds the sorted candidate strike list for a symbol so
// that if the nearest strike fails with error 200 on both legs, the next
// nearest is tried automatically.
type optStrikeRetry struct {
	groupID int
	symbol  string
	expiry  string
	strikes []float64
	nextIdx int
	pending int
	busIdxs []int
}

// posStrikeSub tracks an IB market data subscription pinned to an open
// position's strike. These survive the ATM refresh cycle so stop/exit
// evaluation continues to use the correct option contract's prices.
type posStrikeSub struct {
	symbol string
	right  string
	strike float64
	expiry string
	reqID  int64
	price  float64
	bid    float64
	ask    float64
	delta  float64

	// deltaSource mirrors optMktReq.deltaSource — copied from the matching
	// ATM leg at pin time, if one exists.
	deltaSource string
}

// deltaCandidate tracks one strike subscription used during delta-based
// strike selection. Multiple candidates are subscribed simultaneously; the
// one closest to the target delta is promoted to mktReqs, the rest cancelled.
type deltaCandidate struct {
	groupID int
	symbol  string
	right   string
	strike  float64
	expiry  string
	reqID   int64
	delta   float64
	bid     float64
	ask     float64
	ready   bool
	busIdxs []int
}

// deltaResolution groups the pending candidates for one symbol+right so
// they can be resolved together once deltas arrive.
type deltaResolution struct {
	groupID     int
	symbol      string
	right       string
	targetDelta float64
	expiry      string
	candidates  []*deltaCandidate
	allStrikes  []float64
	busIdxs     []int
}

// optResGroup is one option resolution group — the dedup of a (symbol,
// delay, callδ, putδ) tuple shared by any subscribers that configure it
// identically. Its groupID is assigned once per session and stays stable
// across every ATM strike refresh.
type optResGroup struct {
	groupID         int
	symbol          string
	currency        string
	optionDelay     int
	targetDeltaCall float64
	targetDeltaPut  float64
	busIdxs         []int
}

// optionChainTracker holds all state for option chain lookup and market data.
type optionChainTracker struct {
	mu           sync.Mutex
	nextConIDID  int64
	nextChainID  int64
	nextMktID    int64
	nextPosID    int64
	nextGroupID  int
	conIDReqs    map[int64]*optConIDReq
	chainReqs    map[int64]*optChainReq
	mktReqs      map[int64]*optMktReq
	posSubs      map[int64]*posStrikeSub
	posSubKeys   map[string]int64
	retries      map[string]*optStrikeRetry
	deltaCands   map[int64]*deltaCandidate
	deltaRes     map[string]*deltaResolution
	rotation     []optResGroup
	rotateCursor int

	// lastIV remembers the most recent implied-volatility sample seen for
	// each underlying symbol. Used by approximateStrikeForDelta as the best
	// available IV estimate when a delta probe fails.
	lastIV map[string]float64

	// lastChainInfo caches the most recently resolved (expiry, SMART strike
	// universe) per underlying symbol. ResolveEntryStrike reads this so an
	// entry-time delta probe can subscribe candidates immediately instead
	// of repeating the conId + chain-params round trip synchronously.
	lastChainInfo map[string]chainSnapshot

	// resolvedEntry caches the contract ResolveEntryStrike most recently
	// resolved for each "groupID|right", so a DIFFERENT subscriber
	// resolving the SAME group within resolvedEntryTTL reuses the identical
	// strike instead of running its own probe.
	resolvedEntry map[string]resolvedEntryLeg
}

// resolvedEntryLeg is one group's most recently resolved entry contract
// (strike + expiry + the delta that won it), with the wall-clock time it
// was resolved.
type resolvedEntryLeg struct {
	strike float64
	expiry string
	delta  float64
	at     time.Time
}

// chainSnapshot is one underlying's cached expiry + SMART strike universe.
type chainSnapshot struct {
	expiry  string
	strikes []float64
}

func retryKeyATM(groupID int) string           { return fmt.Sprintf("g%d", groupID) }
func retryKeyLeg(groupID int, r string) string { return fmt.Sprintf("g%d|%s", groupID, r) }

// resolvedEntryTTL bounds how long a resolved entry contract is shared
// across subscribers in a group.
const resolvedEntryTTL = 5 * time.Second

// requestOptionChains fires ReqContractDetails for each distinct option
// resolution group across all subscribers. option_delay and target_delta
// are configured per SymbolSpec, so the same underlying may need to resolve
// to a different expiry/strike for each subscriber. Subscribers that share
// identical (symbol, delay, callδ, putδ) parameters are deduped into one
// group (and one IB feed).
func (s *Session) requestOptionChains() {
	s.buildOptionGroups()

	s.optChain.mu.Lock()
	groups := append([]optResGroup(nil), s.optChain.rotation...)
	s.optChain.mu.Unlock()

	for _, g := range groups {
		s.resolveOptionGroup(g)
	}
}

// buildOptionGroups rebuilds the option resolution-group list from the
// current subscriber symbol lists, deduplicating (symbol, delay, callδ,
// putδ) tuples across subscribers and assigning each group a stable
// session-lifetime groupID.
func (s *Session) buildOptionGroups() {
	type groupKey struct {
		symbol      string
		optionDelay int
		callDelta   float64
		putDelta    float64
	}
	type symInfo struct {
		currency        string
		optionDelay     int
		targetDeltaCall float64
		targetDeltaPut  float64
	}

	groups := make(map[groupKey]*optResGroup)
	var order []groupKey
	for busIdx, syms := range s.subSymbols {
		perSym := make(map[string]*symInfo)
		for _, sy := range syms {
			if !isOptionTag(sy.Tag) {
				continue
			}
			info, exists := perSym[sy.Symbol]
			if !exists {
				currency := "USD"
				if sy.Contract != nil && sy.Contract.Currency != "" {
					currency = sy.Contract.Currency
				}
				info = &symInfo{currency: currency, optionDelay: sy.OptionDelay,
					targetDeltaCall: 0.50, targetDeltaPut: 0.50}
				perSym[sy.Symbol] = info
			}
			td := sy.TargetDelta
			if td <= 0 || td > 1 {
				td = 0.50
			}
			if sy.Tag == "call" {
				info.targetDeltaCall = td
			} else {
				info.targetDeltaPut = td
			}
		}

		for symbol, info := range perSym {
			key := groupKey{symbol, info.optionDelay, info.targetDeltaCall, info.targetDeltaPut}
			g, ok := groups[key]
			if !ok {
				g = &optResGroup{symbol: symbol, currency: info.currency,
					optionDelay:     info.optionDelay,
					targetDeltaCall: info.targetDeltaCall, targetDeltaPut: info.targetDeltaPut}
				groups[key] = g
				order = append(order, key)
			}
			g.busIdxs = append(g.busIdxs, busIdx)
		}
	}

	sort.Slice(order, func(i, j int) bool {
		if order[i].symbol != order[j].symbol {
			return order[i].symbol < order[j].symbol
		}
		if order[i].optionDelay != order[j].optionDelay {
			return order[i].optionDelay < order[j].optionDelay
		}
		if order[i].callDelta != order[j].callDelta {
			return order[i].callDelta < order[j].callDelta
		}
		return order[i].putDelta < order[j].putDelta
	})

	s.optChain.mu.Lock()
	s.optChain.rotation = s.optChain.rotation[:0]
	for _, key := range order {
		g := groups[key]
		g.groupID = s.optChain.nextGroupID
		s.optChain.nextGroupID++
		s.optChain.rotation = append(s.optChain.rotation, *g)
	}
	s.optChain.rotateCursor = 0
	s.optChain.mu.Unlock()
}

// resolveOptionGroup begins ATM strike resolution for one group by firing
// the conId lookup, reusing the group's stable groupID. Skips when a prior
// resolution for the same group is still in flight.
func (s *Session) resolveOptionGroup(g optResGroup) {
	s.optChain.mu.Lock()
	if s.groupResolvingLocked(g.groupID) {
		s.optChain.mu.Unlock()
		s.optionLog.Printf("Option: skipping strike refresh for %s (group=%d) — previous resolution still in flight", g.symbol, g.groupID)
		return
	}
	reqID := s.optChain.nextConIDID
	s.optChain.nextConIDID++
	s.optChain.conIDReqs[reqID] = &optConIDReq{
		groupID: g.groupID, symbol: g.symbol, currency: g.currency,
		optionDelay: g.optionDelay, targetDeltaCall: g.targetDeltaCall,
		targetDeltaPut: g.targetDeltaPut, busIdxs: g.busIdxs,
	}
	s.optChain.mu.Unlock()

	contract := &ibapi.Contract{Symbol: g.symbol, SecType: "STK", Currency: g.currency, Exchange: "SMART"}
	s.optionLog.Printf("Option: resolving conId for %s (group=%d, reqID=%d, delay=%d, targetDelta call=%.2f put=%.2f, subs=%v)",
		g.symbol, g.groupID, reqID, g.optionDelay, g.targetDeltaCall, g.targetDeltaPut, g.busIdxs)
	s.client.ReqContractDetails(reqID, contract)
}

// rotateOptionStrikes re-resolves the next option group in the rotation and
// advances the cursor. Driven by a steady ticker so ATM strike renewal is a
// permanent slow rotation rather than a synchronized burst.
func (s *Session) rotateOptionStrikes() {
	s.optChain.mu.Lock()
	g, ok := s.pickRotationGroupLocked()
	s.optChain.mu.Unlock()
	if !ok {
		return
	}
	s.resolveOptionGroup(g)
}

// pickRotationGroupLocked chooses the next option group to refresh, oldest
// data first. Must be called with s.optChain.mu held.
func (s *Session) pickRotationGroupLocked() (optResGroup, bool) {
	n := len(s.optChain.rotation)
	if n == 0 {
		return optResGroup{}, false
	}
	var best optResGroup
	var bestScore time.Time
	found := false
	for _, g := range s.optChain.rotation {
		if s.groupResolvingLocked(g.groupID) {
			continue
		}
		score := s.groupStalenessLocked(g)
		if !found || score.Before(bestScore) {
			found, best, bestScore = true, g, score
		}
	}
	if !found {
		g := s.optChain.rotation[s.optChain.rotateCursor%n]
		s.optChain.rotateCursor = (s.optChain.rotateCursor + 1) % n
		return g, true
	}
	return best, true
}

// groupStalenessLocked returns the freshness of a group's stalest active
// option leg. Must be called with s.optChain.mu held.
func (s *Session) groupStalenessLocked(g optResGroup) time.Time {
	var score time.Time
	first := true
	for _, right := range [...]string{"call", "put"} {
		leg, ok := s.activeLegLocked(g.symbol, right)
		if !ok {
			continue
		}
		var t time.Time
		if s.book != nil {
			key := quotes.ContractKey{Symbol: g.symbol, Right: right, Strike: leg.strike, Expiry: leg.expiry}
			if q, ok := s.book.Option(key); ok {
				t = q.AskTime
				if q.BidTime.After(t) {
					t = q.BidTime
				}
			}
		}
		if first || t.Before(score) {
			score, first = t, false
		}
	}
	return score
}

// groupResolvingLocked reports whether an option resolution for groupID is
// still in flight. Caller must hold s.optChain.mu.
func (s *Session) groupResolvingLocked(groupID int) bool {
	for _, r := range s.optChain.conIDReqs {
		if r.groupID == groupID {
			return true
		}
	}
	for _, r := range s.optChain.chainReqs {
		if r.groupID == groupID {
			return true
		}
	}
	if _, ok := s.optChain.deltaRes[retryKeyLeg(groupID, "call")]; ok {
		return true
	}
	if _, ok := s.optChain.deltaRes[retryKeyLeg(groupID, "put")]; ok {
		return true
	}
	return false
}

// handleConIDContractDetails stores the first conId received for an option
// conId-lookup request. Returns true if reqID belongs to this phase.
func (s *Session) handleConIDContractDetails(reqID int64, contractDetails *ibapi.ContractDetails) bool {
	s.optChain.mu.Lock()
	req, ok := s.optChain.conIDReqs[reqID]
	if !ok {
		s.optChain.mu.Unlock()
		return false
	}
	if req.conID == 0 && contractDetails != nil {
		req.conID = contractDetails.Contract.ConID
		s.optionLog.Printf("Option: conId for %s = %d", req.symbol, req.conID)
	}
	s.optChain.mu.Unlock()
	return true
}

// handleConIDContractDetailsEnd fires ReqSecDefOptParams with the resolved
// conId. Returns true if handled.
func (s *Session) handleConIDContractDetailsEnd(reqID int64) bool {
	s.optChain.mu.Lock()
	req, ok := s.optChain.conIDReqs[reqID]
	if !ok {
		s.optChain.mu.Unlock()
		return false
	}
	delete(s.optChain.conIDReqs, reqID)
	symbol := req.symbol
	conID := req.conID

	if conID == 0 {
		s.optChain.mu.Unlock()
		s.optionLog.Printf("Option: could not resolve conId for %s, skipping chain lookup", symbol)
		return true
	}

	chainReqID := s.optChain.nextChainID
	s.optChain.nextChainID++
	s.optChain.chainReqs[chainReqID] = &optChainReq{
		groupID: req.groupID, symbol: symbol, optionDelay: req.optionDelay,
		targetDeltaCall: req.targetDeltaCall, targetDeltaPut: req.targetDeltaPut,
		busIdxs: req.busIdxs,
	}
	s.optChain.mu.Unlock()

	s.optionLog.Printf("Option: requesting chain params for %s conId=%d (reqID=%d)", symbol, conID, chainReqID)
	s.client.ReqSecDefOptParams(chainReqID, symbol, "", "STK", conID)
	return true
}

// SecurityDefinitionOptionParameter accumulates exchange callbacks for one
// reqSecDefOptParams call. Only the SMART exchange response is kept — it
// contains exactly the strikes routable via SMART for market data and orders.
func (s *Session) SecurityDefinitionOptionParameter(reqID int64, exchange string, underlyingConID int64, tradingClass string, multiplier string, expirations []string, strikes []float64) {
	if exchange != "SMART" {
		return
	}

	s.optChain.mu.Lock()
	req, ok := s.optChain.chainReqs[reqID]
	if !ok {
		s.optChain.mu.Unlock()
		return
	}

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
	s.optChain.mu.Unlock()
}

// SecurityDefinitionOptionParameterEnd fires when all SMART exchange
// callbacks for one reqSecDefOptParams call have been delivered. Picks the
// nearest expiry and ATM/target-delta strike, stores a sorted retry list in
// case error 200 forces a fallback, then subscribes market data.
func (s *Session) SecurityDefinitionOptionParameterEnd(reqID int64) {
	s.optChain.mu.Lock()
	req, ok := s.optChain.chainReqs[reqID]
	if !ok {
		s.optChain.mu.Unlock()
		return
	}
	delete(s.optChain.chainReqs, reqID)
	groupID := req.groupID
	busIdxs := req.busIdxs
	symbol := req.symbol
	optionDelay := req.optionDelay
	targetDeltaCall := req.targetDeltaCall
	targetDeltaPut := req.targetDeltaPut
	expirations := append([]string(nil), req.expirations...)
	strikes := append([]float64(nil), req.strikes...)
	s.optChain.mu.Unlock()

	s.optionLog.Printf("Option chain end: %s (group=%d) — %d SMART expirations, %d SMART strikes", symbol, groupID, len(expirations), len(strikes))

	if len(expirations) == 0 || len(strikes) == 0 {
		s.logger.Printf("Option chain: %s — no SMART strikes/expirations found, skipping", symbol)
		return
	}

	if filtered := wholeDollarStrikes(strikes); len(filtered) < len(strikes) {
		s.optionLog.Printf("Option chain: %s — filtered %d fractional strikes, %d whole-dollar strikes remain",
			symbol, len(strikes)-len(filtered), len(filtered))
		strikes = filtered
	}

	expiry := nearestExpiry(expirations, optionDelay)
	if expiry == "" {
		s.optionLog.Printf("Option chain: %s — no current or future expirations found", symbol)
		return
	}

	undPrice := s.getUnderlyingPrice(symbol)
	if undPrice <= 0 {
		s.optionLog.Printf("Option chain: %s — underlying price not yet known, using median strike", symbol)
		sort.Float64s(strikes)
		undPrice = strikes[len(strikes)/2]
	}

	sort.Slice(strikes, func(i, j int) bool {
		return math.Abs(strikes[i]-undPrice) < math.Abs(strikes[j]-undPrice)
	})

	s.optChain.mu.Lock()
	s.optChain.lastChainInfo[symbol] = chainSnapshot{expiry: expiry, strikes: append([]float64(nil), strikes...)}
	s.optChain.mu.Unlock()

	isATM := func(td float64) bool { return td >= 0.48 && td <= 0.52 }
	callATM := isATM(targetDeltaCall)
	putATM := isATM(targetDeltaPut)

	if callATM && putATM {
		strike := strikes[0]
		s.optionLog.Printf("Option chain resolved: %s (group=%d) expiry=%s strike=%.2f (underlying≈%.2f, ATM, subs=%v)",
			symbol, groupID, expiry, strike, undPrice, busIdxs)

		s.optChain.mu.Lock()
		s.optChain.retries[retryKeyATM(groupID)] = &optStrikeRetry{
			groupID: groupID, symbol: symbol, expiry: expiry, strikes: strikes, nextIdx: 1, pending: 2, busIdxs: busIdxs,
		}
		s.optChain.mu.Unlock()

		s.subscribeOptionMarketData(groupID, busIdxs, symbol, strike, expiry, false)
		return
	}

	for _, right := range []string{"call", "put"} {
		td := targetDeltaCall
		if right == "put" {
			td = targetDeltaPut
		}

		if isATM(td) {
			strike := strikes[0]
			s.optionLog.Printf("Option chain resolved: %s %s (group=%d) expiry=%s strike=%.2f (underlying≈%.2f, ATM, subs=%v)",
				symbol, right, groupID, expiry, strike, undPrice, busIdxs)
			s.subscribeOptionLeg(groupID, busIdxs, symbol, right, strike, expiry, false)
			s.optChain.mu.Lock()
			s.optChain.retries[retryKeyLeg(groupID, right)] = &optStrikeRetry{
				groupID: groupID, symbol: symbol, expiry: expiry, strikes: strikes, nextIdx: 1, pending: 1, busIdxs: busIdxs,
			}
			s.optChain.mu.Unlock()
			continue
		}

		s.optChain.mu.Lock()
		if active, hasActive := s.activeLegLocked(symbol, right); hasActive &&
			active.deltaSource == "matched" && math.Abs(math.Abs(active.delta)-td) <= deltaDriftTolerance {
			s.optChain.mu.Unlock()
			s.optionLog.Printf("Option: skipping strike re-estimate %s %s (group=%d) — live delta %.4f still within %.2f of target %.2f",
				symbol, right, groupID, active.delta, deltaDriftTolerance, td)
			continue
		}
		s.optChain.mu.Unlock()

		s.optChain.mu.Lock()
		iv := s.optChain.lastIV[symbol]
		s.optChain.mu.Unlock()
		if iv <= 0 {
			iv = defaultFallbackIV
		}
		strike := approximateStrikeForDelta(strikes, undPrice, iv, td, expiry, right)
		s.optionLog.Printf("Option chain: %s %s (group=%d) — estimating strike=%.2f via target delta %.2f (iv=%.4f)",
			symbol, right, groupID, strike, td, iv)
		s.logDeltaMiss(symbol, right, td, undPrice, strikes, "estimate_default", strike, iv)
		if strike > 0 {
			s.subscribeOptionLeg(groupID, busIdxs, symbol, right, strike, expiry, true)
		}
	}
}

// selectITMCandidates picks up to n strikes on the ITM side of the
// underlying. For calls, ITM = strikes below undPrice. For puts, ITM =
// strikes above undPrice. Returns strikes sorted from nearest-ATM to
// deepest-ITM.
func selectITMCandidates(sortedStrikes []float64, undPrice float64, right string, n int) []float64 {
	sorted := make([]float64, len(sortedStrikes))
	copy(sorted, sortedStrikes)
	sort.Float64s(sorted)

	var candidates []float64
	if right == "call" {
		for i := len(sorted) - 1; i >= 0; i-- {
			if sorted[i] < undPrice {
				candidates = append(candidates, sorted[i])
				if len(candidates) >= n {
					break
				}
			}
		}
	} else {
		for _, st := range sorted {
			if st > undPrice {
				candidates = append(candidates, st)
				if len(candidates) >= n {
					break
				}
			}
		}
	}
	return candidates
}

// replaceMktReqLocked removes any existing mktReqs entries for the same
// (symbol, right) other than newReqID and cancels their IB subscriptions.
// Must be called with s.optChain.mu held.
func (s *Session) replaceMktReqLocked(symbol, right string, newReqID int64) {
	for reqID, req := range s.optChain.mktReqs {
		if reqID == newReqID || req.symbol != symbol || req.right != right {
			continue
		}
		delete(s.optChain.mktReqs, reqID)
		s.mdLines.Release(reqID)
		s.client.CancelMktData(reqID)
	}
}

// activeLegLocked returns the ACTIVE (non-pending) mktReqs leg for (symbol,
// right), if one exists. Must be called with s.optChain.mu held.
func (s *Session) activeLegLocked(symbol, right string) (*optMktReq, bool) {
	for _, req := range s.optChain.mktReqs {
		if req.symbol == symbol && req.right == right && !req.pending {
			return req, true
		}
	}
	return nil, false
}

// mergeBusIdxsLocked adds any of newIdxs not already present in req.busIdxs,
// mutating req in place (it is a pointer into s.optChain.mktReqs), and
// returns just the newly-added indices. Must be called with s.optChain.mu
// held.
func mergeBusIdxsLocked(req *optMktReq, newIdxs []int) []int {
	var added []int
	for _, idx := range newIdxs {
		if !slices.Contains(req.busIdxs, idx) {
			req.busIdxs = append(req.busIdxs, idx)
			added = append(added, idx)
		}
	}
	return added
}

// replaceStalePendingLocked cancels any OTHER pending mktReqs legs for
// (symbol, right) — leaving the active leg and keepReqID untouched. Must be
// called with s.optChain.mu held.
func (s *Session) replaceStalePendingLocked(symbol, right string, keepReqID int64) {
	for reqID, req := range s.optChain.mktReqs {
		if reqID == keepReqID || req.symbol != symbol || req.right != right || !req.pending {
			continue
		}
		delete(s.optChain.mktReqs, reqID)
		s.mdLines.Release(reqID)
		s.client.CancelMktData(reqID)
	}
}

// promoteIfReadyLocked promotes a pending leg to active once it carries a
// complete two-sided quote (or after pendingPromoteGrace, on any usable
// price), removing the now-superseded old active leg. Must be called with
// s.optChain.mu held.
func (s *Session) promoteIfReadyLocked(req *optMktReq, reqID int64) bool {
	if !req.pending {
		return true
	}
	complete := req.bid > 0 && req.ask > 0
	graceElapsed := (req.price > 0 || req.bid > 0 || req.ask > 0) && time.Since(req.pendingSince) > pendingPromoteGrace
	if !complete && !graceElapsed {
		return false
	}
	req.pending = false
	s.replaceMktReqLocked(req.symbol, req.right, reqID)
	return true
}

// subscribeOptionLeg subscribes to market data for a single option leg,
// routing the resulting option data to the buses of the owning resolution
// group. fallback marks a strike chosen without a genuine live delta match.
func (s *Session) subscribeOptionLeg(groupID int, busIdxs []int, symbol, right string, strike float64, expiry string, fallback bool) {
	ibRight := "C"
	if right == "put" {
		ibRight = "P"
	}
	contract := makeOptionContract(symbol, ibRight, strike, expiry)

	s.optChain.mu.Lock()
	active, hasActive := s.activeLegLocked(symbol, right)
	if hasActive && active.strike == strike {
		added := mergeBusIdxsLocked(active, busIdxs)
		snapshot := *active
		s.optChain.mu.Unlock()
		if len(added) > 0 {
			// A later-resolving group (e.g. a second robot whose independent
			// delta estimate happened to land on the same strike) reused an
			// already-active leg instead of opening a duplicate subscription.
			// Without this, the joining group's busIdxs would never be added
			// to the leg, so its hub would never receive this option's data
			// — the strike would silently stay blank forever, even though
			// the market data feed for the contract already exists.
			s.optionLog.Printf("Option: skipping background refresh %s %s (group=%d) — estimate strike unchanged at %.2f, joining existing subscription (bus=%v)",
				symbol, right, groupID, strike, added)
			s.publishTo(added, eventbus.Event{
				Kind: eventbus.KindOptionData,
				Payload: eventbus.OptionData{
					Symbol: symbol, Right: right, Strike: snapshot.strike, Expiry: snapshot.expiry,
					Price: snapshot.price, Bid: snapshot.bid, Ask: snapshot.ask, Delta: snapshot.delta,
					DeltaSource: snapshot.deltaSource,
				},
			})
		} else {
			s.optionLog.Printf("Option: skipping background refresh %s %s (group=%d) — estimate strike unchanged at %.2f",
				symbol, right, groupID, strike)
		}
		return
	}
	mktReqID := s.optChain.nextMktID
	s.optChain.nextMktID++
	s.optChain.mu.Unlock()

	var granted bool
	if hasActive {
		granted = s.mdLines.GrantDiscretionaryChurn(mktReqID)
	} else {
		granted = s.mdLines.GrantDiscretionaryNew(mktReqID)
	}
	if !granted {
		return
	}

	s.optChain.mu.Lock()
	_, pending := s.activeLegLocked(symbol, right)
	deltaSource := "matched"
	if fallback {
		deltaSource = "atm_fallback"
	}
	req := &optMktReq{
		groupID: groupID, symbol: symbol, right: right, strike: strike, expiry: expiry,
		busIdxs: busIdxs, pending: pending, deltaSource: deltaSource,
	}
	if pending {
		req.pendingSince = time.Now()
		s.optChain.mktReqs[mktReqID] = req
		s.replaceStalePendingLocked(symbol, right, mktReqID)
	} else {
		s.optChain.mktReqs[mktReqID] = req
		s.replaceMktReqLocked(symbol, right, mktReqID)
	}
	s.optChain.mu.Unlock()

	s.optionLog.Printf("Option: subscribing market data %s %s (group=%d) strike=%.2f expiry=%s (reqID=%d, pending=%v)",
		symbol, right, groupID, strike, expiry, mktReqID, pending)
	s.client.ReqMktData(mktReqID, contract, "", false, false, nil)

	if !pending {
		s.publishTo(busIdxs, eventbus.Event{
			Kind: eventbus.KindOptionData,
			Payload: eventbus.OptionData{
				Symbol: symbol, Right: right, Strike: strike, Expiry: expiry, DeltaSource: deltaSource,
			},
		})
	}
}

// releaseOrphanedProbeCandidate drops a deltaCands entry for a reqID the
// mdlines reaper just freed (ReapProbes) — its owning resolution never
// reached its own release path, so this stops a stray late tick from
// mutating a *deltaCandidate no code will ever read again. Safe to call for
// an unknown reqID (no-op); resolveDeltaCandidates, if it later runs anyway,
// still calls mdLines.Release on the same reqID, which is a no-op too.
func (s *Session) releaseOrphanedProbeCandidate(reqID int64) {
	s.optChain.mu.Lock()
	_, ok := s.optChain.deltaCands[reqID]
	delete(s.optChain.deltaCands, reqID)
	s.optChain.mu.Unlock()
	if ok {
		s.optionLog.Printf("Option: dropped orphaned delta candidate (reqID=%d) reaped by mdlines", reqID)
	}
}

// resolveDeltaCandidates picks the candidate with delta closest to the
// target, promotes it to a live mktReqs subscription, and cancels the
// rest. Only called from ResolveEntryStrike.
func (s *Session) resolveDeltaCandidates(groupID int, symbol, right string) (OptionQuote, bool) {
	resKey := retryKeyLeg(groupID, right)

	s.optChain.mu.Lock()
	res, ok := s.optChain.deltaRes[resKey]
	if !ok {
		s.optChain.mu.Unlock()
		return OptionQuote{}, false
	}
	delete(s.optChain.deltaRes, resKey)

	var best *deltaCandidate
	bestDist := math.MaxFloat64
	for _, c := range res.candidates {
		if !c.ready {
			continue
		}
		dist := math.Abs(math.Abs(c.delta) - res.targetDelta)
		if dist < bestDist {
			bestDist = dist
			best = c
		}
	}

	if best == nil {
		for _, c := range res.candidates {
			delete(s.optChain.deltaCands, c.reqID)
			s.mdLines.Release(c.reqID)
			s.client.CancelMktData(c.reqID)
		}
		iv := s.optChain.lastIV[symbol]
		allStrikes := res.allStrikes
		busIdxs := res.busIdxs
		targetDelta := res.targetDelta
		expiry := res.expiry
		s.optChain.mu.Unlock()

		if iv <= 0 {
			iv = defaultFallbackIV
		}
		undPrice := s.getUnderlyingPrice(symbol)
		estStrike := approximateStrikeForDelta(allStrikes, undPrice, iv, targetDelta, expiry, right)
		s.optionLog.Printf("Option delta resolve: %s %s (group=%d) — no deltas received, estimating strike=%.2f via target delta %.2f (iv=%.4f)",
			symbol, right, groupID, estStrike, targetDelta, iv)
		s.logDeltaMiss(symbol, right, targetDelta, undPrice, allStrikes, "no_deltas_received", estStrike, iv)
		if estStrike > 0 {
			s.subscribeOptionLeg(groupID, busIdxs, symbol, right, estStrike, expiry, true)
		}
		return OptionQuote{}, false
	}

	_, pending := s.activeLegLocked(best.symbol, best.right)
	winner := &optMktReq{
		groupID: groupID, symbol: best.symbol, right: best.right, strike: best.strike, expiry: best.expiry,
		delta: best.delta, bid: best.bid, ask: best.ask, busIdxs: res.busIdxs, pending: pending, deltaSource: "matched",
	}
	s.optChain.mktReqs[best.reqID] = winner
	s.mdLines.Reclassify(best.reqID, mdlines.CategoryDiscretionaryNew)
	if pending {
		winner.pendingSince = time.Now()
		s.replaceStalePendingLocked(best.symbol, best.right, best.reqID)
	} else {
		s.replaceMktReqLocked(best.symbol, best.right, best.reqID)
	}
	delete(s.optChain.deltaCands, best.reqID)

	for _, c := range res.candidates {
		if c.reqID == best.reqID {
			continue
		}
		delete(s.optChain.deltaCands, c.reqID)
		s.mdLines.Release(c.reqID)
		s.client.CancelMktData(c.reqID)
	}

	s.optChain.retries[retryKeyLeg(groupID, right)] = &optStrikeRetry{
		groupID: groupID, symbol: symbol, expiry: res.expiry, strikes: res.allStrikes, nextIdx: 1, pending: 1, busIdxs: res.busIdxs,
	}
	s.optChain.mu.Unlock()

	s.optionLog.Printf("Option delta resolved: %s %s (group=%d) target=%.2f → strike=%.2f (actual delta=%.4f, pending=%v)",
		symbol, right, groupID, res.targetDelta, best.strike, best.delta, pending)

	if !pending {
		s.publishTo(res.busIdxs, eventbus.Event{
			Kind: eventbus.KindOptionData,
			Payload: eventbus.OptionData{
				Symbol: best.symbol, Right: best.right, Strike: best.strike, Expiry: best.expiry,
				Delta: best.delta, DeltaSource: "matched",
			},
		})
	}

	// best.bid/best.ask arrived during this synchronous, bounded probe (within
	// entryDeltaProbeTimeout), so "now" is an accurate freshness stamp for them —
	// there is no separate per-tick timestamp cached on deltaCandidate to read back.
	now := time.Now()
	return OptionQuote{Strike: best.strike, Expiry: best.expiry, Bid: best.bid, Ask: best.ask, Delta: best.delta, BidTime: now, AskTime: now}, true
}

// groupForSymbolLocked returns the resolution group for symbol. Caller must
// hold s.optChain.mu.
func (s *Session) groupForSymbolLocked(symbol string) (optResGroup, bool) {
	for _, g := range s.optChain.rotation {
		if g.symbol == symbol {
			return g, true
		}
	}
	return optResGroup{}, false
}

// deltaCandidatesSettled is ResolveEntryStrike's poll-loop exit condition:
// true once EITHER every candidate has reported OR one already-ready
// candidate is within deltaGoodEnoughTolerance of targetDelta. Caller must
// hold s.optChain.mu.
func deltaCandidatesSettled(candidates []*deltaCandidate, targetDelta float64) bool {
	const deltaGoodEnoughTolerance = 0.02
	allReady := true
	for _, c := range candidates {
		if !c.ready {
			allReady = false
			continue
		}
		if math.Abs(math.Abs(c.delta)-targetDelta) <= deltaGoodEnoughTolerance {
			return true
		}
	}
	return allReady
}

// CurrentLeg returns the currently active leg's strike/delta/bid/ask for
// symbol+right as a single atomic snapshot — no new IB call. ok is true
// only when that leg already carries a live, IB-confirmed delta within
// deltaDriftTolerance of the resolution group's target_delta, and has a
// usable two-sided quote. This is the fast path a caller should check
// before paying for ResolveEntryStrike's multi-candidate probe.
func (s *Session) CurrentLeg(symbol, right string) (OptionQuote, bool) {
	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()

	active, hasActive := s.activeLegLocked(symbol, right)
	if !hasActive || active.deltaSource != "matched" || active.bid <= 0 || active.ask <= 0 {
		return OptionQuote{}, false
	}
	group, hasGroup := s.groupForSymbolLocked(symbol)
	if !hasGroup {
		return OptionQuote{}, false
	}
	target := group.targetDeltaCall
	if right == "put" {
		target = group.targetDeltaPut
	}
	if math.Abs(math.Abs(active.delta)-target) > deltaDriftTolerance {
		return OptionQuote{}, false
	}
	q := OptionQuote{Strike: active.strike, Expiry: active.expiry, Bid: active.bid, Ask: active.ask, Last: active.price, Delta: active.delta}
	// active (optMktReq) caches raw tick values but not when each side last
	// ticked; the Book is already written on every tick (handleOptionTick →
	// bookOption) for this exact contract, so read the timestamps back from
	// there rather than tracking a second, parallel copy.
	if s.book != nil {
		if bq, ok := s.book.Option(quotes.ContractKey{Symbol: symbol, Right: right, Strike: active.strike, Expiry: active.expiry}); ok {
			q.BidTime, q.AskTime = bq.BidTime, bq.AskTime
		}
	}
	return q, true
}

// ResolveEntryStrike runs a synchronous, bounded, real IB delta probe for
// symbol+right — intended to be called just before placing an option
// order, the one moment a real (not estimated) strike is worth the round
// trip. Reuses the most recently cached chain strikes/expiry rather than
// repeating the conId + chain-params lookup. Blocks the calling goroutine
// for up to timeout waiting on IB's TickOptionComputation.
func (s *Session) ResolveEntryStrike(symbol, right string, timeout time.Duration) (OptionQuote, bool) {
	s.optChain.mu.Lock()
	group, hasGroup := s.groupForSymbolLocked(symbol)
	info, hasInfo := s.optChain.lastChainInfo[symbol]
	_, inFlight := s.optChain.deltaRes[retryKeyLeg(group.groupID, right)]
	s.optChain.mu.Unlock()
	if !hasGroup || !hasInfo || len(info.strikes) == 0 {
		return OptionQuote{}, false
	}

	if q, ok := s.sharedResolvedEntry(group.groupID, symbol, right); ok {
		return q, true
	}

	// A sibling subscriber configured identically (same symbol, option_delay,
	// target_delta — the group key) is already probing this exact contract.
	// Don't duplicate the ReqMktData candidate probes and race it for scarce
	// mdlines probe-tier slots; wait for its result and share it instead. This
	// is what makes two robots that get the same crossover on the same
	// underlying converge on the identical strike (or identical failure)
	// rather than one silently losing the race and skipping the entry — see
	// the 2026-07-27 SPY put incident where VWmacdOptionDataRobot's larger
	// symbol universe made it more likely to lose exactly this race.
	if inFlight {
		return s.waitForEntryResolution(group.groupID, symbol, right, timeout)
	}

	targetDelta := group.targetDeltaCall
	if right == "put" {
		targetDelta = group.targetDeltaPut
	}
	if targetDelta >= 0.48 && targetDelta <= 0.52 {
		return OptionQuote{}, false
	}

	undPrice := s.getUnderlyingPrice(symbol)
	candidates := selectITMCandidates(info.strikes, undPrice, right, 5)
	if len(candidates) == 0 {
		return OptionQuote{}, false
	}

	groupID := group.groupID
	resKey := retryKeyLeg(groupID, right)

	s.optChain.mu.Lock()
	res := &deltaResolution{
		groupID: groupID, symbol: symbol, right: right, targetDelta: targetDelta,
		expiry: info.expiry, allStrikes: info.strikes, busIdxs: group.busIdxs,
	}
	for _, cs := range candidates {
		reqID := s.optChain.nextMktID
		s.optChain.nextMktID++
		evicted, ok := s.mdLines.GrantProbe(reqID)
		if !ok {
			continue
		}
		if evicted != 0 {
			delete(s.optChain.mktReqs, evicted)
			s.client.CancelMktData(evicted)
		}
		cand := &deltaCandidate{groupID: groupID, symbol: symbol, right: right, strike: cs, expiry: info.expiry, reqID: reqID, busIdxs: group.busIdxs}
		s.optChain.deltaCands[reqID] = cand
		res.candidates = append(res.candidates, cand)

		ibRight := "C"
		if right == "put" {
			ibRight = "P"
		}
		contract := makeOptionContract(symbol, ibRight, cs, info.expiry)
		s.optionLog.Printf("Option: entry delta candidate %s %s strike=%.2f expiry=%s (reqID=%d)", symbol, right, cs, info.expiry, reqID)
		s.client.ReqMktData(reqID, contract, "", false, false, nil)
	}
	if len(res.candidates) == 0 {
		s.optChain.mu.Unlock()
		return OptionQuote{}, false
	}
	s.optChain.deltaRes[resKey] = res
	s.optChain.mu.Unlock()

	const pollInterval = 100 * time.Millisecond
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s.optChain.mu.Lock()
		settled := deltaCandidatesSettled(res.candidates, targetDelta)
		s.optChain.mu.Unlock()
		if settled {
			break
		}
		time.Sleep(pollInterval)
	}
	q, matched := s.resolveDeltaCandidates(groupID, symbol, right)
	if matched && q.Valid() {
		s.optChain.mu.Lock()
		if s.optChain.resolvedEntry == nil {
			s.optChain.resolvedEntry = make(map[string]resolvedEntryLeg)
		}
		s.optChain.resolvedEntry[retryKeyLeg(groupID, right)] = resolvedEntryLeg{strike: q.Strike, expiry: q.Expiry, delta: q.Delta, at: time.Now()}
		s.optChain.mu.Unlock()
	}
	return q, matched
}

// sharedResolvedEntry returns a fresh, Book-priced quote for the contract
// another subscriber recently resolved for (groupID, right) — the
// shared-selection fast path for ResolveEntryStrike.
func (s *Session) sharedResolvedEntry(groupID int, symbol, right string) (OptionQuote, bool) {
	s.optChain.mu.Lock()
	leg, ok := s.optChain.resolvedEntry[retryKeyLeg(groupID, right)]
	s.optChain.mu.Unlock()
	if !ok || time.Since(leg.at) > resolvedEntryTTL || s.book == nil {
		return OptionQuote{}, false
	}
	bq, ok := s.book.Option(quotes.ContractKey{Symbol: symbol, Right: right, Strike: leg.strike, Expiry: leg.expiry})
	if !ok {
		return OptionQuote{}, false
	}
	q := OptionQuote{Strike: leg.strike, Expiry: leg.expiry, Bid: bq.Bid, Ask: bq.Ask, Last: bq.Last, Delta: leg.delta, BidTime: bq.BidTime, AskTime: bq.AskTime}
	if !q.Valid() {
		return OptionQuote{}, false
	}
	return q, true
}

// waitForEntryResolution is ResolveEntryStrike's path for a caller that found
// deltaRes[groupID|right] already owned by a concurrent sibling call. Rather
// than launching a duplicate set of candidate probes, it polls (same cadence
// the owning call's own loop uses) for that entry to clear — which happens
// the instant resolveDeltaCandidates finishes processing it, success or not —
// then reads the result via the same sharedResolvedEntry fast path the owner
// populates on success. If the owner's probe failed, resolvedEntry stays
// empty and this returns the same (OptionQuote{}, false) the owner got,
// rather than spending a second probe chasing the same answer.
func (s *Session) waitForEntryResolution(groupID int, symbol, right string, timeout time.Duration) (OptionQuote, bool) {
	resKey := retryKeyLeg(groupID, right)
	const pollInterval = 100 * time.Millisecond
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s.optChain.mu.Lock()
		_, stillOwned := s.optChain.deltaRes[resKey]
		s.optChain.mu.Unlock()
		if !stillOwned {
			break
		}
		time.Sleep(pollInterval)
	}
	return s.sharedResolvedEntry(groupID, symbol, right)
}

// subscribeOptionMarketData subscribes to streaming market data for both
// the CALL and PUT at the given strike and expiry, for one resolution group.
func (s *Session) subscribeOptionMarketData(groupID int, busIdxs []int, symbol string, strike float64, expiry string, fallback bool) {
	for _, right := range []string{"call", "put"} {
		s.subscribeOptionLeg(groupID, busIdxs, symbol, right, strike, expiry, fallback)
	}
}

// handleOptionMktError handles error 200 for an option market data
// subscription. When both legs (call and put) of a strike fail, it
// automatically retries with the next nearest SMART strike. Returns true if
// reqID was an option market data request, false otherwise.
func (s *Session) handleOptionMktError(reqID int64, errStr string) bool {
	s.optChain.mu.Lock()

	if cand, ok := s.optChain.deltaCands[reqID]; ok {
		delete(s.optChain.deltaCands, cand.reqID)
		s.mdLines.Release(cand.reqID)
		s.optChain.mu.Unlock()
		s.optionLog.Printf("Option delta candidate FAILED: %s %s strike=%.2f — %s", cand.symbol, cand.right, cand.strike, errStr)
		return true
	}

	if sub, ok := s.optChain.posSubs[reqID]; ok {
		key := fmt.Sprintf("%s|%s|%.0f", sub.symbol, sub.right, sub.strike)
		delete(s.optChain.posSubs, reqID)
		delete(s.optChain.posSubKeys, key)
		s.mdLines.Release(reqID)
		s.optChain.mu.Unlock()
		s.logger.Printf("Position-pinned option FAILED: %s %s strike=%.2f — %s", sub.symbol, sub.right, sub.strike, errStr)
		return true
	}

	req, ok := s.optChain.mktReqs[reqID]
	if !ok {
		s.optChain.mu.Unlock()
		return false
	}
	groupID := req.groupID
	symbol := req.symbol
	right := req.right
	strike := req.strike
	expiry := req.expiry
	delete(s.optChain.mktReqs, reqID)
	s.mdLines.Release(reqID)

	s.optionLog.Printf("Option market data FAILED: %s %s (group=%d) strike=%.2f expiry=%s (reqID=%d) — contract not found, skipping",
		symbol, right, groupID, strike, expiry, reqID)
	s.logger.Printf("Option market data FAILED: %s %s strike=%.2f expiry=%s — %s", symbol, right, strike, expiry, errStr)

	retryKey := retryKeyATM(groupID)
	retry, hasRetry := s.optChain.retries[retryKey]
	if !hasRetry {
		s.optChain.mu.Unlock()
		return true
	}
	retry.pending--
	if retry.pending > 0 {
		s.optChain.mu.Unlock()
		return true
	}

	if retry.nextIdx >= len(retry.strikes) {
		delete(s.optChain.retries, retryKey)
		s.optChain.mu.Unlock()
		s.logger.Printf("Option: %s — all candidate strikes exhausted for expiry=%s, giving up", symbol, expiry)
		return true
	}
	nextStrike := retry.strikes[retry.nextIdx]
	retry.nextIdx++
	retry.pending = 2
	busIdxs := retry.busIdxs
	s.optChain.mu.Unlock()

	s.optionLog.Printf("Option: %s (group=%d) — retrying with next nearest strike=%.2f expiry=%s", symbol, groupID, nextStrike, expiry)
	s.subscribeOptionMarketData(groupID, busIdxs, symbol, nextStrike, expiry, false)
	return true
}

// handleOptionTick updates the cached price/bid/ask for an option market
// data reqID and publishes KindOptionData (ATM) or KindPositionOptionData
// (position-pinned strike) to the owning subscriber bus(es). Returns true
// if reqID was an option market data request, false otherwise.
func (s *Session) handleOptionTick(reqID int64, tickType int64, price float64) bool {
	s.optChain.mu.Lock()

	if req, ok := s.optChain.mktReqs[reqID]; ok {
		switch tickType {
		case ibapi.BID, ibapi.DELAYED_BID:
			req.bid = price
		case ibapi.ASK, ibapi.DELAYED_ASK:
			req.ask = price
		case ibapi.LAST, ibapi.DELAYED_LAST, ibapi.CLOSE, ibapi.DELAYED_CLOSE:
			req.price = price
		default:
			s.optChain.mu.Unlock()
			return true
		}
		if !s.promoteIfReadyLocked(req, reqID) {
			s.optChain.mu.Unlock()
			return true
		}
		od := eventbus.OptionData{
			Symbol: req.symbol, Right: req.right, Strike: req.strike, Expiry: req.expiry,
			Price: req.price, Bid: req.bid, Ask: req.ask, Delta: req.delta, DeltaSource: req.deltaSource,
		}
		busIdxs := req.busIdxs
		s.optChain.mu.Unlock()
		s.bookOption(od)
		s.publishTo(busIdxs, eventbus.Event{Kind: eventbus.KindOptionData, Payload: od})
		return true
	}

	if sub, ok := s.optChain.posSubs[reqID]; ok {
		switch tickType {
		case ibapi.BID, ibapi.DELAYED_BID:
			sub.bid = price
		case ibapi.ASK, ibapi.DELAYED_ASK:
			sub.ask = price
		case ibapi.LAST, ibapi.DELAYED_LAST, ibapi.CLOSE, ibapi.DELAYED_CLOSE:
			sub.price = price
		default:
			s.optChain.mu.Unlock()
			return true
		}
		od := eventbus.OptionData{
			Symbol: sub.symbol, Right: sub.right, Strike: sub.strike, Expiry: sub.expiry,
			Price: sub.price, Bid: sub.bid, Ask: sub.ask, Delta: sub.delta, DeltaSource: sub.deltaSource,
		}
		s.optChain.mu.Unlock()
		s.bookOption(od)
		s.publish(eventbus.Event{Kind: eventbus.KindPositionOptionData, Payload: od})
		return true
	}

	if cand, ok := s.optChain.deltaCands[reqID]; ok {
		switch tickType {
		case ibapi.BID, ibapi.DELAYED_BID:
			cand.bid = price
		case ibapi.ASK, ibapi.DELAYED_ASK:
			cand.ask = price
		}
		s.optChain.mu.Unlock()
		return true
	}

	s.optChain.mu.Unlock()
	return false
}

// resolvedOption is the currently-subscribed ATM option leg for one symbol+right.
type resolvedOption struct {
	contract *ibapi.Contract
	strike   float64
	expiry   string
	bid      float64
	ask      float64
	mid      float64
}

// currentOptionContract returns the live option leg subscribed for symbol +
// right ("call"/"put") for the requesting subscriber (busIdx), or false if
// the chain has not resolved a contract yet.
func (s *Session) currentOptionContract(symbol, right string, busIdx int) (resolvedOption, bool) {
	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()
	var fallback *optMktReq
	for _, req := range s.optChain.mktReqs {
		if req.symbol != symbol || req.right != right {
			continue
		}
		if busIdx >= 0 && !slices.Contains(req.busIdxs, busIdx) {
			continue
		}
		if req.pending {
			if fallback == nil {
				fallback = req
			}
			continue
		}
		return resolveOptionLeg(symbol, right, req), true
	}
	if fallback != nil {
		return resolveOptionLeg(symbol, right, fallback), true
	}
	return resolvedOption{}, false
}

// resolveOptionLeg builds a resolvedOption from an mktReqs leg.
func resolveOptionLeg(symbol, right string, req *optMktReq) resolvedOption {
	ibRight := "C"
	if right == "put" {
		ibRight = "P"
	}
	mid := req.price
	switch {
	case req.bid > 0 && req.ask > 0:
		mid = (req.bid + req.ask) / 2
	case req.ask > 0:
		mid = req.ask
	case req.bid > 0:
		mid = req.bid
	}
	return resolvedOption{
		contract: makeOptionContract(symbol, ibRight, req.strike, req.expiry),
		strike:   req.strike, expiry: req.expiry, bid: req.bid, ask: req.ask, mid: mid,
	}
}

// ── Helper functions ──────────────────────────────────────────────────────

// defaultFallbackIV is used by approximateStrikeForDelta only when no live
// IV sample exists at all for a symbol — a rare edge case limited to the
// very first resolution of a session before any option tick has arrived.
const defaultFallbackIV = 0.25

// deltaDriftTolerance bounds how far a delta-target leg's live,
// IB-confirmed delta may wander from its target_delta before the
// background rotation bothers re-estimating and resubscribing a closer strike.
const deltaDriftTolerance = 0.05

// approximateStrikeForDelta estimates the whole-dollar strike (from the
// candidates actually available in the chain) whose Black-Scholes delta is
// closest to targetDelta, for use when a live IB delta probe returns no
// usable quotes.
//
// Derivation: for a call, delta = N(d1); for a put, delta = N(d1) - 1 (so a
// put magnitude of targetDelta corresponds to N(d1) = 1 - targetDelta).
// Solving the standard d1 = (ln(S/K) + 0.5·σ²·T) / (σ√T) for K given a
// target d1 yields K = S / exp(d1·σ√T − 0.5·σ²·T).
func approximateStrikeForDelta(strikes []float64, undPrice, iv, targetDelta float64, expiry string, right string) float64 {
	if len(strikes) == 0 {
		return 0
	}
	if iv <= 0 || undPrice <= 0 || targetDelta <= 0 || targetDelta >= 1 {
		return strikes[0]
	}
	t := yearsToExpiry(expiry)
	if t <= 0 {
		return strikes[0]
	}

	var d1 float64
	if right == "call" {
		d1 = invNormCDF(targetDelta)
	} else {
		d1 = invNormCDF(1 - targetDelta)
	}

	sqrtT := math.Sqrt(t)
	exponent := d1*iv*sqrtT - 0.5*iv*iv*t
	target := undPrice / math.Exp(exponent)

	best := strikes[0]
	bestDist := math.Abs(strikes[0] - target)
	for _, st := range strikes {
		if d := math.Abs(st - target); d < bestDist {
			bestDist = d
			best = st
		}
	}
	return best
}

// yearsToExpiry converts an option's "YYYYMMDD" expiry into a year fraction
// from now, floored at 30 minutes so a same-day (0DTE) expiry never divides
// by (near-)zero. Treats expiry as 16:00 local time (US market close).
func yearsToExpiry(expiry string) float64 {
	exp, err := time.ParseInLocation("20060102", expiry, time.Local)
	if err != nil {
		return 0
	}
	closeTime := time.Date(exp.Year(), exp.Month(), exp.Day(), 16, 0, 0, 0, time.Local)
	d := time.Until(closeTime)
	const minDuration = 30 * time.Minute
	if d < minDuration {
		d = minDuration
	}
	const hoursPerYear = 24 * 365.25
	return d.Hours() / hoursPerYear
}

// invNormCDF returns the inverse of the standard normal CDF (the probit
// function) via Peter Acklam's rational approximation — accurate to about
// 1.15e-9 relative error. p must be in (0, 1).
func invNormCDF(p float64) float64 {
	if p <= 0 {
		p = 1e-10
	} else if p >= 1 {
		p = 1 - 1e-10
	}

	const (
		a1 = -3.969683028665376e+01
		a2 = 2.209460984245205e+02
		a3 = -2.759285104469687e+02
		a4 = 1.383577518672690e+02
		a5 = -3.066479806614716e+01
		a6 = 2.506628277459239e+00

		b1 = -5.447609879822406e+01
		b2 = 1.615858368580409e+02
		b3 = -1.556989798598866e+02
		b4 = 6.680131188771972e+01
		b5 = -1.328068155288572e+01

		c1 = -7.784894002430293e-03
		c2 = -3.223964580411365e-01
		c3 = -2.400758277161838e+00
		c4 = -2.549732539343734e+00
		c5 = 4.374664141464968e+00
		c6 = 2.938163982698783e+00

		d1 = 7.784695709041462e-03
		d2 = 3.224671290700398e-01
		d3 = 2.445134137142996e+00
		d4 = 3.754408661907416e+00

		pLow  = 0.02425
		pHigh = 1 - pLow
	)

	switch {
	case p < pLow:
		q := math.Sqrt(-2 * math.Log(p))
		return (((((c1*q+c2)*q+c3)*q+c4)*q+c5)*q + c6) /
			((((d1*q+d2)*q+d3)*q+d4)*q + 1)
	case p <= pHigh:
		q := p - 0.5
		r := q * q
		return (((((a1*r+a2)*r+a3)*r+a4)*r+a5)*r + a6) * q /
			(((((b1*r+b2)*r+b3)*r+b4)*r+b5)*r + 1)
	default:
		q := math.Sqrt(-2 * math.Log(1-p))
		return -(((((c1*q+c2)*q+c3)*q+c4)*q+c5)*q + c6) /
			((((d1*q+d2)*q+d3)*q+d4)*q + 1)
	}
}

// ── Delta-miss logging ────────────────────────────────────────────────────

var deltaMissMu sync.Mutex
var deltaMissFile *os.File
var deltaMissWriter *csv.Writer
var deltaMissDate string

var deltaMissHeader = []string{
	"timestamp", "symbol", "right", "target_delta", "underlying_price",
	"candidates_probed", "reason", "fallback_strike", "estimated_via_formula", "iv_used",
}

// logDeltaMiss appends one row for a delta-probe fallback event to a
// day-rotated CSV under "log/" (created if needed) so the frequency of
// delta-probe fallbacks is a simple CSV load away, rather than a manual
// option-log scrape.
func (s *Session) logDeltaMiss(symbol, right string, targetDelta, undPrice float64, candidates []float64, reason string, estimatedStrike, ivUsed float64) {
	deltaMissMu.Lock()
	defer deltaMissMu.Unlock()

	today := time.Now().Format("2006-01-02")
	if deltaMissWriter == nil || deltaMissDate != today {
		if deltaMissFile != nil {
			deltaMissWriter.Flush()
			deltaMissFile.Close()
		}
		dir := "log"
		if err := os.MkdirAll(dir, 0o755); err != nil {
			s.logger.Printf("delta-misses: cannot create log directory: %v", err)
			return
		}
		path := filepath.Join(dir, today+"-delta-misses.csv")
		writeHeader := true
		if info, err := os.Stat(path); err == nil && info.Size() > 0 {
			writeHeader = false
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			s.logger.Printf("delta-misses: cannot open %s: %v", path, err)
			return
		}
		deltaMissFile = f
		deltaMissWriter = csv.NewWriter(f)
		deltaMissDate = today
		if writeHeader {
			_ = deltaMissWriter.Write(deltaMissHeader)
		}
	}

	strikeStrs := make([]string, len(candidates))
	for i, st := range candidates {
		strikeStrs[i] = strconv.FormatFloat(st, 'f', 2, 64)
	}
	estimatedViaFormula := "true"
	if ivUsed <= 0 {
		estimatedViaFormula = "false"
	}
	row := []string{
		time.Now().Format("2006-01-02 15:04:05"), symbol, right,
		strconv.FormatFloat(targetDelta, 'f', 4, 64), strconv.FormatFloat(undPrice, 'f', 2, 64),
		strings.Join(strikeStrs, "|"), reason, strconv.FormatFloat(estimatedStrike, 'f', 2, 64),
		estimatedViaFormula, strconv.FormatFloat(ivUsed, 'f', 4, 64),
	}
	if err := deltaMissWriter.Write(row); err != nil {
		s.logger.Printf("delta-misses: write failed: %v", err)
		return
	}
	deltaMissWriter.Flush()
}

// wholeDollarStrikes returns only the strikes with no fractional dollar
// part (75, 76 — never 75.50). If the underlying lists no whole-dollar
// strikes at all, the original slice is returned unchanged.
func wholeDollarStrikes(strikes []float64) []float64 {
	whole := make([]float64, 0, len(strikes))
	for _, st := range strikes {
		if st == math.Trunc(st) {
			whole = append(whole, st)
		}
	}
	if len(whole) == 0 {
		return strikes
	}
	return whole
}

func nearestExpiry(expirations []string, delayDays int) string {
	target := time.Now().AddDate(0, 0, delayDays).Format("20060102")
	sorted := make([]string, len(expirations))
	copy(sorted, expirations)
	sort.Strings(sorted)
	for _, e := range sorted {
		if e >= target {
			return e
		}
	}
	return ""
}

// makeOptionContract builds an ibapi.Contract for the given option parameters.
func makeOptionContract(symbol, right string, strike float64, expiry string) *ibapi.Contract {
	c := ibapi.NewContract()
	c.Symbol = symbol
	c.SecType = "OPT"
	c.Currency = "USD"
	c.Exchange = "SMART"
	c.Right = right
	c.Strike = strike
	c.LastTradeDateOrContractMonth = expiry
	c.Multiplier = "100"
	return c
}

// SubscribePositionStrike subscribes to IB market data for a specific
// option strike pinned to an open position. These subscriptions are
// independent of the ATM strike rotation, so a held position keeps its own
// feed even as the ATM leg re-resolves to a different strike. No-op if
// already subscribed for this symbol+right+strike combination.
func (s *Session) SubscribePositionStrike(symbol, right string, strike float64, expiry string) {
	key := fmt.Sprintf("%s|%s|%.0f", symbol, right, strike)

	s.optChain.mu.Lock()
	if _, exists := s.optChain.posSubKeys[key]; exists {
		s.optChain.mu.Unlock()
		return
	}
	reqID := s.optChain.nextPosID
	s.optChain.nextPosID++
	sub := &posStrikeSub{symbol: symbol, right: right, strike: strike, expiry: expiry, reqID: reqID}
	for _, req := range s.optChain.mktReqs {
		if req.symbol == symbol && req.right == right && req.strike == strike {
			sub.deltaSource = req.deltaSource
			sub.delta = req.delta
			break
		}
	}
	s.optChain.posSubs[reqID] = sub
	s.optChain.posSubKeys[key] = reqID
	s.optChain.mu.Unlock()

	s.mdLines.GrantGuaranteed(reqID, mdlines.CategoryPosition)

	ibRight := "C"
	if right == "put" {
		ibRight = "P"
	}
	contract := makeOptionContract(symbol, ibRight, strike, expiry)
	s.optionLog.Printf("Option: subscribing POSITION-PINNED %s %s strike=%.2f expiry=%s (reqID=%d)", symbol, right, strike, expiry, reqID)
	s.client.ReqMktData(reqID, contract, "", false, false, nil)
}

// UnsubscribePositionStrike cancels a position-pinned option subscription.
func (s *Session) UnsubscribePositionStrike(symbol, right string, strike float64) {
	key := fmt.Sprintf("%s|%s|%.0f", symbol, right, strike)

	s.optChain.mu.Lock()
	reqID, ok := s.optChain.posSubKeys[key]
	if !ok {
		s.optChain.mu.Unlock()
		return
	}
	delete(s.optChain.posSubs, reqID)
	delete(s.optChain.posSubKeys, key)
	s.optChain.mu.Unlock()

	s.mdLines.Release(reqID)
	s.optionLog.Printf("Option: unsubscribing POSITION-PINNED %s %s strike=%.2f (reqID=%d)", symbol, right, strike, reqID)
	s.client.CancelMktData(reqID)
}

// getUnderlyingPrice returns the current price for a symbol using bid/ask
// midpoint from the streaming cache, or the shared quote book as fallback.
// Returns 0 if unknown.
func (s *Session) getUnderlyingPrice(symbol string) float64 {
	s.mktData.bidAskMu.RLock()
	bid, hasBid := s.mktData.bid[symbol]
	ask, hasAsk := s.mktData.ask[symbol]
	s.mktData.bidAskMu.RUnlock()

	if hasBid && hasAsk && bid.Price > 0 && ask.Price > 0 {
		return (bid.Price + ask.Price) / 2
	}
	if hasBid && bid.Price > 0 {
		return bid.Price
	}
	if hasAsk && ask.Price > 0 {
		return ask.Price
	}

	if s.book != nil {
		if q, ok := s.book.Stock(symbol); ok && q.Last > 0 {
			return q.Last
		}
	}
	return 0
}

package ibkr

import (
	"time"

	"github.com/scmhub/ibapi"
)

// withOfflineClient installs a real-but-unconnected EClient, so paths that
// cancel a market-data subscription can run to completion. Cancels on an
// unconnected client are no-ops; a nil client panics on the first field
// access, which is what makes "this path never reaches the broker" a testable
// property elsewhere.
func withOfflineClient(s *Session) *Session {
	s.client = ibapi.NewEClient(&ibapi.Wrapper{})
	return s
}

// Shared builders for the option-registry tests. Every one of them writes the
// same state the production paths write, so a test never encodes a shape the
// real code cannot produce.

// selKey is the terse legKey constructor the tests use.
func lk(symbol, right string, strike float64, expiry string) legKey {
	return legKey{symbol: symbol, right: right, strike: strike, expiry: expiry}
}

// legOpts tunes seedLeg. The zero value is a leg nobody holds and that has
// never ticked.
type legOpts struct {
	reqID        int64
	selectors    []int // selectors DISPLAYING this contract
	warming      []int // selectors warming into it (pendingSwap)
	pins         int
	bid, ask     float64
	price, delta float64
	deltaSource  string
	subscribedAt time.Time
	lastTickAt   time.Time
	pendingSince time.Time
}

// seedLeg installs a leg in the registry with the given holders and quote.
func seedLeg(s *Session, key legKey, o legOpts) *optLeg {
	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()

	src := o.deltaSource
	if src == "" {
		src = "matched"
	}
	at := o.subscribedAt
	if at.IsZero() {
		at = time.Now()
	}
	leg := s.openLegLocked(key, o.reqID, src, at)
	leg.bid, leg.ask, leg.price, leg.delta = o.bid, o.ask, o.price, o.delta
	leg.lastTickAt = o.lastTickAt
	leg.pins = o.pins
	for _, id := range o.selectors {
		leg.selectors[id] = struct{}{}
		s.optChain.selCurrent[id] = key
	}
	since := o.pendingSince
	if since.IsZero() {
		since = time.Now()
	}
	for _, id := range o.warming {
		leg.selectors[id] = struct{}{}
		s.optChain.selPending[id] = pendingSwap{to: key, since: since}
	}
	return leg
}

// seedSelector adds one selector to the rotation.
func seedSelector(s *Session, sel selector) selector {
	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()
	s.optChain.rotation = append(s.optChain.rotation, sel)
	return sel
}

// seedDisplayedLeg installs a selector for (symbol, right) together with the
// contract it displays — the state a completed rotation pass leaves behind, and
// what currentOptionContract reads.
func seedDisplayedLeg(s *Session, symbol, right string, strike float64, expiry string, bid, ask float64) selector {
	s.optChain.mu.Lock()
	id := len(s.optChain.rotation) + 1
	reqID := int64(1000 + len(s.optChain.legs))
	s.optChain.mu.Unlock()

	sel := seedSelector(s, selector{id: id, symbol: symbol, right: right, targetDelta: 0.60, busIdxs: []int{0}})
	seedLeg(s, lk(symbol, right, strike, expiry), legOpts{
		reqID: reqID, selectors: []int{sel.id}, bid: bid, ask: ask,
	})
	return sel
}

// legCount returns how many contracts are subscribed.
func legCount(s *Session) int {
	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()
	return len(s.optChain.legs)
}

// displayedKey returns the contract a selector is currently showing.
func displayedKey(s *Session, selectorID int) (legKey, bool) {
	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()
	k, ok := s.optChain.selCurrent[selectorID]
	return k, ok
}

// legHolders returns a leg's selector ids and pin count.
func legHolders(s *Session, key legKey) (sels []int, pins int, ok bool) {
	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()
	leg, found := s.optChain.legs[key]
	if !found {
		return nil, 0, false
	}
	for id := range leg.selectors {
		sels = append(sels, id)
	}
	return sels, leg.pins, true
}

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
//
// It used to also carry `selectors` and `warming` — the watchlist rows
// displaying a contract, and those warming into it. A leg has exactly one
// holder kind now: open positions, counted by pins.
type legOpts struct {
	reqID        int64
	pins         int
	bid, ask     float64
	price, delta float64
	deltaSource  string
	subscribedAt time.Time
	lastTickAt   time.Time
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
	return leg
}

// seedSelector adds one selector to the rotation.
func seedSelector(s *Session, sel selector) selector {
	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()
	s.optChain.rotation = append(s.optChain.rotation, sel)
	return sel
}

// legCount returns how many contracts are subscribed.
func legCount(s *Session) int {
	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()
	return len(s.optChain.legs)
}

// legPins returns a leg's pin count.
func legPins(s *Session, key legKey) (pins int, ok bool) {
	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()
	leg, found := s.optChain.legs[key]
	if !found {
		return 0, false
	}
	return leg.pins, true
}

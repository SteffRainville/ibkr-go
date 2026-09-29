// Tests for the chain loader (chains.go): conId once per symbol, chain params
// and the expiry listing once per trading day, strikes taken only from the
// listing, failures retried after a backoff.
package ibkr

import (
	"slices"
	"testing"
	"time"

	"github.com/scmhub/ibapi"
)

var spyKey = chainKey{symbol: "SPY", optionDelay: 0}

// newChainTestSession has one SPY put selector and an offline client, so
// requests are "sent" into the void and the test plays IB's answers.
func newChainTestSession() *Session {
	s := withOfflineClient(NewSession(Options{}, nil, nil))
	s.optChain.selectors = []selector{{id: 1, symbol: "SPY", right: "put", currency: "USD", targetDelta: 0.40}}
	return s
}

func chainOf(s *Session, key chainKey) *chain {
	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()
	return s.optChain.chains[key]
}

func stkDetails(conID int64) *ibapi.ContractDetails {
	cd := ibapi.NewContractDetails()
	cd.Contract = ibapi.Contract{Symbol: "SPY", SecType: "STK", ConID: conID}
	return cd
}

func optDetails(right string, strike float64, expiry string) *ibapi.ContractDetails {
	cd := ibapi.NewContractDetails()
	cd.Contract = ibapi.Contract{Symbol: "SPY", SecType: "OPT", Right: right, Strike: strike, LastTradeDateOrContractMonth: expiry}
	return cd
}

// playFullFetch answers every stage of a fresh fetch and returns the expiry.
func playFullFetch(t *testing.T, s *Session) string {
	t.Helper()
	day := func(n int) string { return time.Now().AddDate(0, 0, n).Format("20060102") }
	expiry := day(0)

	c := chainOf(s, spyKey)
	if c == nil || c.stage != stageConID {
		t.Fatalf("fetch did not start with a conId lookup: %+v", c)
	}
	s.ContractDetails(c.reqID, stkDetails(756733))
	s.ContractDetailsEnd(c.reqID)

	if c.stage != stageParams {
		t.Fatalf("stage after conId = %s, want chain params", c.stage)
	}
	// A phantom class with an earlier expiry must lose to the symbol's own:
	// pairing one class's expiry with another's strikes is the 2026-08-17 MSFT
	// incident (error 200 on every strike, settled on an untradable delta-1.00 leg).
	s.SecurityDefinitionOptionParameter(c.reqID, "SMART", 756733, "SPY9", "100", []string{day(-1), day(7)}, []float64{1, 2})
	s.SecurityDefinitionOptionParameter(c.reqID, "SMART", 756733, "SPY", "100", []string{expiry, day(1)}, []float64{730, 732.5, 735, 737.5, 740})
	s.SecurityDefinitionOptionParameter(c.reqID, "CBOE", 756733, "SPY", "100", []string{day(-5)}, []float64{9})
	s.SecurityDefinitionOptionParameterEnd(c.reqID)

	if c.stage != stageListing || c.expiry != expiry {
		t.Fatalf("after params: stage=%s expiry=%s, want expiry listing of %s", c.stage, c.expiry, expiry)
	}
	// Today's expiry lists no half-dollar puts; 732.50 exists only as a call.
	for _, st := range []float64{730, 735, 740} {
		s.ContractDetails(c.reqID, optDetails("P", st, expiry))
	}
	for _, st := range []float64{730, 732.5, 735, 740} {
		s.ContractDetails(c.reqID, optDetails("C", st, expiry))
	}
	s.ContractDetails(c.reqID, optDetails("P", 737.5, day(1))) // another expiry: ignored
	s.ContractDetailsEnd(c.reqID)
	return expiry
}

func TestChainLoader_FullFetchListsStrikesPerExpiry(t *testing.T) {
	s := newChainTestSession()
	s.startChainFetches()
	expiry := playFullFetch(t, s)

	c := chainOf(s, spyKey)
	if !c.readyOn(tradingDay(time.Now())) || c.stage != stageIdle {
		t.Fatalf("chain not ready after the listing: %+v", c)
	}
	if got, want := c.strikes["put"], []float64{730, 735, 740}; !slices.Equal(got, want) {
		t.Errorf("put strikes = %v, want %v — only what the expiry lists", got, want)
	}
	if got, want := c.strikes["call"], []float64{730, 732.5, 735, 740}; !slices.Equal(got, want) {
		t.Errorf("call strikes = %v, want %v", got, want)
	}
	if s.optChain.conIDs["SPY"] != 756733 {
		t.Errorf("conId not cached: %v", s.optChain.conIDs)
	}
	if len(s.optChain.chainByReq) != 0 {
		t.Errorf("request index not emptied: %v", s.optChain.chainByReq)
	}

	s.optChain.mu.Lock()
	gotExp, strikes, ok := s.chainStrikesLocked(s.optChain.selectors[0])
	s.optChain.mu.Unlock()
	if !ok || gotExp != expiry || !slices.Equal(strikes, []float64{730, 735, 740}) {
		t.Errorf("chainStrikesLocked = (%s, %v, %v)", gotExp, strikes, ok)
	}
}

// Loaded once per day: later ticks cost nothing; a new day refetches, and
// skips the conId lookup because a conId never changes.
func TestChainLoader_OncePerDayAndConIDCached(t *testing.T) {
	s := newChainTestSession()
	s.startChainFetches()
	playFullFetch(t, s)
	c := chainOf(s, spyKey)

	s.loadChains()
	if c.stage != stageIdle {
		t.Fatalf("a chain loaded today was fetched again (stage %s)", c.stage)
	}

	c.day = time.Now().AddDate(0, 0, -1).Format("20060102")
	s.loadChains()
	if c.stage != stageParams {
		t.Fatalf("new day: stage = %s, want chain params straight away (conId cached)", c.stage)
	}
	s.optChain.mu.Lock()
	_, _, ok := s.chainStrikesLocked(s.optChain.selectors[0])
	s.optChain.mu.Unlock()
	if ok {
		t.Error("yesterday's strikes were served while today's chain loads")
	}
}

func TestChainLoader_FailuresRetryAfterBackoff(t *testing.T) {
	cases := map[string]func(s *Session, c *chain){
		"IB error": func(s *Session, c *chain) {
			if !s.handleOptionMktError(c.reqID, "No security definition has been found for the request") {
				t.Error("chain request error not handled")
			}
		},
		"no conId": func(s *Session, c *chain) { s.ContractDetailsEnd(c.reqID) },
		"stage timeout": func(s *Session, c *chain) {
			c.startedAt = time.Now().Add(-2 * chainStageTimeout)
			s.expireChainFetches()
		},
	}
	for name, fail := range cases {
		t.Run(name, func(t *testing.T) {
			s := newChainTestSession()
			s.startChainFetches()
			c := chainOf(s, spyKey)
			fail(s, c)

			if c.stage != stageIdle || c.failedAt.IsZero() || len(s.optChain.chainByReq) != 0 {
				t.Fatalf("failure not recorded: %+v", c)
			}
			s.startChainFetches()
			if c.stage != stageIdle {
				t.Fatal("retried inside the backoff")
			}
			c.failedAt = time.Now().Add(-chainRetryBackoff - time.Second)
			s.startChainFetches()
			if c.stage == stageIdle {
				t.Fatal("not retried after the backoff")
			}
		})
	}
}

// A listing with no contracts is a failure, not an empty chain.
func TestChainLoader_EmptyListingIsNotReady(t *testing.T) {
	s := newChainTestSession()
	s.optChain.conIDs["SPY"] = 756733
	s.startChainFetches()
	c := chainOf(s, spyKey)
	s.SecurityDefinitionOptionParameter(c.reqID, "SMART", 756733, "SPY", "100", []string{tradingDay(time.Now())}, []float64{735})
	s.SecurityDefinitionOptionParameterEnd(c.reqID)
	s.ContractDetailsEnd(c.reqID)

	if c.readyOn(tradingDay(time.Now())) || c.failedAt.IsZero() {
		t.Fatalf("empty listing produced a ready chain: %+v", c)
	}
}

// A callback for a request the chain has moved past is ignored.
func TestChainLoader_StaleReplyIgnored(t *testing.T) {
	s := newChainTestSession()
	s.startChainFetches()
	c := chainOf(s, spyKey)
	old := c.reqID
	s.ContractDetails(old, stkDetails(756733))
	s.ContractDetailsEnd(old)
	if s.handleChainContractDetailsEnd(old) {
		t.Error("a reply to a finished stage was routed to the chain")
	}
}

func TestChainLoader_CapsConcurrentFetches(t *testing.T) {
	s := newChainTestSession()
	s.optChain.selectors = nil
	for i, sym := range []string{"A", "B", "C", "D", "E", "F"} {
		s.optChain.selectors = append(s.optChain.selectors, selector{id: i, symbol: sym, right: "call", targetDelta: 0.4})
	}
	s.startChainFetches()
	inFlight := 0
	for _, c := range s.optChain.chains {
		if c.stage != stageIdle {
			inFlight++
		}
	}
	if inFlight != maxChainFetches {
		t.Fatalf("%d fetches in flight, want %d", inFlight, maxChainFetches)
	}
}

// A chain whose last selector left the watchlist is dropped; a new connection
// forgets in-flight requests but keeps loaded chains.
func TestChainLoader_DropsUnneededAndResetsOnReconnect(t *testing.T) {
	s := newChainTestSession()
	s.optChain.selectors = append(s.optChain.selectors, selector{id: 2, symbol: "QQQ", right: "call", targetDelta: 0.4})
	s.startChainFetches()
	playFullFetch(t, s)
	qqq := chainOf(s, chainKey{symbol: "QQQ"})
	if qqq == nil || qqq.stage == stageIdle {
		t.Fatalf("QQQ fetch not started: %+v", qqq)
	}

	s.resetChainFetches()
	if qqq.stage != stageIdle || !qqq.failedAt.IsZero() || len(s.optChain.chainByReq) != 0 {
		t.Fatalf("reconnect did not forget the in-flight fetch cleanly: %+v", qqq)
	}
	if !chainOf(s, spyKey).readyOn(tradingDay(time.Now())) {
		t.Fatal("reconnect discarded a loaded chain")
	}

	s.optChain.selectors = s.optChain.selectors[:1]
	s.startChainFetches()
	if chainOf(s, chainKey{symbol: "QQQ"}) != nil {
		t.Fatal("chain kept after its last selector left")
	}
}

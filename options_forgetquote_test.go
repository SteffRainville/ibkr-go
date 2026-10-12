// The Book must not outlive the line that fed it. 2026-10-12: a MU 990 call's
// recording line was evicted at 09:46 with bid 8.40 / ask 8.55 / last 8.48 in the
// Book; at 10:14 a position was entered on the same contract at 3.86 and pinned.
// Until its first real ticks arrived the Book served the 28-minute-old triple
// beside the fresh bid — the position read +118% for a few ticks.
package ibkr

import (
	"testing"

	"github.com/SteffRainville/ibkr-go/eventbus"
	"github.com/SteffRainville/ibkr-go/quotes"
)

func muCall990() legKey { return lk("MU", "call", 990, "20261012") }

func bookStaleQuote(s *Session, key legKey) {
	s.bookOption(eventbus.OptionData{
		Symbol: key.symbol, Right: key.right, Strike: key.strike, Expiry: key.expiry,
		Bid: 8.40, Ask: 8.55, Price: 8.48, Delta: 0.5472,
	})
}

func TestEvictedRecordingLine_ClearsItsBookQuote(t *testing.T) {
	s := withOfflineClient(NewSession(Options{TradingAccount: "DU12345"}, quotes.NewBook(), nil))
	key := muCall990()
	seedLeg(s, key, legOpts{reqID: 2265}) // recording-only: no pins
	s.optChain.mu.Lock()
	s.optChain.legs[key].tails = 1
	s.optChain.mu.Unlock()
	bookStaleQuote(s, key)
	if _, ok := bookQuote(t, s, key); !ok {
		t.Fatal("test premise: the quote must be in the Book")
	}

	s.evictRecordingLines([]int64{2265})

	if q, ok := bookQuote(t, s, key); ok {
		t.Fatalf("evicted line left its quote in the Book: %+v", q)
	}
}

func TestLastHolderRelease_ClearsBookQuote_SharedLegKeepsIt(t *testing.T) {
	s := withOfflineClient(NewSession(Options{TradingAccount: "DU12345"}, quotes.NewBook(), nil))
	key := muCall990()
	seedLeg(s, key, legOpts{reqID: 4318, pins: 2}) // two robots hold it
	bookStaleQuote(s, key)

	s.UnsubscribePositionStrike(key.symbol, key.right, key.strike, key.expiry)
	if _, ok := bookQuote(t, s, key); !ok {
		t.Fatal("one holder released but another still streams the contract: quote must stay")
	}

	s.UnsubscribePositionStrike(key.symbol, key.right, key.strike, key.expiry)
	if q, ok := bookQuote(t, s, key); ok {
		t.Fatalf("last holder released but the quote stayed: %+v", q)
	}
}

func TestRepinAfterEviction_FirstBidIsNotBesideTheOldAskAndLast(t *testing.T) {
	s := withOfflineClient(NewSession(Options{TradingAccount: "DU12345"}, quotes.NewBook(), nil))
	key := muCall990()
	seedLeg(s, key, legOpts{reqID: 2265})
	s.optChain.mu.Lock()
	s.optChain.legs[key].tails = 1
	s.optChain.mu.Unlock()
	bookStaleQuote(s, key)
	s.evictRecordingLines([]int64{2265})

	// 10:14:04 — a position pins the contract; its first tick is a bid only
	seedLeg(s, key, legOpts{reqID: 4318, pins: 1})
	s.bookOption(eventbus.OptionData{
		Symbol: key.symbol, Right: key.right, Strike: key.strike, Expiry: key.expiry, Bid: 3.75,
	})

	q, ok := bookQuote(t, s, key)
	if !ok {
		t.Fatal("fresh tick not booked")
	}
	if q.Bid != 3.75 || q.Ask != 0 || q.Last != 0 {
		t.Errorf("Book = bid %.2f ask %.2f last %.2f — the old 8.55/8.48 leaked into the new subscription", q.Bid, q.Ask, q.Last)
	}
}

// A leg dropped by a contract error (200) or an exhausted duplicate-ticker
// repair is also gone at IB: its quote must not outlive it either.
func TestForgetLegLocked_ClearsBookQuote(t *testing.T) {
	s := withOfflineClient(NewSession(Options{TradingAccount: "DU12345"}, quotes.NewBook(), nil))
	key := muCall990()
	seedLeg(s, key, legOpts{reqID: 4318, pins: 1})
	bookStaleQuote(s, key)

	s.optChain.mu.Lock()
	s.forgetLegLocked(s.optChain.legs[key])
	s.optChain.mu.Unlock()

	if q, ok := bookQuote(t, s, key); ok {
		t.Fatalf("a dropped leg left its quote in the Book: %+v", q)
	}
}

// A recording hold keeps the line — and so the quote — alive after the last
// position releases it.
func TestRecordingTailKeepsTheQuoteAfterTheLastPinReleases(t *testing.T) {
	s := withOfflineClient(NewSession(Options{TradingAccount: "DU12345"}, quotes.NewBook(), nil))
	key := muCall990()
	seedLeg(s, key, legOpts{reqID: 4318, pins: 1})
	s.optChain.mu.Lock()
	s.optChain.legs[key].tails = 1
	s.optChain.mu.Unlock()
	bookStaleQuote(s, key)

	s.UnsubscribePositionStrike(key.symbol, key.right, key.strike, key.expiry)

	if _, ok := bookQuote(t, s, key); !ok {
		t.Fatal("the recording hold still streams the contract: its quote must stay")
	}
}

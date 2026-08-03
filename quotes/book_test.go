package quotes

import (
	"sync"
	"testing"
	"time"
)

func TestStockQuoteRoundTrip(t *testing.T) {
	b := NewBook()

	if _, ok := b.Stock("QQQ"); ok {
		t.Fatal("expected no quote before any write")
	}

	b.SetStockBar("QQQ", 690.12, "2026-07-24 09:39:00")
	b.SetStockBid("QQQ", 690.10)
	b.SetStockAsk("QQQ", 690.14)

	q, ok := b.Stock("QQQ")
	if !ok {
		t.Fatal("expected a quote after writes")
	}
	if q.Last != 690.12 || q.Bid != 690.10 || q.Ask != 690.14 {
		t.Fatalf("unexpected quote: %+v", q)
	}
	if q.LastBar != "2026-07-24 09:39:00" {
		t.Fatalf("unexpected LastBar: %q", q.LastBar)
	}
	if q.BidTime.IsZero() || q.AskTime.IsZero() || q.BarTime.IsZero() {
		t.Fatalf("expected timestamps to be set: %+v", q)
	}
}

func TestStockBidTimeAdvancesOnlyOnChange(t *testing.T) {
	b := NewBook()
	b.SetStockBid("QQQ", 690.10)
	q1, _ := b.Stock("QQQ")

	time.Sleep(2 * time.Millisecond)
	b.SetStockBid("QQQ", 690.10) // unchanged — clock must not advance
	q2, _ := b.Stock("QQQ")
	if !q2.BidTime.Equal(q1.BidTime) {
		t.Fatal("BidTime advanced on an unchanged re-tick")
	}

	time.Sleep(2 * time.Millisecond)
	b.SetStockBid("QQQ", 690.20) // changed — clock must advance
	q3, _ := b.Stock("QQQ")
	if !q3.BidTime.After(q1.BidTime) {
		t.Fatal("BidTime did not advance on a changed tick")
	}
}

// TestOptionBidTimeAdvancesOnlyOnChange is the option analogue of the stock
// test above, and exists to document the exact property that makes these
// timestamps unusable as a liveness signal: an unchanged re-tick leaves the
// clock alone, so a contract that is quoting steadily at a flat price is
// indistinguishable here from one IB has stopped serving entirely. That is
// deliberate — BidTime/AskTime answer "how current is this VALUE" — but it is
// why dead-leg detection carries its own per-leg lastTickAt (see deadleg.go)
// rather than reusing these.
func TestOptionBidTimeAdvancesOnlyOnChange(t *testing.T) {
	b := NewBook()
	key := ContractKey{Symbol: "QQQ", Right: "put", Strike: 693, Expiry: "20260805"}

	b.SetOptionBid(key, 9.26)
	q1, ok := b.Option(key)
	if !ok {
		t.Fatal("option quote missing after first write")
	}

	time.Sleep(2 * time.Millisecond)
	b.SetOptionBid(key, 9.26) // unchanged — a live but flat quote
	q2, _ := b.Option(key)
	if !q2.BidTime.Equal(q1.BidTime) {
		t.Fatal("BidTime advanced on an unchanged re-tick")
	}

	time.Sleep(2 * time.Millisecond)
	b.SetOptionBid(key, 12.69) // changed
	q3, _ := b.Option(key)
	if !q3.BidTime.After(q1.BidTime) {
		t.Fatal("BidTime did not advance on a changed tick")
	}
}

func TestNonPositiveWritesIgnored(t *testing.T) {
	b := NewBook()
	b.SetStockBid("QQQ", 0)
	b.SetStockAsk("QQQ", -1)
	b.SetStockBar("QQQ", 0, "x")
	if _, ok := b.Stock("QQQ"); ok {
		t.Fatal("non-positive writes should not create a quote")
	}
}

func TestOptionPartialTicksMerge(t *testing.T) {
	b := NewBook()
	key := ContractKey{Symbol: "QQQ", Right: "put", Strike: 690, Expiry: "20260727"}

	b.SetOptionBid(key, 5.29)
	b.SetOptionAsk(key, 5.32)
	b.SetOptionGreeks(key, -0.4903, 0.22, "matched")
	b.SetOptionLast(key, 5.30)

	q, ok := b.Option(key)
	if !ok {
		t.Fatal("expected an option quote")
	}
	if q.Bid != 5.29 || q.Ask != 5.32 || q.Last != 5.30 {
		t.Fatalf("unexpected option quote: %+v", q)
	}
	if q.Delta != -0.4903 || q.IV != 0.22 || q.DeltaSource != "matched" {
		t.Fatalf("unexpected greeks: %+v", q)
	}

	// A later bid-only tick must not erase the ask or the greeks.
	b.SetOptionBid(key, 5.31)
	q2, _ := b.Option(key)
	if q2.Ask != 5.32 || q2.Delta != -0.4903 || q2.DeltaSource != "matched" {
		t.Fatalf("partial tick erased earlier fields: %+v", q2)
	}
}

func TestGreeksZeroDoesNotErase(t *testing.T) {
	b := NewBook()
	key := ContractKey{Symbol: "QQQ", Right: "put", Strike: 690, Expiry: "20260727"}
	b.SetOptionGreeks(key, -0.49, 0.22, "matched")
	b.SetOptionGreeks(key, 0, 0, "") // all-zero tick — must be a no-op
	q, _ := b.Option(key)
	if q.Delta != -0.49 || q.IV != 0.22 || q.DeltaSource != "matched" {
		t.Fatalf("zero greeks tick erased values: %+v", q)
	}
}

func TestDifferentStrikesAreDistinctKeys(t *testing.T) {
	b := NewBook()
	k690 := ContractKey{Symbol: "QQQ", Right: "put", Strike: 690, Expiry: "20260727"}
	k696 := ContractKey{Symbol: "QQQ", Right: "put", Strike: 696, Expiry: "20260727"}
	b.SetOptionBid(k690, 5.29)
	b.SetOptionAsk(k690, 5.32) // tight ATM market
	b.SetOptionBid(k696, 9.10)
	b.SetOptionAsk(k696, 9.80) // wider ITM market

	q690, _ := b.Option(k690)
	q696, _ := b.Option(k696)
	if q690.Ask == q696.Ask {
		t.Fatal("distinct strikes must not share a quote")
	}
}

// TestResolvedQuoteVisibleToEveryReader is the regression test for the 2026-07-24
// QQQ divergence: once a contract's quote is in the Book, every reader — including
// one that never ran the delta probe that resolved it — sees the same value.
func TestResolvedQuoteVisibleToEveryReader(t *testing.T) {
	b := NewBook()
	key := ContractKey{Symbol: "QQQ", Right: "put", Strike: 690, Expiry: "20260727"}

	// One robot's entry probe resolves the leg and deposits its quote.
	b.SetOptionBid(key, 5.29)
	b.SetOptionAsk(key, 5.32)

	// A second robot that did NOT run the probe reads the same contract.
	q, ok := b.Option(key)
	if !ok {
		t.Fatal("second reader saw no quote")
	}
	spreadPct := (q.Ask - q.Bid) / ((q.Ask + q.Bid) / 2) * 100
	if spreadPct > 5 {
		t.Fatalf("second reader computed a wide spread %.2f%% — divergence not fixed", spreadPct)
	}
}

func TestConcurrentAccess(t *testing.T) {
	b := NewBook()
	key := ContractKey{Symbol: "QQQ", Right: "call", Strike: 690, Expiry: "20260727"}
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(2)
		go func(v float64) { defer wg.Done(); b.SetStockBid("QQQ", v); b.SetOptionAsk(key, v) }(float64(i + 1))
		go func() { defer wg.Done(); _, _ = b.Stock("QQQ"); _, _ = b.Option(key) }()
	}
	wg.Wait()
}

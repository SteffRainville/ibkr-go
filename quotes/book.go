// Package quotes is the single, process-wide source of truth for live market
// data — stock and option quotes — shared by every robot.
//
// Before this package, each robot owned a hub that stored its OWN copy of every
// price, fed by per-robot fan-out (ibclient.updateAllHubs, publishTo(busIdxs),
// KindBidAsk/KindOptionData re-applied per hub) plus a per-call entry-time delta
// probe whose resolved leg was returned only to the calling robot. Two robots
// watching the same instrument could therefore see DIFFERENT prices for it — the
// 2026-07-24 QQQ divergence, where one option robot entered on a freshly resolved
// ATM leg (0.57% spread) while the other was left judging a wider estimate leg
// (7.46%) and skipped. There is no valid reason for a quote to differ by robot:
// the price of an instrument is a property of the market, not of who is looking.
//
// The Book fixes that structurally. IB callbacks (the only writer) deposit every
// quote here once; every robot's hub and bot READ from here. Per-robot *selection*
// — which option strike a robot targets via its own target_delta — still differs
// and is not the Book's concern; the Book answers "what is THIS exact contract
// quoted at", identically for everyone who asks.
//
// The Book is a leaf: it imports nothing from hub, bot, or ibclient, so all three
// can import it without a cycle. It carries its own lock and sits at the bottom of
// the lock hierarchy — never acquire a hub mutex while holding the Book's, and
// snapshot Book reads out before taking hub locks.
package quotes

import (
	"sync"
	"time"
)

// StockQuote is the centralized quote for one underlying symbol. BarTime/LastBar
// track the most recent 30-second bar close; BidTime/AskTime advance only when
// that side's price actually changes, so a consumer can tell a genuinely fresh
// quote from a value merely re-sent unchanged (used for the SSE age fields that
// gate stale outside-RTH orders).
//
// LastTickTime is different on purpose: it advances on EVERY real bid/ask tick
// or bar close IB sends for this symbol, whether or not the price moved. A quiet
// market (price genuinely unchanged for minutes, common outside RTH) and a dead
// subscription (IB stopped sending anything at all) look identical through
// BidTime/AskTime alone — LastTickTime is the signal that tells them apart.
type StockQuote struct {
	Bid, Ask, Last float64
	BidTime        time.Time
	AskTime        time.Time
	BarTime        time.Time
	LastBar        string // "2026-04-09 09:45:00" — the bar date that set Last
	LastTickTime   time.Time
}

// OptionQuote is the centralized quote for one exact option contract (identified
// by its ContractKey). Unlike hub.OptionQuote — the "chosen wholesale" entry unit
// that also carries the strike/expiry — this is only the quote itself: the strike
// and expiry live in the key, so a Book value can never pair a strike with a
// foreign price. BidTime/AskTime advance on change, mirroring StockQuote.
//
// LastTickTime mirrors StockQuote.LastTickTime: it advances on every real bid/
// ask/last tick for this contract, whether or not the price moved, so a quiet-
// but-live contract can be told apart from one IB has stopped serving.
type OptionQuote struct {
	Bid, Ask, Last float64
	Delta, IV      float64
	BidTime        time.Time
	AskTime        time.Time
	LastTickTime   time.Time
	// DeltaSource is "matched" (a genuine live IB delta) or "atm_fallback"/estimate
	// (picked without a confirmed Greek). Empty until the first tick arrives.
	DeltaSource string
}

// ContractKey identifies one exact option contract. Because strike and expiry are
// part of the key, an ATM roll to a new strike is simply a different key — the
// Book never has to merge two strikes' quotes, the merge hazard hub.UpdateOptionData
// guards against by hand.
type ContractKey struct {
	Symbol string  // base ticker, e.g. "QQQ"
	Right  string  // "call" | "put"
	Strike float64 // e.g. 690
	Expiry string  // "YYYYMMDD"
}

// Book is the single source of truth for live quotes. All methods are safe for
// concurrent use.
type Book struct {
	mu     sync.RWMutex
	stocks map[string]StockQuote
	opts   map[ContractKey]OptionQuote
}

// NewBook returns an empty Book ready for concurrent use.
func NewBook() *Book {
	return &Book{
		stocks: make(map[string]StockQuote),
		opts:   make(map[ContractKey]OptionQuote),
	}
}

// ── Stock write path (ibclient) ───────────────────────────────────────────────

// SetStockBar records the latest bar close for symbol and its bar date.
func (b *Book) SetStockBar(symbol string, last float64, barDate string) {
	if last <= 0 {
		return
	}
	b.mu.Lock()
	q := b.stocks[symbol]
	q.Last = last
	q.BarTime = time.Now()
	q.LastTickTime = q.BarTime
	if barDate != "" {
		q.LastBar = barDate
	}
	b.stocks[symbol] = q
	b.mu.Unlock()
}

// SetStockBid records a bid tick for symbol. BidTime advances only when the price
// changes, so an unchanged re-tick does not reset the freshness clock.
func (b *Book) SetStockBid(symbol string, bid float64) {
	if bid <= 0 {
		return
	}
	b.mu.Lock()
	q := b.stocks[symbol]
	now := time.Now()
	if bid != q.Bid {
		q.BidTime = now
	}
	q.Bid = bid
	q.LastTickTime = now
	b.stocks[symbol] = q
	b.mu.Unlock()
}

// SetStockAsk records an ask tick for symbol. AskTime advances only on change.
func (b *Book) SetStockAsk(symbol string, ask float64) {
	if ask <= 0 {
		return
	}
	b.mu.Lock()
	q := b.stocks[symbol]
	now := time.Now()
	if ask != q.Ask {
		q.AskTime = now
	}
	q.Ask = ask
	q.LastTickTime = now
	b.stocks[symbol] = q
	b.mu.Unlock()
}

// ── Option write path (ibclient) ──────────────────────────────────────────────

// SetOptionBid records a bid tick for one contract. BidTime advances only on change.
func (b *Book) SetOptionBid(key ContractKey, bid float64) {
	if bid <= 0 {
		return
	}
	b.mu.Lock()
	q := b.opts[key]
	now := time.Now()
	if bid != q.Bid {
		q.BidTime = now
	}
	q.Bid = bid
	q.LastTickTime = now
	b.opts[key] = q
	b.mu.Unlock()
}

// SetOptionAsk records an ask tick for one contract. AskTime advances only on change.
func (b *Book) SetOptionAsk(key ContractKey, ask float64) {
	if ask <= 0 {
		return
	}
	b.mu.Lock()
	q := b.opts[key]
	now := time.Now()
	if ask != q.Ask {
		q.AskTime = now
	}
	q.Ask = ask
	q.LastTickTime = now
	b.opts[key] = q
	b.mu.Unlock()
}

// SetOptionLast records a last/close price for one contract.
func (b *Book) SetOptionLast(key ContractKey, last float64) {
	if last <= 0 {
		return
	}
	b.mu.Lock()
	q := b.opts[key]
	q.Last = last
	q.LastTickTime = time.Now()
	b.opts[key] = q
	b.mu.Unlock()
}

// SetOptionGreeks records delta/iv/deltaSource for one contract. Zero-valued
// fields are left unchanged so a partial tick never erases an earlier value.
func (b *Book) SetOptionGreeks(key ContractKey, delta, iv float64, deltaSource string) {
	b.mu.Lock()
	q := b.opts[key]
	if delta != 0 {
		q.Delta = delta
	}
	if iv > 0 {
		q.IV = iv
	}
	if deltaSource != "" {
		q.DeltaSource = deltaSource
	}
	b.opts[key] = q
	b.mu.Unlock()
}

// TouchStockTick records that IB sent SOME tick for symbol without necessarily
// carrying a new price — a size-only tick, most commonly. SetStockBid/SetStockAsk
// can't express this: they need a positive price to write. Size ticks are the
// most valuable liveness signal available outside RTH: they keep arriving on a
// line whose price is flat, which is exactly what BidTime/AskTime/Bid/Ask alone
// cannot distinguish from a dead subscription. Creates the entry if this is the
// very first tick seen for symbol, so a size tick arriving before any price tick
// still proves the line is alive.
func (b *Book) TouchStockTick(symbol string) {
	b.mu.Lock()
	q := b.stocks[symbol]
	q.LastTickTime = time.Now()
	b.stocks[symbol] = q
	b.mu.Unlock()
}

// TouchOptionTick is the option analogue of TouchStockTick — records a size (or
// other non-price) tick for one exact contract.
func (b *Book) TouchOptionTick(key ContractKey) {
	b.mu.Lock()
	q := b.opts[key]
	q.LastTickTime = time.Now()
	b.opts[key] = q
	b.mu.Unlock()
}

// ── Read path (hub, bot) ──────────────────────────────────────────────────────

// Stock returns the current quote for symbol, and whether one exists.
func (b *Book) Stock(symbol string) (StockQuote, bool) {
	b.mu.RLock()
	q, ok := b.stocks[symbol]
	b.mu.RUnlock()
	return q, ok
}

// Option returns the current quote for one exact contract, and whether one exists.
func (b *Book) Option(key ContractKey) (OptionQuote, bool) {
	b.mu.RLock()
	q, ok := b.opts[key]
	b.mu.RUnlock()
	return q, ok
}

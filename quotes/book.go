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

	// Window extremes (TrackExtremes). extEvery 0 = not tracked.
	extEvery time.Duration
	stockExt map[string]*extPair
	optExt   map[ContractKey]*extPair
}

// NewBook returns an empty Book ready for concurrent use.
func NewBook() *Book {
	return &Book{
		stocks:   make(map[string]StockQuote),
		opts:     make(map[ContractKey]OptionQuote),
		stockExt: make(map[string]*extPair),
		optExt:   make(map[ContractKey]*extPair),
	}
}

// ── Window extremes ───────────────────────────────────────────────────────────

// Extremes is the range each side of one instrument's quote covered during one
// wall-clock window [Start, Start+every): the lowest and highest bid, ask and
// last it held at any moment — the value prevailing when the window opened
// included, so a side that never ticked reads its standing price. 0 = that
// side had no price in the window.
//
// A consumer that samples the Book every few seconds (the quote recorder) sees
// only the instant it samples; these are everything in between — the spike
// that armed a live trailing stop, the dip that touched a stop loss.
type Extremes struct {
	Start             time.Time
	BidLow, BidHigh   float64
	AskLow, AskHigh   float64
	LastLow, LastHigh float64
	// Open* is what prevailed when the window opened — the whole window's
	// value for a side with no tick in it.
	OpenBid, OpenAsk, OpenLast float64
}

// extPair keeps the window in progress and the one before it: a reader that
// asks for the window just ended must still find it after a tick has already
// opened the next.
type extPair struct{ cur, prev Extremes }

const (
	sideBid = iota
	sideAsk
	sideLast
)

// TrackExtremes turns on window-extreme tracking with windows aligned to
// multiples of every (the quote recorder's interval, so its windows are the
// recorder's samples). Call once, before quotes arrive; 0 turns it off.
func (b *Book) TrackExtremes(every time.Duration) {
	b.mu.Lock()
	b.extEvery = every
	b.mu.Unlock()
}

// noteExt folds one price update into a pair's current window. was is the
// quote (bid, ask, last) prevailing BEFORE this update. Caller holds b.mu.
func (b *Book) noteExt(e *extPair, now time.Time, side int, price float64, was [3]float64) {
	start := now.Truncate(b.extEvery)
	if !e.cur.Start.Equal(start) {
		if e.cur.Start.Equal(start.Add(-b.extEvery)) {
			e.prev = e.cur
		} else {
			e.prev = Extremes{} // the window just ended saw no tick
		}
		e.cur = Extremes{Start: start, OpenBid: was[sideBid], OpenAsk: was[sideAsk], OpenLast: was[sideLast]}
		for i, v := range was {
			e.cur.include(i, v)
		}
	}
	e.cur.include(side, price)
}

func (x *Extremes) include(side int, v float64) {
	if v <= 0 {
		return
	}
	lo, hi := &x.BidLow, &x.BidHigh
	switch side {
	case sideAsk:
		lo, hi = &x.AskLow, &x.AskHigh
	case sideLast:
		lo, hi = &x.LastLow, &x.LastHigh
	}
	if *lo == 0 || v < *lo {
		*lo = v
	}
	if v > *hi {
		*hi = v
	}
}

// extremesAt answers for the window starting at start. cur is the quote now.
func extremesAt(e *extPair, start time.Time, cur [3]float64) Extremes {
	switch {
	case e != nil && e.cur.Start.Equal(start):
		return e.cur
	case e != nil && e.prev.Start.Equal(start):
		return e.prev
	case e != nil && e.cur.Start.After(start) && e.prev.Start.IsZero():
		// No tick in the asked window, one since: it held the next window's opening quote.
		return constant(start, [3]float64{e.cur.OpenBid, e.cur.OpenAsk, e.cur.OpenLast})
	default:
		// No tick since the window opened: the quote now held throughout.
		return constant(start, cur)
	}
}

func constant(start time.Time, v [3]float64) Extremes {
	x := Extremes{Start: start, OpenBid: v[sideBid], OpenAsk: v[sideAsk], OpenLast: v[sideLast]}
	for i, p := range v {
		x.include(i, p)
	}
	return x
}

// StockExtremes returns symbol's extremes over the window starting at start.
// ok is false when tracking is off or the symbol has never been quoted.
func (b *Book) StockExtremes(symbol string, start time.Time) (Extremes, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	q, ok := b.stocks[symbol]
	if b.extEvery <= 0 || !ok {
		return Extremes{}, false
	}
	return extremesAt(b.stockExt[symbol], start, [3]float64{q.Bid, q.Ask, q.Last}), true
}

// OptionExtremes is StockExtremes for one exact contract.
func (b *Book) OptionExtremes(key ContractKey, start time.Time) (Extremes, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	q, ok := b.opts[key]
	if b.extEvery <= 0 || !ok {
		return Extremes{}, false
	}
	return extremesAt(b.optExt[key], start, [3]float64{q.Bid, q.Ask, q.Last}), true
}

// stockExtFor / optExtFor return (creating) an instrument's pair, or nil when
// tracking is off. Caller holds b.mu.
func (b *Book) stockExtFor(symbol string) *extPair {
	if b.extEvery <= 0 {
		return nil
	}
	e := b.stockExt[symbol]
	if e == nil {
		e = &extPair{}
		b.stockExt[symbol] = e
	}
	return e
}

func (b *Book) optExtFor(key ContractKey) *extPair {
	if b.extEvery <= 0 {
		return nil
	}
	e := b.optExt[key]
	if e == nil {
		e = &extPair{}
		b.optExt[key] = e
	}
	return e
}

// ── Stock write path (ibclient) ───────────────────────────────────────────────

// SetStockBar records the latest bar close for symbol and its bar date.
func (b *Book) SetStockBar(symbol string, last float64, barDate string) {
	if last <= 0 {
		return
	}
	b.mu.Lock()
	q := b.stocks[symbol]
	if e := b.stockExtFor(symbol); e != nil {
		b.noteExt(e, time.Now(), sideLast, last, [3]float64{q.Bid, q.Ask, q.Last})
	}
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
	if e := b.stockExtFor(symbol); e != nil {
		b.noteExt(e, now, sideBid, bid, [3]float64{q.Bid, q.Ask, q.Last})
	}
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
	if e := b.stockExtFor(symbol); e != nil {
		b.noteExt(e, now, sideAsk, ask, [3]float64{q.Bid, q.Ask, q.Last})
	}
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
	if e := b.optExtFor(key); e != nil {
		b.noteExt(e, now, sideBid, bid, [3]float64{q.Bid, q.Ask, q.Last})
	}
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
	if e := b.optExtFor(key); e != nil {
		b.noteExt(e, now, sideAsk, ask, [3]float64{q.Bid, q.Ask, q.Last})
	}
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
	if e := b.optExtFor(key); e != nil {
		b.noteExt(e, time.Now(), sideLast, last, [3]float64{q.Bid, q.Ask, q.Last})
	}
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

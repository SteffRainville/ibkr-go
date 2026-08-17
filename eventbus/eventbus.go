// Package eventbus provides a lightweight, typed publish/subscribe bus for
// broker-level events: bars, ticks, fills, commissions, and order-level
// errors. It is deliberately generic — nothing here encodes a trading
// strategy. A consuming application typically defines its own additional
// Kind constants and payload types (e.g. a "buy signal" or "trade
// completed" event) and publishes them on the same *Bus value returned by
// New(), since Bus/Kind/Event carry no assumptions about which Kinds exist.
package eventbus

import (
	"maps"
	"sync"
	"time"
)

// Kind identifies the type of event being published.
type Kind string

const (
	// KindCandle is published for every completed OHLCV bar received from
	// the broker (both historical backfill and live). Payload: Candle.
	KindCandle Kind = "candle"

	// KindLiveTick is published for every partial-bar price update while a
	// bar is still forming (e.g. IB's 5-second HistoricalDataUpdate ticks).
	// Payload: LiveTick.
	KindLiveTick Kind = "live_tick"

	// KindOrderFilled is published for every incremental fill the broker
	// reports for an order. Payload: OrderFilled.
	KindOrderFilled Kind = "order_filled"

	// KindCommissionReport is published when the broker sends commission
	// data for a fill. Payload: CommissionReport.
	KindCommissionReport Kind = "commission_report"

	// KindOrderRejected is published when the broker rejects an order.
	// Payload: OrderRejected.
	KindOrderRejected Kind = "order_rejected"

	// KindOrderQtyMismatch is published when an order reported as fully
	// filled delivered a different quantity than was requested at
	// placement. Payload: OrderQtyMismatch.
	KindOrderQtyMismatch Kind = "order_qty_mismatch"

	// KindOptionData is published when a bid, ask, or last price tick
	// arrives for a resolved option contract. Payload: OptionData.
	KindOptionData Kind = "option_data"

	// KindPositionOptionData is published when a tick arrives for a
	// position-pinned option subscription — a contract held by an open
	// position that differs from whatever contract is currently the
	// caller's "display" selection for that underlying. Payload: OptionData.
	KindPositionOptionData Kind = "position_option_data"
)

// Candle holds OHLCV data for one completed bar.
type Candle struct {
	Symbol string
	Date   string // "2026-04-09 09:45:00"
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Volume float64 // total shares traded in the bar
	WAP    float64 // volume-weighted average price for the bar
	IsLive bool    // false = historical backfill, true = live forming bar
}

// LiveTick carries a single partial-bar price update.
type LiveTick struct {
	Symbol string
	Price  float64
	Date   string // "2026-04-09 09:34:05"
}

// OrderFilled is published for every incremental fill the broker reports
// for an order. OrderID is the broker's own order identifier, stable for
// the life of the order across every partial fill — the correct key for
// reconciling an order's progress (not symbol+direction, which two
// concurrent orders on the same leg would collide on).
type OrderFilled struct {
	OrderID   int64
	Symbol    string
	Action    string  // "BUY" or "SELL"
	Tag       string  // opaque caller tag carried from the originating SymbolSpec/order
	Qty       float64 // this event's fill delta
	Remaining float64 // qty still unfilled on this order (0 once fully filled); BUY-side only
	AvgFill   float64 // average fill price
}

// CommissionReport is published when the broker sends commission data for
// a filled order. OrderID/ExecID are the broker's own identifiers.
type CommissionReport struct {
	OrderID    int64
	ExecID     string // per-execution identifier, unique per fill
	Symbol     string
	Action     string // "BUY" or "SELL"
	Tag        string
	Price      float64 // average fill price
	Qty        float64
	Ts         time.Time
	Commission float64
	Currency   string
}

// OrderRejected is published when the broker rejects a placed order.
type OrderRejected struct {
	OrderID int64
	// Symbol is the bare contract symbol ("MU"), identical to what
	// OrderFilled carries — consumers key position maps by it. It is NEVER a
	// display string: the session's error path also builds a decorated label
	// ("MU (order)") for logs, and publishing that here made every keyed
	// lookup downstream miss without failing (2026-08-17: a rejected entry
	// stayed "pending" on the dashboard because the position it named did not
	// exist under that key). Message is the field for human-facing text.
	Symbol  string
	Tag     string
	Action  string // "BUY" | "SELL" | "SHORT" | "COVER"
	Code    int64
	Message string
}

// OrderQtyMismatch is published when a "Filled" order's actual filled
// quantity differs from the quantity requested when it was placed.
type OrderQtyMismatch struct {
	OrderID   int64
	Symbol    string
	Tag       string
	Action    string
	Requested float64
	Filled    float64
}

// OptionData carries live market data for one resolved option contract.
type OptionData struct {
	Symbol string
	Right  string // "call" or "put"
	Strike float64
	Expiry string // "YYYYMMDD"
	Price  float64
	Bid    float64
	Ask    float64
	Delta  float64
	IV     float64

	// DeltaSource is "matched" when Strike was chosen from a genuine live
	// delta probe, or "atm_fallback" when no usable delta was available and
	// the strike was picked without one. Empty until the first tick arrives.
	DeltaSource string
}

// Event is the envelope that flows through the bus.
type Event struct {
	Kind    Kind
	Payload any
}

// Bus is a non-blocking, fan-out publish/subscribe bus.
// Publish never blocks — slow subscribers drop events (16-event buffer per
// subscriber by default; see SubscribeBuffered for a caller-sized buffer, and
// DropStats to detect drops when they happen).
type Bus struct {
	mu   sync.RWMutex
	subs map[Kind][]chan Event

	dropMu  sync.Mutex
	dropped map[Kind]uint64
}

// New creates a ready-to-use Bus.
func New() *Bus {
	return &Bus{subs: make(map[Kind][]chan Event), dropped: make(map[Kind]uint64)}
}

// Subscribe returns a channel that receives events of the requested kinds,
// buffered to the default 16 slots. The caller must eventually call
// Unsubscribe to avoid a goroutine/channel leak.
func (b *Bus) Subscribe(kinds ...Kind) <-chan Event {
	return b.SubscribeBuffered(16, kinds...)
}

// SubscribeBuffered is like Subscribe but lets the caller size the channel's
// buffer explicitly instead of the default 16. Use a dedicated, larger buffer
// for a Kind whose loss is unrecoverable (e.g. one Kind feeds a continuously-
// accumulating consumer with no reset) and that must not share headroom with
// a high-volume Kind subscribed on the same call.
func (b *Bus) SubscribeBuffered(size int, kinds ...Kind) <-chan Event {
	ch := make(chan Event, size)
	b.mu.Lock()
	for _, k := range kinds {
		b.subs[k] = append(b.subs[k], ch)
	}
	b.mu.Unlock()
	return ch
}

// DropStats returns a snapshot of cumulative, per-Kind counts of events this
// Bus discarded because a subscriber channel was full (see safeSend). It is
// monotonic — callers should diff successive snapshots to get a rate.
func (b *Bus) DropStats() map[Kind]uint64 {
	b.dropMu.Lock()
	defer b.dropMu.Unlock()
	out := make(map[Kind]uint64, len(b.dropped))
	maps.Copy(out, b.dropped)
	return out
}

// Unsubscribe removes ch from all subscriber lists and closes it once.
func (b *Bus) Unsubscribe(ch <-chan Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	closed := false
	for k, list := range b.subs {
		out := list[:0]
		for _, c := range list {
			if c == ch {
				if !closed {
					close(c)
					closed = true
				}
			} else {
				out = append(out, c)
			}
		}
		b.subs[k] = out
	}
}

// Publish fans out evt to every subscriber registered for evt.Kind.
// Each send is non-blocking; a full or closed subscriber channel drops the
// event and increments DropStats()[evt.Kind].
func (b *Bus) Publish(evt Event) {
	b.mu.RLock()
	list := append([]chan Event(nil), b.subs[evt.Kind]...)
	b.mu.RUnlock()
	for _, ch := range list {
		b.safeSend(ch, evt)
	}
}

func (b *Bus) safeSend(ch chan Event, evt Event) {
	defer func() { recover() }() //nolint:errcheck // closed channel → drop silently
	select {
	case ch <- evt:
	default:
		b.dropMu.Lock()
		b.dropped[evt.Kind]++
		b.dropMu.Unlock()
	}
}

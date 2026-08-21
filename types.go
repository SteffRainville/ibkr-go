package ibkr

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/scmhub/ibapi"

	"github.com/SteffRainville/ibkr-go/eventbus"
)

// SymbolSpec is one IB subscription unit — a symbol this session should
// stream bars/quotes for, plus enough context to resolve an option chain
// against it. Tag is an opaque caller label (e.g. "long"/"short"/"call"/
// "put") — the library never interprets it, only threads it through events
// so the caller can route them back to the right dashboard row.
type SymbolSpec struct {
	Symbol   string
	Tag      string
	Name     string
	Exchange string
	Currency string
	Contract *ibapi.Contract

	// OptionDelay and TargetDelta select WHICH option contract to track for
	// this symbol — they are broker-subscription parameters, not P&L risk
	// config, so they travel with the spec even though a caller's own
	// risk-parameter file (stop-loss, budget, etc.) stays entirely on the
	// caller's side. Zero values are treated as "delay 0 (today), ATM".
	OptionDelay int     // days forward for option expiry: 0=today, 1=tomorrow, etc.
	TargetDelta float64 // target |delta| for strike selection: 0.50 = ATM (default), 0.65 = ITM
}

// Subscriber is the per-consumer extension point — one per independent
// trading strategy/robot sharing this session's single IB connection. The
// library knows nothing about hubs, dashboards, or bot services: it only
// needs a symbol list, a place to publish events, an order-priority lookup,
// and a hook to call once the session is connected and subscribed.
type Subscriber interface {
	// Symbols returns the symbol list this subscriber wants IB subscriptions
	// for. The session deduplicates by base symbol across all subscribers.
	Symbols() []SymbolSpec

	// Bus returns this subscriber's event bus. The session publishes bars,
	// ticks, fills, commissions, and option data here.
	Bus() *eventbus.Bus

	// OrderPriority resolves the IB Adaptive Algo urgency ("Patient" /
	// "Normal" / "Urgent") for a transaction category (e.g. "buy",
	// "take_profit", "stop_loss", "trailing_loss"). Return "" for the
	// default (Normal).
	OrderPriority(category string) string

	// OnConnected is called once per session, after the connection is
	// established and all subscriptions have been issued. ctx is cancelled
	// when the session disconnects, so any goroutine started here should
	// select on ctx.Done().
	OnConnected(ctx context.Context)
}

// OrderRequest is an instruction to open or close a position leg. For
// options, when Strike/OptionExpiry are set they identify the EXACT
// contract to trade and the session never re-derives it from the current
// ATM chain — this is what makes an order structurally unable to drift to a
// different strike than the one the caller already committed to (e.g. the
// one a position was opened against). Strike <= 0 (or OptionExpiry == "")
// falls back to the session's current ATM leg for that symbol+tag.
type OrderRequest struct {
	Symbol       string
	Tag          string  // opaque caller tag (was "direction": long/short/call/put)
	SecType      string  // "STK" | "OPT"
	Qty          float64 // signed for closes (negative = short/put side → COVER); positive for opens
	Category     string  // "take_profit" | "stop_loss" | "trailing_loss" | "" — selects Adaptive Algo urgency
	Strike       float64 // options only (0 = unspecified → resolve current ATM leg)
	OptionExpiry string  // options only, "YYYYMMDD"
	Bid, Ask     float64 // option quote for outside-RTH limit pricing
}

// OrderResult reports what OpenPosition/ClosePosition actually submitted —
// including the option contract resolved, when the caller didn't specify
// one — so the caller can build its own pending-position record without
// re-deriving anything the session already resolved.
type OrderResult struct {
	OrderID      int64
	Strike       float64 // resolved option strike (0 for stocks)
	OptionExpiry string  // resolved option expiry (empty for stocks)
	Bid, Ask     float64 // option quote used to price the order
	Mid          float64 // option mid-price, when available
	Underlying   float64 // underlying price at order time (options only)
}

// PositionInfo mirrors one broker-reported portfolio position — all
// accounts, all instrument types, not just symbols this session subscribed
// to.
type PositionInfo struct {
	Account  string
	Symbol   string
	SecType  string
	Exchange string
	Currency string
	Position float64
	AvgCost  float64

	// Option-only fields (zero for stocks).
	Right  string // "C" | "P"
	Strike float64
	Expiry string // "YYYYMMDD"
}

// OptionQuote is one resolved option contract's current quote. Strike/Expiry
// travel WITH the quote so a caller can never pair a strike with a foreign
// price.
type OptionQuote struct {
	Strike float64
	Expiry string // "YYYYMMDD"
	Bid    float64
	Ask    float64
	Last   float64
	Delta  float64
	// IV is the implied volatility IB reported alongside Delta on the same
	// greeks tick. Carried on the quote because the entry probe is now the ONLY
	// place an option IV is observed — there is no background leg accumulating
	// one per watchlist row any more — and the trade log records it per entry.
	IV float64
	// BidTime/AskTime record when each side last actually ticked (not when this
	// struct was built) — the freshness signal callers use to tell a genuinely
	// live quote from a stale one that just hasn't been overwritten yet on a thin
	// contract. Zero when the source couldn't provide it.
	BidTime time.Time
	AskTime time.Time
}

// Valid reports whether this quote fully identifies a real contract AND
// carries a usable two-sided price for it — the precondition for trading
// against it. A quote with a strike but no bid/ask (e.g. a delta-only probe
// match) is NOT valid.
//
// The expiry counts as identity: strike alone does not name a contract, and a
// caller that pins a position on an incomplete identity cannot close it later.
func (q OptionQuote) Valid() bool {
	return q.Strike > 0 && q.Expiry != "" && q.Bid > 0 && q.Ask > 0
}

// AccountSummary holds cash and margin info for one IB account, populated
// from AccountSummary callbacks.
type AccountSummary struct {
	Account                 string
	AccountType             string
	AccountAlias            string
	TradingType             string
	TotalCashValue          float64
	AvailableFunds          float64
	BuyingPower             float64
	LookAheadAvailableFunds float64
	InitMarginReq           float64
	NetLiquidation          float64
	Currency                string
}

// ScanParams configures a market scanner run.
type ScanParams struct {
	Instrument   string // e.g. "STK"
	LocationCode string // e.g. "STK.US.MAJOR"
	ScanCode     string // e.g. "TOP_PERC_GAIN"
	AbovePrice   float64
	BelowPrice   float64
	AboveVolume  int64
}

// ScanResult is one row returned by a market scan, optionally enriched with
// a market data snapshot and contract details.
type ScanResult struct {
	Rank         int
	Symbol       string
	SecType      string
	Exchange     string
	Currency     string
	LocalSymbol  string
	TradingClass string
	MarketName   string
	ConID        int64
	Strike       float64
	Right        string
	Expiry       string
	Distance     string
	Benchmark    string
	Projection   string
	LegsStr      string

	Bid           float64
	Ask           float64
	Last          float64
	PrevClose     float64
	Open          float64
	Volume        int64
	ChangePercent float64
	GapPercent    float64

	LongName          string
	StockType         string
	LowLiquidity      bool
	IneligibleReasons string
	CloseOnly         bool
	NoShorts          bool
	Halted            int
}

// ConnectionsStatus is a snapshot of IB market-data subscription usage,
// broken down by priority category, plus enough symbol-count context for a
// diagnostics page to explain the gap between a watchlist's size and the
// live line count.
type ConnectionsStatus struct {
	Used, Max         int
	HistUsed, HistMax int

	// The two PERSISTENT line kinds, then the two TRANSIENT ones. There were
	// formerly two more persistent tiers between them (DiscretionaryNew and
	// DiscretionaryChurn) holding one streaming line per watchlist option row;
	// they are gone, which is why a healthy account now sits far below Max.
	StockLines    int
	PositionLines int
	SnapshotLines int
	ProbeLines    int
	BufferLines   int

	ConfiguredStockRows  int
	ConfiguredOptionRows int
	UniqueUnderlyings    int

	// OptionSelectors is how many distinct (symbol, right, delay, target_delta)
	// configurations exist, and CachedChains how many option chains are held in
	// the snapshot cache. Neither costs a market-data line — that is the point.
	// A selector is now purely a strike-selection RECIPE consulted at entry
	// time; ConfiguredOptionRows greatly exceeding PositionLines is the normal,
	// healthy state rather than the shortfall it used to signal.
	OptionSelectors int
	CachedChains    int

	ChainRefreshIntervalSeconds int
}

// ErrorEvent is a session-level error notification — connection drops,
// subscription problems, order rejections that don't fit the typed
// eventbus.OrderRejected payload.
type ErrorEvent struct {
	Type    string // "connection" | "subscription" | "order" | …
	Message string
}

// AccountMismatchError is returned by Connect when the configured trading
// account is not among the accounts TWS reports for the current login —
// almost always a sign of connecting to the wrong TWS/Gateway instance
// (e.g. live vs paper).
type AccountMismatchError struct {
	Configured string
	Actual     []string
}

func (e *AccountMismatchError) Error() string {
	return fmt.Sprintf("configured trading account %q not found in TWS accounts %v", e.Configured, e.Actual)
}

// Options configures a Session. Only Host/Port/ClientID/TradingAccount are
// required; everything else has a sensible default.
type Options struct {
	Host           string // default "127.0.0.1"
	Port           int
	ClientID       int64
	TradingAccount string

	ConnectTimeout          time.Duration // default 10s
	PositionRefreshInterval time.Duration // default 5m
	BarStallTimeout         time.Duration // default 120s (RTH); widened automatically outside RTH
	MaxMarketDataLines      int           // default 100
	MaxHistoricalStreams    int           // default 50

	// OptionChainRefreshInterval paces the background sweep that keeps each
	// watched underlying's option-chain SNAPSHOT (expiries + strike ladder)
	// inside chainSnapshotTTL. One chain per tick, round-robin.
	//
	// A chain lookup is conId + ReqSecDefOptParams — it costs no market-data
	// line, which is why this sweep survived the removal of the background
	// strike subscriptions it used to accompany. It is load-bearing:
	// ResolveEntryStrike reads the cached snapshot and never fetches one
	// itself, so a stalled sweep means entries fall back to an on-demand
	// lookup. Default 5s.
	OptionChainRefreshInterval time.Duration

	Logger                         *log.Logger // default log.Default()
	ScanLog, OptionLog, AccountLog io.Writer   // default io.Discard

	// Push callbacks — broadcast-style notifications mirroring what every
	// subscriber's dashboard needs to stay in sync with IB's own portfolio
	// and account state. All optional; nil callbacks are simply not called.
	OnPositionSnapshot    func([]PositionInfo)                                   // full portfolio snapshot (PositionEnd)
	OnPositionUpdate      func(PositionInfo)                                     // one confirmed real-time position update
	OnPositionClosed      func(PositionInfo)                                     // a position confirmed closed by a fresh snapshot
	OnPositionPriceUpdate func(symbol string, price float64)                     // fresh underlying price, for a positions-page display
	OnAccountValue        func(summary AccountSummary, tag string, value string) // raw AccountSummary tag/value
	OnError               func(ErrorEvent)
	OnStall               func() // bar feed watchdog forced a reconnect
}

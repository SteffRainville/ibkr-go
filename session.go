// Package ibkr is a standalone Interactive Brokers (TWS / IB Gateway)
// connectivity layer: connection lifecycle, bar/tick streaming, order
// placement, position and account sync, option chain resolution with
// delta-based strike selection and position-pinning, a market scanner, and
// a market-data-line-cap governor. It knows nothing about any particular
// trading strategy, dashboard, or bot framework — see Subscriber and
// Options for the extension points a caller wires up.
package ibkr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scmhub/ibapi"

	"github.com/SteffRainville/ibkr-go/candlestore"
	"github.com/SteffRainville/ibkr-go/eventbus"
	"github.com/SteffRainville/ibkr-go/mdlines"
	"github.com/SteffRainville/ibkr-go/quotes"
)

// ErrConnect is returned by Connect when it cannot establish the initial
// TCP connection to TWS / IB Gateway.
var ErrConnect = errors.New("could not connect to TWS / IB Gateway")

// ErrReady is returned when the connection succeeds but TWS never sends the
// NextValidID ready signal within the timeout.
var ErrReady = errors.New("connected but TWS did not become ready")

// ReqID allocation per session (N = number of tracked symbols):
//
//	1001 … 1000+N  — historical data + live bar subscriptions (one per symbol)
//	2001 … 2000+N  — streaming market data / bid-ask (one per symbol)
//	3001 …         — market scanner subscription requests (one per scan run)
//	4001 …         — on-demand historical data fetches
//	5001 …         — on-demand market data snapshots (positions page prices)
//	6001 …         — reqSecDefOptParams (option chain params)
//	7001 …         — option market data subscriptions
//	8001 …         — ReqContractDetails for option underlying conId lookup
//	9001           — account summary request
//	10001 …        — position-pinned option market data subscriptions
//	11001 …        — ad-hoc option chain query: ReqContractDetails (conId lookup)
//	12001 …        — ad-hoc option chain query: reqSecDefOptParams
const (
	reqIDHistBase          int64 = 1001
	reqIDStreamBase        int64 = 2001
	reqIDScannerBase       int64 = 3001
	reqIDOnDemandHistBase  int64 = 4001
	reqIDSnapBase          int64 = 5001
	reqIDOptChainBase      int64 = 6001
	reqIDOptMktBase        int64 = 7001
	reqIDOptConIDBase      int64 = 8001
	reqIDAcctSummary       int64 = 9001
	reqIDPosMktBase        int64 = 10001
	reqIDOptQueryConIDBase int64 = 11001
	reqIDOptQueryChainBase int64 = 12001
)

const bidAskMaxAge = 30 * time.Second

// quote is a price with the time it was captured — used for the internal
// streaming bid/ask cache that feeds outside-RTH order pricing.
type quote struct {
	Price float64
	Time  time.Time
}

// marketData tracks snapshot requests and live bid/ask streaming data.
type marketData struct {
	snapReqSymbol map[int64]string
	nextSnapID    int64
	mktDataSymbol map[int64]string

	bidAskMu  sync.RWMutex
	bid       map[string]quote
	ask       map[string]quote
	prevClose map[string]float64
}

// pendingExec holds fill data cached from ExecDetails, keyed by ExecID,
// until CommissionAndFeesReport arrives with the matching commission data.
type pendingExec struct {
	orderID int64
	symbol  string
	action  string
	tag     string
	price   float64
	qty     float64
	ts      time.Time
	bus     *eventbus.Bus
}

// subRoute records which subscriber's bus placed an order, and its leg
// shape, so fill/commission/rejection callbacks route back to the correct
// subscriber.
type subRoute struct {
	bus     *eventbus.Bus
	secType string
	tag     string
}

// inFlightSellRef records the option contract key and share quantity a
// submitted SELL/COVER order has reserved against the naked-short gate.
type inFlightSellRef struct {
	key string
	qty float64
}

// orderTracker manages IB order IDs and maps them back to symbols/actions/routes.
type orderTracker struct {
	orderMu       sync.Mutex
	nextOrderID   int64
	orderSymbol   map[int64]string
	orderAction   map[int64]string
	orderRoutes   map[int64]subRoute
	orderQty      map[int64]float64
	orderFilled   map[int64]float64
	pendingExecs  map[string]pendingExec
	inFlightSell  map[string]float64
	orderInFlight map[int64]inFlightSellRef
}

// posTracker accumulates position data from IB Position() callbacks.
type posTracker struct {
	posMapMu     sync.Mutex
	posMap       map[string]PositionInfo
	initialDone  bool
	pendingClose map[string]PositionInfo
}

// errTracker debounces error notifications so the caller sees each error
// class only once per IB session.
type errTracker struct {
	subErrMu              sync.Mutex
	subscriptionErrorSent bool
}

type scanSnapEntry struct {
	parentReqID int64
	resultIdx   int
	bid         float64
	ask         float64
	last        float64
	prevClose   float64
	open        float64
	volume      int64
	haltStatus  float64
}

type scanCDEntry struct {
	parentReqID int64
	resultIdx   int
}

type onDemandTracker struct {
	mu        sync.Mutex
	nextID    int64
	reqSymbol map[int64]string
	done      map[int64]chan error
}

type scanTracker struct {
	mu         sync.Mutex
	nextID     int64
	results    map[int64][]ScanResult
	enriched   map[int64][]ScanResult
	pending    map[int64]chan []ScanResult
	snapData   map[int64]*scanSnapEntry
	snapCount  map[int64]int
	snapReqIDs map[int64][]int64
	cdData     map[int64]*scanCDEntry
	cdCount    map[int64]int
	cdReqIDs   map[int64][]int64
}

// acctTracker accumulates AccountSummary callback data, keyed by account ID.
type acctTracker struct {
	mu       sync.RWMutex
	accounts map[string]AccountSummary
}

// Session is a live (or about-to-connect) IB session — the IB API wrapper
// that receives every callback, plus the bookkeeping needed to serve
// Session's public methods (OpenPosition, ClosePosition, RunScanner, …).
type Session struct {
	ibapi.Wrapper

	opts Options

	client *ibapi.EClient
	ready  chan struct{}

	subs  []Subscriber
	buses []*eventbus.Bus
	book  *quotes.Book

	liveStart   time.Time
	lastBarNano atomic.Int64

	Candles *candlestore.Store

	// symMu guards every field of the tracked-symbol registry below:
	// reqSymbol, symbols, subSymbols, histReqID, streamReqID, the two reqID
	// counters, and mktData.mktDataSymbol. Run used to write all of them once,
	// before any subscription existed, so unsynchronized reads from the
	// callback goroutines were safe by construction. ResyncSymbols mutates
	// them mid-session — from an HTTP handler goroutine, while bars and ticks
	// are arriving — so the registry now needs a real lock. It is a leaf:
	// never hold it while calling into the IB client or taking optChain.mu.
	symMu          sync.RWMutex
	reqSymbol      map[int64]string
	symbols        []SymbolSpec     // full list (all subscribers, with tags)
	subSymbols     [][]SymbolSpec   // per-subscriber symbol list, indexed like subs/buses
	histReqID      map[string]int64 // base symbol → keepUpToDate bar-stream reqID
	streamReqID    map[string]int64 // base symbol → streaming market-data reqID
	nextHistID     int64            // next free reqID in the reqIDHistBase block
	nextStreamID   int64            // next free reqID in the reqIDStreamBase block
	tradingAccount string
	accountsReady  chan []string

	mktData  marketData
	orders   orderTracker
	pos      posTracker
	errors   errTracker
	scanner  scanTracker
	optChain optionChainTracker
	optQuery optionQueryTracker
	onDemand onDemandTracker
	acct     acctTracker

	mdLines *mdlines.Ledger

	logger    *log.Logger
	scanLog   *log.Logger
	optionLog *log.Logger
	acctLog   *log.Logger
}

func writerLogger(w io.Writer) *log.Logger {
	if w == nil {
		w = io.Discard
	}
	return log.New(w, "", log.LstdFlags)
}

func withDefaults(o Options) Options {
	if o.Host == "" {
		o.Host = "127.0.0.1"
	}
	if o.ConnectTimeout <= 0 {
		o.ConnectTimeout = 10 * time.Second
	}
	if o.OptionRotationInterval <= 0 {
		o.OptionRotationInterval = 5 * time.Second
	}
	if o.PositionRefreshInterval <= 0 {
		o.PositionRefreshInterval = 5 * time.Minute
	}
	if o.BarStallTimeout <= 0 {
		o.BarStallTimeout = 120 * time.Second
	}
	if o.Logger == nil {
		o.Logger = log.Default()
	}
	return o
}

// NewSession constructs a Session ready to Connect. No I/O happens here.
// book and cs may be nil, in which case the session creates its own
// (equivalent to a private, single-session Book/Store).
func NewSession(opts Options, book *quotes.Book, cs *candlestore.Store) *Session {
	opts = withDefaults(opts)
	if book == nil {
		book = quotes.NewBook()
	}
	if cs == nil {
		cs = candlestore.New()
	}
	return &Session{
		opts:           opts,
		ready:          make(chan struct{}, 1),
		book:           book,
		Candles:        cs,
		reqSymbol:      make(map[int64]string),
		histReqID:      make(map[string]int64),
		streamReqID:    make(map[string]int64),
		nextHistID:     reqIDHistBase,
		nextStreamID:   reqIDStreamBase,
		tradingAccount: opts.TradingAccount,
		accountsReady:  make(chan []string, 1),

		mktData: marketData{
			snapReqSymbol: make(map[int64]string),
			nextSnapID:    reqIDSnapBase,
			mktDataSymbol: make(map[int64]string),
			bid:           make(map[string]quote),
			ask:           make(map[string]quote),
			prevClose:     make(map[string]float64),
		},
		orders: orderTracker{
			orderSymbol:   make(map[int64]string),
			orderAction:   make(map[int64]string),
			orderRoutes:   make(map[int64]subRoute),
			orderQty:      make(map[int64]float64),
			orderFilled:   make(map[int64]float64),
			pendingExecs:  make(map[string]pendingExec),
			inFlightSell:  make(map[string]float64),
			orderInFlight: make(map[int64]inFlightSellRef),
		},
		pos: posTracker{
			posMap:       make(map[string]PositionInfo),
			pendingClose: make(map[string]PositionInfo),
		},
		scanner: scanTracker{
			nextID:     reqIDScannerBase,
			results:    make(map[int64][]ScanResult),
			enriched:   make(map[int64][]ScanResult),
			pending:    make(map[int64]chan []ScanResult),
			snapData:   make(map[int64]*scanSnapEntry),
			snapCount:  make(map[int64]int),
			snapReqIDs: make(map[int64][]int64),
			cdData:     make(map[int64]*scanCDEntry),
			cdCount:    make(map[int64]int),
			cdReqIDs:   make(map[int64][]int64),
		},
		optChain: optionChainTracker{
			nextConIDID:      reqIDOptConIDBase,
			nextChainID:      reqIDOptChainBase,
			nextMktID:        reqIDOptMktBase,
			nextPosID:        reqIDPosMktBase,
			conIDReqs:        make(map[int64]*optConIDReq),
			chainReqs:        make(map[int64]*optChainReq),
			legs:             make(map[legKey]*optLeg),
			legByReqID:       make(map[int64]legKey),
			selCurrent:       make(map[int]legKey),
			selPending:       make(map[int]pendingSwap),
			retries:          make(map[int]*optStrikeRetry),
			deltaCands:       make(map[int64]*deltaCandidate),
			deltaRes:         make(map[int]*deltaResolution),
			lastIV:           make(map[string]float64),
			lastChainInfo:    make(map[chainKey]chainSnapshot),
			resolvedEntry:    make(map[int]resolvedEntryLeg),
			lastEntryFailure: make(map[int]EntryStrikeResult),
			lastProbeLaunch:  make(map[int]time.Time),
			lastAttempt:      make(map[int]time.Time),
			forcedResub:      make(map[legKey]resubState),
		},
		onDemand: onDemandTracker{
			nextID:    reqIDOnDemandHistBase,
			reqSymbol: make(map[int64]string),
			done:      make(map[int64]chan error),
		},
		optQuery: optionQueryTracker{
			nextConID:   reqIDOptQueryConIDBase,
			nextChainID: reqIDOptQueryChainBase,
			conIDReqs:   make(map[int64]*optQueryReq),
			chainReqs:   make(map[int64]*optQueryReq),
		},
		acct: acctTracker{accounts: make(map[string]AccountSummary)},

		mdLines: mdlines.NewLedger(opts.MaxMarketDataLines, opts.MaxHistoricalStreams),

		logger:    opts.Logger,
		scanLog:   writerLogger(opts.ScanLog),
		optionLog: writerLogger(opts.OptionLog),
		acctLog:   writerLogger(opts.AccountLog),
	}
}

// Done returns a channel that is closed when this session's connection ends
// (cleanly or otherwise). Useful for a caller that wants to run its own
// background work (e.g. re-subscribing to position-pinned data) alongside
// Run without outliving the session. Only valid after Connect succeeds.
func (s *Session) Done() <-chan struct{} {
	return s.client.Ctx().Done()
}

// Connect dials TWS / IB Gateway, waits for the ready signal, and verifies
// the configured trading account is among the accounts TWS reports for the
// current login. It does not subscribe to anything — call Run for that.
func (s *Session) Connect(ctx context.Context) error {
	client := ibapi.NewEClient(s)
	s.client = client

	if err := client.Connect(s.opts.Host, s.opts.Port, s.opts.ClientID); err != nil {
		s.logger.Println("Connection failed:", err)
		return ErrConnect
	}

	select {
	case <-s.ready:
	case <-time.After(s.opts.ConnectTimeout):
		s.logger.Println("Timed out waiting for TWS ready signal")
		client.Disconnect()
		return ErrReady
	case <-client.Ctx().Done():
		return ErrConnect
	case <-ctx.Done():
		client.Disconnect()
		return ctx.Err()
	}

	client.ReqManagedAccts()
	select {
	case accounts := <-s.accountsReady:
		if !slices.Contains(accounts, s.opts.TradingAccount) {
			s.logger.Printf("Configured trading account %q not found in TWS accounts %v", s.opts.TradingAccount, accounts)
			client.Disconnect()
			return &AccountMismatchError{Configured: s.opts.TradingAccount, Actual: accounts}
		}
	case <-time.After(s.opts.ConnectTimeout):
		s.logger.Println("Timed out waiting for TWS managed accounts list")
		client.Disconnect()
		return ErrReady
	case <-client.Ctx().Done():
		return ErrConnect
	case <-ctx.Done():
		client.Disconnect()
		return ctx.Err()
	}
	return nil
}

// Run subscribes every subscriber's symbols, calls each Subscriber's
// OnConnected hook, and blocks until either stop is reached or the
// connection drops. Call Connect first.
//
// Returns:
//   - (true,  nil) — stop time reached, clean shutdown
//   - (false, nil) — mid-session disconnect, caller should reconnect
func (s *Session) Run(subs []Subscriber, stop time.Time) (bool, error) {
	s.Candles.Reset()
	client := s.client

	seen := make(map[string]bool)
	var allSymbols []SymbolSpec
	var deduped []SymbolSpec
	for _, sub := range subs {
		for _, sy := range sub.Symbols() {
			allSymbols = append(allSymbols, sy)
			if !seen[sy.Symbol] {
				seen[sy.Symbol] = true
				deduped = append(deduped, sy)
			}
		}
	}
	var buses []*eventbus.Bus
	for _, sub := range subs {
		buses = append(buses, sub.Bus())
	}
	subSymbols := make([][]SymbolSpec, len(subs))
	for i, sub := range subs {
		subSymbols[i] = sub.Symbols()
	}

	s.symMu.Lock()
	s.subs = subs
	s.buses = buses
	s.reqSymbol = make(map[int64]string, len(deduped))
	s.symbols = allSymbols
	s.subSymbols = subSymbols
	s.histReqID = make(map[string]int64, len(deduped))
	s.streamReqID = make(map[string]int64, len(deduped))
	s.nextHistID = reqIDHistBase
	s.nextStreamID = reqIDStreamBase
	s.mktData.mktDataSymbol = make(map[int64]string, len(deduped))
	// reqIDs are assigned from a counter rather than the loop index because
	// ResyncSymbols hands out more of them later, after the initial block has
	// developed holes — see subscribeSymbolLocked.
	type sub struct {
		spec          SymbolSpec
		histID, mktID int64
	}
	pending := make([]sub, 0, len(deduped))
	for _, sy := range deduped {
		histID, mktID := s.reserveSymbolIDsLocked(sy.Symbol)
		pending = append(pending, sub{spec: sy, histID: histID, mktID: mktID})
	}
	s.symMu.Unlock()

	ctx := client.Ctx()
	for _, sb := range subs {
		sb.OnConnected(ctx)
	}

	s.liveStart = time.Now()

	for _, p := range pending {
		if !s.mdLines.GrantHist(p.histID) {
			_, _, _, histMax, _, _ := s.mdLines.StatusAll()
			s.logger.Printf("Live bars: %s SKIPPED — over the %d concurrent keepUpToDate stream limit; this symbol will get NO live bars. Trim the watchlist or raise MaxHistoricalStreams.", p.spec.Symbol, histMax)
			continue
		}
		client.ReqHistoricalData(p.histID, p.spec.Contract, "", "1 D", "30 secs", "TRADES", false, 1, true, nil)
	}

	for _, p := range pending {
		s.mdLines.GrantGuaranteed(p.mktID, mdlines.CategoryStock)
		client.ReqMktData(p.mktID, p.spec.Contract, "", false, false, nil)
	}

	client.ReqAccountSummary(reqIDAcctSummary, "All", "AccountType,TradingType,AccountCode,AccountAlias,NetLiquidation,TotalCashValue,AvailableFunds,BuyingPower,MaintExcessLiquidity,Leverage,AccountStatusLocked,AccountStatusPendingApproval,Cushion,FullInitMarginReq,FullMaintMarginReq,InitMarginReq,LookAheadAvailableFunds,LookAheadInitMarginReq,SMA")
	client.ReqPositions()

	go func() {
		select {
		case <-time.After(3 * time.Second):
			s.requestOptionChains()
		case <-ctx.Done():
		}
	}()

	go func() {
		ticker := time.NewTicker(s.opts.PositionRefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				for _, reqID := range s.mdLines.ReapSnapshots(mdlines.SnapshotMaxAge) {
					client.CancelMktData(reqID)
				}
				for _, reqID := range s.mdLines.ReapProbes(mdlines.ProbeMaxAge) {
					client.CancelMktData(reqID)
					s.releaseOrphanedProbeCandidate(reqID)
				}
				s.refreshPositions()
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(s.opts.OptionRotationInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.rotateOptionStrikes()
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(barWatchdogInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				now := time.Now()
				if !s.barFeedStalled(now) {
					continue
				}
				last := time.Unix(0, s.lastBarNano.Load())
				s.logger.Printf("Bar-feed watchdog: no live bar for %s (last %s) — forcing reconnect",
					now.Sub(last).Round(time.Second), last.Format("15:04:05"))
				if s.opts.OnStall != nil {
					s.opts.OnStall()
				}
				s.cancelAllSubscriptions()
				client.Disconnect()
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
		return false, nil
	case <-time.After(time.Until(stop)):
		s.logger.Println("Stop time reached, shutting down")
		s.cancelAllSubscriptions()
		client.Disconnect()
		return true, nil
	}
}

// cancelAllSubscriptions issues a broker cancel for every market-data line
// and keepUpToDate historical-bar stream this session opened.
func (s *Session) cancelAllSubscriptions() {
	lineIDs := s.mdLines.AllReqIDs()
	histIDs := s.mdLines.AllHistReqIDs()
	for _, reqID := range lineIDs {
		s.client.CancelMktData(reqID)
	}
	for _, reqID := range histIDs {
		s.client.CancelHistoricalData(reqID)
	}
	if n := len(lineIDs) + len(histIDs); n > 0 {
		s.logger.Printf("Teardown: cancelled %d market-data line(s) and %d historical stream(s)", len(lineIDs), len(histIDs))
	}
}

// publish fans evt out to all subscriber buses.
func (s *Session) publish(evt eventbus.Event) {
	for _, b := range s.buses {
		b.Publish(evt)
	}
}

// publishTo routes evt only to the named subscriber buses (by index into s.buses).
func (s *Session) publishTo(busIdxs []int, evt eventbus.Event) {
	for _, i := range busIdxs {
		if i >= 0 && i < len(s.buses) {
			s.buses[i].Publish(evt)
		}
	}
}

// busIndex returns the index of bus b in s.buses, or -1 if not found.
func (s *Session) busIndex(b *eventbus.Bus) int {
	for i, bb := range s.buses {
		if bb == b {
			return i
		}
	}
	return -1
}

// updateAllHubs records a bar in the shared quote book and notifies the
// caller's own position-price display, if wired.
func (s *Session) updateBookPrice(symbol string, price float64, date string) {
	if s.book != nil {
		s.book.SetStockBar(symbol, price, date)
	}
}

// bookOption records an option-data event in the central quote book, keyed
// by the exact contract, so every reader sees the identical quote.
func (s *Session) bookOption(od eventbus.OptionData) {
	if s.book == nil || od.Strike <= 0 || od.Expiry == "" {
		return
	}
	key := quotes.ContractKey{Symbol: od.Symbol, Right: od.Right, Strike: od.Strike, Expiry: od.Expiry}
	s.book.SetOptionBid(key, od.Bid)
	s.book.SetOptionAsk(key, od.Ask)
	s.book.SetOptionLast(key, od.Price)
	s.book.SetOptionGreeks(key, od.Delta, od.IV, od.DeltaSource)
}

// resolveReqID looks up the symbol associated with a request ID across all
// known reqID→symbol maps, for error-log context.
func (s *Session) resolveReqID(reqID int64) string {
	if reqID == -1 {
		return "n/a"
	}
	if sym := s.histSymbol(reqID); sym != "" {
		return sym + " (hist)"
	}
	if sym, ok := s.streamSymbol(reqID); ok {
		return sym + " (stream)"
	}
	if sym, ok := s.mktData.snapReqSymbol[reqID]; ok {
		return sym + " (snap)"
	}
	s.orders.orderMu.Lock()
	sym, ok := s.orders.orderSymbol[reqID]
	s.orders.orderMu.Unlock()
	if ok {
		return sym + " (order)"
	}
	s.optChain.mu.Lock()
	if leg, ok := s.legByReqIDLocked(reqID); ok {
		kind := "opt-mkt"
		if leg.pins > 0 {
			kind = "opt-pinned"
		}
		r := fmt.Sprintf("%s %s strike=%.2f expiry=%s (%s)", leg.symbol, leg.right, leg.strike, leg.expiry, kind)
		s.optChain.mu.Unlock()
		return r
	}
	if req, ok := s.optChain.chainReqs[reqID]; ok {
		r := req.chain.symbol + " (opt-chain)"
		s.optChain.mu.Unlock()
		return r
	}
	if req, ok := s.optChain.conIDReqs[reqID]; ok {
		r := req.chain.symbol + " (opt-conid)"
		s.optChain.mu.Unlock()
		return r
	}
	// Entry delta probes were missing here, so every IB error against one —
	// including the not-subscribed family, the errors most worth reading —
	// logged "symbol=unknown" and could not be tied back to the contract that
	// provoked it.
	if cand, ok := s.optChain.deltaCands[reqID]; ok {
		r := fmt.Sprintf("%s %s strike=%.2f expiry=%s (opt-probe)", cand.symbol, cand.right, cand.strike, cand.expiry)
		s.optChain.mu.Unlock()
		return r
	}
	s.optChain.mu.Unlock()
	s.onDemand.mu.Lock()
	if sym, ok := s.onDemand.reqSymbol[reqID]; ok {
		s.onDemand.mu.Unlock()
		return sym + " (on-demand)"
	}
	s.onDemand.mu.Unlock()
	return "unknown"
}

// Error is called when IB sends an error or warning message.
func (s *Session) Error(reqID int64, errTime int64, errCode int64, errString string, advancedOrderRejectJSON string) {
	sym := s.resolveReqID(reqID)
	// Codes in [2000,10000) are IB's informational/warning band (farm
	// connection notices and the like); everything outside it is a real
	// error. Both are logged, but only the latter is labelled as such, so the
	// severity is greppable. This used to emit a second, byte-identical line
	// for real errors, which distinguished nothing and just doubled them.
	label := "IB Notice"
	if errCode < 2000 || errCode >= 10000 {
		label = "IB Error"
	}
	s.logger.Printf("%s: reqID=%d symbol=%s code=%d msg=%s", label, reqID, sym, errCode, errString)

	// Attribute the error to an in-flight entry delta probe, if this reqID is
	// one. Purely observational — see noteCandidateError. Only code 200 gets
	// the full handleOptionMktError treatment below, which is why every OTHER
	// code (354/10167/10197 and the rest of the not-subscribed family — the
	// ones that actually explain a permanently quote-less account) used to
	// vanish into the log line above and never reach the trading decision that
	// they caused to fail.
	s.noteCandidateError(reqID, errCode, errString)

	if errCode == 200 {
		if s.handleOptionMktError(reqID, errString) {
			return
		}
		s.onDemand.mu.Lock()
		if ch, ok := s.onDemand.done[reqID]; ok {
			select {
			case ch <- fmt.Errorf("unknown symbol"):
			default:
			}
		}
		s.onDemand.mu.Unlock()
	}

	if errCode < 1000 && sym != "" {
		s.orders.orderMu.Lock()
		action, isOrder := s.orders.orderAction[reqID]
		route := s.orders.orderRoutes[reqID]
		s.orders.orderMu.Unlock()
		if isOrder {
			s.settleInFlightSell(reqID)

			if s.opts.OnError != nil {
				s.opts.OnError(ErrorEvent{
					Type:    "order",
					Message: fmt.Sprintf("IB rejected %s %s order (code %d): %s", action, sym, errCode, errString),
				})
			}
			if route.bus != nil {
				route.bus.Publish(eventbus.Event{
					Kind: eventbus.KindOrderRejected,
					Payload: eventbus.OrderRejected{
						OrderID: reqID, Symbol: sym, Tag: route.tag, Action: action,
						Code: errCode, Message: errString,
					},
				})
			}
			s.logger.Printf("Order %d for %s (%s) rejected: %s", reqID, sym, action, errString)
		}
	}

	if _, ok := s.mktData.snapReqSymbol[reqID]; ok {
		s.mdLines.Release(reqID)
		s.client.CancelMktData(reqID)
		delete(s.mktData.snapReqSymbol, reqID)
		return
	}

	s.scanner.mu.Lock()
	if snap, ok := s.scanner.snapData[reqID]; ok {
		parentReqID := snap.parentReqID
		s.mdLines.Release(reqID)
		delete(s.scanner.snapData, reqID)
		s.scanner.snapCount[parentReqID]--
		finalResults, ch := s.checkAndFinalize(parentReqID)
		s.scanner.mu.Unlock()
		s.client.CancelMktData(reqID)
		if ch != nil {
			s.scanLog.Printf("Scanner: reqID=%d snap error, finalising %d results", parentReqID, len(finalResults))
			select {
			case ch <- finalResults:
			default:
			}
		}
		return
	}
	if cd, ok := s.scanner.cdData[reqID]; ok {
		parentReqID := cd.parentReqID
		delete(s.scanner.cdData, reqID)
		s.scanner.cdCount[parentReqID]--
		finalResults, ch := s.checkAndFinalize(parentReqID)
		s.scanner.mu.Unlock()
		if ch != nil {
			s.scanLog.Printf("Scanner: reqID=%d CD error, finalising %d results", parentReqID, len(finalResults))
			select {
			case ch <- finalResults:
			default:
			}
		}
		return
	}
	s.scanner.mu.Unlock()

	if errCode == 10089 {
		s.errors.subErrMu.Lock()
		if !s.errors.subscriptionErrorSent {
			s.errors.subscriptionErrorSent = true
			s.errors.subErrMu.Unlock()
			if s.opts.OnError != nil {
				s.opts.OnError(ErrorEvent{
					Type:    "subscription",
					Message: "Some symbols require additional market data subscriptions.",
				})
			}
		} else {
			s.errors.subErrMu.Unlock()
		}
	}
}

// NextValidID is called by TWS after the connection handshake completes.
func (s *Session) NextValidID(reqID int64) {
	s.Wrapper.NextValidID(reqID)
	s.orders.orderMu.Lock()
	s.orders.nextOrderID = reqID
	s.orders.orderMu.Unlock()
	select {
	case s.ready <- struct{}{}:
	default:
	}
}

// ManagedAccounts is called by TWS in response to ReqManagedAccts.
func (s *Session) ManagedAccounts(accountsList []string) {
	s.Wrapper.ManagedAccounts(accountsList)
	var accounts []string
	for _, acct := range accountsList {
		if acct = strings.TrimSpace(acct); acct != "" {
			accounts = append(accounts, acct)
		}
	}
	select {
	case s.accountsReady <- accounts:
	default:
	}
}

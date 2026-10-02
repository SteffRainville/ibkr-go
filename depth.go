package ibkr

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/scmhub/ibapi"
)

// Level II (market depth) sampler.
//
// IB serves only a handful of simultaneous depth books (about 3 by default,
// scaled by account tier) and counts them apart from the 100 market-data
// lines, so depth cannot be streamed for every watched symbol. Instead a
// small pool of workers (Options.DepthSlots, default 1) takes one symbol at a
// time: subscribe, let the book fill for Options.DepthSampleDuration, copy it
// out, cancel, move to the next symbol round-robin. The latest copy per
// symbol is what DepthSnapshot returns, stamped with its age — readers
// decide when it is too old to trust. Depth never touches the quote Book or
// the mdlines ledger: the cap is the worker count, and a worker always cancels
// its own request, so the number of live books can never exceed it.
//
// IB has no historical depth, so nothing can be backfilled: a reading exists
// only for a sample taken while the process was running.

const (
	depthRows          = 10 // rows requested per side
	defaultDepthSample = 3 * time.Second

	// IB's operation / side codes in UpdateMktDepth.
	depthInsert, depthUpdate, depthDelete = 0, 1, 2
	depthSideBid                          = 1 // side 0 is the ask
)

// DepthLevel is one price level of a book side.
type DepthLevel struct {
	Price float64
	Size  float64 // shares (lots on some venues — IB reports what the venue does)
}

// DepthSnapshot is one completed sample of a symbol's book. Bids are best
// (highest) first, Asks best (lowest) first.
type DepthSnapshot struct {
	Symbol  string
	At      time.Time // when the sample was taken
	Bids    []DepthLevel
	Asks    []DepthLevel
	Err     string // set when IB refused the request (no subscription, limit, …)
	ErrCode int64  // IB's error code for Err (309 = too many depth books); 0 when Err is empty
}

// liveBook is a book being filled by UpdateMktDepth callbacks for one request.
// IB addresses rows by position, so each side is a slice edited in place.
type liveBook struct {
	symbol     string
	bids, asks []DepthLevel
	gotRows    bool
	err        string
	errCode    int64
	done       chan struct{} // closed on the first IB error so the worker stops waiting
	doneOnce   sync.Once
}

type depthTracker struct {
	mu     sync.Mutex
	live   map[int64]*liveBook
	latest map[string]DepthSnapshot
	next   int // round-robin cursor into the symbol list
}

func (t *depthTracker) init() {
	t.mu.Lock()
	t.live = make(map[int64]*liveBook)
	t.latest = make(map[string]DepthSnapshot)
	t.next = 0
	t.mu.Unlock()
}

// insertRow / updateRow / deleteRow apply one IB row operation to a side.
func applyDepthRow(side []DepthLevel, op, pos int, lvl DepthLevel) []DepthLevel {
	if pos < 0 || pos > 64 {
		return side
	}
	switch op {
	case depthInsert:
		if pos > len(side) {
			pos = len(side)
		}
		side = append(side, DepthLevel{})
		copy(side[pos+1:], side[pos:])
		side[pos] = lvl
	case depthUpdate:
		if pos < len(side) {
			side[pos] = lvl
		}
	case depthDelete:
		if pos < len(side) {
			side = append(side[:pos], side[pos+1:]...)
		}
	}
	return side
}

func (t *depthTracker) apply(reqID int64, pos, op, side int64, price float64, size float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	b, ok := t.live[reqID]
	if !ok {
		return // a late update for a cancelled sample
	}
	b.gotRows = true
	lvl := DepthLevel{Price: price, Size: size}
	if side == depthSideBid {
		b.bids = applyDepthRow(b.bids, int(op), int(pos), lvl)
	} else {
		b.asks = applyDepthRow(b.asks, int(op), int(pos), lvl)
	}
}

// UpdateMktDepth receives one row change of a non-SMART depth book.
func (s *Session) UpdateMktDepth(reqID int64, position int64, operation int64, side int64, price float64, size ibapi.Decimal) {
	s.depth.apply(reqID, position, operation, side, price, size.Float())
}

// UpdateMktDepthL2 receives one row change of a (SMART-aggregated) depth book.
func (s *Session) UpdateMktDepthL2(reqID int64, position int64, marketMaker string, operation int64, side int64, price float64, size ibapi.Decimal, isSmartDepth bool) {
	s.depth.apply(reqID, position, operation, side, price, size.Float())
}

// noteDepthError attaches an IB error to the depth sample it belongs to and
// wakes its worker. Reports whether reqID was a depth request.
func (s *Session) noteDepthError(reqID int64, errCode int64, errString string) bool {
	s.depth.mu.Lock()
	defer s.depth.mu.Unlock()
	b, ok := s.depth.live[reqID]
	if !ok {
		return false
	}
	// Informational notices (2xxx) don't end a sample.
	if errCode >= 2000 && errCode < 10000 {
		return true
	}
	b.err, b.errCode = errString, errCode
	b.doneOnce.Do(func() { close(b.done) })
	return true
}

// DepthSnapshot returns the latest completed sample for symbol.
func (s *Session) DepthSnapshot(symbol string) (DepthSnapshot, bool) {
	s.depth.mu.Lock()
	defer s.depth.mu.Unlock()
	snap, ok := s.depth.latest[symbol]
	return snap, ok
}

// depthSymbols is the sampler's work list: every distinct tracked symbol's
// contract, in a stable order.
func (s *Session) depthSymbols() []SymbolSpec {
	s.symMu.RLock()
	seen := make(map[string]bool, len(s.symbols))
	out := make([]SymbolSpec, 0, len(s.symbols))
	for _, sp := range s.symbols {
		if sp.Contract == nil || seen[sp.Symbol] {
			continue
		}
		seen[sp.Symbol] = true
		out = append(out, sp)
	}
	s.symMu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Symbol < out[j].Symbol })
	return out
}

// pickDepthSymbol advances the shared round-robin cursor.
func (s *Session) pickDepthSymbol() (SymbolSpec, bool) {
	syms := s.depthSymbols()
	if len(syms) == 0 {
		return SymbolSpec{}, false
	}
	s.depth.mu.Lock()
	defer s.depth.mu.Unlock()
	sp := syms[s.depth.next%len(syms)]
	s.depth.next++
	return sp, true
}

// startDepthSampler launches the worker pool. It is a no-op unless the caller
// enabled depth (Options.DepthSlots > 0), because depth needs a market-data
// subscription the account may not have.
func (s *Session) startDepthSampler(ctx context.Context) {
	slots := s.opts.DepthSlots
	if slots <= 0 {
		return
	}
	sample := s.opts.DepthSampleDuration
	if sample <= 0 {
		sample = defaultDepthSample
	}
	s.depth.init()
	for range slots {
		go s.depthWorker(ctx, sample)
	}
}

func (s *Session) depthWorker(ctx context.Context, sample time.Duration) {
	pacer := &reqPacer{}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if w := s.opts.DepthWindow; w != nil && !w(time.Now()) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
			continue
		}
		sp, ok := s.pickDepthSymbol()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}
		s.sampleDepth(ctx, sp, sample, pacer)
	}
}

// sampleDepth takes one sample of sp's book and stores it. The request is
// always cancelled before it returns.
func (s *Session) sampleDepth(ctx context.Context, sp SymbolSpec, sample time.Duration, pacer *reqPacer) {
	reqID := s.nextReqID()
	b := &liveBook{symbol: sp.Symbol, done: make(chan struct{})}
	s.depth.mu.Lock()
	s.depth.live[reqID] = b
	s.depth.mu.Unlock()

	s.client.ReqMktDepth(reqID, sp.Contract, depthRows, true, nil)
	pacer.pace()

	timer := time.NewTimer(sample)
	select {
	case <-timer.C:
	case <-b.done:
		timer.Stop()
	case <-ctx.Done():
		timer.Stop()
	}

	s.client.CancelMktDepth(reqID, true)

	s.depth.mu.Lock()
	delete(s.depth.live, reqID)
	snap := DepthSnapshot{
		Symbol:  sp.Symbol,
		At:      time.Now(),
		Bids:    append([]DepthLevel(nil), b.bids...),
		Asks:    append([]DepthLevel(nil), b.asks...),
		Err:     b.err,
		ErrCode: b.errCode,
	}
	// A refused or empty sample must not overwrite a good older one; the
	// reader sees the older reading's real age instead of a blank.
	if prev, had := s.depth.latest[sp.Symbol]; snap.Err != "" || !b.gotRows {
		if !had || prev.Err != "" || len(prev.Bids)+len(prev.Asks) == 0 {
			s.depth.latest[sp.Symbol] = snap
		}
	} else {
		s.depth.latest[sp.Symbol] = snap
	}
	s.depth.mu.Unlock()

	if s.opts.OnDepth != nil {
		s.opts.OnDepth(snap)
	}
}

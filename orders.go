package ibkr

import (
	"fmt"
	"math"
	"time"

	"github.com/scmhub/ibapi"

	"github.com/SteffRainville/ibkr-go/eventbus"
)

// contractForSymbol returns the IB contract for a tracked symbol, or nil.
func (s *Session) contractForSymbol(symbol string) *ibapi.Contract {
	for _, sy := range s.symbolSpecs() {
		if sy.Symbol == symbol {
			return sy.Contract
		}
	}
	return nil
}

// isRTH returns true if the current local time is within US regular trading hours.
func isRTH() bool {
	return isRTHAt(time.Now())
}

// isRTHAt reports whether t (local time) is within US regular trading hours
// (weekday, 09:30–16:00). Split out from isRTH so callers that already hold
// a timestamp can test it deterministically.
func isRTHAt(t time.Time) bool {
	if wd := t.Weekday(); wd == time.Saturday || wd == time.Sunday {
		return false
	}
	hhmm := t.Hour()*100 + t.Minute()
	return hhmm >= 930 && hhmm < 1600
}

// isOptionTag reports whether tag identifies an option leg. "call"/"put" are
// the recognized option-leg vocabulary the option-chain engine understands;
// any other tag (e.g. "long"/"short", or an arbitrary caller label for a
// non-option symbol) is treated as a stock leg. This is the one place Tag is
// not fully opaque — everywhere else it is threaded through untouched.
func isOptionTag(tag string) bool {
	return tag == "call" || tag == "put"
}

// placedOrder bundles everything placeOrder needs to submit one order leg.
// qtyShares is hub-facing quantity (shares for stocks; options: contracts ×
// 100); placeOrder converts it to IB wire units (contracts) for options.
type placedOrder struct {
	bus        *eventbus.Bus
	contract   *ibapi.Contract
	action     string // internal action: BUY/SELL/SHORT/COVER
	tag        string
	secType    string // "STK" | "OPT"
	qtyShares  float64
	strike     float64
	expiry     string
	underlying float64
	optMid     float64
	optBid     float64
	optAsk     float64
	priority   string
	category   string
	// sellReserveKey, when non-empty, is the optSellKey reservation this
	// SELL/COVER order holds against the naked-short gate.
	sellReserveKey string
}

// placeOrder submits an order to TWS. Two modes based on time-of-day:
//
//   - RTH (09:30–16:00): Adaptive Algo MKT order via SMART routing.
//     SMART routing is mandatory — directed exchanges are rejected with
//     error 442 when combined with Adaptive. Exchange is always overridden.
//
//   - Outside RTH: Limit order at current bid (SELL/SHORT) or ask
//     (BUY/COVER). Requires a live quote less than bidAskMaxAge old.
func (s *Session) placeOrder(r placedOrder) (int64, error) {
	contract := r.contract
	action := r.action

	wireQty := r.qtyShares
	if r.secType == "OPT" {
		wireQty = r.qtyShares / 100
	}

	ibAction := action
	if action == "SHORT" {
		ibAction = "SELL"
	} else if action == "COVER" {
		ibAction = "BUY"
	}

	s.orders.orderMu.Lock()
	orderID := s.orders.nextOrderID
	s.orders.nextOrderID++
	s.orders.orderSymbol[orderID] = contract.Symbol
	s.orders.orderAction[orderID] = action
	s.orders.orderRoutes[orderID] = subRoute{bus: r.bus, secType: r.secType, tag: r.tag}
	s.orders.orderQty[orderID] = r.qtyShares
	s.orders.orderFilled[orderID] = 0
	if r.sellReserveKey != "" {
		s.orders.orderInFlight[orderID] = inFlightSellRef{key: r.sellReserveKey, qty: r.qtyShares}
	}
	s.orders.orderMu.Unlock()

	cleanup := func() {
		s.orders.orderMu.Lock()
		delete(s.orders.orderSymbol, orderID)
		delete(s.orders.orderAction, orderID)
		delete(s.orders.orderRoutes, orderID)
		delete(s.orders.orderQty, orderID)
		delete(s.orders.orderFilled, orderID)
		delete(s.orders.orderInFlight, orderID)
		s.orders.orderMu.Unlock()
	}

	order := ibapi.NewOrder()
	order.OrderID = orderID
	order.Action = ibAction
	order.TotalQuantity = ibapi.StringToDecimal(fmt.Sprintf("%.0f", wireQty))
	order.OrderType = "MKT"
	order.TIF = "DAY"
	order.Transmit = true
	order.Account = s.tradingAccount

	if isRTH() {
		smartContract := *contract
		smartContract.Exchange = "SMART"
		contract = &smartContract

		prio := r.priority
		if prio == "" {
			prio = "Normal"
		}
		order.OrderType = "MKT"
		order.AlgoStrategy = "Adaptive"
		order.AlgoParams = []ibapi.TagValue{{Tag: "adaptivePriority", Value: prio}}
		s.logger.Printf("Placing %s Adaptive/MKT prio=%s %.0f %s %s account=%s RTH (orderID=%d)",
			action, prio, wireQty, r.secType, contract.Symbol, s.tradingAccount, orderID)
	} else {
		var quotePrice float64
		var quoteTime time.Time
		if r.secType == "OPT" {
			switch action {
			case "BUY", "COVER":
				quotePrice = r.optAsk
			default:
				quotePrice = r.optBid
			}
			quoteTime = time.Now()
		} else {
			s.mktData.bidAskMu.RLock()
			var q quote
			switch action {
			case "BUY", "COVER":
				q = s.mktData.ask[contract.Symbol]
			default:
				q = s.mktData.bid[contract.Symbol]
			}
			s.mktData.bidAskMu.RUnlock()
			quotePrice = q.Price
			quoteTime = q.Time
		}

		priceType := map[string]string{"BUY": "ask", "COVER": "ask", "SELL": "bid", "SHORT": "bid"}[action]

		if quotePrice <= 0 {
			cleanup()
			return 0, fmt.Errorf("no %s price for %s yet — try again in a moment", priceType, contract.Symbol)
		}
		if r.secType != "OPT" {
			if age := time.Since(quoteTime); age > bidAskMaxAge {
				cleanup()
				return 0, fmt.Errorf("stale %s quote for %s (%.0fs old — exceeds 30s threshold)", priceType, contract.Symbol, age.Seconds())
			}
		}
		order.OrderType = "LMT"
		order.LmtPrice = quotePrice
		order.OutsideRTH = true
		s.logger.Printf("Placing %s LMT %.4f %.0f %s %s account=%s outsideRTH (orderID=%d)",
			action, quotePrice, wireQty, r.secType, contract.Symbol, s.tradingAccount, orderID)
	}

	s.client.PlaceOrder(orderID, contract, order)
	return orderID, nil
}

// OrderStatus is called by TWS whenever an order's status changes.
//
// BUY/SHORT: filled/remaining are IB's CUMULATIVE counts for this orderID
// (stable for the order's whole life), so every callback is processed here,
// applying only the delta above what was already applied
// (s.orders.orderFilled). The same cumulative report arriving twice can
// never be double-applied (delta is zero). SELL/COVER remain
// terminal-fill-only.
func (s *Session) OrderStatus(orderID int64, status string, filled ibapi.Decimal, remaining ibapi.Decimal,
	avgFillPrice float64, permID int64, parentID int64, lastFillPrice float64,
	clientID int64, whyHeld string, mktCapPrice float64) {

	s.orders.orderMu.Lock()
	sym := s.orders.orderSymbol[orderID]
	act := s.orders.orderAction[orderID]
	route := s.orders.orderRoutes[orderID]
	reqQty := s.orders.orderQty[orderID]
	prevFilled := s.orders.orderFilled[orderID]
	s.orders.orderMu.Unlock()

	s.logger.Printf("OrderStatus: orderID=%d symbol=%s action=%s status=%s filled=%s remaining=%s avgFill=%.4f",
		orderID, sym, act, status, filled, remaining, avgFillPrice)

	switch {
	case sym == "":
		// Untracked order ID — nothing below applies.

	case act == "BUY" || act == "SHORT":
		cumQty := float64(filled.Int())
		remQty := float64(remaining.Int())
		if route.secType == "OPT" {
			cumQty *= 100
			remQty *= 100
		}

		if delta := cumQty - prevFilled; delta > 0.5 {
			s.orders.orderMu.Lock()
			s.orders.orderFilled[orderID] = cumQty
			s.orders.orderMu.Unlock()

			if route.bus != nil {
				route.bus.Publish(eventbus.Event{
					Kind: eventbus.KindOrderFilled,
					Payload: eventbus.OrderFilled{
						OrderID: orderID, Symbol: sym, Action: act, Tag: route.tag,
						Qty: delta, Remaining: remQty, AvgFill: avgFillPrice,
					},
				})
			}
		}

		if status == "Filled" && remaining.Int() == 0 {
			// A "Filled" status with a final cumulative quantity that doesn't
			// match what was requested at placement means IB delivered more
			// (or less) than the order asked for. Surface this loudly.
			if reqQty > 0 && math.Abs(cumQty-reqQty) > 0.5 {
				msg := fmt.Sprintf("order %d %s %s %s: requested %.0f, IB filled %.0f (Δ%+.0f)",
					orderID, act, sym, route.tag, reqQty, cumQty, cumQty-reqQty)
				s.logger.Printf("FILL QUANTITY MISMATCH: %s", msg)
				if s.opts.OnError != nil {
					s.opts.OnError(ErrorEvent{Type: "order", Message: "FILL QTY MISMATCH: " + msg})
				}
				if route.bus != nil {
					route.bus.Publish(eventbus.Event{
						Kind: eventbus.KindOrderQtyMismatch,
						Payload: eventbus.OrderQtyMismatch{
							OrderID: orderID, Symbol: sym, Tag: route.tag, Action: act,
							Requested: reqQty, Filled: cumQty,
						},
					})
				}
			}

			s.logger.Printf("Order %d for %s filled", orderID, sym)

			s.settleInFlightSell(orderID)
			s.orders.orderMu.Lock()
			delete(s.orders.orderSymbol, orderID)
			delete(s.orders.orderAction, orderID)
			delete(s.orders.orderRoutes, orderID)
			delete(s.orders.orderQty, orderID)
			delete(s.orders.orderFilled, orderID)
			s.orders.orderMu.Unlock()
		}

	case status == "Filled" && remaining.Int() == 0 && (act == "SELL" || act == "COVER"):
		qty := float64(filled.Int())
		if route.secType == "OPT" {
			qty *= 100
		}

		if route.bus != nil {
			route.bus.Publish(eventbus.Event{
				Kind: eventbus.KindOrderFilled,
				Payload: eventbus.OrderFilled{
					OrderID: orderID, Symbol: sym, Action: act, Tag: route.tag,
					Qty: qty, AvgFill: avgFillPrice,
				},
			})
		}

		s.logger.Printf("Order %d for %s filled", orderID, sym)

		s.settleInFlightSell(orderID)
		s.orders.orderMu.Lock()
		delete(s.orders.orderSymbol, orderID)
		delete(s.orders.orderAction, orderID)
		delete(s.orders.orderRoutes, orderID)
		delete(s.orders.orderQty, orderID)
		delete(s.orders.orderFilled, orderID)
		s.orders.orderMu.Unlock()

	case status == "Cancelled" || status == "ApiCancelled" || status == "Inactive":
		s.settleInFlightSell(orderID)
		s.orders.orderMu.Lock()
		delete(s.orders.orderSymbol, orderID)
		delete(s.orders.orderAction, orderID)
		delete(s.orders.orderRoutes, orderID)
		delete(s.orders.orderQty, orderID)
		delete(s.orders.orderFilled, orderID)
		s.orders.orderMu.Unlock()
	}
}

// ExecDetails is called by TWS with execution details for each order fill.
// It caches the fill data (symbol, action, price, qty) keyed by ExecID so
// that CommissionAndFeesReport can assemble the complete commission event
// when it arrives.
func (s *Session) ExecDetails(reqID int64, contract *ibapi.Contract, execution *ibapi.Execution) {
	s.orders.orderMu.Lock()
	sym := s.orders.orderSymbol[execution.OrderID]
	act := s.orders.orderAction[execution.OrderID]
	route := s.orders.orderRoutes[execution.OrderID]
	s.orders.orderMu.Unlock()

	if sym == "" {
		return
	}

	qty := float64(execution.Shares.Int())
	if route.secType == "OPT" {
		qty *= 100
	}

	s.orders.orderMu.Lock()
	s.orders.pendingExecs[execution.ExecID] = pendingExec{
		orderID: execution.OrderID, symbol: sym, action: act, tag: route.tag,
		price: execution.Price, qty: qty, ts: time.Now(), bus: route.bus,
	}
	s.orders.orderMu.Unlock()

	s.logger.Printf("ExecDetails: orderID=%d execID=%s symbol=%s action=%s shares=%s price=%.4f",
		execution.OrderID, execution.ExecID, sym, act, execution.Shares, execution.Price)
}

// CommissionAndFeesReport is called by TWS with commission data for a
// completed fill. Looks up the matching ExecDetails entry by ExecID, then
// publishes a KindCommissionReport event.
func (s *Session) CommissionAndFeesReport(report ibapi.CommissionAndFeesReport) {
	s.orders.orderMu.Lock()
	exec, ok := s.orders.pendingExecs[report.ExecID]
	if ok {
		delete(s.orders.pendingExecs, report.ExecID)
	}
	s.orders.orderMu.Unlock()

	if !ok {
		return
	}

	s.logger.Printf("CommissionAndFeesReport: orderID=%d execID=%s symbol=%s action=%s %.0f @ %.4f commission=%.4f %s",
		exec.orderID, report.ExecID, exec.symbol, exec.action, exec.qty, exec.price, report.CommissionAndFees, report.Currency)

	if exec.bus != nil {
		exec.bus.Publish(eventbus.Event{
			Kind: eventbus.KindCommissionReport,
			Payload: eventbus.CommissionReport{
				OrderID: exec.orderID, ExecID: report.ExecID, Symbol: exec.symbol, Action: exec.action,
				Tag: exec.tag, Price: exec.price, Qty: exec.qty, Ts: exec.ts,
				Commission: report.CommissionAndFees, Currency: report.Currency,
			},
		})
	}
}

// ── Naked-short safety gate ──────────────────────────────────────────────

// heldOptionQtyShares returns how many option shares (contracts × 100) of
// the exact contract (symbol, right "C"/"P", strike, expiry) IB's own
// portfolio feed confirms are held long. This is ground truth — populated
// directly from IB's real-time Position() callbacks, independent of
// anything the caller tracks locally — so it is the single source of truth
// the naked-short safety gate checks before every SELL-to-close option
// order. A short existing position yields a negative total, which correctly
// fails the "held >= requested" check below.
func (s *Session) heldOptionQtyShares(symbol, ibRight string, strike float64, expiry string) float64 {
	var total float64
	for _, p := range s.Positions() {
		if p.SecType != "OPT" || p.Symbol != symbol || p.Right != ibRight || p.Strike != strike || p.Expiry != expiry {
			continue
		}
		total += p.Position * 100
	}
	return total
}

// ensureOptionHeldForClose is the naked-short safety gate: it returns nil
// only when IB's own portfolio confirms at least qtyShares of the exact
// contract are held long, and a descriptive error otherwise.
func (s *Session) ensureOptionHeldForClose(symbol, tag, ibRight string, strike float64, expiry string, qtyShares float64) error {
	return s.ensureOptionSellable(symbol, tag, ibRight, strike, expiry, qtyShares, 0)
}

// ensureOptionSellable is ensureOptionHeldForClose generalised to also
// subtract inFlightShares — SELL/COVER quantity already submitted for the
// same contract but not yet settled. Subtracting in-flight sells is what
// makes a burst of rapid partial exits safe.
func (s *Session) ensureOptionSellable(symbol, tag, ibRight string, strike float64, expiry string, qtyShares, inFlightShares float64) error {
	held := s.heldOptionQtyShares(symbol, ibRight, strike, expiry)
	available := held - inFlightShares
	if available+1e-6 < qtyShares {
		return fmt.Errorf("refusing to sell %s %s %.0f contract(s) strike=%.0f expiry=%s: IB confirms %.0f held long, %.0f already in-flight — only %.0f sellable, refusing to open a naked short",
			symbol, tag, qtyShares/100, strike, expiry, held/100, inFlightShares/100, available/100)
	}
	return nil
}

// optSellKey is the map key identifying one exact option contract for
// in-flight SELL/COVER reservation tracking.
func optSellKey(symbol, ibRight string, strike float64, expiry string) string {
	return fmt.Sprintf("%s|%s|%.4f|%s", symbol, ibRight, strike, expiry)
}

// reserveOptionSell atomically checks the naked-short gate (against
// IB-confirmed held inventory minus already-in-flight sells) and, if the
// sell is safe, records the reservation so concurrent/subsequent sells see
// it. Returns the reservation key (for release on placeOrder failure) or an
// error if the sell would oversell.
func (s *Session) reserveOptionSell(symbol, tag, ibRight string, strike float64, expiry string, qtyShares float64) (string, error) {
	key := optSellKey(symbol, ibRight, strike, expiry)
	s.orders.orderMu.Lock()
	defer s.orders.orderMu.Unlock()
	if err := s.ensureOptionSellable(symbol, tag, ibRight, strike, expiry, qtyShares, s.orders.inFlightSell[key]); err != nil {
		return "", err
	}
	s.orders.inFlightSell[key] += qtyShares
	return key, nil
}

// releaseOptionSell rolls back a reservation made by reserveOptionSell when
// the order was never actually placed.
func (s *Session) releaseOptionSell(key string, qtyShares float64) {
	if key == "" {
		return
	}
	s.orders.orderMu.Lock()
	s.orders.inFlightSell[key] -= qtyShares
	if s.orders.inFlightSell[key] <= 1e-6 {
		delete(s.orders.inFlightSell, key)
	}
	s.orders.orderMu.Unlock()
}

// settleInFlightSell releases the reservation an order made, keyed by
// orderID. Called on every terminal order event (full fill, cancel,
// rejection). No-op for orders that never reserved. Safe to call more than
// once for the same order.
func (s *Session) settleInFlightSell(orderID int64) {
	s.orders.orderMu.Lock()
	if ref, ok := s.orders.orderInFlight[orderID]; ok {
		delete(s.orders.orderInFlight, orderID)
		s.orders.inFlightSell[ref.key] -= ref.qty
		if s.orders.inFlightSell[ref.key] <= 1e-6 {
			delete(s.orders.inFlightSell, ref.key)
		}
	}
	s.orders.orderMu.Unlock()
}

// ── Public order entry points ────────────────────────────────────────────

// resolveCloseContract determines the exact option contract to close.
//
// The identity is CARRIED on OrderRequest (o.Strike + o.OptionExpiry). When
// present, those are returned verbatim and the current ATM chain state is
// NEVER consulted — this is what makes the drifted-ATM class of bug
// structurally impossible for a tracked close.
//
// Only when the identity is absent does it fall back to the current ATM
// leg. That fallback stays safe because ClosePosition gates the resulting
// SELL through ensureOptionHeldForClose against IB's own portfolio.
func (s *Session) resolveCloseContract(o OrderRequest, busIdx int) (strike float64, expiry string, bid, ask, mid float64, err error) {
	switch {
	case o.Strike > 0 && o.OptionExpiry != "":
		return o.Strike, o.OptionExpiry, o.Bid, o.Ask, 0, nil
	case o.Strike <= 0 && o.OptionExpiry == "":
		opt, ok := s.currentOptionContract(o.Symbol, o.Tag, busIdx)
		if !ok {
			return 0, "", 0, 0, 0, fmt.Errorf("no resolved option contract for %s %s yet", o.Symbol, o.Tag)
		}
		return opt.strike, opt.expiry, opt.bid, opt.ask, opt.mid, nil
	default:
		return 0, "", 0, 0, 0, fmt.Errorf("cannot close %s %s: position missing strike/expiry (strike=%.0f expiry=%q)",
			o.Symbol, o.Tag, o.Strike, o.OptionExpiry)
	}
}

// ClosePosition places an order to close a position leg. Stocks: a
// positive Qty sells long, negative covers short. Options (call/put): SELL
// to close the EXACT contract carried on the OrderRequest.
func (s *Session) ClosePosition(sub Subscriber, o OrderRequest) (OrderResult, error) {
	bus := sub.Bus()
	priority := sub.OrderPriority(o.Category)
	qtyShares := math.Abs(o.Qty)

	if isOptionTag(o.Tag) {
		ibRight := "C"
		if o.Tag == "put" {
			ibRight = "P"
		}

		strike, expiry, bid, ask, mid, err := s.resolveCloseContract(o, s.busIndex(bus))
		if err != nil {
			return OrderResult{}, err
		}

		reserveKey, err := s.reserveOptionSell(o.Symbol, o.Tag, ibRight, strike, expiry, qtyShares)
		if err != nil {
			return OrderResult{}, err
		}

		contract := makeOptionContract(o.Symbol, ibRight, strike, expiry)
		orderID, err := s.placeOrder(placedOrder{
			bus: bus, contract: contract, action: "SELL", tag: o.Tag,
			secType: "OPT", qtyShares: qtyShares, strike: strike, expiry: expiry,
			optMid: mid, optBid: bid, optAsk: ask, priority: priority, category: o.Category,
			sellReserveKey: reserveKey,
		})
		if err != nil {
			s.releaseOptionSell(reserveKey, qtyShares)
			return OrderResult{}, err
		}
		return OrderResult{OrderID: orderID, Strike: strike, OptionExpiry: expiry, Bid: bid, Ask: ask, Mid: mid}, nil
	}

	contract := s.contractForSymbol(o.Symbol)
	if contract == nil {
		return OrderResult{}, fmt.Errorf("symbol %s not in symbol list", o.Symbol)
	}
	action := "SELL"
	if o.Qty < 0 {
		action = "COVER"
	}
	orderID, err := s.placeOrder(placedOrder{
		bus: bus, contract: contract, action: action, tag: o.Tag,
		secType: "STK", qtyShares: qtyShares, priority: priority, category: o.Category,
	})
	if err != nil {
		return OrderResult{}, err
	}
	return OrderResult{OrderID: orderID}, nil
}

// resolveBuyContract determines the exact option contract to buy.
//
// The identity is CARRIED on OrderRequest (o.Strike + o.OptionExpiry),
// typically resolved by the caller's entry-time IB delta probe
// (ResolveEntryStrike) for the contract the entry was priced against. When
// present, it is returned verbatim and the current ATM chain is NEVER
// consulted — mirrors resolveCloseContract, and for the same reason:
// without this, the real order could silently target a different strike
// than the one the caller priced and recorded.
//
// Only when the identity is fully absent does it fall back to the current
// ATM leg. A partial identity (exactly one of strike/expiry set) is refused
// rather than guessed.
func (s *Session) resolveBuyContract(o OrderRequest, busIdx int) (strike float64, expiry string, bid, ask, mid float64, err error) {
	switch {
	case o.Strike > 0 && o.OptionExpiry != "":
		switch {
		case o.Bid > 0 && o.Ask > 0:
			mid = (o.Bid + o.Ask) / 2
		case o.Ask > 0:
			mid = o.Ask
		case o.Bid > 0:
			mid = o.Bid
		}
		return o.Strike, o.OptionExpiry, o.Bid, o.Ask, mid, nil
	case o.Strike <= 0 && o.OptionExpiry == "":
		opt, ok := s.currentOptionContract(o.Symbol, o.Tag, busIdx)
		if !ok {
			return 0, "", 0, 0, 0, fmt.Errorf("no resolved option contract for %s %s yet", o.Symbol, o.Tag)
		}
		return opt.strike, opt.expiry, opt.bid, opt.ask, opt.mid, nil
	default:
		return 0, "", 0, 0, 0, fmt.Errorf("cannot buy %s %s: partial contract identity (strike=%.0f expiry=%q)",
			o.Symbol, o.Tag, o.Strike, o.OptionExpiry)
	}
}

// OpenPosition opens a position leg. o.Tag is "long"/"short" (stock) or
// "call"/"put" (option, BUY to open).
func (s *Session) OpenPosition(sub Subscriber, o OrderRequest) (OrderResult, error) {
	if o.Qty <= 0 {
		return OrderResult{}, fmt.Errorf("open quantity must be positive, got %.0f", o.Qty)
	}
	bus := sub.Bus()
	priority := sub.OrderPriority("buy")

	if isOptionTag(o.Tag) {
		strike, expiry, bid, ask, mid, err := s.resolveBuyContract(o, s.busIndex(bus))
		if err != nil {
			return OrderResult{}, err
		}
		ibRight := "C"
		if o.Tag == "put" {
			ibRight = "P"
		}
		contract := makeOptionContract(o.Symbol, ibRight, strike, expiry)
		underlying := s.getUnderlyingPrice(o.Symbol)
		orderID, err := s.placeOrder(placedOrder{
			bus: bus, contract: contract, action: "BUY", tag: o.Tag,
			secType: "OPT", qtyShares: o.Qty, strike: strike, expiry: expiry,
			underlying: underlying, optMid: mid, optBid: bid, optAsk: ask, priority: priority,
		})
		if err != nil {
			return OrderResult{}, err
		}
		return OrderResult{OrderID: orderID, Strike: strike, OptionExpiry: expiry, Bid: bid, Ask: ask, Mid: mid, Underlying: underlying}, nil
	}

	contract := s.contractForSymbol(o.Symbol)
	if contract == nil {
		return OrderResult{}, fmt.Errorf("symbol %s not in symbol list", o.Symbol)
	}
	action := "BUY"
	if o.Tag == "short" {
		action = "SHORT"
	}
	orderID, err := s.placeOrder(placedOrder{
		bus: bus, contract: contract, action: action, tag: o.Tag,
		secType: "STK", qtyShares: o.Qty, priority: priority,
	})
	if err != nil {
		return OrderResult{}, err
	}
	return OrderResult{OrderID: orderID}, nil
}

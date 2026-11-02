package ibkr

import (
	"testing"
	"time"

	"github.com/SteffRainville/ibkr-go/eventbus"
	"github.com/SteffRainville/ibkr-go/quotes"
	"github.com/scmhub/ibapi"
)

// The send time and quote placeOrder stamps on an order's route must reach that
// order's CommissionReport, so a consumer can measure Ts − OrderSent and Price
// against the quote per execution.
func TestCommissionReport_CarriesOrderSent(t *testing.T) {
	s := newTestSession()
	bus := eventbus.New()
	ch := bus.Subscribe(eventbus.KindCommissionReport)
	defer bus.Unsubscribe(ch)

	sent := time.Now().Add(-3 * time.Second)
	s.orders.orderRoutes[1] = subRoute{bus: bus, secType: "OPT", tag: "call", sent: sent, bid: 2.05, ask: 2.15}
	s.orders.orderSymbol[1] = "MSFT"
	s.orders.orderAction[1] = "SELL"

	s.ExecDetails(0, &ibapi.Contract{Symbol: "MSFT"},
		&ibapi.Execution{OrderID: 1, ExecID: "e1", Shares: ibapi.StringToDecimal("2"), Price: 2.1})
	s.CommissionAndFeesReport(ibapi.CommissionAndFeesReport{ExecID: "e1", CommissionAndFees: 1.3})

	select {
	case evt := <-ch:
		d := evt.Payload.(eventbus.CommissionReport)
		if !d.OrderSent.Equal(sent) {
			t.Errorf("OrderSent = %v, want %v", d.OrderSent, sent)
		}
		if d.Ts.Before(sent) {
			t.Errorf("Ts %v is before OrderSent %v", d.Ts, sent)
		}
		if d.OrderBid != 2.05 || d.OrderAsk != 2.15 {
			t.Errorf("OrderBid/OrderAsk = %v/%v, want 2.05/2.15", d.OrderBid, d.OrderAsk)
		}
	default:
		t.Fatal("no KindCommissionReport published")
	}
}

// The Book is read first, for stocks and for the exact option contract; the
// quote carried on the request only fills a side the Book does not have.
func TestQuoteAtSend(t *testing.T) {
	s := newTestSession()
	s.book.SetStockBid("MSFT", 500.10)
	s.book.SetStockAsk("MSFT", 500.20)
	key := quotes.ContractKey{Symbol: "MSFT", Right: "call", Strike: 505, Expiry: "20261106"}
	s.book.SetOptionAsk(key, 2.15)
	contract := &ibapi.Contract{Symbol: "MSFT"}

	for name, c := range map[string]struct {
		r        placedOrder
		bid, ask float64
	}{
		"stock": {placedOrder{contract: contract, secType: "STK", tag: "long"}, 500.10, 500.20},
		"option side missing from the Book": {placedOrder{contract: contract, secType: "OPT", tag: "call",
			strike: 505, expiry: "20261106", optBid: 2.05, optAsk: 9.99}, 2.05, 2.15},
		"another strike": {placedOrder{contract: contract, secType: "OPT", tag: "call",
			strike: 510, expiry: "20261106", optBid: 1.05, optAsk: 1.15}, 1.05, 1.15},
		"no quote anywhere": {placedOrder{contract: &ibapi.Contract{Symbol: "TSLA"}, secType: "STK", tag: "long"}, 0, 0},
	} {
		if bid, ask := s.quoteAtSend(c.r); bid != c.bid || ask != c.ask {
			t.Errorf("%s: quote = %v/%v, want %v/%v", name, bid, ask, c.bid, c.ask)
		}
	}
}

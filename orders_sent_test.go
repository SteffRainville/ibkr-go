package ibkr

import (
	"testing"
	"time"

	"github.com/SteffRainville/ibkr-go/eventbus"
	"github.com/scmhub/ibapi"
)

// The send time placeOrder stamps on an order's route must reach that order's
// CommissionReport, so a consumer can measure Ts − OrderSent per execution.
func TestCommissionReport_CarriesOrderSent(t *testing.T) {
	s := newTestSession()
	bus := eventbus.New()
	ch := bus.Subscribe(eventbus.KindCommissionReport)
	defer bus.Unsubscribe(ch)

	sent := time.Now().Add(-3 * time.Second)
	s.orders.orderRoutes[1] = subRoute{bus: bus, secType: "OPT", tag: "call", sent: sent}
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
	default:
		t.Fatal("no KindCommissionReport published")
	}
}

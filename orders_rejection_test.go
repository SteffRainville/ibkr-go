package ibkr

import (
	"testing"

	"github.com/SteffRainville/ibkr-go/eventbus"
)

// A rejection's typed payload must carry the BARE symbol, because that is what
// the consumer keys its position map by.
//
// This is the test that was missing on 2026-08-17. The Error path builds a
// decorated label for its log lines — resolveReqID returns "MU (order)" for an
// order reqID — and that string used to go straight into OrderRejected.Symbol.
// Every consumer then looked up a key that could not exist:
// hub.MarkPositionFailed("MU (order)", "call") and dropMirror both no-opped
// without failing, so a rejected MU entry sat on the dashboard as "pending"
// for 5½ minutes until an unrelated order-timeout swept it up. The consumer
// side had tests, but they hand-built the event with a clean symbol — only a
// test at the point of construction can see this class of bug.
func TestError_OrderRejectionPublishesBareSymbol(t *testing.T) {
	s := newTestSession()
	bus := eventbus.New()
	ch := bus.Subscribe(eventbus.KindOrderRejected)
	defer bus.Unsubscribe(ch)

	const orderID = 243
	s.orders.orderRoutes[orderID] = subRoute{bus: bus, secType: "OPT", tag: "call"}
	s.orders.orderSymbol[orderID] = "MU"
	s.orders.orderAction[orderID] = "BUY"

	// The real code-201 text from the incident, <br> markup and all.
	const ibMsg = "Order rejected - reason:We are unable to accept your order. " +
		"Your Available Funds are in sufficient to cover the change in the <br>account's " +
		"margin requirements if this order executes."
	s.Error(orderID, 0, 201, ibMsg, "")

	select {
	case evt := <-ch:
		d, ok := evt.Payload.(eventbus.OrderRejected)
		if !ok {
			t.Fatalf("payload type = %T, want eventbus.OrderRejected", evt.Payload)
		}
		if d.Symbol != "MU" {
			t.Errorf("Symbol = %q, want %q — a display label here makes every keyed lookup downstream miss silently", d.Symbol, "MU")
		}
		if d.OrderID != orderID || d.Action != "BUY" || d.Tag != "call" || d.Code != 201 {
			t.Errorf("payload = %+v, want orderID=%d action=BUY tag=call code=201", d, orderID)
		}
		if d.Message != ibMsg {
			t.Errorf("Message = %q, want IB's text verbatim", d.Message)
		}
	default:
		t.Fatal("no KindOrderRejected published on the owning robot's bus")
	}
}

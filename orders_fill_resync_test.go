package ibkr

import (
	"testing"

	"github.com/scmhub/ibapi"

	"github.com/SteffRainville/ibkr-go/eventbus"
)

// These tests lock the cumulative-fill-delta design that fixed a real
// double-fill incident: a naive "apply every OrderStatus callback" approach
// double-counted a fill that arrived through two code paths. IB's own
// per-order cumulative "filled" counter (not a locally-decremented
// countdown) is the source of truth for how much of an order is new — that
// delta is what OrderStatus publishes, once, per genuinely new increment.
// OrderStatus also never calls refreshPositions() on a fill: a full IB
// resubscribe would blind the naked-short gate to real-time updates until
// the next PositionEnd() round trip, and back-to-back fills would each
// restart that blind window.

func TestOrderStatus_FillTracksOrderIDCumulativeFillWithoutResync(t *testing.T) {
	s := newTestSession() // s.client is nil — a refreshPositions() call would panic

	s.orders.orderRoutes[1] = subRoute{secType: "OPT", tag: "put"}
	s.orders.orderSymbol[1] = "GLD"
	s.orders.orderAction[1] = "BUY"

	s.OrderStatus(1, "Filled", ibapi.StringToDecimal("14"), ibapi.StringToDecimal("0"),
		3.38, 0, 0, 3.38, 0, "", 0)

	// A terminal fill cleans up its own tracking entries — including
	// orderFilled, proving the delta was applied without needing a full
	// position resync to reconcile it.
	if _, tracked := s.orders.orderFilled[1]; tracked {
		t.Error("orderFilled entry for orderID 1 not cleaned up after terminal fill")
	}
}

// TestOrderStatus_QtyMismatchSurfacedLoudly is the direct reproduction of a
// real fill-quantity-mismatch incident: an order placed for 14 contracts
// that IB reports as "Filled" for 28. This must never be silent — it has to
// reach the owning bus via a KindOrderQtyMismatch event.
func TestOrderStatus_QtyMismatchSurfacedLoudly(t *testing.T) {
	s := newTestSession()
	bus := eventbus.New()
	ch := bus.Subscribe(eventbus.KindOrderQtyMismatch)
	defer bus.Unsubscribe(ch)

	s.orders.orderRoutes[1] = subRoute{bus: bus, secType: "OPT", tag: "put"}
	s.orders.orderSymbol[1] = "GLD"
	s.orders.orderAction[1] = "BUY"
	s.orders.orderQty[1] = 1400 // requested 14 contracts (hub-facing shares)

	s.OrderStatus(1, "Filled", ibapi.StringToDecimal("28"), ibapi.StringToDecimal("0"),
		3.38, 0, 0, 3.38, 0, "", 0)

	select {
	case evt := <-ch:
		d, ok := evt.Payload.(eventbus.OrderQtyMismatch)
		if !ok {
			t.Fatalf("payload type = %T, want eventbus.OrderQtyMismatch", evt.Payload)
		}
		if d.Requested != 1400 || d.Filled != 2800 {
			t.Errorf("mismatch event = %+v, want Requested=1400 Filled=2800", d)
		}
		if d.Tag != "put" {
			t.Errorf("Tag = %q, want %q", d.Tag, "put")
		}
	default:
		t.Fatal("no KindOrderQtyMismatch event published on the owning bus")
	}
}

// TestOrderStatus_MatchingQtyNoMismatchEvent guards against false positives: a
// routine fill whose quantity matches the request must not trigger the
// mismatch event.
func TestOrderStatus_MatchingQtyNoMismatchEvent(t *testing.T) {
	s := newTestSession()
	bus := eventbus.New()
	ch := bus.Subscribe(eventbus.KindOrderQtyMismatch)
	defer bus.Unsubscribe(ch)

	s.orders.orderRoutes[1] = subRoute{bus: bus, secType: "OPT", tag: "put"}
	s.orders.orderSymbol[1] = "GLD"
	s.orders.orderAction[1] = "BUY"
	s.orders.orderQty[1] = 1400

	s.OrderStatus(1, "Filled", ibapi.StringToDecimal("14"), ibapi.StringToDecimal("0"),
		3.38, 0, 0, 3.38, 0, "", 0)

	select {
	case evt := <-ch:
		t.Fatalf("unexpected KindOrderQtyMismatch event for a matching fill: %+v", evt.Payload)
	default:
		// expected — no mismatch event
	}
}

// TestOrderStatus_PartialFillsApplyIncrementally is the direct reproduction of
// what "handle partial fills that take a few minutes" requires: two
// OrderStatus callbacks for the SAME orderID (IB's cumulative filled/remaining
// convention — confirmed against the ibapi wire decoder), the first a partial
// fill, the second the terminal one. Each callback must publish only the
// NEW delta above what was already applied — the first fires a
// KindOrderFilled with Qty=600 (6 contracts), the second with Qty=800 (the
// remaining 8) — never the raw cumulative count twice. No
// fill-quantity-mismatch event should fire since the final cumulative total
// matches what was requested.
func TestOrderStatus_PartialFillsApplyIncrementally(t *testing.T) {
	s := newTestSession()
	bus := eventbus.New()
	filledCh := bus.Subscribe(eventbus.KindOrderFilled)
	mismatchCh := bus.Subscribe(eventbus.KindOrderQtyMismatch)
	defer bus.Unsubscribe(filledCh)
	defer bus.Unsubscribe(mismatchCh)

	s.orders.orderRoutes[1] = subRoute{bus: bus, secType: "OPT", tag: "put"}
	s.orders.orderSymbol[1] = "GLD"
	s.orders.orderAction[1] = "BUY"
	s.orders.orderQty[1] = 1400 // requested 14 contracts (hub-facing shares)

	// First partial fill: 6 of 14 contracts, order still working.
	s.OrderStatus(1, "Filled", ibapi.StringToDecimal("6"), ibapi.StringToDecimal("8"),
		3.38, 0, 0, 3.38, 0, "", 0)

	select {
	case evt := <-filledCh:
		d := evt.Payload.(eventbus.OrderFilled)
		if d.Qty != 600 {
			t.Errorf("first fill delta = %.0f, want 600 (6 contracts)", d.Qty)
		}
		if d.Remaining != 800 {
			t.Errorf("first fill remaining = %.0f, want 800", d.Remaining)
		}
	default:
		t.Fatal("expected a KindOrderFilled event for the first partial fill")
	}

	// Second callback, same orderID: the remaining 8 contracts, order complete.
	s.OrderStatus(1, "Filled", ibapi.StringToDecimal("14"), ibapi.StringToDecimal("0"),
		3.38, 0, 0, 3.38, 0, "", 0)

	select {
	case evt := <-filledCh:
		d := evt.Payload.(eventbus.OrderFilled)
		if d.Qty != 800 {
			t.Errorf("second fill delta = %.0f, want 800 (the remaining 8 contracts, not the cumulative 14)", d.Qty)
		}
	default:
		t.Fatal("expected a second KindOrderFilled event for the terminal fill")
	}

	select {
	case evt := <-mismatchCh:
		t.Fatalf("unexpected KindOrderQtyMismatch event for a fill that matched the request: %+v", evt.Payload)
	default:
		// expected — final cumulative (14) matches requested (14)
	}

	if _, tracked := s.orders.orderFilled[1]; tracked {
		t.Error("orderFilled entry for orderID 1 not cleaned up after terminal fill")
	}
}

package ibkr

import (
	"context"
	"strings"
	"testing"

	"github.com/SteffRainville/ibkr-go/eventbus"
)

// These tests lock the fix for a real oversell incident: a live options
// robot sold 12 contracts against an 11-contract position and ended -1
// naked short, because each rapid partial exit (a take-profit ladder plus a
// trailing stop) was gate-checked independently against a lagging broker
// snapshot. The gate subtracts SELL/COVER quantity already in flight, so
// the CUMULATIVE sold quantity can never exceed the held inventory.

// testSubscriber is a minimal Subscriber for exercising Session methods
// that need one (OpenPosition/ClosePosition) without any dashboard/bot
// framework.
type testSubscriber struct {
	bus *eventbus.Bus
}

func newTestSubscriber() *testSubscriber { return &testSubscriber{bus: eventbus.New()} }

func (t *testSubscriber) Symbols() []SymbolSpec           { return nil }
func (t *testSubscriber) Bus() *eventbus.Bus              { return t.bus }
func (t *testSubscriber) OrderPriority(string) string     { return "" }
func (t *testSubscriber) OnConnected(context.Context)     {}

// newTestSession returns a Session with the order-tracker maps initialised,
// ready to exercise order/position logic without a live IB connection.
func newTestSession() *Session {
	s := NewSession(Options{TradingAccount: "DU12345"}, nil, nil)
	return s
}

// seedHeldOption directly seeds s's confirmed-position map, as if IB's
// PositionEnd had just reported contracts of this exact option held.
func seedHeldOption(s *Session, symbol, right string, strike float64, expiry string, contracts float64) {
	s.pos.posMap["DU12345|"+symbol+"|OPT|"+right] = PositionInfo{
		Account: "DU12345", Symbol: symbol, SecType: "OPT", Right: right, Strike: strike, Expiry: expiry, Position: contracts,
	}
	s.pos.initialDone = true
}

func heldQQQ708(s *Session, contracts float64) {
	seedHeldOption(s, "QQQ", "C", 708, "20260710", contracts)
}

// ── ensureOptionSellable: in-flight subtraction ─────────────────────────────

func TestEnsureOptionSellable_SubtractsInFlight(t *testing.T) {
	s := newTestSession()
	heldQQQ708(s, 6) // 600 shares held

	if err := s.ensureOptionSellable("QQQ", "call", "C", 708, "20260710", 600, 0); err != nil {
		t.Errorf("sellable(req=600, inflight=0) = %v, want nil", err)
	}
	if err := s.ensureOptionSellable("QQQ", "call", "C", 708, "20260710", 300, 300); err != nil {
		t.Errorf("sellable(req=300, inflight=300) = %v, want nil", err)
	}
	if err := s.ensureOptionSellable("QQQ", "call", "C", 708, "20260710", 400, 300); err == nil {
		t.Fatal("sellable(req=400, inflight=300) = nil, want refusal — would oversell by 100")
	}
	if err := s.ensureOptionSellable("QQQ", "call", "C", 708, "20260710", 100, 600); err == nil {
		t.Fatal("sellable(req=100, inflight=600) = nil, want refusal — whole holding already committed")
	} else if !strings.Contains(err.Error(), "naked short") {
		t.Errorf("error = %q, want it to mention a naked short", err.Error())
	}
}

// ── reserveOptionSell: cumulative reservation across rapid exits ─────────────

func TestReserveOptionSell_BlocksCumulativeOversell(t *testing.T) {
	s := newTestSession()
	heldQQQ708(s, 11) // 1100 shares held

	if _, err := s.reserveOptionSell("QQQ", "call", "C", 708, "20260710", 500); err != nil {
		t.Fatalf("first reserve(500) = %v, want nil", err)
	}
	if _, err := s.reserveOptionSell("QQQ", "call", "C", 708, "20260710", 500); err != nil {
		t.Fatalf("second reserve(500) = %v, want nil (1000 <= 1100)", err)
	}
	if _, err := s.reserveOptionSell("QQQ", "call", "C", 708, "20260710", 200); err == nil {
		t.Fatal("third reserve(200) = nil, want refusal — cumulative 1200 > 1100 held (the naked short)")
	}
	if _, err := s.reserveOptionSell("QQQ", "call", "C", 708, "20260710", 100); err != nil {
		t.Errorf("reserve(100) filling remainder = %v, want nil", err)
	}
}

func TestReserveOptionSell_ReleaseFreesReservation(t *testing.T) {
	s := newTestSession()
	heldQQQ708(s, 6) // 600 shares

	key, err := s.reserveOptionSell("QQQ", "call", "C", 708, "20260710", 600)
	if err != nil {
		t.Fatalf("reserve(600) = %v, want nil", err)
	}
	if _, err := s.reserveOptionSell("QQQ", "call", "C", 708, "20260710", 100); err == nil {
		t.Fatal("reserve(100) while 600 in flight = nil, want refusal")
	}
	s.releaseOptionSell(key, 600)
	if _, err := s.reserveOptionSell("QQQ", "call", "C", 708, "20260710", 100); err != nil {
		t.Errorf("reserve(100) after release = %v, want nil — inventory should be free again", err)
	}
}

func TestSettleInFlightSell_FreesReservationOnFill(t *testing.T) {
	s := newTestSession()
	heldQQQ708(s, 6)

	key, err := s.reserveOptionSell("QQQ", "call", "C", 708, "20260710", 600)
	if err != nil {
		t.Fatalf("reserve(600) = %v, want nil", err)
	}
	s.orders.orderInFlight[42] = inFlightSellRef{key: key, qty: 600}

	s.settleInFlightSell(42)
	if s.orders.inFlightSell[key] != 0 {
		t.Errorf("inFlightSell after settle = %v, want 0", s.orders.inFlightSell[key])
	}
	if _, ok := s.orders.orderInFlight[42]; ok {
		t.Error("orderInFlight[42] still present after settle, want removed")
	}
	s.settleInFlightSell(42)
	if s.orders.inFlightSell[key] != 0 {
		t.Errorf("inFlightSell after double settle = %v, want 0", s.orders.inFlightSell[key])
	}
}

// TestClosePosition_RefusesOversellWhenSellsInFlight is the end-to-end lock:
// IB confirms 6 contracts held, but a full 6-contract sell is already in
// flight. A second close must be refused BEFORE placeOrder (nil client
// would panic), proving the in-flight subtraction closes the oversell hole
// at the real entry point.
func TestClosePosition_RefusesOversellWhenSellsInFlight(t *testing.T) {
	s := newTestSession()
	heldQQQ708(s, 6) // 600 shares held
	sub := newTestSubscriber()

	s.orders.inFlightSell[optSellKey("QQQ", "C", 708, "20260710")] = 600

	o := OrderRequest{Symbol: "QQQ", Tag: "call", SecType: "OPT", Qty: 100, Category: "trailing_loss", Strike: 708, OptionExpiry: "20260710"}

	_, err := s.ClosePosition(sub, o)
	if err == nil {
		t.Fatal("ClosePosition() = nil, want refusal — whole holding already in flight, this would oversell")
	}
	if !strings.Contains(err.Error(), "naked short") {
		t.Errorf("error = %q, want it to mention a naked short", err.Error())
	}
}

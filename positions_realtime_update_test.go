package ibkr

import (
	"fmt"
	"testing"

	"github.com/scmhub/ibapi"
)

// This test locks the fix for a real oversell-gate incident: a live IWM
// call was bought and confirmed, but the very next close attempt (a
// take-profit, then a cascading stop-loss retry) was refused with "IB
// confirms 0 held long" — because a real-time Position() push for a fresh
// fill only fired OnPositionUpdate and never updated posMap, the sole data
// source behind the naked-short safety gate. The position sat open,
// unmanaged, through repeated failed retries (2026-08-10).

// realtimePosition drives Session.Position() as IB would for a live
// streaming update — i.e. after the initial snapshot (PositionEnd) has
// already completed.
func realtimePosition(s *Session, symbol, right string, strike float64, expiry string, contracts float64) {
	c := &ibapi.Contract{Symbol: symbol, SecType: "OPT", Right: right, Strike: strike, LastTradeDateOrContractMonth: expiry}
	s.Position("DU12345", c, ibapi.StringToDecimal(fmt.Sprintf("%.0f", contracts)), 2.07)
}

// TestPosition_RealtimeFillUpdatesPosMap reproduces the IWM incident: a
// fresh fill arrives as a real-time Position() push (initialDone already
// true, as it is throughout a live trading session), and the naked-short
// gate must see it as held immediately — not only after the next full
// resync.
func TestPosition_RealtimeFillUpdatesPosMap(t *testing.T) {
	s := newTestSession()
	s.pos.initialDone = true // simulate: startup snapshot already completed

	if err := s.ensureOptionHeldForClose("IWM", "call", "C", 300, "20260812", 2400); err == nil {
		t.Fatal("before the fill lands, selling must still be refused")
	}

	realtimePosition(s, "IWM", "C", 300, "20260812", 24) // 24 contracts = 2400 shares

	if err := s.ensureOptionHeldForClose("IWM", "call", "C", 300, "20260812", 2400); err != nil {
		t.Fatalf("ensureOptionHeldForClose after real-time fill = %v, want nil — IB already confirmed this fill", err)
	}
}

// TestPosition_RealtimeUpdateKeyedByFullLegIdentity reproduces the second
// bug found alongside: two different option legs of the same underlying
// symbol must not collide into one posMap slot. Two robots can legitimately
// hold different IWM strikes/rights at once.
func TestPosition_RealtimeUpdateKeyedByFullLegIdentity(t *testing.T) {
	s := newTestSession()
	s.pos.initialDone = true

	realtimePosition(s, "IWM", "C", 300, "20260812", 24)
	realtimePosition(s, "IWM", "P", 305, "20260812", 10)

	if err := s.ensureOptionHeldForClose("IWM", "call", "C", 300, "20260812", 2400); err != nil {
		t.Errorf("IWM 300 call sellable = %v, want nil — must not be clobbered by the put leg", err)
	}
	if err := s.ensureOptionHeldForClose("IWM", "put", "P", 305, "20260812", 1000); err != nil {
		t.Errorf("IWM 305 put sellable = %v, want nil — must not be clobbered by the call leg", err)
	}
}

package ibkr

import (
	"fmt"
	"testing"
	"time"

	"github.com/scmhub/ibapi"
)

// These tests lock the fix for the 2026-10-23 position-refresh storm
// (VwMacdOptionFL, Crackwood). IB echoes every contract flat today as pos=0;
// each echo re-requested positions, whose replay carried the same echo, so
// positions were re-requested twice a second from 09:50 to the close. Every
// re-request wiped posMap — the naked-short gate's only truth — and the zero
// wiped legs IB had streamed before it, so an NVDA and an AVGO call could not
// be sold for four minutes ("IB confirms 0 held long").

func optPosition(s *Session, symbol, right string, strike float64, contracts float64) {
	c := &ibapi.Contract{Symbol: symbol, SecType: "OPT", Right: right, Strike: strike, LastTradeDateOrContractMonth: "20261023"}
	s.Position("DU12345", c, ibapi.StringToDecimal(fmt.Sprintf("%.0f", contracts)), 1.0)
}

// A zero for a leg this session does not hold is the flat echo: no deferred
// close, no refresh, and the held legs stay sellable.
func TestPosition_FlatEchoOfUnheldLegIsIgnored(t *testing.T) {
	s := newTestSession()
	s.pos.initialDone = true
	optPosition(s, "AVGO", "C", 327.5, 18)
	optPosition(s, "NVDA", "C", 215, 35)

	optPosition(s, "NVDA", "P", 212.5, 0) // closed at 09:50; IB keeps echoing it

	if len(s.pos.pendingClose) != 0 {
		t.Errorf("pendingClose = %v, want none — a flat echo of an unheld leg confirms nothing", s.pos.pendingClose)
	}
	if s.pos.refreshing || !s.pos.initialDone {
		t.Error("a flat echo must not start a position refresh")
	}
	if err := s.ensureOptionHeldForClose("AVGO", "call", "C", 327.5, "20261023", 1800); err != nil {
		t.Errorf("AVGO call sellable = %v, want nil", err)
	}
	if err := s.ensureOptionHeldForClose("NVDA", "call", "C", 215, "20261023", 3500); err != nil {
		t.Errorf("NVDA call sellable = %v, want nil", err)
	}
}

// A zero for a leg that IS held still goes through confirm-before-close.
func TestPosition_ZeroForHeldLegIsDeferredAndRefreshes(t *testing.T) {
	s := newTestSession()
	s.pos.initialDone = true
	optPosition(s, "AVGO", "C", 327.5, 18)

	optPosition(s, "AVGO", "C", 327.5, 0)

	if len(s.pos.pendingClose) != 1 {
		t.Errorf("pendingClose = %d, want the held leg deferred", len(s.pos.pendingClose))
	}
	if !s.pos.refreshing || s.pos.initialDone || s.pos.accum == nil {
		t.Error("a zero for a held leg must start one staged refresh")
	}
	if err := s.ensureOptionHeldForClose("AVGO", "call", "C", 327.5, "20261023", 1800); err != nil {
		t.Errorf("AVGO sellable during the confirming refresh = %v, want nil", err)
	}
}

// While a refresh replay is in flight posMap keeps serving the gate, and the
// complete snapshot replaces it at PositionEnd.
func TestRefreshReplay_IsStagedAndSwappedAtPositionEnd(t *testing.T) {
	s := newTestSession()
	s.pos.initialDone = true
	optPosition(s, "AVGO", "C", 327.5, 18)
	optPosition(s, "NVDA", "C", 215, 35)

	// What refreshPositions sets up before it re-requests.
	s.pos.refreshing, s.pos.refreshStart = true, time.Now()
	s.pos.accum = make(map[string]PositionInfo)
	s.pos.initialDone = false

	if err := s.ensureOptionHeldForClose("AVGO", "call", "C", 327.5, "20261023", 1800); err != nil {
		t.Fatalf("AVGO sellable mid-refresh = %v, want nil — posMap must not be wiped", err)
	}

	// Replay: only NVDA arrives before the end; AVGO was closed meanwhile.
	optPosition(s, "NVDA", "C", 215, 35)
	optPosition(s, "NVDA", "P", 212.5, 0) // flat echo inside the replay is skipped

	if got := len(s.pos.accum); got != 1 {
		t.Fatalf("accum = %d legs, want 1 (the zero must not be staged)", got)
	}
	if err := s.ensureOptionHeldForClose("NVDA", "call", "C", 215, "20261023", 3500); err != nil {
		t.Errorf("NVDA sellable mid-refresh = %v, want nil", err)
	}

	s.commitPositionSnapshot() // PositionEnd's swap (the rest needs a live client)

	if s.pos.refreshing || !s.pos.initialDone || s.pos.accum != nil {
		t.Error("PositionEnd must end the refresh")
	}
	if err := s.ensureOptionHeldForClose("AVGO", "call", "C", 327.5, "20261023", 1800); err == nil {
		t.Error("AVGO closed during the replay: the swapped-in snapshot must no longer show it held")
	}
	if err := s.ensureOptionHeldForClose("NVDA", "call", "C", 215, "20261023", 3500); err != nil {
		t.Errorf("NVDA sellable after the swap = %v, want nil", err)
	}
}

// A refresh already waiting for its PositionEnd is not stacked with another.
func TestRefreshPositions_SingleFlight(t *testing.T) {
	s := newTestSession()
	s.pos.refreshing, s.pos.refreshStart = true, time.Now()
	s.pos.initialDone = false

	s.refreshPositions() // would dereference the nil client if it proceeded
}

// Stock price snapshots are spaced, so a PositionEnd storm cannot flood
// market-data lines.
func TestStockSnapshotDue_SpacedPerSymbol(t *testing.T) {
	s := newTestSession()
	if !s.stockSnapshotDue("AAPL") {
		t.Fatal("first snapshot of a symbol must be due")
	}
	if s.stockSnapshotDue("AAPL") {
		t.Error("a second snapshot inside the interval must not be due")
	}
	if !s.stockSnapshotDue("MSFT") {
		t.Error("another symbol is independent")
	}
	s.pos.posMapMu.Lock()
	s.pos.snapAt["AAPL"] = time.Now().Add(-2 * stockSnapshotMinInterval)
	s.pos.posMapMu.Unlock()
	if !s.stockSnapshotDue("AAPL") {
		t.Error("due again after the interval")
	}
}

// A held leg that closes while a replay is in flight (its zero lands after the
// replay listed it) must not survive the swap as "held".
func TestRefreshReplay_ZeroMidReplayConfirmsTheClose(t *testing.T) {
	s := newTestSession()
	s.pos.initialDone = true
	optPosition(s, "AVGO", "C", 327.5, 18)

	s.pos.refreshing, s.pos.refreshStart = true, time.Now()
	s.pos.accum = make(map[string]PositionInfo)
	s.pos.initialDone = false

	optPosition(s, "AVGO", "C", 327.5, 18) // the replay lists it
	optPosition(s, "AVGO", "C", 327.5, 0)  // then it is sold

	if len(s.pos.accum) != 0 {
		t.Error("the closed leg must leave the staged snapshot")
	}
	if len(s.pos.pendingClose) != 1 {
		t.Error("the close must be left pending for PositionEnd to confirm")
	}
	s.commitPositionSnapshot()
	if err := s.ensureOptionHeldForClose("AVGO", "call", "C", 327.5, "20261023", 1800); err == nil {
		t.Error("a leg that closed mid-replay must not be held after the swap")
	}
}

// A refresh suppressed by the single-flight guard is remembered, so the close
// that asked for it is still confirmed once the replay in flight ends.
func TestRefreshPositions_SuppressedRefreshRunsAgainAfterPositionEnd(t *testing.T) {
	s := newTestSession()
	s.pos.refreshing, s.pos.refreshStart = true, time.Now()
	s.pos.initialDone = false

	s.refreshPositions()
	if !s.pos.refreshAgain {
		t.Fatal("a suppressed refresh must be remembered")
	}
}

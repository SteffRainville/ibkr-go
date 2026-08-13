package ibkr

import (
	"strings"
	"testing"
)

// ── heldOptionQtyShares ───────────────────────────────────────────────────────

func TestHeldOptionQtyShares_MatchesExactContract(t *testing.T) {
	s := newTestSession()
	seedHeldOption(s, "QQQ", "C", 708, "20260710", 6)

	if got := s.heldOptionQtyShares("QQQ", "C", 708, "20260710"); got != 600 {
		t.Errorf("held = %v, want 600 (6 contracts x 100)", got)
	}
}

func TestHeldOptionQtyShares_ZeroForDifferentStrikeOrExpiry(t *testing.T) {
	s := newTestSession()
	seedHeldOption(s, "QQQ", "C", 708, "20260710", 6)

	if got := s.heldOptionQtyShares("QQQ", "C", 709, "20260710"); got != 0 {
		t.Errorf("held(709) = %v, want 0 — a different strike must never count as held", got)
	}
	if got := s.heldOptionQtyShares("QQQ", "C", 708, "20260717"); got != 0 {
		t.Errorf("held(different expiry) = %v, want 0", got)
	}
	if got := s.heldOptionQtyShares("QQQ", "P", 708, "20260710"); got != 0 {
		t.Errorf("held(put) = %v, want 0 — must not match the call leg", got)
	}
}

func TestHeldOptionQtyShares_NegativeWhenAlreadyShort(t *testing.T) {
	s := newTestSession()
	// IB reports contracts as negative quantity for a short position.
	seedHeldOption(s, "QQQ", "C", 709, "20260710", -6)

	if got := s.heldOptionQtyShares("QQQ", "C", 709, "20260710"); got != -600 {
		t.Errorf("held = %v, want -600", got)
	}
}

// ── ensureOptionHeldForClose ─────────────────────────────────────────────────

func TestEnsureOptionHeldForClose_AllowsWhenFullyHeld(t *testing.T) {
	s := newTestSession()
	seedHeldOption(s, "QQQ", "C", 708, "20260710", 6)

	if err := s.ensureOptionHeldForClose("QQQ", "call", "C", 708, "20260710", 600); err != nil {
		t.Errorf("ensureOptionHeldForClose() = %v, want nil — the exact held contract must be sellable", err)
	}
}

func TestEnsureOptionHeldForClose_BlocksWhenNotHeld(t *testing.T) {
	s := newTestSession() // no IB positions at all

	err := s.ensureOptionHeldForClose("QQQ", "call", "C", 709, "20260710", 600)
	if err == nil {
		t.Fatal("ensureOptionHeldForClose() = nil, want refusal when IB shows nothing held")
	}
	if !strings.Contains(err.Error(), "naked short") {
		t.Errorf("error = %q, want it to mention a naked short", err.Error())
	}
}

func TestEnsureOptionHeldForClose_BlocksPartialHolding(t *testing.T) {
	s := newTestSession()
	// Only 2 contracts held, but the caller tries to close 6 — must not sell
	// the extra 4 naked.
	seedHeldOption(s, "QQQ", "C", 708, "20260710", 2)

	if err := s.ensureOptionHeldForClose("QQQ", "call", "C", 708, "20260710", 600); err == nil {
		t.Fatal("ensureOptionHeldForClose() = nil, want refusal when held qty is less than requested")
	}
}

// TestEnsureOptionHeldForClose_BlocksDriftedATMFallback reproduces a
// drifted-ATM incident end-to-end at the gate level: the caller holds QQQ
// 708 CALL, but by the time the close order is resolved the ATM leg has
// drifted to 709. The gate must refuse the 709 sell even though a (wrong)
// contract resolved successfully — IB's own portfolio, not the resolved
// contract, is the truth.
func TestEnsureOptionHeldForClose_BlocksDriftedATMFallback(t *testing.T) {
	s := newTestSession()
	seedHeldOption(s, "QQQ", "C", 708, "20260710", 6)

	err := s.ensureOptionHeldForClose("QQQ", "call", "C", 709, "20260710", 600)
	if err == nil {
		t.Fatal("ensureOptionHeldForClose() = nil, want refusal — 709 is not held, only 708 is")
	}
	if !strings.Contains(err.Error(), "naked short") {
		t.Errorf("error = %q, want it to mention a naked short", err.Error())
	}
}

// ── resolveCloseContract: the identity-carrying core ─────────────────────────

// TestResolveCloseContract_UsesCarriedIdentityNotATM is the key regression
// lock for a drifted-ATM incident. The OrderRequest carries the held
// contract's exact strike/expiry (708). Even though the option chain's
// current ATM leg has drifted to 709, resolveCloseContract must return 708
// and MUST NOT consult the chain — proving the wrong-strike class of bug is
// structurally impossible once the identity is carried on the order.
func TestResolveCloseContract_UsesCarriedIdentityNotATM(t *testing.T) {
	s := newTestSession()
	seedDisplayedLeg(s, "QQQ", "call", 709, "20260710", 0, 0)

	o := OrderRequest{Symbol: "QQQ", Tag: "call", SecType: "OPT", Qty: 600, Strike: 708, OptionExpiry: "20260710", Bid: 6.10, Ask: 6.20}

	strike, expiry, bid, ask, _, err := s.resolveCloseContract(o, -1)
	if err != nil {
		t.Fatalf("resolveCloseContract() error = %v, want nil", err)
	}
	if strike != 708 {
		t.Errorf("strike = %v, want 708 (the carried identity), NOT the drifted ATM 709", strike)
	}
	if expiry != "20260710" {
		t.Errorf("expiry = %q, want carried 20260710", expiry)
	}
	if bid != 6.10 || ask != 6.20 {
		t.Errorf("bid/ask = %v/%v, want carried 6.10/6.20", bid, ask)
	}
}

// TestResolveCloseContract_FallsBackToATMWhenIdentityAbsent verifies the
// one legitimate ATM path: a fully-absent identity (untracked manual close)
// resolves the current ATM leg.
func TestResolveCloseContract_FallsBackToATMWhenIdentityAbsent(t *testing.T) {
	s := newTestSession()
	seedDisplayedLeg(s, "QQQ", "call", 709, "20260710", 5.9, 6.0)

	o := OrderRequest{Symbol: "QQQ", Tag: "call", Qty: 600} // no strike/expiry

	strike, expiry, _, _, _, err := s.resolveCloseContract(o, -1)
	if err != nil {
		t.Fatalf("resolveCloseContract() error = %v, want nil", err)
	}
	if strike != 709 || expiry != "20260710" {
		t.Errorf("strike/expiry = %v/%q, want ATM 709/20260710", strike, expiry)
	}
}

// TestResolveCloseContract_RefusesPartialIdentity verifies a corrupt
// identity (strike present, expiry missing) is refused rather than
// silently ATM-resolved.
func TestResolveCloseContract_RefusesPartialIdentity(t *testing.T) {
	s := newTestSession()
	o := OrderRequest{Symbol: "QQQ", Tag: "call", SecType: "OPT", Qty: 700, Strike: 723}

	_, _, _, _, _, err := s.resolveCloseContract(o, -1)
	if err == nil {
		t.Fatal("resolveCloseContract() = nil error, want refusal for partial identity")
	}
	if !strings.Contains(err.Error(), "missing strike/expiry") {
		t.Errorf("error = %q, want it to mention missing strike/expiry", err.Error())
	}
}

// ── ClosePosition: end-to-end gate behavior ─────────────────────────────────

// TestClosePosition_RefusesNakedShort_IdentityNotAtIB reproduces the core
// of a drifted-strike incident through the real entry point: the
// OrderRequest carries a valid identity (708/20260710), but IB's own
// portfolio does not confirm it is held (e.g. already closed at the
// broker). The gate must refuse before ever reaching placeOrder.
func TestClosePosition_RefusesNakedShort_IdentityNotAtIB(t *testing.T) {
	s := newTestSession() // IB portfolio deliberately left empty
	sub := newTestSubscriber()

	o := OrderRequest{Symbol: "QQQ", Tag: "call", SecType: "OPT", Qty: 600, Strike: 708, OptionExpiry: "20260710"}

	_, err := s.ClosePosition(sub, o)
	if err == nil {
		t.Fatal("ClosePosition() = nil error, want refusal — IB does not confirm the position is held")
	}
	if !strings.Contains(err.Error(), "naked short") {
		t.Errorf("error = %q, want it to mention a naked short", err.Error())
	}
}

// TestClosePosition_RefusesNakedShort_DriftedATMFallback covers the
// untracked manual-close path: the OrderRequest has no carried identity, so
// resolution falls back to the current ATM leg, which has drifted to a
// strike (709) the account does not actually hold (only 708 is held). The
// gate must refuse rather than short 709.
func TestClosePosition_RefusesNakedShort_DriftedATMFallback(t *testing.T) {
	s := newTestSession()
	seedHeldOption(s, "QQQ", "C", 708, "20260710", 6)
	seedDisplayedLeg(s, "QQQ", "call", 709, "20260710", 0, 0)
	sub := newTestSubscriber()

	o := OrderRequest{Symbol: "QQQ", Tag: "call", Qty: 600} // untracked -> ATM fallback

	_, err := s.ClosePosition(sub, o)
	if err == nil {
		t.Fatal("ClosePosition() = nil error, want refusal — the drifted ATM leg (709) is not held, only 708 is")
	}
	if !strings.Contains(err.Error(), "naked short") {
		t.Errorf("error = %q, want it to mention a naked short", err.Error())
	}
}

// TestClosePosition_RefusesPartialIdentity verifies an OrderRequest with a
// strike but no expiry is refused rather than falling back to the ATM leg.
func TestClosePosition_RefusesPartialIdentity(t *testing.T) {
	s := newTestSession()
	sub := newTestSubscriber()
	o := OrderRequest{Symbol: "QQQ", Tag: "call", SecType: "OPT", Qty: 700, Strike: 723}

	_, err := s.ClosePosition(sub, o)
	if err == nil {
		t.Fatal("ClosePosition() = nil error, want refusal for missing expiry")
	}
	if !strings.Contains(err.Error(), "missing strike/expiry") {
		t.Errorf("error = %q, want it to mention missing strike/expiry", err.Error())
	}
}

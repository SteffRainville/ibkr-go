package ibkr

import (
	"strings"
	"testing"
)

// ── resolveBuyContract: the identity-carrying core ───────────────────────────

// TestResolveBuyContract_UsesCarriedIdentityNotATM is the regression lock for
// a "wrong contract actually purchased" class of incident: the caller
// resolved and priced strike 580, but the dashboard's active leg was still
// the older estimate (565) while 580 sat pending its own quote. If
// resolveBuyContract consulted the current ATM chain instead of the identity
// carried on the OrderRequest, the real order would buy 565 while the
// caller's own records say 580.
func TestResolveBuyContract_UsesCarriedIdentityNotATM(t *testing.T) {
	s := newTestSession()
	// The ATM chain's active leg has drifted to 565 — if resolveBuyContract
	// consulted it, the test would catch the regression by returning 565.
	s.optChain.mktReqs = map[int64]*optMktReq{
		1: {symbol: "AMD", right: "put", strike: 565, expiry: "20260727", bid: 6.60, ask: 6.85},
	}

	o := OrderRequest{Symbol: "AMD", Tag: "put", SecType: "OPT", Qty: 200, Strike: 580, OptionExpiry: "20260727", Bid: 40.10, Ask: 41.00}

	strike, expiry, bid, ask, mid, err := s.resolveBuyContract(o, -1)
	if err != nil {
		t.Fatalf("resolveBuyContract() error = %v, want nil", err)
	}
	if strike != 580 {
		t.Errorf("strike = %v, want 580 (the carried identity), NOT the drifted active leg 565", strike)
	}
	if expiry != "20260727" {
		t.Errorf("expiry = %q, want carried 20260727", expiry)
	}
	if bid != 40.10 || ask != 41.00 {
		t.Errorf("bid/ask = %v/%v, want carried 40.10/41.00", bid, ask)
	}
	if want := (40.10 + 41.00) / 2; mid != want {
		t.Errorf("mid = %v, want %v (bid/ask average of the carried quote)", mid, want)
	}
}

// TestResolveBuyContract_FallsBackToATMWhenIdentityAbsent verifies the one
// legitimate ATM path: a manual buy has no resolved contract yet, so
// resolveBuyContract falls back to the current active leg — same fallback
// resolveCloseContract already uses for an untracked manual close.
func TestResolveBuyContract_FallsBackToATMWhenIdentityAbsent(t *testing.T) {
	s := newTestSession()
	s.optChain.mktReqs = map[int64]*optMktReq{
		1: {symbol: "AMD", right: "put", strike: 565, expiry: "20260727", bid: 6.60, ask: 6.85},
	}

	o := OrderRequest{Symbol: "AMD", Tag: "put", Qty: 600} // no strike/expiry (manual buy)

	strike, expiry, bid, ask, _, err := s.resolveBuyContract(o, -1)
	if err != nil {
		t.Fatalf("resolveBuyContract() error = %v, want nil", err)
	}
	if strike != 565 || expiry != "20260727" {
		t.Errorf("strike/expiry = %v/%q, want ATM 565/20260727", strike, expiry)
	}
	if bid != 6.60 || ask != 6.85 {
		t.Errorf("bid/ask = %v/%v, want the ATM leg's quote 6.60/6.85", bid, ask)
	}
}

// TestResolveBuyContract_ErrorsWhenNoActiveLegAndIdentityAbsent verifies the
// defensive failure path: no carried identity and no resolved ATM leg either
// (e.g. before the option chain has resolved at all) must error rather than
// buy an empty contract.
func TestResolveBuyContract_ErrorsWhenNoActiveLegAndIdentityAbsent(t *testing.T) {
	s := newTestSession() // no mktReqs at all
	o := OrderRequest{Symbol: "AMD", Tag: "put", Qty: 600}

	if _, _, _, _, _, err := s.resolveBuyContract(o, -1); err == nil {
		t.Fatal("resolveBuyContract() = nil error, want a resolution failure when no contract is resolved yet")
	}
}

// TestResolveBuyContract_RefusesPartialIdentity mirrors
// TestResolveCloseContract_RefusesPartialIdentity: a partial identity (strike
// set, expiry absent) is refused rather than silently ATM-resolved.
func TestResolveBuyContract_RefusesPartialIdentity(t *testing.T) {
	s := newTestSession()
	s.optChain.mktReqs = map[int64]*optMktReq{
		1: {symbol: "AMD", right: "put", strike: 565, expiry: "20260727", bid: 6.60, ask: 6.85},
	}
	o := OrderRequest{Symbol: "AMD", Tag: "put", Qty: 600, Strike: 580} // strike set, no expiry

	_, _, _, _, _, err := s.resolveBuyContract(o, -1)
	if err == nil {
		t.Fatal("resolveBuyContract() = nil error, want refusal for partial identity")
	}
	if !strings.Contains(err.Error(), "partial contract identity") {
		t.Errorf("error = %q, want it to mention partial contract identity", err.Error())
	}
}

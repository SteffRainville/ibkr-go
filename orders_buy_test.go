package ibkr

import (
	"strings"
	"testing"
)

// ── resolveBuyContract: the identity-carrying core ───────────────────────────

// TestResolveBuyContract_UsesCarriedIdentity is the regression lock for a
// "wrong contract actually purchased" class of incident: the caller resolved
// and priced strike 580, and the real order must buy exactly that, with exactly
// the quote it was priced against.
func TestResolveBuyContract_UsesCarriedIdentity(t *testing.T) {
	s := newTestSession()

	o := OrderRequest{Symbol: "AMD", Tag: "put", SecType: "OPT", Qty: 200, Strike: 580, OptionExpiry: "20260727", Bid: 40.10, Ask: 41.00}

	strike, expiry, bid, ask, mid, err := s.resolveBuyContract(o)
	if err != nil {
		t.Fatalf("resolveBuyContract() error = %v, want nil", err)
	}
	if strike != 580 {
		t.Errorf("strike = %v, want the carried 580", strike)
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

// TestResolveBuyContract_RefusesAbsentIdentity pins the change that removed the
// last ATM fallback.
//
// An order with no strike/expiry used to be resolved against "whatever contract
// the background ATM leg is currently displaying" — the manual dashboard buy
// path, the one caller that never ran a delta probe. Those legs are gone, and
// letting a display heuristic choose a real order's strike was the drifted-ATM
// bug (2026-07-08) with extra steps. The manual path now resolves its own
// contract through ResolveEntryStrike, exactly as the bot does, and arrives here
// with a full identity.
func TestResolveBuyContract_RefusesAbsentIdentity(t *testing.T) {
	s := newTestSession()
	o := OrderRequest{Symbol: "AMD", Tag: "put", Qty: 600} // no strike/expiry

	_, _, _, _, _, err := s.resolveBuyContract(o)
	if err == nil {
		t.Fatal("resolveBuyContract() = nil error, want a refusal — there is nothing to fall back to")
	}
	if !strings.Contains(err.Error(), "contract identity required") {
		t.Errorf("error = %q, want it to say the identity is required", err.Error())
	}
}

// TestResolveBuyContract_RefusesPartialIdentity: a half-identity (strike set,
// expiry absent) is refused rather than guessed at. A position pinned with an
// empty expiry can never be closed.
func TestResolveBuyContract_RefusesPartialIdentity(t *testing.T) {
	s := newTestSession()
	o := OrderRequest{Symbol: "AMD", Tag: "put", Qty: 600, Strike: 580} // strike set, no expiry

	_, _, _, _, _, err := s.resolveBuyContract(o)
	if err == nil {
		t.Fatal("resolveBuyContract() = nil error, want refusal for partial identity")
	}
	if !strings.Contains(err.Error(), "contract identity required") {
		t.Errorf("error = %q, want it to say the identity is required", err.Error())
	}
}

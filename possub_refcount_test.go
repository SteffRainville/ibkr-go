package ibkr

import "testing"

// TestPositionStrikeSub_SharedAcrossHolders reproduces the 2026-08-04 IWM
// incident: two independent positions (in production, two different robots)
// resolve to the same contract and share one IB subscription by design. Before
// the refcount, UnsubscribePositionStrike tore the subscription down
// unconditionally, so the first holder to exit silently killed the feed for
// every other holder still open on that exact contract — freezing their
// stop-loss/trailing-stop valuation at whatever price last ticked.
// VWmacdOptionRobot's IWM 298 PUT hit its trailing stop and unsubscribed reqID
// 10011 while OrbOptionRobot's own IWM 298 PUT, opened a minute earlier and
// sharing the same contract, was still open — its dashboard then showed a
// frozen $1.83 quote for the rest of the session while the real market fell to
// $0.02.
//
// The refcount makes subscribe/unsubscribe additive: the feed is only released
// once every holder has released it.
func TestPositionStrikeSub_SharedAcrossHolders(t *testing.T) {
	s := newRotationTestSession(nil)
	key := lk("IWM", "put", 298, "20260806")

	// Seed the state as if the first holder (VWmacdOptionRobot) already
	// subscribed — bypasses the real ReqMktData/mdLines call, which needs a
	// live IB client this test has none of. This is exactly the state
	// SubscribePositionStrike's "new subscription" branch would have left.
	seedLeg(s, key, legOpts{reqID: 10011, pins: 1})

	// Second holder (OrbOptionRobot) subscribes to the identical contract —
	// must join the existing subscription, not attempt a second ReqMktData.
	s.SubscribePositionStrike("IWM", "put", 298, "20260806")

	pins, ok := legPins(s, key)
	if !ok {
		t.Fatal("subscription for second holder vanished — expected it to join the existing one")
	}
	if pins != 2 {
		t.Fatalf("pins after second holder joins = %d, want 2", pins)
	}

	// First holder (VWmacdOptionRobot) exits and releases its interest. The
	// feed must survive — OrbOptionRobot's position is still open on it.
	s.UnsubscribePositionStrike("IWM", "put", 298, "20260806")

	pins, ok = legPins(s, key)
	if !ok {
		t.Fatal("subscription was torn down after only one of two holders released it — this is the 2026-08-04 IWM incident: a sibling's exit freezes this position's stop-loss quote")
	}
	if pins != 1 {
		t.Fatalf("pins after first release = %d, want 1", pins)
	}
}

// TestPositionStrikeSub_SameStrikeDifferentExpiryIsADifferentContract pins the
// expiry into the identity. The old posSubKey was "symbol|right|strike" with no
// expiry, so a weekly and a monthly at the same strike collided: subscribing
// the second was a no-op that silently handed it the first's feed, and the
// first exit released a contract the second still needed.
func TestPositionStrikeSub_SameStrikeDifferentExpiryIsADifferentContract(t *testing.T) {
	s := withOfflineClient(newRotationTestSession(nil))
	weekly := lk("SPY", "put", 640, "20260807")
	monthly := lk("SPY", "put", 640, "20260821")

	seedLeg(s, weekly, legOpts{reqID: 10011, pins: 1})
	seedLeg(s, monthly, legOpts{reqID: 10012, pins: 1})

	s.UnsubscribePositionStrike("SPY", "put", 640, "20260807")

	if _, ok := legPins(s, weekly); ok {
		t.Fatal("the weekly's leg survived its only holder releasing it")
	}
	if pins, ok := legPins(s, monthly); !ok || pins != 1 {
		t.Fatal("releasing the WEEKLY tore down the MONTHLY — same strike, different contract")
	}
}

// TestPositionStrikeSub_LastHolderTearsTheFeedDown is the other half of the
// refcount guarantee: once nothing holds a contract, its line goes back.
//
// This used to be TestPositionStrikeSub_SelectorKeepsTheFeedAfterTheExit,
// asserting that an exit did not blank a watchlist row displaying the same
// contract. Rows hold no contract now, so pins are the only holders and
// "the last one leaves" is the whole condition — which makes the release path
// simpler, not weaker: the sibling-position case above is the one that
// actually mattered (the 2026-08-04 IWM incident) and it still holds.
func TestPositionStrikeSub_LastHolderTearsTheFeedDown(t *testing.T) {
	s := withOfflineClient(newRotationTestSession(nil))
	key := lk("IWM", "put", 298, "20260806")
	seedLeg(s, key, legOpts{reqID: 10011, pins: 1})

	s.UnsubscribePositionStrike("IWM", "put", 298, "20260806")

	if pins, ok := legPins(s, key); ok {
		t.Fatalf("the leg survived its last holder releasing it (pins=%d) — its market-data line is leaked", pins)
	}
}

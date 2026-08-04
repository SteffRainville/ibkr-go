package ibkr

import "testing"

// TestPositionStrikeSub_SharedAcrossHolders reproduces the 2026-08-04 IWM
// incident: two independent positions (in production, two different robots)
// resolve to the same symbol+right+strike and share one IB subscription by
// design (posSubKey carries no robot identity). Before this fix,
// UnsubscribePositionStrike tore the subscription down unconditionally, so
// the first holder to exit silently killed the feed for every other holder
// still open on that exact contract — freezing their stop-loss/trailing-stop
// valuation at whatever price last ticked. VWmacdOptionRobot's IWM 298 PUT
// hit its trailing stop and unsubscribed reqID 10011 while OrbOptionRobot's
// own IWM 298 PUT, opened a minute earlier and sharing the same key, was
// still open — its dashboard then showed a frozen $1.83 quote for the rest
// of the session while the real market fell to $0.02.
//
// refCount makes subscribe/unsubscribe additive: the feed is only released
// once every holder has released it.
func TestPositionStrikeSub_SharedAcrossHolders(t *testing.T) {
	s := newRotationTestSession(nil)
	key := posSubKey("IWM", "put", 298)

	// Seed the state as if the first holder (VWmacdOptionRobot) already
	// subscribed — bypasses the real ReqMktData/mdLines call, which needs a
	// live IB client this test has none of. This is exactly the state
	// SubscribePositionStrike's "new subscription" branch would have left.
	s.optChain.posSubs[10011] = &posStrikeSub{
		symbol: "IWM", right: "put", strike: 298, expiry: "20260806",
		reqID: 10011, refCount: 1,
	}
	s.optChain.posSubKeys[key] = 10011

	// Second holder (OrbOptionRobot) subscribes to the identical contract —
	// must join the existing subscription, not attempt a second ReqMktData.
	s.SubscribePositionStrike("IWM", "put", 298, "20260806")

	sub, ok := s.optChain.posSubs[10011]
	if !ok {
		t.Fatal("subscription for second holder vanished — expected it to join the existing one")
	}
	if sub.refCount != 2 {
		t.Fatalf("refCount after second holder joins = %d, want 2", sub.refCount)
	}

	// First holder (VWmacdOptionRobot) exits and releases its interest. The
	// feed must survive — OrbOptionRobot's position is still open on it.
	s.UnsubscribePositionStrike("IWM", "put", 298)

	sub, ok = s.optChain.posSubs[10011]
	if !ok {
		t.Fatal("subscription was torn down after only one of two holders released it — this is the 2026-08-04 IWM incident: a sibling's exit freezes this position's stop-loss quote")
	}
	if sub.refCount != 1 {
		t.Fatalf("refCount after first release = %d, want 1", sub.refCount)
	}
	if _, ok := s.optChain.posSubKeys[key]; !ok {
		t.Fatal("posSubKeys entry removed while a holder still holds the subscription")
	}
}

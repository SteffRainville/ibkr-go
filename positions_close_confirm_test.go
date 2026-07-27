package ibkr

import "testing"

// These tests lock the "a close must be confirmed" fix for a real
// mislabel incident: a real-time IB qty=0 for a still-held position must
// NOT finalise a close. Only a leg genuinely absent from a fresh complete
// snapshot is closed.

func TestLegIdentity_OptionMatchesExactContract(t *testing.T) {
	held := PositionInfo{Symbol: "GLD", SecType: "OPT", Right: "C", Strike: 370, Expiry: "20260715"}
	same := PositionInfo{Symbol: "GLD", SecType: "OPT", Right: "C", Strike: 370, Expiry: "20260715"}
	diffStrike := PositionInfo{Symbol: "GLD", SecType: "OPT", Right: "C", Strike: 371, Expiry: "20260715"}
	diffRight := PositionInfo{Symbol: "GLD", SecType: "OPT", Right: "P", Strike: 370, Expiry: "20260715"}

	if legIdentity(held) != legIdentity(same) {
		t.Error("identical contracts must share a leg identity")
	}
	if legIdentity(held) == legIdentity(diffStrike) {
		t.Error("different strike must not share a leg identity")
	}
	if legIdentity(held) == legIdentity(diffRight) {
		t.Error("call and put must not share a leg identity")
	}
}

// TestPartitionPendingCloses_TransientQtyZeroIgnored is the core reproduction:
// a qty=0 arrived for the GLD 370 call, but the fresh complete snapshot still shows
// it held. It must be classified "still held" (transient — ignore), NOT closed.
func TestPartitionPendingCloses_TransientQtyZeroIgnored(t *testing.T) {
	gld := PositionInfo{Symbol: "GLD", SecType: "OPT", Right: "C", Strike: 370, Expiry: "20260715", Position: 12}
	pending := map[string]PositionInfo{
		legIdentity(gld): gld,
	}
	// Fresh complete snapshot still reports the GLD call as held.
	snapshot := []PositionInfo{gld}

	confirmed, stillHeld := partitionPendingCloses(pending, snapshot)
	if len(confirmed) != 0 {
		t.Fatalf("confirmed = %d, want 0 — a still-held position must never be closed on a transient qty=0", len(confirmed))
	}
	if len(stillHeld) != 1 || stillHeld[0].Symbol != "GLD" {
		t.Fatalf("stillHeld = %v, want the GLD leg", stillHeld)
	}
}

// TestPartitionPendingCloses_GenuineCloseConfirmed verifies a real close (leg absent
// from the fresh snapshot) is finalised.
func TestPartitionPendingCloses_GenuineCloseConfirmed(t *testing.T) {
	gld := PositionInfo{Symbol: "GLD", SecType: "OPT", Right: "C", Strike: 370, Expiry: "20260715", Position: 0}
	pending := map[string]PositionInfo{
		legIdentity(gld): gld,
	}
	// Fresh snapshot holds an unrelated position only — GLD 370 call is truly gone.
	snapshot := []PositionInfo{
		{Symbol: "SPY", SecType: "OPT", Right: "P", Strike: 754, Expiry: "20260715", Position: 5},
	}

	confirmed, stillHeld := partitionPendingCloses(pending, snapshot)
	if len(stillHeld) != 0 {
		t.Fatalf("stillHeld = %d, want 0", len(stillHeld))
	}
	if len(confirmed) != 1 || confirmed[0].Symbol != "GLD" {
		t.Fatalf("confirmed = %v, want the GLD leg finalised", confirmed)
	}
}

// TestPartitionPendingCloses_MixedAndStock covers a stock leg and multiple pending
// legs resolved in one snapshot.
func TestPartitionPendingCloses_MixedAndStock(t *testing.T) {
	heldOpt := PositionInfo{Symbol: "IWM", SecType: "OPT", Right: "P", Strike: 296, Expiry: "20260715", Position: 10}
	goneOpt := PositionInfo{Symbol: "TQQQ", SecType: "OPT", Right: "C", Strike: 72, Expiry: "20260717"}
	goneStk := PositionInfo{Symbol: "AAPL", SecType: "STK"}
	pending := map[string]PositionInfo{
		legIdentity(heldOpt): heldOpt,
		legIdentity(goneOpt): goneOpt,
		legIdentity(goneStk): goneStk,
	}
	snapshot := []PositionInfo{heldOpt} // only IWM put still held

	confirmed, stillHeld := partitionPendingCloses(pending, snapshot)
	if len(stillHeld) != 1 || stillHeld[0].Symbol != "IWM" {
		t.Fatalf("stillHeld = %v, want only IWM", stillHeld)
	}
	if len(confirmed) != 2 {
		t.Fatalf("confirmed = %d, want 2 (TQQQ opt + AAPL stk)", len(confirmed))
	}
}

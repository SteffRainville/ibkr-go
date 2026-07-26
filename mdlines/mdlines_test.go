package mdlines

import (
	"testing"
	"time"
)

// TestLedger_DiscretionaryNewYieldsToReserve verifies first-quote background
// lines stop being granted once usage reaches (max - ReserveNew), leaving
// headroom.
func TestLedger_DiscretionaryNewYieldsToReserve(t *testing.T) {
	max := 100
	l := NewLedger(max, 50)

	granted := 0
	for i := int64(0); i < 200; i++ {
		if l.GrantDiscretionaryNew(i) {
			granted++
		}
	}
	if want := max - ReserveNew; granted != want {
		t.Fatalf("DiscretionaryNew grants = %d, want %d (max %d − reserve %d)", granted, want, max, ReserveNew)
	}
	if used, _ := l.Status(); used != max-ReserveNew {
		t.Fatalf("used = %d, want %d", used, max-ReserveNew)
	}
}

// TestLedger_DiscretionaryChurnYieldsToReserve verifies background-refresh
// lines stop being granted at the tighter (max - ReserveChurn) threshold.
func TestLedger_DiscretionaryChurnYieldsToReserve(t *testing.T) {
	max := 100
	l := NewLedger(max, 50)

	granted := 0
	for i := int64(0); i < 200; i++ {
		if l.GrantDiscretionaryChurn(i) {
			granted++
		}
	}
	if want := max - ReserveChurn; granted != want {
		t.Fatalf("DiscretionaryChurn grants = %d, want %d (max %d − reserve %d)", granted, want, max, ReserveChurn)
	}
	if used, _ := l.Status(); used != max-ReserveChurn {
		t.Fatalf("used = %d, want %d", used, max-ReserveChurn)
	}
}

// TestLedger_DiscretionaryNewOutranksChurn is the core priority-tiering
// test: once usage reaches the churn threshold, churn requests stop but
// first-quote (new) requests keep being granted all the way to the wider
// ReserveNew threshold, and guaranteed position lines are never refused
// throughout.
func TestLedger_DiscretionaryNewOutranksChurn(t *testing.T) {
	max := 100
	l := NewLedger(max, 50)

	// Saturate to the churn threshold using churn grants.
	for i := int64(0); i < int64(max-ReserveChurn); i++ {
		if !l.GrantDiscretionaryChurn(i) {
			t.Fatalf("churn grant %d refused before reaching the churn threshold", i)
		}
	}
	if used, _ := l.Status(); used != max-ReserveChurn {
		t.Fatalf("used after saturating churn = %d, want %d", used, max-ReserveChurn)
	}

	// Further churn requests must now be refused.
	if l.GrantDiscretionaryChurn(1000) {
		t.Fatal("churn grant succeeded past the churn threshold — should be refused")
	}

	// But new requests must keep being granted, since it's a wider,
	// higher-priority reserve — up to max-ReserveNew.
	newGranted := 0
	for i := int64(2000); i < int64(2000+ReserveChurn-ReserveNew+5); i++ {
		if l.GrantDiscretionaryNew(i) {
			newGranted++
		}
	}
	if want := ReserveChurn - ReserveNew; newGranted != want {
		t.Fatalf("DiscretionaryNew grants past the churn threshold = %d, want %d (the freed 75-85 band)", newGranted, want)
	}
	if used, _ := l.Status(); used != max-ReserveNew {
		t.Fatalf("used after saturating DiscretionaryNew too = %d, want %d", used, max-ReserveNew)
	}

	// Guaranteed position lines must never be refused, even now.
	for i := int64(5000); i < 5010; i++ {
		if !l.GrantGuaranteed(i, CategoryPosition) {
			t.Fatalf("position line %d refused — positions must never be refused", i)
		}
	}
}

// TestLedger_PositionsUseReserve verifies that guaranteed position lines can
// be placed in the reserve band that discretionary lines are forbidden
// from — the core "positions win" guarantee.
func TestLedger_PositionsUseReserve(t *testing.T) {
	l := NewLedger(100, 50)

	// Saturate the DiscretionaryNew discretionary budget.
	for i := int64(0); i < 100; i++ {
		l.GrantDiscretionaryNew(i)
	}
	used, _ := l.Status()
	if used != 85 {
		t.Fatalf("after saturation used = %d, want 85", used)
	}

	// Positions must still be grantable — they draw from the reserve.
	for i := int64(1000); i < 1020; i++ {
		if !l.GrantGuaranteed(i, CategoryPosition) {
			t.Fatalf("position line %d refused — positions must never be refused", i)
		}
	}
	used, _ = l.Status()
	if used != 105 {
		t.Fatalf("used = %d, want 105 (85 DiscretionaryNew + 20 positions)", used)
	}

	// A fresh DiscretionaryNew request is still blocked.
	if l.GrantDiscretionaryNew(2000) {
		t.Fatal("DiscretionaryNew grant succeeded inside the positions reserve")
	}
}

// TestLedger_GuaranteedNeverRefusedOverCap verifies guaranteed lines are
// placed even past the cap (correctness over cap adherence).
func TestLedger_GuaranteedNeverRefusedOverCap(t *testing.T) {
	l := NewLedger(10, 50)
	for i := int64(0); i < 25; i++ {
		if !l.GrantGuaranteed(i, CategoryStock) {
			t.Fatalf("guaranteed line %d refused", i)
		}
	}
	if used, max := l.Status(); used != 25 || max != 10 {
		t.Fatalf("used/max = %d/%d, want 25/10", used, max)
	}
}

// TestLedger_ReleaseFreesLine verifies Release decrements usage and re-opens
// discretionary budget, and that double-release / unknown-release are safe.
func TestLedger_ReleaseFreesLine(t *testing.T) {
	l := NewLedger(100, 50)
	for i := int64(0); i < 85; i++ {
		l.GrantDiscretionaryNew(i)
	}
	if l.GrantDiscretionaryNew(999) {
		t.Fatal("expected DiscretionaryNew block at reserve boundary")
	}
	l.Release(0)      // free one
	l.Release(123456) // unknown — must be a no-op
	if !l.GrantDiscretionaryNew(999) {
		t.Fatal("DiscretionaryNew should be grantable after a release")
	}
	l.Release(999)
	l.Release(999) // double release — must not underflow
	if used, _ := l.Status(); used != 84 {
		t.Fatalf("used = %d, want 84", used)
	}
}

// TestLedger_Snapshots verifies snapshot lines count against the cap, are
// capped once full, and release cleanly.
func TestLedger_Snapshots(t *testing.T) {
	l := NewLedger(10, 50)
	// Fill 8 lines with guaranteed streams.
	for i := int64(0); i < 8; i++ {
		l.GrantGuaranteed(i, CategoryStock)
	}
	// Two snapshots fit (used 8 → 10), the third is capped.
	if !l.GrantSnapshot(100) || !l.GrantSnapshot(101) {
		t.Fatal("snapshots within the cap should be granted")
	}
	if l.GrantSnapshot(102) {
		t.Fatal("snapshot past the cap must be refused")
	}
	if used, max, _, _, _, _ := l.StatusAll(); used != 10 || max != 10 {
		t.Fatalf("used/max = %d/%d, want 10/10", used, max)
	}
	// Releasing a snapshot re-opens a slot.
	l.Release(100)
	if !l.GrantSnapshot(102) {
		t.Fatal("snapshot should fit after a release")
	}
}

// TestLedger_HistoricalPool verifies the keep-up-to-date stream pool is
// separate from the line pool and enforces its own ceiling.
func TestLedger_HistoricalPool(t *testing.T) {
	l := NewLedger(100, 3)
	for i := int64(0); i < 3; i++ {
		if !l.GrantHist(i) {
			t.Fatalf("hist stream %d refused under ceiling", i)
		}
	}
	if l.GrantHist(99) {
		t.Fatal("hist stream past the ceiling must be refused")
	}
	// Historical streams do NOT count against the line pool.
	if used, _, histUsed, histMax, _, _ := l.StatusAll(); used != 0 || histUsed != 3 || histMax != 3 {
		t.Fatalf("StatusAll = used %d histUsed %d histMax %d, want 0/3/3", used, histUsed, histMax)
	}
	l.ReleaseHist(0)
	if !l.GrantHist(99) {
		t.Fatal("hist stream should fit after a release")
	}
}

// TestLedger_StatusAllSplitsStockAndOption verifies StatusAll's stock/option
// breakdown sums the right categories and excludes snapshots.
func TestLedger_StatusAllSplitsStockAndOption(t *testing.T) {
	l := NewLedger(100, 50)

	for i := int64(0); i < 5; i++ {
		l.GrantGuaranteed(i, CategoryStock) // 5 stock lines
	}
	for i := int64(10); i < 13; i++ {
		l.GrantGuaranteed(i, CategoryPosition) // 3 position lines
	}
	for i := int64(20); i < 24; i++ {
		l.GrantDiscretionaryNew(i) // 4 first-quote lines
	}
	for i := int64(30); i < 32; i++ {
		l.GrantDiscretionaryChurn(i) // 2 refresh lines
	}
	l.TrackSnapshot(40) // 1 snapshot line — must be excluded from both buckets

	used, max, _, _, stockUsed, optionUsed := l.StatusAll()
	if stockUsed != 5 {
		t.Errorf("stockUsed = %d, want 5", stockUsed)
	}
	if optionUsed != 9 { // 3 position + 4 new + 2 churn
		t.Errorf("optionUsed = %d, want 9", optionUsed)
	}
	if used != 15 { // 5 + 3 + 4 + 2 + 1 snapshot
		t.Errorf("used = %d, want 15", used)
	}
	if max != 100 {
		t.Errorf("max = %d, want 100", max)
	}
	if stockUsed+optionUsed == used {
		t.Errorf("stockUsed+optionUsed (%d) unexpectedly equals used (%d) — the snapshot line should make used one higher",
			stockUsed+optionUsed, used)
	}
}

// TestLedger_OnChangeFires verifies the status callback fires on grant/release.
func TestLedger_OnChangeFires(t *testing.T) {
	l := NewLedger(100, 50)
	var lastUsed, calls int
	l.SetOnChange(func(used, _ int) { lastUsed = used; calls++ })

	l.GrantGuaranteed(1, CategoryStock)
	l.GrantDiscretionaryNew(2)
	l.Release(1)
	if calls < 3 {
		t.Fatalf("onChange calls = %d, want ≥3", calls)
	}
	if lastUsed != 1 {
		t.Fatalf("lastUsed = %d, want 1", lastUsed)
	}
}

// TestLedger_ReapSnapshots verifies the backstop reaper frees only snapshot
// lines older than maxAge, returns them for cancellation, and never touches
// non-snapshot (guaranteed/discretionary) lines regardless of age.
func TestLedger_ReapSnapshots(t *testing.T) {
	l := NewLedger(100, 50)

	l.GrantGuaranteed(1, CategoryStock) // non-snapshot — must never be reaped
	l.GrantSnapshot(100)                // fresh snapshot — must survive
	l.GrantSnapshot(101)                // will be aged past maxAge — must be reaped
	l.GrantSnapshot(102)                // will be aged past maxAge — must be reaped

	// Age two of the three snapshots well past maxAge.
	old := time.Now().Add(-5 * time.Minute)
	l.snapAt[101] = old
	l.snapAt[102] = old

	reaped := l.ReapSnapshots(time.Minute)

	if len(reaped) != 2 {
		t.Fatalf("reaped %d lines (%v), want 2 (101,102)", len(reaped), reaped)
	}
	got := map[int64]bool{reaped[0]: true, reaped[1]: true}
	if !got[101] || !got[102] {
		t.Errorf("reaped = %v, want exactly {101,102}", reaped)
	}
	// The fresh snapshot and the guaranteed line remain.
	if _, _, _, _, snap, _ := l.CategoryCounts(); snap != 1 {
		t.Errorf("snapshot count = %d after reap, want 1 (the fresh one)", snap)
	}
	if used := func() int { u, _ := l.Status(); return u }(); used != 2 {
		t.Errorf("used = %d after reap, want 2 (1 stock + 1 fresh snapshot)", used)
	}
	if _, ok := l.snapAt[100]; !ok {
		t.Error("fresh snapshot 100 was dropped from snapAt")
	}
	if _, ok := l.snapAt[101]; ok {
		t.Error("reaped snapshot 101 still in snapAt")
	}
	// A second reap with nothing stale returns nothing.
	if again := l.ReapSnapshots(time.Minute); len(again) != 0 {
		t.Errorf("second reap returned %v, want none", again)
	}
}

// TestLedger_AllReqIDs verifies the teardown enumeration returns every
// market-data line reqID (all categories) but keeps the keep-up-to-date
// historical streams in the separate AllHistReqIDs set.
func TestLedger_AllReqIDs(t *testing.T) {
	l := NewLedger(100, 50)
	l.GrantGuaranteed(1, CategoryStock)
	l.GrantGuaranteed(2, CategoryPosition)
	l.GrantDiscretionaryNew(3)
	l.GrantDiscretionaryChurn(4)
	l.GrantSnapshot(5)
	l.GrantHist(1001)
	l.GrantHist(1002)

	ids := l.AllReqIDs()
	if len(ids) != 5 {
		t.Fatalf("AllReqIDs len = %d (%v), want 5", len(ids), ids)
	}
	want := map[int64]bool{1: true, 2: true, 3: true, 4: true, 5: true}
	for _, id := range ids {
		if !want[id] {
			t.Errorf("AllReqIDs returned unexpected reqID %d", id)
		}
		delete(want, id)
	}
	if len(want) != 0 {
		t.Errorf("AllReqIDs missing reqIDs %v", want)
	}

	hist := l.AllHistReqIDs()
	if len(hist) != 2 {
		t.Fatalf("AllHistReqIDs len = %d (%v), want 2", len(hist), hist)
	}
	for _, id := range hist {
		if id != 1001 && id != 1002 {
			t.Errorf("AllHistReqIDs returned unexpected reqID %d", id)
		}
	}
}

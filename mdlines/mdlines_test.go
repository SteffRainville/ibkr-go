package mdlines

import (
	"testing"
	"time"
)

// The three reserve tests that used to open this file are gone with the
// reserves themselves. They pinned how far CategoryDiscretionaryNew and
// CategoryDiscretionaryChurn could grow before yielding headroom to positions
// and probes — a policy that only existed because one streaming line per
// watchlist option row competed with them. Nothing is rationed any more; what
// remains is the guarantee those reserves were protecting, tested directly
// below: a position line is never refused, and a probe is refused only at the
// hard cap.

// TestLedger_GuaranteedGrantsSurviveAFullPool is the "positions win" guarantee
// that the reserve band used to protect: with the pool completely full of
// probes, an open position's line must still be placed.
func TestLedger_GuaranteedGrantsSurviveAFullPool(t *testing.T) {
	l := NewLedger(100, 50)

	granted := 0
	for i := int64(0); i < 200; i++ {
		if l.GrantProbe(i) {
			granted++
		}
	}
	// Probes fill to the hard cap — buffer included, since a probe may dip into
	// it — and are refused beyond it.
	if granted != 100 {
		t.Fatalf("probe grants = %d, want 100 (up to the hard cap)", granted)
	}
	if l.GrantProbe(9999) {
		t.Fatal("a probe was granted past the hard cap")
	}

	// Positions are still grantable, and go over the cap rather than be refused.
	for i := int64(1000); i < 1020; i++ {
		if !l.GrantGuaranteed(i, CategoryPosition) {
			t.Fatalf("position line %d refused — a held contract must never lose its feed", i)
		}
	}
	if used, _ := l.Status(); used != 120 {
		t.Fatalf("used = %d, want 120 (100 probes + 20 positions)", used)
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
	for i := int64(0); i < 100; i++ {
		l.GrantProbe(i)
	}
	if l.GrantProbe(999) {
		t.Fatal("expected a probe to be refused at the hard cap")
	}
	l.Release(0)      // free one
	l.Release(123456) // unknown — must be a no-op
	if !l.GrantProbe(999) {
		t.Fatal("a probe should be grantable after a release")
	}
	l.Release(999)
	l.Release(999) // double release — must not underflow
	if used, _ := l.Status(); used != 99 {
		t.Fatalf("used = %d, want 99", used)
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
	for i := int64(20); i < 26; i++ {
		l.GrantProbe(i) // 6 in-flight entry probes
	}
	l.TrackSnapshot(40) // 1 snapshot line — must be excluded from both buckets

	used, max, _, _, stockUsed, optionUsed := l.StatusAll()
	if stockUsed != 5 {
		t.Errorf("stockUsed = %d, want 5", stockUsed)
	}
	if optionUsed != 9 { // 3 position + 6 probe
		t.Errorf("optionUsed = %d, want 9", optionUsed)
	}
	if used != 15 { // 5 + 3 + 6 + 1 snapshot
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
	l.GrantProbe(2)
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
	if _, _, snap, _ := l.CategoryCounts(); snap != 1 {
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

// TestLedger_ReapProbes verifies the backstop reaper frees only probe lines
// older than maxAge, mirroring TestLedger_ReapSnapshots — a probe whose
// owning resolution never released it must not sit stuck for the rest of the
// TCP session.
func TestLedger_ReapProbes(t *testing.T) {
	l := NewLedger(100, 50)

	l.GrantGuaranteed(1, CategoryStock) // non-probe — must never be reaped
	if !l.GrantProbe(100) {             // fresh probe — must survive
		t.Fatal("GrantProbe(100) refused")
	}
	if !l.GrantProbe(101) { // will be aged past maxAge — must be reaped
		t.Fatal("GrantProbe(101) refused")
	}
	if !l.GrantProbe(102) { // will be aged past maxAge — must be reaped
		t.Fatal("GrantProbe(102) refused")
	}

	old := time.Now().Add(-5 * time.Minute)
	l.probeAt[101] = old
	l.probeAt[102] = old

	reaped := l.ReapProbes(time.Minute)

	if len(reaped) != 2 {
		t.Fatalf("reaped %d lines (%v), want 2 (101,102)", len(reaped), reaped)
	}
	got := map[int64]bool{reaped[0]: true, reaped[1]: true}
	if !got[101] || !got[102] {
		t.Errorf("reaped = %v, want exactly {101,102}", reaped)
	}
	if _, _, _, probe := l.CategoryCounts(); probe != 1 {
		t.Errorf("probe count = %d after reap, want 1 (the fresh one)", probe)
	}
	if used := func() int { u, _ := l.Status(); return u }(); used != 2 {
		t.Errorf("used = %d after reap, want 2 (1 stock + 1 fresh probe)", used)
	}
	if _, ok := l.probeAt[100]; !ok {
		t.Error("fresh probe 100 was dropped from probeAt")
	}
	if _, ok := l.probeAt[101]; ok {
		t.Error("reaped probe 101 still in probeAt")
	}
	// A second reap with nothing stale returns nothing.
	if again := l.ReapProbes(time.Minute); len(again) != 0 {
		t.Errorf("second reap returned %v, want none", again)
	}
}

// TestLedger_ReclassifyClearsProbeAge verifies a probe promoted to a
// background line via Reclassify (the winning delta candidate) stops being
// probe-aged, so it is never later swept up by ReapProbes even though it
// keeps the same reqID and stays in the ledger indefinitely as a real
// subscription.
func TestLedger_ReclassifyClearsProbeAge(t *testing.T) {
	l := NewLedger(100, 50)

	if !l.GrantProbe(1) {
		t.Fatal("GrantProbe(1) refused")
	}
	l.Reclassify(1, CategoryPosition)
	if _, ok := l.probeAt[1]; ok {
		t.Fatal("probeAt[1] still set after Reclassify out of CategoryProbe")
	}

	l.probeAt[1] = time.Now().Add(-time.Hour) // simulate stale bookkeeping, should be inert
	reaped := l.ReapProbes(time.Minute)
	if len(reaped) != 0 {
		t.Errorf("ReapProbes reaped %v, want none — line 1 is no longer CategoryProbe", reaped)
	}
	if _, pos, _, _ := l.CategoryCounts(); pos != 1 {
		t.Errorf("position count = %d, want 1 — Reclassify must not have been undone", pos)
	}
}

// TestLedger_AllReqIDs verifies the teardown enumeration returns every
// market-data line reqID (all categories) but keeps the keep-up-to-date
// historical streams in the separate AllHistReqIDs set.
func TestLedger_AllReqIDs(t *testing.T) {
	l := NewLedger(100, 50)
	l.GrantGuaranteed(1, CategoryStock)
	l.GrantGuaranteed(2, CategoryPosition)
	l.GrantProbe(3)
	l.GrantGuaranteed(4, CategoryPosition)
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

// TestReserves_ProportionalToTheCap and TestNewLedgerWithReserves_Overrides
// used to close this file. Both tested the percentage-based discretionary
// reserves — that at a cap of 100 they reproduced the fixed 15/25 they replaced,
// and that a caller could override them. There are no reserves to size or
// override now: the categories they rationed have no members, and a pool that
// holds only underlyings, held positions and in-flight probes has nothing to
// ration between.

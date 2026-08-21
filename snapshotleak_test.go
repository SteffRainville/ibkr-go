package ibkr

import (
	"testing"

	"github.com/scmhub/ibapi"

	"github.com/SteffRainville/ibkr-go/mdlines"
)

// These tests lock the market-data-line snapshot leak fix: a snapshot request
// that errors never receives TickSnapshotEnd, so the Error handler must release
// the ledger line itself — otherwise the "Snapshot (transient)" category wedges
// above zero and eventually pushes total usage past the cap.

// benignClientSession returns a Session wired with just enough state to
// drive the Error handler, plus a NOT-connected EClient whose
// CancelMktData/CancelHistoricalData safely no-op, so the cancel calls in
// the error/teardown paths do not panic. Built via NewSession (not a bare
// &Session{}) so every logger field the Error path touches — logger,
// scanLog, optionLog — is a real io.Discard-backed *log.Logger, not a nil
// one that panics on the first Printf.
func benignClientSession() *Session {
	s := NewSession(Options{}, nil, nil)
	s.client = ibapi.NewEClient(&ibapi.Wrapper{}) // not connected → cancels no-op
	s.mdLines = mdlines.NewLedger(100, 50)
	s.mktData.snapReqSymbol = make(map[int64]string)
	s.scanner.snapData = make(map[int64]*scanSnapEntry)
	s.scanner.snapCount = make(map[int64]int)
	s.scanner.cdData = make(map[int64]*scanCDEntry)
	s.scanner.enriched = make(map[int64][]ScanResult)
	s.scanner.pending = make(map[int64]chan []ScanResult)
	s.orders.orderSymbol = make(map[int64]string)
	s.orders.orderAction = make(map[int64]string)
	s.orders.orderRoutes = make(map[int64]subRoute)
	return s
}

// TestError_ReleasesScannerSnapshotLine is the dominant leak: an errored scanner
// enrichment snapshot must free its ledger line. Because the scanner branch
// decrements the counter and finalises the scan normally, the ctx.Done cleanup
// that would otherwise release the line never runs — so the release must happen
// here or the line leaks for the whole session.
func TestError_ReleasesScannerSnapshotLine(t *testing.T) {
	s := benignClientSession()

	const parent = int64(3001)
	const snapID = int64(3002)
	done := make(chan []ScanResult, 1)
	s.scanner.pending[parent] = done
	s.scanner.enriched[parent] = make([]ScanResult, 1)
	s.scanner.snapData[snapID] = &scanSnapEntry{parentReqID: parent, resultIdx: 0}
	s.scanner.snapCount[parent] = 1
	if !s.mdLines.GrantSnapshot(snapID) {
		t.Fatal("failed to grant the snapshot line under test")
	}

	// IB error 354 = "requested market data is not subscribed" — no TickSnapshotEnd follows.
	s.Error(snapID, 0, 354, "not subscribed", "")

	if _, _, snap, _ := s.mdLines.CategoryCounts(); snap != 0 {
		t.Errorf("snapshot line count = %d after errored scanner snapshot, want 0 (leaked)", snap)
	}
	if used, _ := s.mdLines.Status(); used != 0 {
		t.Errorf("used lines = %d after errored scanner snapshot, want 0", used)
	}
	if _, ok := s.scanner.snapData[snapID]; ok {
		t.Error("scanner snapData entry not cleaned up on error")
	}
	// The scan still finalises so it does not hang on the errored row.
	select {
	case <-done:
	default:
		t.Error("scan was not finalised after the snapshot error")
	}
}

// TestError_ReleasesPositionSnapshotLine covers the second leak: an errored
// positions-page price snapshot (tracked in snapReqSymbol) has no
// TickSnapshotEnd, so the Error handler must release its line and map entry.
func TestError_ReleasesPositionSnapshotLine(t *testing.T) {
	s := benignClientSession()

	const reqID = int64(5001)
	s.mktData.snapReqSymbol[reqID] = "AAPL"
	s.mdLines.TrackSnapshot(reqID)

	if _, _, snap, _ := s.mdLines.CategoryCounts(); snap != 1 {
		t.Fatalf("precondition: snapshot count = %d, want 1", snap)
	}

	s.Error(reqID, 0, 354, "not subscribed", "")

	if _, _, snap, _ := s.mdLines.CategoryCounts(); snap != 0 {
		t.Errorf("snapshot line count = %d after errored position snapshot, want 0 (leaked)", snap)
	}
	if _, ok := s.mktData.snapReqSymbol[reqID]; ok {
		t.Error("snapReqSymbol entry not cleaned up on error")
	}
}

// TestCancelAllSubscriptions_NotConnectedNoPanic verifies the teardown sweep
// enumerates every opened line and issues a cancel without panicking when the
// socket is already down (the cancels harmlessly no-op on a not-connected client).
func TestCancelAllSubscriptions_NotConnectedNoPanic(t *testing.T) {
	s := benignClientSession()

	s.mdLines.GrantGuaranteed(2001, mdlines.CategoryStock)
	s.mdLines.GrantGuaranteed(10001, mdlines.CategoryPosition)
	s.mdLines.GrantProbe(7001)
	s.mdLines.TrackSnapshot(5001)
	s.mdLines.GrantHist(1001)

	// The sweep set must cover every market-data line and the hist stream.
	if got := len(s.mdLines.AllReqIDs()); got != 4 {
		t.Fatalf("allReqIDs len = %d, want 4 (stock+position+probe+snapshot)", got)
	}
	if got := len(s.mdLines.AllHistReqIDs()); got != 1 {
		t.Fatalf("allHistReqIDs len = %d, want 1", got)
	}

	// Must not panic against a not-connected client.
	s.cancelAllSubscriptions()
}

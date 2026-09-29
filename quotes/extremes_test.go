package quotes

import (
	"testing"
	"time"
)

// Within one window every price a side held counts, the standing value at the
// window's opening included.
func TestExtremesCoverEveryTickInTheWindow(t *testing.T) {
	b := NewBook()
	b.TrackExtremes(time.Hour) // every update lands in the same window
	k := ContractKey{Symbol: "QQQ", Right: "call", Strike: 700, Expiry: "20260928"}
	for _, bid := range []float64{2.00, 2.12, 1.92, 2.05} {
		b.SetOptionBid(k, bid)
	}
	b.SetOptionAsk(k, 2.10)
	b.SetOptionAsk(k, 2.20)
	x, ok := b.OptionExtremes(k, time.Now().Truncate(time.Hour))
	if !ok {
		t.Fatal("no extremes")
	}
	if x.BidLow != 1.92 || x.BidHigh != 2.12 || x.AskLow != 2.10 || x.AskHigh != 2.20 {
		t.Fatalf("got %+v", x)
	}
}

func TestExtremesOffByDefault(t *testing.T) {
	b := NewBook()
	b.SetStockBid("QQQ", 1)
	if _, ok := b.StockExtremes("QQQ", time.Now()); ok {
		t.Fatal("extremes without TrackExtremes")
	}
}

// The window boundaries: the window just ended is still answered after a tick
// opens the next; a window with no tick reads the price it held throughout.
func TestExtremesAtWindowBoundaries(t *testing.T) {
	const every = 5 * time.Second
	b := &Book{extEvery: every}
	t0 := time.Date(2026, 9, 28, 9, 45, 0, 0, time.Local)
	e := &extPair{}

	b.noteExt(e, t0.Add(1*time.Second), sideBid, 2.12, [3]float64{2.00, 2.20, 0})
	b.noteExt(e, t0.Add(3*time.Second), sideBid, 1.92, [3]float64{2.12, 2.20, 0})
	b.noteExt(e, t0.Add(6*time.Second), sideBid, 2.05, [3]float64{1.92, 2.20, 0}) // opens [5s,10s)

	w := extremesAt(e, t0, [3]float64{2.05, 2.20, 0})
	if w.BidLow != 1.92 || w.BidHigh != 2.12 || w.OpenBid != 2.00 {
		t.Fatalf("ended window: %+v", w)
	}
	w = extremesAt(e, t0.Add(every), [3]float64{2.05, 2.20, 0})
	if w.BidLow != 1.92 || w.BidHigh != 2.05 {
		t.Fatalf("current window must include the 1.92 it opened on: %+v", w)
	}

	// Nothing from 10s to 20s, then a tick at 21s: [15s,20s) held 2.05.
	b.noteExt(e, t0.Add(21*time.Second), sideBid, 2.30, [3]float64{2.05, 2.20, 0})
	w = extremesAt(e, t0.Add(15*time.Second), [3]float64{2.30, 2.20, 0})
	if w.BidLow != 2.05 || w.BidHigh != 2.05 {
		t.Fatalf("quiet window: %+v", w)
	}
	// No tick at all since the window opened: the quote now held throughout.
	w = extremesAt(e, t0.Add(30*time.Second), [3]float64{2.30, 2.20, 0})
	if w.BidLow != 2.30 || w.BidHigh != 2.30 || w.AskHigh != 2.20 {
		t.Fatalf("untouched window: %+v", w)
	}
}

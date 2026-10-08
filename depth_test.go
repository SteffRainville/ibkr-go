package ibkr

import "testing"

func TestApplyDepthRow(t *testing.T) {
	var side []DepthLevel
	side = applyDepthRow(side, depthInsert, 0, DepthLevel{100, 5})
	side = applyDepthRow(side, depthInsert, 1, DepthLevel{99, 7})
	side = applyDepthRow(side, depthInsert, 1, DepthLevel{99.5, 3}) // pushes 99 down
	if len(side) != 3 || side[1].Price != 99.5 || side[2].Price != 99 {
		t.Fatalf("insert: %+v", side)
	}
	side = applyDepthRow(side, depthUpdate, 0, DepthLevel{100, 9})
	if side[0].Size != 9 {
		t.Fatalf("update: %+v", side)
	}
	side = applyDepthRow(side, depthDelete, 1, DepthLevel{})
	if len(side) != 2 || side[1].Price != 99 {
		t.Fatalf("delete: %+v", side)
	}
	// Out-of-range operations are ignored, never panic.
	side = applyDepthRow(side, depthUpdate, 9, DepthLevel{1, 1})
	side = applyDepthRow(side, depthDelete, 9, DepthLevel{})
	if len(side) != 2 {
		t.Fatalf("out of range: %+v", side)
	}
}

func TestDepthApplyIgnoresCancelledRequest(t *testing.T) {
	var tr depthTracker
	tr.init()
	tr.apply(42, 0, depthInsert, depthSideBid, 100, 5) // no live book: dropped
	b := &liveBook{done: make(chan struct{})}
	tr.live[7] = b
	tr.apply(7, 0, depthInsert, depthSideBid, 100, 5)
	tr.apply(7, 0, depthInsert, 0, 100.1, 4)
	if len(b.bids) != 1 || len(b.asks) != 1 || !b.gotRows {
		t.Fatalf("book: %+v", b)
	}
}

func TestDepthStatusCountsOpenBooks(t *testing.T) {
	s := &Session{}
	s.opts.DepthSlots = 2
	s.depth.init()
	if open, slots := s.DepthStatus(); open != 0 || slots != 2 {
		t.Fatalf("idle: open=%d slots=%d", open, slots)
	}
	s.depth.live[7] = &liveBook{symbol: "AAA"}
	if open, _ := s.DepthStatus(); open != 1 {
		t.Fatalf("one in flight: open=%d", open)
	}
}

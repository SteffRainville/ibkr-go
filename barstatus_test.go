package ibkr

import "testing"

// The table answers "why no bars?": nothing for an unknown symbol, the request
// time, the first and latest bar, and the last error, per symbol.
func TestBarStatusTable(t *testing.T) {
	var tb barStatusTable
	if _, ok := tb.get("ORCL"); ok {
		t.Fatal("status for a symbol never touched")
	}
	tb.reset()
	tb.update("ORCL", func(st *BarStreamStatus) { st.Skipped = true })
	tb.noteBar("SPY")
	tb.noteBar("SPY")
	tb.update("SPY", func(st *BarStreamStatus) { st.ErrCode, st.ErrMsg = 162, "no data" })

	orcl, _ := tb.get("ORCL")
	if !orcl.Skipped || !orcl.FirstBar.IsZero() {
		t.Fatalf("ORCL %+v", orcl)
	}
	spy, _ := tb.get("SPY")
	if spy.FirstBar.IsZero() || spy.LastBar.Before(spy.FirstBar) || spy.ErrCode != 162 {
		t.Fatalf("SPY %+v", spy)
	}
	tb.reset()
	if _, ok := tb.get("SPY"); ok {
		t.Fatal("reset kept a previous session's status")
	}
}

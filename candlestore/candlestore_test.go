package candlestore

import (
	"math"
	"testing"
)

const ts0 = "2026-04-09 09:45:00"
const ts1 = "2026-04-09 09:46:00"

func TestAddHistorical(t *testing.T) {
	s := New()
	s.AddHistorical("TQQQ", ts0, 48.10, 48.50, 47.90, 48.30, 1000, 48.20, 250)

	candles := s.Candles("TQQQ")
	if len(candles) != 1 {
		t.Fatalf("expected 1 candle, got %d", len(candles))
	}
	c := candles[0]
	if c.Open != 48.10 || c.High != 48.50 || c.Low != 47.90 || c.Close != 48.30 {
		t.Errorf("OHLC mismatch: %+v", c)
	}
	if c.Volume != 1000 || c.Wap != 48.20 || c.Count != 250 {
		t.Errorf("Volume/Wap/Count mismatch: %+v", c)
	}
}

// TestUpdateLive verifies 5-second tick reconstruction into a 1-minute candle.
//
// Scenario: 3 ticks within 09:45, then 1 tick at 09:46 (triggers finalisation).
//
//	Tick 1: price=48.10, vol=100 (cumul), wap=48.10, count=20
//	Tick 2: price=48.50, vol=250 (cumul), wap=48.40, count=30  ← incr=150
//	Tick 3: price=47.90, vol=400 (cumul), wap=48.00, count=40  ← incr=150
//
// Expected 09:45 candle:
//
//	Open=48.10 (first tick), High=48.50, Low=47.90, Close=47.90 (last tick)
//	Volume=400 (last cumulative)
//	Count=20+30+40=90
//	WAP = (48.10×100 + 48.40×150 + 48.00×150) / 400
//	    = (4810 + 7260 + 7200) / 400 = 19270 / 400 = 48.175
func TestUpdateLive(t *testing.T) {
	s := New()
	s.UpdateLive("TQQQ", ts0, 48.10, 48.10, 48.10, 48.10, 100, 48.10, 20)
	s.UpdateLive("TQQQ", ts0, 48.50, 48.50, 48.00, 48.50, 250, 48.40, 30)
	s.UpdateLive("TQQQ", ts0, 47.90, 48.50, 47.90, 47.90, 400, 48.00, 40)

	// Trigger finalisation by sending first tick of next minute.
	s.UpdateLive("TQQQ", ts1, 48.20, 48.20, 48.20, 48.20, 50, 48.20, 10)

	candles := s.Candles("TQQQ")
	if len(candles) != 1 {
		t.Fatalf("expected 1 completed candle, got %d", len(candles))
	}
	c := candles[0]
	if c.Open != 48.10 {
		t.Errorf("Open: want 48.10, got %.4f", c.Open)
	}
	if c.High != 48.50 {
		t.Errorf("High: want 48.50, got %.4f", c.High)
	}
	if c.Low != 47.90 {
		t.Errorf("Low: want 47.90, got %.4f", c.Low)
	}
	if c.Close != 47.90 {
		t.Errorf("Close: want 47.90, got %.4f", c.Close)
	}
	if c.Volume != 400 {
		t.Errorf("Volume: want 400, got %.0f", c.Volume)
	}
	if c.Count != 90 {
		t.Errorf("Count: want 90, got %d", c.Count)
	}
	wantWap := 19270.0 / 400.0 // 48.175
	if math.Abs(c.Wap-wantWap) > 1e-9 {
		t.Errorf("WAP: want %.6f, got %.6f", wantWap, c.Wap)
	}
}

func TestLast(t *testing.T) {
	s := New()
	for i := range 5 {
		s.AddHistorical("TQQQ", ts0, float64(i), float64(i), float64(i), float64(i), 0, 0, 0)
	}
	got := s.Last("TQQQ", 3)
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
	if got[0].Open != 2 || got[2].Open != 4 {
		t.Errorf("Last returned wrong candles: %v", got)
	}
}

func TestLen(t *testing.T) {
	s := New()
	if s.Len("TQQQ") != 0 {
		t.Error("expected 0 for unknown symbol")
	}
	s.AddHistorical("TQQQ", ts0, 1, 1, 1, 1, 0, 0, 0)
	if s.Len("TQQQ") != 1 {
		t.Error("expected 1 after one add")
	}
}

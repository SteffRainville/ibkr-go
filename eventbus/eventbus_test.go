package eventbus

import "testing"

// TestBus_Publish_DropsCandleBehindSharedOptionDataFlood reproduces the bug
// class that caused two independent consumers of the same underlying candle
// feed to silently diverge: subscribing a low-rate, unrecoverable Kind
// (KindCandle) together with a high-volume Kind (KindOptionData) on the same
// Subscribe call shares one small buffer, so a flood of the high-volume Kind
// can push the low-rate Kind's event out entirely.
func TestBus_Publish_DropsCandleBehindSharedOptionDataFlood(t *testing.T) {
	b := New()
	ch := b.Subscribe(KindCandle, KindOptionData)

	// Flood past the default 16-slot buffer with the high-volume kind only.
	for range 20 {
		b.Publish(Event{Kind: KindOptionData, Payload: OptionData{Symbol: "IWM"}})
	}

	b.Publish(Event{Kind: KindCandle, Payload: Candle{Symbol: "IWM", Close: 292.72}})

	found := false
drain:
	for {
		select {
		case evt := <-ch:
			if evt.Kind == KindCandle {
				found = true
			}
		default:
			break drain
		}
	}

	if found {
		t.Fatal("expected the KindCandle event to be dropped behind a KindOptionData flood on a shared channel, but it was received")
	}
	if b.DropStats()[KindCandle] == 0 {
		t.Fatal("expected DropStats()[KindCandle] > 0 after the candle was dropped")
	}
}

// TestBus_Publish_ControlBusCandleAloneNotDropped is the control case for the
// test above: the same flood volume, but KindCandle is the only subscribed
// kind on this bus, so it gets its own channel and is never crowded out.
func TestBus_Publish_ControlBusCandleAloneNotDropped(t *testing.T) {
	b := New()
	ch := b.Subscribe(KindCandle)

	// No subscriber for KindOptionData on this bus, so these are no-ops —
	// this control isolates "candle alone" from "candle sharing a channel".
	for range 20 {
		b.Publish(Event{Kind: KindOptionData, Payload: OptionData{Symbol: "IWM"}})
	}

	b.Publish(Event{Kind: KindCandle, Payload: Candle{Symbol: "IWM", Close: 292.72}})

	select {
	case evt := <-ch:
		if evt.Kind != KindCandle {
			t.Fatalf("expected KindCandle, got %v", evt.Kind)
		}
	default:
		t.Fatal("expected the KindCandle event to be received when subscribed alone")
	}
	if b.DropStats()[KindCandle] != 0 {
		t.Fatalf("expected no KindCandle drops, got %d", b.DropStats()[KindCandle])
	}
}

// TestBus_SubscribeBuffered_CandleSurvivesOptionDataFlood proves the fix
// primitive: giving KindCandle its own dedicated, larger-buffered channel via
// SubscribeBuffered means a flood on a separate KindOptionData channel (even
// on the same Bus) can no longer crowd it out.
func TestBus_SubscribeBuffered_CandleSurvivesOptionDataFlood(t *testing.T) {
	b := New()
	chCandle := b.SubscribeBuffered(256, KindCandle)
	chOther := b.Subscribe(KindOptionData)

	for range 20 {
		b.Publish(Event{Kind: KindOptionData, Payload: OptionData{Symbol: "IWM"}})
	}
	b.Publish(Event{Kind: KindCandle, Payload: Candle{Symbol: "IWM", Close: 292.72}})

	select {
	case evt := <-chCandle:
		if evt.Kind != KindCandle {
			t.Fatalf("expected KindCandle, got %v", evt.Kind)
		}
	default:
		t.Fatal("expected the KindCandle event to survive on its own dedicated channel")
	}

	// chOther legitimately drops some of the flood past its 16-slot buffer —
	// that's expected and fine for a self-correcting tick kind.
	_ = chOther
	if b.DropStats()[KindCandle] != 0 {
		t.Fatalf("expected no KindCandle drops once given a dedicated channel, got %d", b.DropStats()[KindCandle])
	}
}

// TestBus_DropStats_SnapshotIsIndependentPerKind checks DropStats tracks
// kinds independently and a snapshot read doesn't reset the running counter,
// so callers can diff successive snapshots to get a rate.
func TestBus_DropStats_SnapshotIsIndependentPerKind(t *testing.T) {
	b := New()
	b.Subscribe(KindCandle, KindOptionData) // shared, tiny effective headroom once flooded

	for range 20 {
		b.Publish(Event{Kind: KindOptionData, Payload: OptionData{Symbol: "IWM"}})
	}
	first := b.DropStats()
	if first[KindOptionData] == 0 {
		t.Fatal("expected some KindOptionData drops after flooding past the buffer")
	}
	if first[KindCandle] != 0 {
		t.Fatalf("expected zero KindCandle drops (none published yet), got %d", first[KindCandle])
	}

	// Reading DropStats must not reset the counters.
	second := b.DropStats()
	if second[KindOptionData] != first[KindOptionData] {
		t.Fatalf("expected DropStats to be stable across reads, got %d then %d", first[KindOptionData], second[KindOptionData])
	}

	for range 20 {
		b.Publish(Event{Kind: KindOptionData, Payload: OptionData{Symbol: "IWM"}})
	}
	third := b.DropStats()
	if third[KindOptionData] <= second[KindOptionData] {
		t.Fatalf("expected DropStats to accumulate further drops, got %d then %d", second[KindOptionData], third[KindOptionData])
	}
}

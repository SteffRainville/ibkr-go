// IB tick type 9 (CLOSE) carries the PRIOR session's closing price. It is not a
// quote, it is not a trade that happened today, and nothing downstream may
// treat it as either — yet handleOptionTick filed it in the same slot as LAST,
// the slot hub.optSellPrice falls back to whenever bid and ask are both still
// zero. A freshly pinned leg is in exactly that state: IB answers a new
// subscription with an opening burst, and CLOSE routinely arrives before the
// first BID/ASK.
//
// On 2026-09-04 that put yesterday's 23.23 close on a TSLA 355 call entered at
// 5.48 (TSLA had been 20 points higher the day before), which set the
// trailing-stop trigger to 22.07 and closed the position 41 seconds after it
// opened. Nine such exits that day across three option robots; the same
// signature recurs on most days back through August.
//
// Publishing nothing on a CLOSE tick is half the fix and is tested here. The
// other half — that ONE price republished twice cannot corroborate itself —
// lives in ORBtrader's hub, because the republish came from a different
// callback (TickOptionComputation) re-emitting the same cached leg snapshot.
package ibkr

import (
	"testing"

	"github.com/scmhub/ibapi"

	"github.com/SteffRainville/ibkr-go/eventbus"
	"github.com/SteffRainville/ibkr-go/quotes"
)

// tslaCall355 is the contract from the incident.
func tslaCall355() legKey { return lk("TSLA", "call", 355, "20260909") }

// pinnedLegSession returns a Session holding one pinned TSLA 355 call leg with
// no quote yet (the state a leg is in the instant after SubscribePositionStrike),
// plus the bus channel a position-pinned publish would land on.
func pinnedLegSession(t *testing.T) (*Session, legKey, <-chan eventbus.Event) {
	t.Helper()
	s := NewSession(Options{TradingAccount: "DU12345"}, quotes.NewBook(), nil)
	bus := eventbus.New()
	ch := bus.Subscribe(eventbus.KindPositionOptionData)
	s.buses = []*eventbus.Bus{bus}

	key := tslaCall355()
	seedLeg(s, key, legOpts{reqID: 9912, pins: 1})
	return s, key, ch
}

func legState(t *testing.T, s *Session, key legKey) optLeg {
	t.Helper()
	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()
	leg, ok := s.optChain.legs[key]
	if !ok {
		t.Fatalf("leg %v missing from registry", key)
	}
	return *leg
}

func bookQuote(t *testing.T, s *Session, key legKey) (quotes.OptionQuote, bool) {
	t.Helper()
	return s.book.Option(quotes.ContractKey{
		Symbol: key.symbol, Right: key.right, Strike: key.strike, Expiry: key.expiry,
	})
}

// TestHandleOptionTick_CloseDoesNotBecomeLast is the incident, reduced: the
// exact tick IB delivered at 15:25:06 on 2026-09-04, against a leg that has not
// yet received a bid or an ask.
func TestHandleOptionTick_CloseDoesNotBecomeLast(t *testing.T) {
	s, key, ch := pinnedLegSession(t)

	if !s.handleOptionTick(9912, ibapi.CLOSE, 23.23) {
		t.Fatal("handleOptionTick returned false for a known option reqID")
	}

	leg := legState(t, s, key)
	if leg.price != 0 {
		t.Errorf("leg.price = %.2f, want 0 — yesterday's close is not a live price", leg.price)
	}
	if leg.prevClose != 23.23 {
		t.Errorf("leg.prevClose = %.2f, want 23.23 — the close must still be recorded, just quarantined", leg.prevClose)
	}

	// Nothing may reach the bus. This is the load-bearing half: a publish here
	// re-emits the whole cached leg snapshot, which is what handed
	// ConfirmedWatermarkPrice a corroborating second voice for one price.
	select {
	case evt := <-ch:
		od, _ := evt.Payload.(eventbus.OptionData)
		t.Errorf("a CLOSE tick published %+v; want no event at all", od)
	default:
	}

	// Nor the Book: quotes.Book never expires an entry, so a prev close written
	// as Last outlives the session and poisons every later optSellPrice
	// fallback for the contract.
	if q, ok := bookQuote(t, s, key); ok && q.Last != 0 {
		t.Errorf("book Last = %.2f after a CLOSE tick, want it untouched", q.Last)
	}

	// Liveness is still stamped — a CLOSE tick IS proof IB is serving the line,
	// and the dead-leg reaper must not condemn a leg for being quarantined.
	if leg.lastTickAt.IsZero() {
		t.Error("lastTickAt not stamped; a CLOSE tick is still proof the subscription is alive")
	}
}

// TestHandleOptionTick_LastStillBecomesLast guards against over-correcting: a
// genuine trade print must still price the leg, book it and reach the bus.
func TestHandleOptionTick_LastStillBecomesLast(t *testing.T) {
	s, key, ch := pinnedLegSession(t)

	if !s.handleOptionTick(9912, ibapi.LAST, 5.47) {
		t.Fatal("handleOptionTick returned false for a known option reqID")
	}

	leg := legState(t, s, key)
	if leg.price != 5.47 {
		t.Errorf("leg.price = %.2f, want 5.47", leg.price)
	}
	if leg.prevClose != 0 {
		t.Errorf("leg.prevClose = %.2f, want 0 — a LAST tick is not a close", leg.prevClose)
	}

	select {
	case evt := <-ch:
		od, ok := evt.Payload.(eventbus.OptionData)
		if !ok {
			t.Fatalf("payload %T, want eventbus.OptionData", evt.Payload)
		}
		if od.Price != 5.47 || od.Strike != 355 || od.Expiry != "20260909" {
			t.Errorf("published %+v, want price 5.47 on TSLA 355 20260909", od)
		}
	default:
		t.Error("a LAST tick on a pinned leg published nothing; the position would stop being priced")
	}

	if q, ok := bookQuote(t, s, key); !ok || q.Last != 5.47 {
		t.Errorf("book Last = %.2f (present=%v), want 5.47", q.Last, ok)
	}
}

// TestHandleOptionTick_CloseDoesNotDisplayAnEarlierLast pins the ordering that
// actually occurs on a leg that has traded: a real print, then IB's CLOSE for
// the same contract. The close must not overwrite the print.
func TestHandleOptionTick_CloseDoesNotDisplayAnEarlierLast(t *testing.T) {
	s, key, _ := pinnedLegSession(t)

	s.handleOptionTick(9912, ibapi.LAST, 5.47)
	s.handleOptionTick(9912, ibapi.CLOSE, 23.23)

	if leg := legState(t, s, key); leg.price != 5.47 {
		t.Errorf("leg.price = %.2f after a CLOSE tick, want 5.47 (the real print)", leg.price)
	}
	if q, _ := bookQuote(t, s, key); q.Last != 5.47 {
		t.Errorf("book Last = %.2f, want 5.47", q.Last)
	}
}

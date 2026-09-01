package ibkr

import (
	"sync"
	"testing"
)

// TestReqIDAllocatorNeverRepeats is the whole point of the type: two ids from
// one session are never equal, whatever order the call sites run in.
func TestReqIDAllocatorNeverRepeats(t *testing.T) {
	s := NewSession(Options{}, nil, nil)

	const n = 10000
	seen := make(map[int64]bool, n)
	for i := 0; i < n; i++ {
		id := s.nextReqID()
		if seen[id] {
			t.Fatalf("reqID %d issued twice after %d allocations", id, i)
		}
		seen[id] = true
	}
}

// TestReqIDAllocatorIsConcurrencySafe pins the mutex. Option probes allocate
// from the ResolveEntryStrike caller's goroutine while the dead-leg reaper
// allocates from the rotation ticker, so unsynchronised increments would tear
// under -race and hand out the same id twice.
func TestReqIDAllocatorIsConcurrencySafe(t *testing.T) {
	s := NewSession(Options{}, nil, nil)

	const goroutines, each = 8, 500
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := make(map[int64]bool, goroutines*each)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids := make([]int64, each)
			for i := range ids {
				ids[i] = s.nextReqID()
			}
			mu.Lock()
			defer mu.Unlock()
			for _, id := range ids {
				if seen[id] {
					t.Errorf("reqID %d issued twice", id)
				}
				seen[id] = true
			}
		}()
	}
	wg.Wait()

	if len(seen) != goroutines*each {
		t.Errorf("got %d distinct ids, want %d", len(seen), goroutines*each)
	}
}

// TestReqIDsDoNotCollideAcrossPurposes is the regression test for 2026-09-01.
// The old scheme seeded each purpose 1,000 apart and let each counter run
// free; an entry probe burns six ids at a time, so the option market-data
// counter reached the position-pinned band every afternoon and started
// issuing ids that pinned legs were already streaming on.
//
// Rather than assert on a band (there are none now), this exercises the two
// counters that actually collided — the one behind delta probes and forced
// re-subscribes, and the one behind SubscribePositionStrike — for far more
// allocations than a session makes in a day, and requires the union to stay
// distinct.
func TestReqIDsDoNotCollideAcrossPurposes(t *testing.T) {
	s := NewSession(Options{}, nil, nil)

	seen := make(map[int64]bool)
	claim := func(id int64, who string) {
		if seen[id] {
			t.Fatalf("%s was issued reqID %d, which is already live", who, id)
		}
		seen[id] = true
	}

	// Interleaved, in the proportion a real session uses: six probe ids per
	// entry attempt against one pinned id per fill.
	for i := 0; i < 5000; i++ {
		for c := 0; c < 6; c++ {
			claim(s.nextReqID(), "delta probe candidate")
		}
		claim(s.nextReqID(), "position-pinned leg")
		claim(s.nextReqID(), "forced re-subscribe")
	}
}

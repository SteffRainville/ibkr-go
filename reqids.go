package ibkr

import "sync"

// reqIDFirst is where this session's single request-id sequence starts. The
// value is arbitrary — IB only requires that an id is unique among the
// requests currently live on the connection — but starting above zero keeps
// ids visually distinct from array indices and error codes in the logs.
const reqIDFirst int64 = 1001

// reqIDAllocator hands out IB request ids. There is exactly one per Session,
// and every request the session issues — bars, ticks, scanner runs, option
// chains, delta probes, position-pinned option feeds — draws its id from it.
//
// It replaced a set of per-purpose counters seeded 1000 apart (reqIDHistBase,
// reqIDOptMktBase, reqIDPosMktBase, …). That scheme was correct only while no
// purpose issued more than 1,000 requests in one session, and it silently
// stopped being true: an entry delta probe subscribes six candidates at once,
// so the option market-data counter consumed >3,000 ids a day and walked into
// the position-pinned band every afternoon.
//
// What that cost is worth stating, because it is not "a duplicate request
// failed". On 2026-09-01 at 14:23 the probe counter reached 10,185 — an id
// already streaming HOOD's held 103 call. IB refused the probe (error 322,
// duplicate ticker id) and the probe's own cleanup then cancelled the id,
// killing HOOD's real feed. The dead-leg reaper re-subscribed HOOD under the
// next id, 10,194, which was MU's live 925 call: IB refused that too, but the
// reverse index legByReqID had already been repointed, so MU's ticks arrived
// as HOOD's. A $2.91 call was marked at $21.80, the take-profit ladder fired
// all six rungs, and the trade booked +639%. MU, now starved of its own ticks,
// was declared silent 90 seconds later and stole the next id in turn. Twelve
// collisions cascaded through four robots that day.
//
// One sequence makes a duplicate unrepresentable rather than merely unlikely,
// which is why this is an allocator and not a wider set of bands: bands only
// move the crossing point later in the day.
//
// The counter is monotonic for the life of the Session and is deliberately
// never rewound, not even on reconnect. Three call sites used to rewind their
// counter while idle to stay inside their band (RunScanner, PositionEnd,
// resolveOptionQueryConID); a sequence with no band has nothing to stay inside,
// and never reusing an id means a request IB is somehow still serving across a
// half-open reconnect cannot collide with a fresh one. int64 at a few thousand
// ids a day does not run out.
type reqIDAllocator struct {
	mu   sync.Mutex
	next int64
}

// alloc returns the next unused request id. It is a leaf: it takes only its
// own mutex and calls nothing, so it is safe to call while holding any other
// session lock (several call sites allocate under s.optChain.mu or s.symMu).
func (a *reqIDAllocator) alloc() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.next < reqIDFirst {
		a.next = reqIDFirst // zero value: a Session built without New
	}
	id := a.next
	a.next++
	return id
}

// nextReqID allocates one request id from the session's single sequence.
func (s *Session) nextReqID() int64 { return s.reqIDs.alloc() }

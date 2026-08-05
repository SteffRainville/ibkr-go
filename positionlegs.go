package ibkr

import (
	"sort"
	"time"
)

// Observation of position-pinned option legs, for callers that manage open
// positions and need to know whether a held contract's market data is
// actually alive.
//
// This is the read-only counterpart to deadleg.go: that file DETECTS and
// REPAIRS dead legs on the rotation ticker, this one simply reports what it
// would see. They are kept apart because they answer different questions and
// have different audiences — repair is internal policy, observation crosses
// the module boundary.
//
// Why this needs to be exported at all: a caller holding an open position
// cannot determine liveness from quotes.Book. Book's BidTime/AskTime advance
// only when a price CHANGES (by design — they measure how current a VALUE
// is), so a contract quoting steadily at a flat price is indistinguishable
// there from one IB has stopped serving entirely. optMktReq/posStrikeSub's
// lastTickAt is stamped on EVERY IB message including size ticks, which is
// the only signal in the library that can tell those two apart.

// LegHealthState is the exported spelling of the internal legHealth
// classification. Values are legHealth.String()'s output verbatim: there is
// exactly one classifier (legHealthAt) and this is a projection of it, never
// a second implementation. TestLegHealthStateCoversEveryLegHealth fails if
// the two ever drift.
type LegHealthState string

const (
	// LegHealthHealthy — received an IB message within the silence threshold.
	LegHealthHealthy LegHealthState = "healthy"
	// LegHealthWarming — subscribed too recently to judge.
	LegHealthWarming LegHealthState = "warming"
	// LegHealthSilent — past warmup and has NEVER received a message.
	LegHealthSilent LegHealthState = "silent"
	// LegHealthStale — was ticking, then stopped, while peers kept flowing.
	LegHealthStale LegHealthState = "stale"
	// LegHealthFeedQuiet — silent, but so is every other leg, so nothing can
	// be concluded about this one. Never act on it.
	LegHealthFeedQuiet LegHealthState = "feed_quiet"
)

// PositionLegHealth is one position-pinned option leg's contract identity and
// liveness. Strike/Right/Symbol together are the caller's join key back onto
// its own held position.
type PositionLegHealth struct {
	Symbol string
	Right  string // "call" | "put"
	Strike float64
	Expiry string // "YYYYMMDD"
	ReqID  int64

	// RefCount is how many independent holders share this subscription. Two
	// robots holding the same contract share one IB feed; the feed is torn
	// down only when the last of them releases it (see posStrikeSub.refCount).
	RefCount int

	SubscribedAt time.Time
	// LastTickAt is the arrival time of the most recent IB message of ANY
	// kind for this leg. Zero means it has never ticked at all.
	LastTickAt time.Time

	Health LegHealthState
	// SilentFor is how long since the last message; -1 when never ticked, so
	// "never" is distinguishable from "just ticked" without inspecting
	// LastTickAt.IsZero() at every call site.
	SilentFor time.Duration
}

// PositionLegSnapshot is every pinned leg as of one single instant, plus the
// session-wide feed context that makes the per-leg verdicts interpretable.
//
// FeedAlive is the load-bearing caveat: when it is false, the whole option
// feed is quiet (a halt, a weekend, an off-hours lull, an IB outage) and no
// individual leg's silence means anything. Legs are already classified
// LegHealthFeedQuiet in that case, so a caller that simply ignores non-stale
// states inherits the guard for free.
type PositionLegSnapshot struct {
	At          time.Time
	LastAnyTick time.Time
	FeedAlive   bool
	Legs        []PositionLegHealth
}

// Leg returns the snapshot row for an exact contract, if one is present.
// Absence is itself meaningful to a caller holding that contract: it means
// the position has NO market-data subscription behind it at all, so whatever
// price its stops are evaluating is frozen at the last value ever received.
func (s PositionLegSnapshot) Leg(symbol, right string, strike float64) (PositionLegHealth, bool) {
	for _, l := range s.Legs {
		if l.Symbol == symbol && l.Right == right && l.Strike == strike {
			return l, true
		}
	}
	return PositionLegHealth{}, false
}

// PositionLegs reports the liveness of every position-pinned option leg.
func (s *Session) PositionLegs() PositionLegSnapshot {
	return s.positionLegsAt(time.Now())
}

// positionLegsAt is the now-explicit form. Split out for the same reason
// legHealthAt and isRTHAt are: it makes the whole matrix table-testable
// against fixed instants, with no wall clock and no broker client. Capturing
// `now` once also means every row in a snapshot is judged against a single
// instant, which is what keeps a scan internally consistent.
func (s *Session) positionLegsAt(now time.Time) PositionLegSnapshot {
	s.optChain.mu.Lock()
	lastAny := s.optChain.lastAnyOptionTick
	legs := make([]PositionLegHealth, 0, len(s.optChain.posSubs))
	for reqID, sub := range s.optChain.posSubs {
		silentFor := time.Duration(-1)
		if !sub.lastTickAt.IsZero() {
			silentFor = now.Sub(sub.lastTickAt)
		}
		legs = append(legs, PositionLegHealth{
			Symbol: sub.symbol, Right: sub.right, Strike: sub.strike, Expiry: sub.expiry,
			ReqID: reqID, RefCount: sub.refCount,
			SubscribedAt: sub.subscribedAt, LastTickAt: sub.lastTickAt,
			Health:    LegHealthState(legHealthAt(sub.subscribedAt, sub.lastTickAt, lastAny, now).String()),
			SilentFor: silentFor,
		})
	}
	s.optChain.mu.Unlock()

	// Sorted outside the lock: map iteration order is random, and a caller
	// logging or diffing successive snapshots needs them to read identically.
	sort.Slice(legs, func(i, j int) bool {
		if legs[i].Symbol != legs[j].Symbol {
			return legs[i].Symbol < legs[j].Symbol
		}
		if legs[i].Right != legs[j].Right {
			return legs[i].Right < legs[j].Right
		}
		return legs[i].Strike < legs[j].Strike
	})

	return PositionLegSnapshot{
		At:          now,
		LastAnyTick: lastAny,
		FeedAlive:   !lastAny.IsZero() && now.Sub(lastAny) <= feedAliveWindow,
		Legs:        legs,
	}
}

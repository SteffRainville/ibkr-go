// Option selectors, the option leg registry (position-pinned market data),
// and the pieces of the entry delta probe shared with entryprobe.go. Chain
// loading lives in chains.go.
package ibkr

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/scmhub/ibapi"

	"github.com/SteffRainville/ibkr-go/eventbus"
	"github.com/SteffRainville/ibkr-go/mdlines"
	"github.com/SteffRainville/ibkr-go/quotes"
)

// The library separates three concerns that used to share one unit (the
// "option resolution group", a (symbol, delay, callδ, putδ) tuple):
//
//	chain lookup       (symbol, optionDelay)                — conId + ReqSecDefOptParams
//	strike selection   (symbol, right, optionDelay, δ)      — a SELECTOR, one per caller row
//	market data        (symbol, right, strike, expiry)      — a CONTRACT, one IB line
//
// Collapsing the last two into the group is what made a call's fate depend on
// its put's configuration and let two groups on one underlying evict each
// other's single (symbol, right) leg. Calls and puts are unrelated instruments
// and now appear in no shared key anywhere; a contract's line is refcounted
// across every holder (any selector, any open position) instead of owned by
// whichever group subscribed last.

// chainKey identifies one option chain lookup. optionDelay is part of the key
// because it selects the expiry; the strike universe does not depend on it,
// but both arrive from the same round trip so there is nothing to gain by
// splitting them.
type chainKey struct {
	symbol      string
	optionDelay int
}

// selectorKey identifies one strike-selection configuration: "which contract
// does this caller want for this underlying and this right?". It is exactly
// what one row of a caller's symbol list specifies, which is why an absent row
// must produce no selector rather than a default-δ one.
type selectorKey struct {
	symbol      string
	right       string // "call" | "put"
	optionDelay int
	targetDelta float64
}

// selector is one strike-selection configuration plus the subscriber buses
// that share it. Its id is assigned once per session and stays stable across
// every rebuild (see buildSelectors).
type selector struct {
	id          int
	symbol      string
	currency    string
	right       string
	optionDelay int
	targetDelta float64
	busIdxs     []int
}

func (sel selector) chainKey() chainKey { return chainKey{sel.symbol, sel.optionDelay} }

// chainClass is one trading class's own view of an underlying's option chain.
type chainClass struct {
	tradingClass string
	multiplier   string
	expirations  []string
	strikes      []float64
}

// mergeChainParams folds one SecurityDefinitionOptionParameter callback into
// the class, deduplicating — IB may repeat a class across several callbacks.
func (c *chainClass) mergeChainParams(expirations []string, strikes []float64) {
	expSet := make(map[string]bool, len(c.expirations))
	for _, e := range c.expirations {
		expSet[e] = true
	}
	for _, e := range expirations {
		if !expSet[e] {
			expSet[e] = true
			c.expirations = append(c.expirations, e)
		}
	}

	strikeSet := make(map[float64]bool, len(c.strikes))
	for _, st := range c.strikes {
		strikeSet[st] = true
	}
	for _, st := range strikes {
		if !strikeSet[st] {
			strikeSet[st] = true
			c.strikes = append(c.strikes, st)
		}
	}
}

// standardMultiplier is the deliverable of an ordinary equity option: 100
// shares. A class with any other multiplier is a mini or an adjusted contract
// from a corporate action — a different instrument, never what a watchlist row
// asking for a δ-target strike means.
const standardMultiplier = "100"

// pickChainClass chooses the one trading class whose expiry calendar and
// strike ladder the selection will use, preferring the underlying's own
// standard class. Returns the choice and the classes passed over, so the
// decision is visible in option-chain.log rather than implicit.
//
// Order: the class named after the symbol at multiplier 100; else the richest
// multiplier-100 class; else the richest class of any multiplier (reported by
// the caller as a warning — selecting strikes on a non-standard deliverable is
// a last resort, but it beats returning nothing).
func pickChainClass(symbol string, classes map[string]*chainClass) (chosen *chainClass, ignored []*chainClass) {
	names := make([]string, 0, len(classes))
	for name := range classes {
		names = append(names, name)
	}
	sort.Strings(names) // deterministic tie-breaking

	better := func(a, b *chainClass) bool {
		if a.tradingClass == symbol && b.tradingClass != symbol {
			return true
		}
		if a.tradingClass != symbol && b.tradingClass == symbol {
			return false
		}
		aStd, bStd := a.multiplier == standardMultiplier, b.multiplier == standardMultiplier
		if aStd != bStd {
			return aStd
		}
		return len(a.expirations) > len(b.expirations)
	}

	for _, name := range names {
		c := classes[name]
		if len(c.expirations) == 0 || len(c.strikes) == 0 {
			continue
		}
		if chosen == nil || better(c, chosen) {
			chosen = c
		}
	}
	for _, name := range names {
		if c := classes[name]; c != chosen {
			ignored = append(ignored, c)
		}
	}
	return chosen, ignored
}

// describeChainClasses renders classes for a log line: "MSFT1 mult=100 (18 exp,
// 62 strikes)".
func describeChainClasses(classes []*chainClass) string {
	if len(classes) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(classes))
	for _, c := range classes {
		parts = append(parts, fmt.Sprintf("%s mult=%s (%d exp, %d strikes)",
			c.tradingClass, c.multiplier, len(c.expirations), len(c.strikes)))
	}
	return strings.Join(parts, ", ")
}

// legKey identifies one option contract — the unit of market-data
// subscription, and deliberately identical to quotes.ContractKey. Every holder
// that wants this contract shares the one IB line behind it.
//
// Keying legs by contract rather than by (symbol, right) is what stops two
// selectors on the same underlying from evicting each other. Under the old
// key, `replaceMktReqLocked(symbol, right, …)` cancelled every other leg for
// that pair whatever configuration owned it, so N selectors on one underlying
// shared a single slot and only the last subscriber's buses received data. On
// 2026-08-13 that blanked QQQ — the most liquid underlying in the watchlist —
// on a live robot for 41 minutes, because commenting out its QQQ put row split
// it into a third selector that lost every contest for the shared slot.
type legKey struct {
	symbol string
	right  string // "call" | "put"
	strike float64
	expiry string
}

func (k legKey) contractKey() quotes.ContractKey {
	return quotes.ContractKey{Symbol: k.symbol, Right: k.right, Strike: k.strike, Expiry: k.expiry}
}

// optLeg is one subscribed option contract and its live prices, shared by
// every holder that wants it.
type optLeg struct {
	reqID  int64
	symbol string
	right  string
	strike float64
	expiry string
	price  float64
	bid    float64
	ask    float64
	delta  float64

	// prevClose is IB tick type 9 (CLOSE): the PRIOR session's closing price,
	// never a live quote. It is recorded here for diagnostics only, and is
	// deliberately never published on the bus nor written to the quotes.Book,
	// because every consumer of a leg's price treats it as current and
	// realizable. Folding it into price is what let a TSLA 355 call entered at
	// 5.48 record a 23.23 watermark on 2026-09-04 — yesterday's close, struck
	// when TSLA was 20 points higher — which put the trailing-stop trigger at
	// 22.07 and closed the position 41 seconds after it opened. The stock path
	// has always kept the two apart (mktData.prevClose, ticks.go); this is the
	// option equivalent.
	prevClose float64

	// quoteSeq counts genuine PRICE changes on this leg — bumped only when bid,
	// ask or price actually moves, never on a re-emission. It rides out on
	// every OptionData so a consumer can tell one observation republished from
	// two observations agreeing; see eventbus.OptionData.QuoteSeq.
	quoteSeq uint64

	// deltaSource is "matched" (a deliberate ATM target, or a genuine live
	// IB delta match) or "atm_fallback" (no usable delta, strike picked
	// without one).
	deltaSource string

	// pins counts open-position holders — the holder kind that matters for
	// trading: a leg exists because some position needs its contract priced.
	//
	// The refcount stays essential even so. It is what stopped one robot's exit
	// cancelling a contract a sibling's still-open IWM 298 PUT was pricing its
	// stops against (2026-08-04), and two robots holding the same contract —
	// including a live position and its own simulated mirror — is routine.
	//
	// A `selectors map[int]struct{}` sat beside this, counting watchlist rows
	// displaying the contract. Those rows no longer hold subscriptions.
	pins int

	// tails counts recording holds (HoldForRecording): a caller recording this
	// contract's quotes wants it kept streaming after the last position closes,
	// so a replay can price a wider exit than the one actually taken. A leg
	// held only by tails sits in mdlines.CategoryRecording — the lowest tier,
	// evicted whenever anything else needs the line.
	tails int

	// subscribedAt is when ReqMktData was issued for this reqID, and
	// lastTickAt when IB last delivered ANY message for it (zero = never).
	// These are liveness, deliberately distinct from the quotes.Book's
	// BidTime/AskTime, which advance only when a price actually CHANGES
	// (quotes/book.go) and therefore cannot tell "alive but unchanged" from
	// "dead". Without a separate liveness clock a leg IB has silently
	// stopped serving is indistinguishable from a quiet one, which is how
	// the 2026-08-03 QQQ/SPY/IWM legs sat frozen for two hours while the
	// dashboard showed a plausible price 35% away from the real market.
	subscribedAt time.Time
	lastTickAt   time.Time
}

func (l *optLeg) key() legKey {
	return legKey{symbol: l.symbol, right: l.right, strike: l.strike, expiry: l.expiry}
}

// held reports whether anything still wants this contract.
func (l *optLeg) held() bool { return l.pins > 0 || l.tails > 0 }

// deltaCandidate tracks one strike subscription used during delta-based
// strike selection. Multiple candidates are subscribed simultaneously; the one
// closest to the target delta supplies the entry quote, and ALL of them are
// then cancelled — the winner included, since nothing displays it afterwards.
type deltaCandidate struct {
	selectorID int
	symbol     string
	right      string
	strike     float64
	expiry     string
	reqID      int64
	delta      float64
	iv         float64
	bid        float64
	ask        float64
	ready      bool

	// errCode/errMsg record an IB error delivered against this candidate's
	// reqID (noteCandidateError). Purely diagnostic: they are what lets
	// classifyCandidateErrors tell "IB refused for lack of subscription
	// rights" apart from "IB accepted the request and never answered", the
	// two cases that used to arrive at the bot as one indistinguishable
	// "option quote unavailable". Deliberately NOT consulted by
	// deltaCandidatesSettled — recording a cause must not change probe timing.
	//
	// No timestamp is kept: candidates are allocated fresh per probe and
	// dropped from deltaCands once it resolves, so an error can only ever
	// belong to the probe currently in flight. There is no staleness to check.
	errCode int64
	errMsg  string

	// dupTicker records that IB refused this candidate's request as a
	// duplicate ticker id, i.e. the id is another live request's. It gates the
	// cleanup in resolveDeltaCandidates: cancelling or releasing an id we were
	// refused tears down whoever does own it. See dupticker.go.
	dupTicker bool

	// rejected records that IB refused this candidate's contract outright
	// (error 200, "no security definition"). Unlike errCode it DOES count toward
	// deltaCandidatesSettled: the request is gone and its line released, so the
	// candidate can never report, and treating it as "still pending" forced
	// every probe with one dead strike to run its full timeout (2026-09-28
	// AGQ, strike 71.50).
	rejected bool
}

// EntryStrikeResult reports the outcome of ResolveEntryStrike. It replaces a
// bare bool because a failed entry probe has EIGHT structurally different
// causes (no cached chain, probe cooldown, no market-data lines, an ATM target
// delta, no ITM candidates, a failed sibling probe, a delta match with no
// price, and IB simply never answering) and collapsing them into one token
// left the dashboard reporting a symptom with no route to a diagnosis.
//
// Reason is a stable token (see the entryFail* constants); Detail is the
// human-readable elaboration — for an IB-attributed failure, the code and
// message IB actually sent.
type EntryStrikeResult struct {
	OK     bool
	Reason string
	Detail string
	IBCode int64
}

// Entry-probe failure reasons. Each maps to exactly one branch, so a token in
// a log or on a dashboard row identifies a single line of code.
const (
	// entryFailNoSubscription — IB explicitly refused the market data for
	// lack of entitlement. THE answer to "why does this account never get an
	// option quote": nothing about the app will fix it.
	entryFailNoSubscription = "option_no_subscription"
	// entryFailQuoteTimeout — probes launched, IB raised no error at all, and
	// nothing ticked before the deadline. The genuine "no response" case.
	entryFailQuoteTimeout = "option_quote_timeout"
	// entryFailContractInvalid — IB 200, no security definition for the
	// contract we asked about (bad expiry, delisted, wrong exchange).
	entryFailContractInvalid = "option_contract_invalid"
	// entryFailMDError — some other IB error against a candidate (pacing,
	// request validation, duplicate ticker id, …). Kept distinct from the
	// above so an unrecognised code is visibly unclassified rather than
	// silently filed as a timeout.
	entryFailMDError = "option_md_error"
	// entryFailDeltaNoPrice — a candidate won on delta but carried no
	// two-sided price, so the quote is not Valid() and must not be traded.
	entryFailDeltaNoPrice = "option_delta_no_price"
	// entryFailNoChain — no selector for this subscriber, or its chain is not
	// loaded for today yet. Normal in the first seconds after connect.
	entryFailNoChain = "option_no_chain"
	// entryFailDeltaTargetATM — target_delta sits in the refused ATM band.
	entryFailDeltaTargetATM = "option_delta_target_atm"
	// entryFailNoCandidates — selectStrikeCandidates found no strike to probe.
	entryFailNoCandidates = "option_no_candidates"
	// entryFailNoMDLines — every GrantProbe was refused; the market-data line
	// budget is exhausted and nothing was preemptible.
	entryFailNoMDLines = "option_no_md_lines"
	// entryFailSiblingFailed — joined a sibling's in-flight probe that did not
	// finish in time. The owner always finishes by its deadline, so this means
	// an internal defect (see probeJoinSafety).
	entryFailSiblingFailed = "option_sibling_failed"
)

// subscriptionErrorCodes are the IB error codes that mean "your account is not
// entitled to this market data" in one form or another. Grouped because the
// distinction between them (delayed data substituted vs. not enabled vs. a
// competing live session holding the subscription) does not change what the
// operator must do, and the exact message is carried through in Detail anyway.
//
//	354   Requested market data is not subscribed
//	10089 Requested market data requires additional subscription for API
//	10090 Part of requested market data is not subscribed
//	10167 Not subscribed — displaying delayed market data
//	10168 Not subscribed — delayed market data is not enabled
//	10197 No market data during competing live session
var subscriptionErrorCodes = map[int64]bool{
	354: true, 10089: true, 10090: true, 10167: true, 10168: true, 10197: true,
}

// classifyEntryIBCode maps one IB error code to its entry-failure reason.
func classifyEntryIBCode(code int64) string {
	switch {
	case subscriptionErrorCodes[code]:
		return entryFailNoSubscription
	case code == 200:
		return entryFailContractInvalid
	default:
		return entryFailMDError
	}
}

// entryFailRank orders reasons by how conclusively they explain the failure,
// so that when candidates failed differently the most actionable one is
// reported. A single entitlement error is the whole story; four siblings that
// merely timed out are not, and must not mask it.
func entryFailRank(reason string) int {
	switch reason {
	case entryFailNoSubscription:
		return 3
	case entryFailContractInvalid:
		return 2
	case entryFailMDError:
		return 1
	default:
		return 0
	}
}

// classifyCandidateErrors reduces a finished probe's candidates to the single
// reason worth reporting. With no IB error recorded anywhere, the probe simply
// went unanswered — entryFailQuoteTimeout, which is the honest description and
// the one the old code could never distinguish from the rest.
func classifyCandidateErrors(candidates []*deltaCandidate) EntryStrikeResult {
	best := EntryStrikeResult{Reason: entryFailQuoteTimeout}
	best.Detail = fmt.Sprintf("IB returned no quote and no error within the probe window (%d candidate strikes)", len(candidates))
	bestRank := 0

	for _, c := range candidates {
		if c.errCode == 0 {
			continue
		}
		reason := classifyEntryIBCode(c.errCode)
		rank := entryFailRank(reason)
		if rank <= bestRank {
			continue
		}
		bestRank = rank
		best = EntryStrikeResult{
			Reason: reason,
			Detail: fmt.Sprintf("IB %d on %s %s strike %.2f: %s", c.errCode, c.symbol, c.right, c.strike, c.errMsg),
			IBCode: c.errCode,
		}
	}
	return best
}

// noteCandidateError stamps an IB error onto the in-flight delta candidate it
// was reported against, so the cause survives to classifyCandidateErrors.
//
// Observational ONLY: it does not delete the candidate, release its
// market-data line, or influence deltaCandidatesSettled. Recording why a probe
// failed must not change when the probe gives up — the timing here is load-
// bearing for entries and is deliberately left exactly as it was.
func (s *Session) noteCandidateError(reqID, code int64, msg string) {
	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()
	if cand, ok := s.optChain.deltaCands[reqID]; ok {
		cand.errCode = code
		cand.errMsg = msg
	}
}

// optionChainTracker holds all state for option chain lookup and market data.
type optionChainTracker struct {
	mu        sync.Mutex
	nextSelID int

	// chains holds each (symbol, optionDelay)'s strikes for the trading day
	// (chains.go); chainByReq maps a chain's in-flight request to it, and
	// conIDs caches underlying conIds, which never change.
	chains     map[chainKey]*chain
	chainByReq map[int64]chainKey
	conIDs     map[string]int64

	// legs is the one registry of subscribed option contracts, background and
	// position-pinned alike, keyed by contract and refcounted across holders.
	// legByReqID is its reverse index for the IB callbacks, which only ever
	// carry a reqID.
	legs       map[legKey]*optLeg
	legByReqID map[int64]legKey

	deltaCands map[int64]*deltaCandidate

	// probes holds each selector's current or most recent entry probe
	// (entryprobe.go).
	probes map[int]*probeCall

	// selectors is every (symbol, right, delay, δ) configuration across all
	// subscribers, rebuilt by buildSelectors.
	selectors []selector

	// selectorIDs maps a selector's configuration tuple to the id assigned the
	// first time it was seen, so rebuilding the list (which ResyncSymbols
	// does after every watchlist edit) preserves the identity of every selector
	// that did not change. See buildSelectors for why renumbering would be
	// destructive.
	selectorIDs map[selectorKey]int

	// lastAnyOptionTick is the most recent moment ANY option reqID received
	// a message from IB. It is what separates "this one leg died" from "the
	// whole option feed is quiet" — off-RTH, a halt, a broker outage. Judging
	// a leg dead only while its peers are demonstrably alive collapses all of
	// those false-positive cases into a single rule, and stops a market-wide
	// lull from condemning every leg at once and triggering a re-subscribe
	// storm against a broker that is not answering anyway.
	lastAnyOptionTick time.Time

	// dupRepairs counts per-contract repairs after a duplicate-ticker-id
	// refusal, bounding handleDuplicateTickerID so an error path can never
	// become a request loop. Cleared with the leg.
	dupRepairs map[legKey]int

	// forcedResub tracks per-contract forced re-subscribe attempts so a
	// contract IB will never quote (delisted, bad expiry) cannot burn a
	// market-data line every tick forever.
	forcedResub map[legKey]resubState
}

// resubState is one leg's forced-re-subscribe backoff record.
type resubState struct {
	last     time.Time
	attempts int
}

// buildSelectors rebuilds the selector list from the current subscriber symbol
// lists, deduplicating (symbol, right, delay, δ) tuples across subscribers and
// assigning each a stable id. It returns the selectors that did not exist
// before this call.
//
// A right with no row in any subscriber's list produces NO selector. The old
// group-based build defaulted an absent right's delta to 0.50, which had two
// costs: it subscribed an ATM leg for a right nobody was watching (a wasted
// market-data line), and — because the default was part of the group key — it
// changed the identity of the group the WATCHED right belonged to. That is
// what broke QQQ on 2026-08-13: commenting out one put row moved the call into
// a group of its own.
//
// Selector IDs are keyed by configuration, not by position in the list, and
// are remembered for the life of the session. That matters because a rebuild
// is not a once-per-session event: ResyncSymbols rebuilds after every
// watchlist edit, and an in-flight entry probe is keyed by selector id
// (probes), so renumbering would hand one selector's probe to another. With
// stable IDs an untouched selector is bit-identical across a rebuild, and only
// genuinely new configurations are returned.
func (s *Session) buildSelectors() []selector {
	sels := make(map[selectorKey]*selector)
	var order []selectorKey
	for busIdx, syms := range s.subSymbolLists() {
		seen := make(map[selectorKey]bool)
		for _, sy := range syms {
			if !isOptionTag(sy.Tag) {
				continue
			}
			td := sy.TargetDelta
			if td <= 0 || td > 1 {
				td = 0.50
			}
			key := selectorKey{sy.Symbol, sy.Tag, sy.OptionDelay, td}
			sel, ok := sels[key]
			if !ok {
				currency := "USD"
				if sy.Contract != nil && sy.Contract.Currency != "" {
					currency = sy.Contract.Currency
				}
				sel = &selector{symbol: sy.Symbol, currency: currency, right: sy.Tag,
					optionDelay: sy.OptionDelay, targetDelta: td}
				sels[key] = sel
				order = append(order, key)
			}
			// One subscriber listing the same tuple twice (a duplicated CSV row)
			// must not add its bus twice, or every publish to this selector
			// would be doubled.
			if !seen[key] {
				seen[key] = true
				sel.busIdxs = append(sel.busIdxs, busIdx)
			}
		}
	}

	sort.Slice(order, func(i, j int) bool {
		if order[i].symbol != order[j].symbol {
			return order[i].symbol < order[j].symbol
		}
		if order[i].right != order[j].right {
			return order[i].right < order[j].right
		}
		if order[i].optionDelay != order[j].optionDelay {
			return order[i].optionDelay < order[j].optionDelay
		}
		return order[i].targetDelta < order[j].targetDelta
	})

	var fresh []selector
	s.optChain.mu.Lock()
	if s.optChain.selectorIDs == nil {
		s.optChain.selectorIDs = make(map[selectorKey]int)
	}
	s.optChain.selectors = s.optChain.selectors[:0]
	for _, key := range order {
		sel := sels[key]
		id, known := s.optChain.selectorIDs[key]
		if !known {
			id = s.optChain.nextSelID
			s.optChain.nextSelID++
			s.optChain.selectorIDs[key] = id
		}
		sel.id = id
		s.optChain.selectors = append(s.optChain.selectors, *sel)
		if !known {
			fresh = append(fresh, *sel)
		}
	}
	s.optChain.mu.Unlock()
	return fresh
}

// isATMDelta reports whether a target delta sits in the band where the library
// refuses delta-based selection and simply takes the nearest strike.
func isATMDelta(td float64) bool { return td >= 0.48 && td <= 0.52 }

// How wide the entry delta probe reaches on each side of the underlying.
//
// A ~0.55 target sits AT or just OUTSIDE the money, so a probe that reaches
// only inside it can never find the strike being asked for -- see
// selectStrikeCandidates.
const (
	deltaProbeITMCandidates = 3 // strikes at or inside the money
	deltaProbeOTMCandidates = 3 // strikes outside it -- where a ~0.55 target lives
)

// deltaMissWarnThreshold is how far a resolved delta may sit from its target
// before the resolution is reported as an anomaly. Not an acceptance test --
// the entry is taken regardless (see resolveDeltaCandidates).
const deltaMissWarnThreshold = 0.15

// selectStrikeCandidates picks up to itm strikes at-or-inside the money and up
// to otm strikes outside it, returned NEAREST-THE-MONEY FIRST.
//
// This used to be selectITMCandidates, which took n strikes STRICTLY inside the
// money (for a call, sorted[i] < undPrice) and nothing else. Delta is monotonic
// in strike, so candidate #1 -- the rung nearest the money -- always carried the
// LOWEST delta of the set, and every further candidate was one rung deeper ITM.
// Widening that window moved strictly AWAY from a ~0.5 target; the count was
// never the constraint, the direction was.
//
// The strikes carrying delta ~0.55 are at the money and just outside it, and
// they were never subscribed. Over 2026-08-21..26 that put 82 entries above
// delta 0.65 and 24 above 0.75, on the wrong side of a step the ladder offered
// for free: AAPL at spot 309.80 resolved to strike 305 at delta 0.84 when 310,
// one rung up, was ~0.52. Those deep entries are also nearly all intrinsic
// value, so a percentage stop on the PREMIUM becomes a few cents of underlying
// movement -- they stopped out 71% of the time at -$104 a leg.
//
// Four properties, each load-bearing:
//
//   - Both sides. For a call, at-or-inside is strike <= undPrice and outside is
//     strike > undPrice; for a put, mirrored. This is the whole fix.
//   - A strike exactly AT the underlying is included, on the at-or-inside side.
//     The old strict < dropped the single rung most likely to carry delta 0.5.
//   - Ordered by |strike - undPrice| ascending, because GrantProbe refusals skip
//     candidates in loop order and the farthest-from-target ones must be last.
//     Selection itself is a min over every ready candidate, so ordering never
//     decides which one wins.
//   - Backfilled from the other side when one runs short (the underlying sitting
//     near the end of the ladder), so a thin chain still gets a full-width probe.
func selectStrikeCandidates(sortedStrikes []float64, undPrice float64, right string, itm, otm int) []float64 {
	if len(sortedStrikes) == 0 || undPrice <= 0 || itm+otm <= 0 {
		return nil
	}
	sorted := make([]float64, len(sortedStrikes))
	copy(sorted, sortedStrikes)
	sort.Float64s(sorted)

	// atOrBelow runs from the money downwards, above runs upwards. A strike
	// exactly at undPrice belongs to atOrBelow.
	var atOrBelow, above []float64
	for i := len(sorted) - 1; i >= 0; i-- {
		if sorted[i] <= undPrice {
			atOrBelow = append(atOrBelow, sorted[i])
		}
	}
	for _, st := range sorted {
		if st > undPrice {
			above = append(above, st)
		}
	}

	// A call is in the money below the underlying, a put above it.
	inside, outside := atOrBelow, above
	if right != "call" {
		inside, outside = above, atOrBelow
	}

	take := func(src []float64, n int) []float64 {
		if n > len(src) {
			n = len(src)
		}
		return src[:n]
	}
	picked := append(take(inside, itm), take(outside, otm)...)

	// Backfill: a side that came up short lends its budget to the other, so the
	// probe keeps its full width against a chain that ends near the money.
	if short := (itm + otm) - len(picked); short > 0 {
		if extra := take(inside[min(itm, len(inside)):], short); len(extra) > 0 {
			picked = append(picked, extra...)
			short -= len(extra)
		}
		if short > 0 {
			picked = append(picked, take(outside[min(otm, len(outside)):], short)...)
		}
	}

	sort.SliceStable(picked, func(i, j int) bool {
		return math.Abs(picked[i]-undPrice) < math.Abs(picked[j]-undPrice)
	})
	return picked
}

// ── Leg registry ──────────────────────────────────────────────────────────
//
// One leg per contract, refcounted across holders. attach/detach are the only
// ways in and out; nothing else may delete a leg, because "is anyone else
// still using this?" is the question the old (symbol, right) ownership model
// could not ask.

// releaseLegIfUnheldLocked removes a leg once nothing holds it, returning the
// reqID to cancel (0 when it is still held). Caller holds s.optChain.mu.
func (s *Session) releaseLegIfUnheldLocked(leg *optLeg) int64 {
	if leg.held() {
		s.mdLines.Reclassify(leg.reqID, legCategory(leg))
		return 0
	}
	delete(s.optChain.legs, leg.key())
	delete(s.optChain.legByReqID, leg.reqID)
	delete(s.optChain.forcedResub, leg.key())
	delete(s.optChain.dupRepairs, leg.key())
	s.mdLines.Release(leg.reqID)
	return leg.reqID
}

// legCategory is a leg's market-data priority: guaranteed while any open
// position holds it, the lowest (evictable) recording tier once only
// recording holds remain.
func legCategory(l *optLeg) mdlines.Category {
	if l.pins > 0 {
		return mdlines.CategoryPosition
	}
	return mdlines.CategoryRecording
}

// legAgeString renders a leg's last-tick age for logs, distinguishing "never
// ticked" from "ticked a while ago" — they call for different repairs.
func legAgeString(lastTickAt, now time.Time) string {
	if lastTickAt.IsZero() {
		return "never"
	}
	return now.Sub(lastTickAt).Truncate(time.Second).String()
}

// touchOptionLegLocked records that IB delivered a message for reqID, on
// whichever of the three option request maps owns it, and advances the
// session-wide lastAnyOptionTick. Must be called with s.optChain.mu held.
//
// It is deliberately called for EVERY tick type — including size and generic
// ticks whose values we discard — because the question it answers is "is this
// subscription still being served", not "did the price move". Those are
// different questions with different answers: a liquid option with a flat
// quote still receives a steady stream of size ticks, so restricting the
// stamp to ticks we happen to store would re-create the exact blind spot the
// quotes.Book's advance-on-change timestamps already have.
func (s *Session) touchOptionLegLocked(reqID int64, now time.Time) {
	touched := false
	if leg, ok := s.legByReqIDLocked(reqID); ok {
		leg.lastTickAt = now
		touched = true
	}
	if _, ok := s.optChain.deltaCands[reqID]; ok {
		touched = true
	}
	if touched {
		s.optChain.lastAnyOptionTick = now
	}
}

// legByReqIDLocked resolves an IB reqID to its leg. Caller holds s.optChain.mu.
func (s *Session) legByReqIDLocked(reqID int64) (*optLeg, bool) {
	key, ok := s.optChain.legByReqID[reqID]
	if !ok {
		return nil, false
	}
	leg, ok := s.optChain.legs[key]
	return leg, ok
}

// optionKeyForReqIDLocked resolves reqID to the ContractKey of the option leg
// it currently identifies (ATM background, position-pinned, or a background
// delta-probe candidate) — for touch-only liveness stamps that have no price
// to associate, such as a size tick or a greeks-only tick. Must be called with
// s.optChain.mu held. ok is false when reqID is not (or no longer, e.g. mid
// strike-roll) a resolved option subscription — most commonly because it is a
// stock's reqID instead.
func (s *Session) optionKeyForReqIDLocked(reqID int64) (key quotes.ContractKey, ok bool) {
	if leg, found := s.legByReqIDLocked(reqID); found && leg.strike > 0 && leg.expiry != "" {
		return leg.key().contractKey(), true
	}
	if cand, found := s.optChain.deltaCands[reqID]; found && cand.strike > 0 && cand.expiry != "" {
		return quotes.ContractKey{Symbol: cand.symbol, Right: cand.right, Strike: cand.strike, Expiry: cand.expiry}, true
	}
	return quotes.ContractKey{}, false
}

// openLegLocked creates a leg for key with a freshly allocated reqID, ready
// for the caller to ReqMktData outside the lock. It does NOT grant a
// market-data line — callers pick the tier. Caller holds s.optChain.mu.
func (s *Session) openLegLocked(key legKey, reqID int64, deltaSource string, now time.Time) *optLeg {
	leg := &optLeg{
		reqID: reqID, symbol: key.symbol, right: key.right, strike: key.strike, expiry: key.expiry,
		deltaSource: deltaSource, subscribedAt: now,
	}
	s.optChain.legs[key] = leg
	s.optChain.legByReqID[reqID] = key
	return leg
}

// cancelLines cancels market-data subscriptions whose last holder released
// them. Always called outside s.optChain.mu — the IB client must never be
// invoked under it.
func (s *Session) cancelLines(reqIDs []int64) {
	for _, id := range reqIDs {
		s.client.CancelMktData(id)
	}
}

// forceResubscribeLeg re-requests an existing contract under a fresh reqID,
// keeping every holder attached. This is the repair for a leg IB has silently
// stopped serving: the contract is right, the subscription behind it is dead,
// and re-asking for the same contract is the only thing that fixes it.
//
// Every leg reaching here is position-pinned — the only kind that exists now —
// so the replacement line is granted, never refused. A held contract losing its
// feed is what silently disarms stop-loss and trailing-stop evaluation, and the
// old code's careful dance around discretionary reserves (surrender the dead
// leg's line, retry, put it back if that failed too) existed only because
// background legs competed for the same headroom. They no longer exist.
func (s *Session) forceResubscribeLeg(key legKey) {
	s.optChain.mu.Lock()
	old, ok := s.optChain.legs[key]
	if !ok {
		s.optChain.mu.Unlock()
		return
	}
	oldReqID := old.reqID
	reqID := s.nextReqID()
	s.optChain.mu.Unlock()

	s.mdLines.GrantGuaranteed(reqID, mdlines.CategoryPosition)

	s.optChain.mu.Lock()
	cur, stillThere := s.optChain.legs[key]
	if !stillThere || cur.reqID != oldReqID {
		s.optChain.mu.Unlock() // released or already recovered since the scan
		s.mdLines.Release(reqID)
		return
	}
	// Carry the last known values forward so no row blanks while the
	// replacement warms up, but reset the liveness clocks so the new
	// subscription is judged on its own behaviour.
	cur.reqID = reqID
	cur.subscribedAt = time.Now()
	cur.lastTickAt = time.Time{}
	delete(s.optChain.legByReqID, oldReqID)
	s.optChain.legByReqID[reqID] = key
	s.optChain.mu.Unlock()

	s.mdLines.Release(oldReqID)
	ibRight := "C"
	if key.right == "put" {
		ibRight = "P"
	}
	s.client.ReqMktData(reqID, makeOptionContract(key.symbol, ibRight, key.strike, key.expiry), "", false, false, nil)
	s.client.CancelMktData(oldReqID)
	s.optionLog.Printf("Option: %s %s strike=%.2f re-subscribed (reqID %d → %d)", key.symbol, key.right, key.strike, oldReqID, reqID)
}

// releaseOrphanedProbeCandidate drops a deltaCands entry for a reqID the
// mdlines reaper just freed (ReapProbes) — its owning resolution never
// reached its own release path, so this stops a stray late tick from
// mutating a *deltaCandidate no code will ever read again. Safe to call for
// an unknown reqID (no-op); resolveDeltaCandidates, if it later runs anyway,
// still calls mdLines.Release on the same reqID, which is a no-op too.
func (s *Session) releaseOrphanedProbeCandidate(reqID int64) {
	s.optChain.mu.Lock()
	_, ok := s.optChain.deltaCands[reqID]
	delete(s.optChain.deltaCands, reqID)
	s.optChain.mu.Unlock()
	if ok {
		s.optionLog.Printf("Option: dropped orphaned delta candidate (reqID=%d) reaped by mdlines", reqID)
	}
}

// releaseCandidatesLocked drops every candidate from the registry and returns
// its market-data line to the ledger, returning the reqIDs that still need a
// CancelMktData. A dupTicker candidate's id belongs to another live request
// (see dupticker.go), so cancelling it would kill that one instead.
//
// Caller holds s.optChain.mu and cancels the returned ids after unlocking.
func (s *Session) releaseCandidatesLocked(cands []*deltaCandidate) []int64 {
	var cancel []int64
	for _, c := range cands {
		delete(s.optChain.deltaCands, c.reqID)
		if c.dupTicker {
			continue
		}
		s.mdLines.Release(c.reqID)
		cancel = append(cancel, c.reqID)
	}
	return cancel
}

// selectorForLocked returns the selector for (symbol, right) that busIdx
// belongs to. busIdx < 0 (subscriber's bus not found in s.buses) falls back to
// the first selector matching symbol+right. Caller must hold s.optChain.mu.
//
// The right is part of the lookup, which is what makes this correct by
// construction. Its predecessor keyed on symbol alone and had to disambiguate
// afterwards, since one group covered both rights at two different target
// deltas; that is how VWmacdOptionRobot came to price an IWM call entry
// against VWmacdOptionDataRobot's target_delta on 2026-08-04.
func (s *Session) selectorForLocked(symbol, right string, busIdx int) (selector, bool) {
	for _, sel := range s.optChain.selectors {
		if sel.symbol != symbol || sel.right != right {
			continue
		}
		if busIdx < 0 || slices.Contains(sel.busIdxs, busIdx) {
			return sel, true
		}
	}
	return selector{}, false
}

// quoted reports a two-sided price. The Greeks tick (which sets ready) and the
// bid/ask ticks arrive independently and the Greeks routinely come first, so
// "ready" alone must not end the probe: it used to, and the winner was then read
// before its price landed (2026-10-05 META call, option_delta_no_price).
func (c *deltaCandidate) quoted() bool { return c.bid > 0 && c.ask > 0 }

// deltaCandidatesSettled is ResolveEntryStrike's poll-loop exit condition:
// true once EITHER every candidate has reported (Greeks AND a two-sided price)
// OR one already-reported candidate is within deltaGoodEnoughTolerance of
// targetDelta. A candidate IB rejected outright counts as reported — it never
// will. Caller must hold s.optChain.mu.
func deltaCandidatesSettled(candidates []*deltaCandidate, targetDelta float64) bool {
	const deltaGoodEnoughTolerance = 0.02
	allReady := true
	for _, c := range candidates {
		if c.rejected {
			continue
		}
		if !c.ready || !c.quoted() {
			allReady = false
			continue
		}
		if math.Abs(math.Abs(c.delta)-targetDelta) <= deltaGoodEnoughTolerance {
			return true
		}
	}
	return allReady
}

// forgetLegLocked drops a leg from the registry without touching the ledger —
// for a line the ledger has ALREADY taken back, or one IB has told us does not
// exist (error 200). Caller holds s.optChain.mu.
//
// It deliberately does NOT decrement pins or notify the positions holding it.
// A leg reaching here is gone at IB whatever the hub believes; leaving the pin
// count alone means the next UnsubscribePositionStrike still balances, and
// ORBtrader's stale-position monitor sees a held position with no leg — its
// staleNoLeg case — and re-subscribes. That is the intended repair path, and
// it is the one that can also tell the operator.
func (s *Session) forgetLegLocked(leg *optLeg) {
	key := leg.key()
	delete(s.optChain.legs, key)
	delete(s.optChain.legByReqID, leg.reqID)
	delete(s.optChain.forcedResub, key)
	delete(s.optChain.dupRepairs, key)
}

// handleOptionMktError handles error 200 against a chain request, an entry
// probe candidate, or a position-pinned leg. Returns true if reqID was one of
// those, false otherwise.
func (s *Session) handleOptionMktError(reqID int64, errStr string) bool {
	if s.handleChainError(reqID, errStr) {
		return true
	}
	s.optChain.mu.Lock()

	if cand, ok := s.optChain.deltaCands[reqID]; ok {
		// Stamp the cause on the candidate BEFORE dropping it from the map:
		// res.candidates still holds this pointer, so classifyCandidateErrors
		// can read it even though the candidate is no longer discoverable by
		// reqID.
		cand.errCode = 200
		cand.errMsg = errStr
		cand.rejected = true
		delete(s.optChain.deltaCands, cand.reqID)
		s.mdLines.Release(cand.reqID)
		s.optChain.mu.Unlock()
		s.optionLog.Printf("Option delta candidate FAILED: %s %s strike=%.2f — %s", cand.symbol, cand.right, cand.strike, errStr)
		return true
	}

	leg, ok := s.legByReqIDLocked(reqID)
	if !ok {
		s.optChain.mu.Unlock()
		return false
	}
	key := leg.key()
	symbol, right, strike, expiry := key.symbol, key.right, key.strike, key.expiry
	pinned := leg.pins > 0

	// The contract itself is bad, so the leg must go — its IB subscription does
	// not exist.
	s.forgetLegLocked(leg)
	s.mdLines.Release(reqID)
	s.optChain.mu.Unlock()

	s.optionLog.Printf("Option market data FAILED: %s %s strike=%.2f expiry=%s (reqID=%d, pinned=%v) — contract not found, dropping the leg",
		symbol, right, strike, expiry, reqID, pinned)
	s.logger.Printf("Option market data FAILED: %s %s strike=%.2f expiry=%s — %s", symbol, right, strike, expiry, errStr)

	// There is deliberately no next-nearest-strike retry walk here any more.
	// It existed to keep a watchlist row populated when the estimated strike
	// turned out not to be listed: step to the next strike, and the next, until
	// something resolved. Those rows are gone, and with them the only caller
	// that wanted "any contract that works" rather than "the contract I asked
	// for". Both remaining leg kinds want the opposite —
	//
	//   a position-pinned leg names a contract the account demonstrably holds,
	//   so error 200 against it is a real anomaly to surface, not something to
	//   paper over by subscribing a DIFFERENT strike the position does not hold;
	//
	//   an entry probe subscribes several candidates at once and picks on
	//   delta; a refused one counts as settled (deltaCandidatesSettled) and
	//   needs no walk.
	//
	// The walk was also a liability in its own right: unbounded work in an
	// error path, firing the next ReqMktData synchronously from the callback,
	// which is how MSFT issued 35 subscribe attempts in 8 seconds on 2026-08-17
	// and settled on a δ≈1.00 leg with no bid and no ask.
	return true
}

// handleOptionTick updates the cached price/bid/ask for an option market
// data reqID and publishes KindOptionData (ATM) or KindPositionOptionData
// (position-pinned strike) to the owning subscriber bus(es). Returns true
// if reqID was an option market data request, false otherwise.
func (s *Session) handleOptionTick(reqID int64, tickType int64, price float64) bool {
	s.optChain.mu.Lock()

	// Stamp liveness before the type switches below: each has a default
	// branch that returns early on a tick type we do not store, and those
	// ticks are still proof the subscription is being served.
	s.touchOptionLegLocked(reqID, time.Now())

	if leg, ok := s.legByReqIDLocked(reqID); ok {
		switch tickType {
		case ibapi.BID, ibapi.DELAYED_BID:
			if price != leg.bid {
				leg.bid = price
				leg.quoteSeq++
			}
		case ibapi.ASK, ibapi.DELAYED_ASK:
			if price != leg.ask {
				leg.ask = price
				leg.quoteSeq++
			}
		case ibapi.LAST, ibapi.DELAYED_LAST:
			if price != leg.price {
				leg.price = price
				leg.quoteSeq++
			}
		case ibapi.CLOSE, ibapi.DELAYED_CLOSE:
			// The prior session's close, not a price anything may trade or
			// value against. Park it and publish NOTHING — returning here is
			// load-bearing twice over. It keeps yesterday's number out of
			// leg.price (which optSellPrice falls back to whenever bid and ask
			// are both still zero, exactly the state a freshly pinned leg is in
			// when IB's opening burst delivers this tick), and it suppresses
			// the od publish below, which would republish the unchanged
			// leg.price a second time and hand ConfirmedWatermarkPrice the
			// corroborating "second tick" its guard asks for. Liveness was
			// already stamped by touchOptionLegLocked above, so the leg is
			// still visibly being served.
			leg.prevClose = price
			s.optChain.mu.Unlock()
			return true
		default:
			s.optChain.mu.Unlock()
			return true
		}
		od := eventbus.OptionData{
			Symbol: leg.symbol, Right: leg.right, Strike: leg.strike, Expiry: leg.expiry,
			Price: leg.price, Bid: leg.bid, Ask: leg.ask, Delta: leg.delta, DeltaSource: leg.deltaSource,
			QuoteSeq: leg.quoteSeq,
		}
		pinned := leg.pins > 0
		s.optChain.mu.Unlock()

		s.bookOption(od)
		// Only KindPositionOptionData is published. Its sibling KindOptionData
		// carried a watchlist row's display contract to the dashboard; there is
		// no such contract any more, so a leg reaching here is always an open
		// position's and always belongs on the position channel.
		if pinned {
			s.publish(eventbus.Event{Kind: eventbus.KindPositionOptionData, Payload: od})
		}
		return true
	}

	if cand, ok := s.optChain.deltaCands[reqID]; ok {
		switch tickType {
		case ibapi.BID, ibapi.DELAYED_BID:
			cand.bid = price
		case ibapi.ASK, ibapi.DELAYED_ASK:
			cand.ask = price
		}
		s.optChain.mu.Unlock()
		return true
	}

	s.optChain.mu.Unlock()
	return false
}

// ── Helper functions ──────────────────────────────────────────────────────

func nearestExpiry(expirations []string, delayDays int) string {
	target := time.Now().AddDate(0, 0, delayDays).Format("20060102")
	sorted := make([]string, len(expirations))
	copy(sorted, expirations)
	sort.Strings(sorted)
	for _, e := range sorted {
		if e >= target {
			return e
		}
	}
	return ""
}

// makeOptionContract builds an ibapi.Contract for the given option parameters.
func makeOptionContract(symbol, right string, strike float64, expiry string) *ibapi.Contract {
	c := ibapi.NewContract()
	c.Symbol = symbol
	c.SecType = "OPT"
	c.Currency = "USD"
	c.Exchange = "SMART"
	c.Right = right
	c.Strike = strike
	c.LastTradeDateOrContractMonth = expiry
	c.Multiplier = "100"
	return c
}

// SubscribePositionStrike subscribes to IB market data for a specific
// option strike pinned to an open position — the only kind of option market
// data this package holds open. No-op if already subscribed for this
// symbol+right+strike combination.
func (s *Session) SubscribePositionStrike(symbol, right string, strike float64, expiry string) {
	key := legKey{symbol, right, strike, expiry}

	s.optChain.mu.Lock()
	if leg, exists := s.optChain.legs[key]; exists {
		// The contract is already subscribed — by another position, or by a
		// watchlist row whose selector happens to point at this exact strike.
		// Either way there is nothing to request: take a reference and upgrade
		// the line to CategoryPosition so it can no longer be preempted.
		leg.pins++
		s.mdLines.Reclassify(leg.reqID, mdlines.CategoryPosition)
		s.optChain.mu.Unlock()
		s.optionLog.Printf("Option: POSITION-PINNED %s %s strike=%.2f expiry=%s sharing existing subscription (reqID=%d, pins=%d)",
			symbol, right, strike, expiry, leg.reqID, leg.pins)
		return
	}
	reqID := s.nextReqID()
	leg := s.openLegLocked(key, reqID, "", time.Now())
	leg.pins = 1
	s.optChain.mu.Unlock()

	s.mdLines.GrantGuaranteed(reqID, mdlines.CategoryPosition)

	ibRight := "C"
	if right == "put" {
		ibRight = "P"
	}
	contract := makeOptionContract(symbol, ibRight, strike, expiry)
	s.optionLog.Printf("Option: subscribing POSITION-PINNED %s %s strike=%.2f expiry=%s (reqID=%d)", symbol, right, strike, expiry, reqID)
	s.client.ReqMktData(reqID, contract, "", false, false, nil)
}

// UnsubscribePositionStrike releases one holder's interest in a
// position-pinned option subscription. It only tears down the IB feed once
// nothing holds the contract any more — neither another position nor a
// watchlist row's selector.
//
// A still-open sibling position must never lose its feed because another
// position on the same contract exited. That is exactly what happened on
// 2026-08-04: VWmacdOptionRobot's IWM 298 PUT hit its trailing stop and
// unsubscribed the line at the same moment OrbOptionRobot's own IWM 298 PUT —
// opened a minute earlier on the same contract — was still open, freezing its
// quote at ~$1.83 for the rest of the session while the real market fell to
// $0.02. Since background legs now live in the same registry, the guarantee
// extends to them: an exit can no longer blank a watchlist row either.
//
// expiry is part of the identity: two positions on the same strike at
// different expiries are different contracts and must not share a refcount.
func (s *Session) UnsubscribePositionStrike(symbol, right string, strike float64, expiry string) {
	key := legKey{symbol, right, strike, expiry}

	s.optChain.mu.Lock()
	leg, ok := s.optChain.legs[key]
	if !ok {
		s.optChain.mu.Unlock()
		return
	}
	if leg.pins > 0 {
		leg.pins--
	}
	reqID := leg.reqID
	remaining := leg.pins
	held := leg.held()
	cancel := s.releaseLegIfUnheldLocked(leg)
	s.optChain.mu.Unlock()

	if held {
		s.optionLog.Printf("Option: released one POSITION-PINNED hold on %s %s strike=%.2f expiry=%s — keeping the feed (reqID=%d, pins=%d, still watched)",
			symbol, right, strike, expiry, reqID, remaining)
		return
	}
	s.optionLog.Printf("Option: unsubscribing POSITION-PINNED %s %s strike=%.2f expiry=%s (reqID=%d)", symbol, right, strike, expiry, reqID)
	s.cancelLines([]int64{cancel})
}

// HoldForRecording keeps an option contract streaming for quote recording,
// independent of positions: while a position holds the contract this only adds
// a reference, and when the last position releases it the line drops to
// mdlines.CategoryRecording instead of being cancelled. A contract nothing
// holds (after a reconnect, say) is subscribed afresh — but only from free
// headroom; with none, it goes unrecorded. Pair every call with
// ReleaseRecording.
func (s *Session) HoldForRecording(symbol, right string, strike float64, expiry string) {
	key := legKey{symbol, right, strike, expiry}

	s.optChain.mu.Lock()
	if leg, exists := s.optChain.legs[key]; exists {
		leg.tails++
		s.optChain.mu.Unlock()
		return
	}
	reqID := s.nextReqID()
	s.optChain.mu.Unlock()

	if !s.mdLines.GrantRecording(reqID) {
		s.optionLog.Printf("Option: no headroom to RECORD %s %s strike=%.2f expiry=%s — left unrecorded", symbol, right, strike, expiry)
		return
	}
	s.optChain.mu.Lock()
	if leg, exists := s.optChain.legs[key]; exists {
		// A position subscribed it while the grant was in flight.
		leg.tails++
		s.optChain.mu.Unlock()
		s.mdLines.Release(reqID)
		return
	}
	leg := s.openLegLocked(key, reqID, "", time.Now())
	leg.tails = 1
	s.optChain.mu.Unlock()

	ibRight := "C"
	if right == "put" {
		ibRight = "P"
	}
	s.optionLog.Printf("Option: subscribing for RECORDING %s %s strike=%.2f expiry=%s (reqID=%d)", symbol, right, strike, expiry, reqID)
	s.client.ReqMktData(reqID, makeOptionContract(symbol, ibRight, strike, expiry), "", false, false, nil)
}

// ReleaseRecording drops one recording hold, cancelling the feed once nothing
// — no position, no other recording hold — still wants the contract.
func (s *Session) ReleaseRecording(symbol, right string, strike float64, expiry string) {
	key := legKey{symbol, right, strike, expiry}

	s.optChain.mu.Lock()
	leg, ok := s.optChain.legs[key]
	if !ok || leg.tails == 0 {
		s.optChain.mu.Unlock()
		return
	}
	leg.tails--
	cancel := s.releaseLegIfUnheldLocked(leg)
	s.optChain.mu.Unlock()
	if cancel != 0 {
		s.optionLog.Printf("Option: unsubscribing RECORDING %s %s strike=%.2f expiry=%s (reqID=%d)", symbol, right, strike, expiry, cancel)
		s.cancelLines([]int64{cancel})
	}
}

// evictRecordingLines is the mdlines eviction handler: the ledger took these
// recording lines back to make room for a higher-priority one. A leg that a
// position re-pinned in the meantime keeps its feed — its line is re-granted
// as a position line; any other is dropped and cancelled.
func (s *Session) evictRecordingLines(reqIDs []int64) {
	var cancel []int64
	s.optChain.mu.Lock()
	for _, id := range reqIDs {
		leg, ok := s.legByReqIDLocked(id)
		if !ok {
			continue
		}
		if leg.pins > 0 {
			s.mdLines.GrantGuaranteed(id, mdlines.CategoryPosition)
			continue
		}
		s.optionLog.Printf("Option: EVICTED recording line %s %s strike=%.2f expiry=%s (reqID=%d) — a higher-priority line needed it",
			leg.symbol, leg.right, leg.strike, leg.expiry, id)
		s.forgetLegLocked(leg)
		cancel = append(cancel, id)
	}
	s.optChain.mu.Unlock()
	s.cancelLines(cancel)
}

// getUnderlyingPrice returns the current price for a symbol using bid/ask
// midpoint from the streaming cache, or the shared quote book as fallback.
// Returns 0 if unknown.
func (s *Session) getUnderlyingPrice(symbol string) float64 {
	s.mktData.bidAskMu.RLock()
	bid, hasBid := s.mktData.bid[symbol]
	ask, hasAsk := s.mktData.ask[symbol]
	s.mktData.bidAskMu.RUnlock()

	if hasBid && hasAsk && bid.Price > 0 && ask.Price > 0 {
		return (bid.Price + ask.Price) / 2
	}
	if hasBid && bid.Price > 0 {
		return bid.Price
	}
	if hasAsk && ask.Price > 0 {
		return ask.Price
	}

	if s.book != nil {
		if q, ok := s.book.Stock(symbol); ok && q.Last > 0 {
			return q.Last
		}
	}
	return 0
}

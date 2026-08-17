// Option chain lookup and market data subscription.
//
// Flow per session:
//  1. requestOptionChains fires ReqContractDetails for each unique option
//     underlying to obtain its conId.
//  2. ContractDetailsEnd for those reqIDs calls ReqSecDefOptParams with the
//     real conId.
//  3. SecurityDefinitionOptionParameter accumulates expirations/strikes from
//     the SMART exchange only (non-SMART exchanges may include phantom strikes
//     not routable via SMART), bucketed by TRADING CLASS — IBKR calls this once
//     per (exchange, trading class), and two classes on one underlying have
//     different expiry calendars and different strike ladders.
//  4. SecurityDefinitionOptionParameterEnd fires when all callbacks have
//     responded; picks ONE trading class, then the nearest expiry +
//     ATM/target-delta strike from that class alone, then subscribes to
//     streaming market data.
//  5. If IB returns error 200 for both legs of a strike, handleOptionMktError
//     automatically retries with the next nearest strike.
//  6. TickPrice for option reqIDs updates cached prices and publishes
//     KindOptionData/KindPositionOptionData.
package ibkr

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
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

// optConIDReq tracks one pending ReqContractDetails call used to resolve
// the underlying conId before calling ReqSecDefOptParams. waiters holds every
// selector that asked for this chain, so a call and a put on the same
// underlying share the round trip instead of each paying for one.
type optConIDReq struct {
	chain       chainKey
	currency    string
	conID       int64
	waiters     []int
	requestedAt time.Time
}

// optChainReq tracks one pending reqSecDefOptParams call. IBKR sends one
// SecurityDefinitionOptionParameter callback per (exchange, TRADING CLASS) —
// not merely per exchange, which is what the old comment here claimed and the
// old code assumed. One underlying routinely has several SMART classes: the
// standard one, plus adjusted/mini classes from corporate actions, each with
// its OWN expiry calendar and its OWN strike ladder.
//
// Flattening them into one expirations+strikes pair produces contracts that do
// not exist: an expiry taken from class A paired with a strike taken from
// class B. On 2026-08-17 that put MSFT on expiry 20260820 with the $5 ladder of
// a different class, IB answered error 200 to every strike from 405 to 580,
// and the retry walk finally settled on strike 400 — δ≈1.00, no bid, no ask,
// untradable, and (because it then counted as an existing leg) unable to move
// off it for the rest of the session.
//
// So the callbacks are bucketed per class and exactly one class is chosen at
// the end. An expiry and a strike can then only ever come from the same ladder.
type optChainReq struct {
	chain       chainKey
	waiters     []int
	classes     map[string]*chainClass
	requestedAt time.Time
}

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

	// deltaSource is "matched" (a deliberate ATM target, or a genuine live
	// IB delta match) or "atm_fallback" (no usable delta, strike picked
	// without one).
	deltaSource string

	// selectors is every selector holding this contract — whether it is the
	// one that selector currently displays or one it is warming into (see
	// pendingSwap). pins counts open-position holders.
	//
	// These are the two holder kinds, and the line is released only when both
	// are empty. Position-pinned legs have worked this way since the
	// 2026-08-04 incident where one robot's exit cancelled a contract a
	// sibling's still-open IWM 298 PUT was pricing its stops against;
	// background legs get the same treatment here, since the failure is
	// identical and the old code had no defence against it at all.
	selectors map[int]struct{}
	pins      int

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
func (l *optLeg) held() bool { return len(l.selectors) > 0 || l.pins > 0 }

// quoteComplete reports whether the leg carries a two-sided price.
func (l *optLeg) quoteComplete() bool { return l.bid > 0 && l.ask > 0 }

// pendingSwap is one selector's in-flight move onto a different contract. The
// selector keeps displaying its previous leg until `to` carries a complete
// quote (or pendingPromoteGrace elapses on any usable price), so a strike roll
// never blanks the row or leaves a buy without a price.
type pendingSwap struct {
	to    legKey
	since time.Time
}

// pendingPromoteGrace bounds how long a replacement option leg may stay
// pending waiting for a complete two-sided quote before it is promoted
// anyway (on any usable price).
const pendingPromoteGrace = 4 * time.Second

// maxStrikeRetries bounds how many next-nearest strikes one selection may try
// after error 200. The walk is unbounded work in the error path: each failure
// fires the next ReqMktData synchronously from the callback, so an expiry whose
// strikes are simply not listed produces a burst against IB's pacing limits.
// On 2026-08-17 MSFT issued 35 subscribe attempts in 8 seconds.
const maxStrikeRetries = 4

// maxStrikeRetryDistancePct bounds how far from the underlying a retry may
// wander, as a percentage. This is the bound that matters: the walk exists to
// step over a gap in the ladder, not to find *any* contract that resolves. The
// MSFT walk ran from 490 down to 400 against a $485 underlying and ended
// holding a δ≈1.00 leg with no bid and no ask — nominally a success, in
// practice an untradable row that could no longer be corrected. Refusing to
// subscribe is the better answer: a blank row states the problem, and the
// selector stays legless so its next attempt is a first-quote grant.
const maxStrikeRetryDistancePct = 10.0

// optStrikeRetry holds the sorted candidate strike list for a selector so
// that if the nearest strike fails with error 200, the next nearest is tried
// automatically — subject to the two bounds above.
type optStrikeRetry struct {
	selectorID int
	symbol     string
	right      string
	expiry     string
	strikes    []float64 // sorted by distance from undPrice, nearest first
	nextIdx    int
	undPrice   float64
	attempts   int
	tried      []float64
	busIdxs    []int
}

// nextCandidate advances to the next strike worth trying. ok is false when this
// selection gives up, with reason naming which bound stopped it.
func (r *optStrikeRetry) nextCandidate() (strike float64, reason string, ok bool) {
	if r.attempts >= maxStrikeRetries {
		return 0, fmt.Sprintf("stopped after %d retries", maxStrikeRetries), false
	}
	if r.nextIdx >= len(r.strikes) {
		return 0, "no candidate strikes left in the chain", false
	}
	cand := r.strikes[r.nextIdx]
	r.nextIdx++
	// strikes is sorted nearest-first, so the first candidate outside the band
	// means every remaining one is further still.
	if r.undPrice > 0 && math.Abs(cand-r.undPrice)/r.undPrice*100 > maxStrikeRetryDistancePct {
		return 0, fmt.Sprintf("nearest untried strike %.2f is more than %.0f%% from the underlying %.2f",
			cand, maxStrikeRetryDistancePct, r.undPrice), false
	}
	r.attempts++
	r.tried = append(r.tried, cand)
	return cand, "", true
}

// deltaCandidate tracks one strike subscription used during delta-based
// strike selection. Multiple candidates are subscribed simultaneously; the
// one closest to the target delta is promoted to a leg, the rest cancelled.
type deltaCandidate struct {
	selectorID int
	symbol     string
	right      string
	strike     float64
	expiry     string
	reqID      int64
	delta      float64
	bid        float64
	ask        float64
	ready      bool
	busIdxs    []int

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
	// entryFailNoChain — no resolution group for this subscriber, or no
	// cached chain/strikes yet. Normal in the first seconds after connect.
	entryFailNoChain = "option_no_chain"
	// entryFailProbeCooldown — inside entryProbeLaunchCooldown from the last
	// launch for this group+right.
	entryFailProbeCooldown = "option_probe_cooldown"
	// entryFailDeltaTargetATM — target_delta sits in the refused ATM band.
	entryFailDeltaTargetATM = "option_delta_target_atm"
	// entryFailNoCandidates — selectITMCandidates found no strike to probe.
	entryFailNoCandidates = "option_no_candidates"
	// entryFailNoMDLines — every GrantProbe was refused; the market-data line
	// budget is exhausted and nothing was preemptible.
	entryFailNoMDLines = "option_no_md_lines"
	// entryFailSiblingFailed — joined an in-flight sibling probe that failed
	// without leaving a recorded cause behind.
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

// deltaResolution groups the pending candidates for one selector so they can
// be resolved together once deltas arrive.
type deltaResolution struct {
	selectorID  int
	symbol      string
	right       string
	targetDelta float64
	expiry      string
	candidates  []*deltaCandidate
	allStrikes  []float64
	busIdxs     []int
}

// optionChainTracker holds all state for option chain lookup and market data.
type optionChainTracker struct {
	mu          sync.Mutex
	nextConIDID int64
	nextChainID int64
	nextMktID   int64
	nextPosID   int64
	nextSelID   int
	conIDReqs   map[int64]*optConIDReq
	chainReqs   map[int64]*optChainReq

	// legs is the one registry of subscribed option contracts, background and
	// position-pinned alike, keyed by contract and refcounted across holders.
	// legByReqID is its reverse index for the IB callbacks, which only ever
	// carry a reqID.
	legs       map[legKey]*optLeg
	legByReqID map[int64]legKey

	// selCurrent is the contract each selector currently displays; selPending
	// is an in-flight move onto a different one. A selector with neither has
	// no leg yet.
	selCurrent map[int]legKey
	selPending map[int]pendingSwap

	retries      map[int]*optStrikeRetry
	deltaCands   map[int64]*deltaCandidate
	deltaRes     map[int]*deltaResolution
	rotation     []selector
	rotateCursor int

	// selectorIDs maps a selector's configuration tuple to the id assigned the
	// first time it was seen, so rebuilding the rotation (which ResyncSymbols
	// does after every watchlist edit) preserves the identity of every selector
	// that did not change. See buildSelectors for why renumbering would be
	// destructive.
	selectorIDs map[selectorKey]int

	// lastIV remembers the most recent implied-volatility sample seen for
	// each underlying symbol. Used by approximateStrikeForDelta as the best
	// available IV estimate when a delta probe fails.
	lastIV map[string]float64

	// lastChainInfo caches the resolved (expiry, SMART strike universe) per
	// (symbol, optionDelay). ResolveEntryStrike reads it so an entry-time delta
	// probe can subscribe candidates immediately instead of repeating the conId
	// + chain-params round trip synchronously, and the rotation reads it so a
	// selector whose sibling right just fetched the chain re-selects its strike
	// from cache instead of paying for the same round trip again.
	lastChainInfo map[chainKey]chainSnapshot

	// resolvedEntry caches the contract ResolveEntryStrike most recently
	// resolved for each selector, so a DIFFERENT subscriber sharing that
	// selector within resolvedEntryTTL reuses the identical strike instead of
	// running its own probe.
	resolvedEntry map[int]resolvedEntryLeg

	// lastEntryFailure caches the classified cause of the most recent failed
	// entry probe per selector, the failure-side counterpart to resolvedEntry.
	// It exists so a subscriber that joined an in-flight sibling's probe
	// (waitForEntryResolution) reports the sibling's REAL cause — "not
	// subscribed", say — instead of the useless "the other robot failed too",
	// which would defeat the whole point of classifying at all for whichever
	// robot happened to arrive second.
	lastEntryFailure map[int]EntryStrikeResult

	// lastProbeLaunch records when ResolveEntryStrike last became the OWNER
	// of a fresh set of delta-candidate probes for a selector — i.e. launched
	// new ReqMktData calls, as opposed to reusing sharedResolvedEntry or
	// joining an in-flight sibling via waitForEntryResolution (neither of
	// which costs a new IB round trip and so neither is throttled by this).
	// Scoped per selector rather than per caller so it protects the
	// configuration as a whole regardless of which subscriber last launched —
	// a single robot re-polling a stuck symbol every 5s and two robots each
	// independently probing the same contract are the same failure mode from
	// IB's perspective.
	lastProbeLaunch map[int]time.Time

	// lastAttempt records when the rotation last committed to resolving a
	// selector — i.e. resolveSelector got past the selectorResolvingLocked
	// guard and actually did the work, as opposed to being skipped because a
	// resolution was already in flight. Read by selectorLastServicedLocked as
	// the rotation score (see that function for why it must NOT be quote
	// freshness). Keyed separately from any per-leg clock because it must
	// persist even across a "skip — estimate strike unchanged" cycle, which
	// creates no new leg at all.
	lastAttempt map[int]time.Time

	// lastAnyOptionTick is the most recent moment ANY option reqID received
	// a message from IB. It is what separates "this one leg died" from "the
	// whole option feed is quiet" — off-RTH, a halt, a broker outage. Judging
	// a leg dead only while its peers are demonstrably alive collapses all of
	// those false-positive cases into a single rule, and stops a market-wide
	// lull from condemning every leg at once and triggering a re-subscribe
	// storm against a broker that is not answering anyway.
	lastAnyOptionTick time.Time

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

// resolvedEntryLeg is one selector's most recently resolved entry contract
// (strike + expiry + the delta that won it), with the wall-clock time it
// was resolved.
type resolvedEntryLeg struct {
	strike float64
	expiry string
	delta  float64
	at     time.Time
}

// chainSnapshot is one (symbol, optionDelay)'s cached expiry + SMART strike
// universe, with the wall-clock time the chain-params round trip returned it.
type chainSnapshot struct {
	expiry  string
	strikes []float64
	at      time.Time
}

// chainSnapshotTTL bounds how long a cached chain may be re-used before the
// rotation pays for another conId + chain-params round trip.
//
// A chain's expirations and SMART strike universe are near-static intraday —
// the expiry list rolls once a day, and strikes are only added as the
// underlying moves far enough to need them. What actually needs refreshing on
// the rotation is the STRIKE SELECTION, which is pure computation over the
// cached list plus a live underlying price. Re-fetching the chain to recompute
// it, as every rotation tick used to, spent an IB round trip per tick to
// re-learn a list that had not changed.
//
// Caching also keeps the rotation's cost per selector flat now that calls and
// puts are separate selectors: the underlying's two rights share one chain, so
// splitting them roughly doubled the rotation's length without this.
const chainSnapshotTTL = 5 * time.Minute

// resolvedEntryTTL bounds how long a resolved entry contract is shared
// across subscribers in a group.
const resolvedEntryTTL = 5 * time.Second

// entryProbeLaunchCooldown bounds how often ResolveEntryStrike may become the
// OWNER of a fresh delta-candidate probe for a given "groupID|right" — i.e.
// actually launch new ReqMktData calls. It does not gate sharedResolvedEntry
// reads or joining an already-in-flight sibling probe, since neither of those
// paths issues a new IB request.
const entryProbeLaunchCooldown = 15 * time.Second

// requestOptionChains resolves every selector across all subscribers once, at
// connect. option_delay and target_delta are configured per SymbolSpec, so the
// same underlying may need a different expiry/strike for each subscriber and
// for each right; subscribers configuring an identical (symbol, right, delay,
// δ) tuple share one selector, and any two selectors landing on the same
// contract share one IB feed.
func (s *Session) requestOptionChains() {
	s.buildSelectors()

	s.optChain.mu.Lock()
	sels := append([]selector(nil), s.optChain.rotation...)
	s.optChain.mu.Unlock()

	for _, sel := range sels {
		s.resolveSelector(sel)
	}
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
// Selector IDs are keyed by configuration, not by position in the rotation,
// and are remembered for the life of the session. That matters because a
// rebuild is not a once-per-session event: ResyncSymbols rebuilds after every
// watchlist edit. Renumbering would invalidate everything keyed by selector id
// — the resolvedEntry share cache, the rotation's lastAttempt scores, the
// in-flight-resolution guard — so an unrelated edit would silently re-probe
// everything at once. With stable IDs an untouched selector is bit-identical
// across a rebuild, and only genuinely new configurations are returned.
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
	s.optChain.rotation = s.optChain.rotation[:0]
	for _, key := range order {
		sel := sels[key]
		id, known := s.optChain.selectorIDs[key]
		if !known {
			id = s.optChain.nextSelID
			s.optChain.nextSelID++
			s.optChain.selectorIDs[key] = id
		}
		sel.id = id
		s.optChain.rotation = append(s.optChain.rotation, *sel)
		if !known {
			fresh = append(fresh, *sel)
		}
	}
	// Reset the cursor only on the first build. Later rebuilds keep it so a
	// watchlist edit does not restart the rotation from the top and re-service
	// selectors that were just serviced.
	if s.optChain.rotateCursor >= len(s.optChain.rotation) {
		s.optChain.rotateCursor = 0
	}
	s.optChain.mu.Unlock()
	return fresh
}

// resolveSelector re-selects the contract one selector should track. It uses
// the cached chain when there is a fresh one for this (symbol, optionDelay) —
// selecting a strike is pure computation over the strike list and a live
// underlying price — and otherwise fires the conId lookup that leads to one.
// Skips when a prior resolution for the same selector is still in flight.
func (s *Session) resolveSelector(sel selector) {
	now := time.Now()

	s.optChain.mu.Lock()
	if s.selectorResolvingLocked(sel.id) {
		s.optChain.mu.Unlock()
		s.optionLog.Printf("Option: skipping strike refresh for %s %s (sel=%d) — previous resolution still in flight", sel.symbol, sel.right, sel.id)
		return
	}
	s.optChain.lastAttempt[sel.id] = now
	snap, cached := s.optChain.lastChainInfo[sel.chainKey()]
	if cached && now.Sub(snap.at) > chainSnapshotTTL {
		cached = false
	}
	if cached {
		s.optChain.mu.Unlock()
		s.selectStrike(sel, snap)
		return
	}

	// No usable chain. If another selector on the same underlying+delay is
	// already fetching one, join its waiter list rather than duplicating the
	// round trip — this is what keeps a call and a put on one underlying
	// costing a single chain lookup between them.
	if reqID, ok := s.chainRequestInFlightLocked(sel.chainKey()); ok {
		s.addChainWaiterLocked(reqID, sel.id)
		s.optChain.mu.Unlock()
		s.optionLog.Printf("Option: %s %s (sel=%d) — joining in-flight chain lookup for %s delay=%d",
			sel.symbol, sel.right, sel.id, sel.symbol, sel.optionDelay)
		return
	}

	reqID := s.optChain.nextConIDID
	s.optChain.nextConIDID++
	s.optChain.conIDReqs[reqID] = &optConIDReq{
		chain: sel.chainKey(), currency: sel.currency,
		waiters: []int{sel.id}, requestedAt: now,
	}
	s.optChain.mu.Unlock()

	contract := &ibapi.Contract{Symbol: sel.symbol, SecType: "STK", Currency: sel.currency, Exchange: "SMART"}
	s.optionLog.Printf("Option: resolving conId for %s (sel=%d, reqID=%d, right=%s, delay=%d, targetDelta=%.2f, subs=%v)",
		sel.symbol, sel.id, reqID, sel.right, sel.optionDelay, sel.targetDelta, sel.busIdxs)
	s.client.ReqContractDetails(reqID, contract)
}

// chainRequestInFlightLocked reports the reqID of a pending chain lookup for
// key, at either of its two phases. Caller holds s.optChain.mu.
func (s *Session) chainRequestInFlightLocked(key chainKey) (int64, bool) {
	for reqID, r := range s.optChain.conIDReqs {
		if r.chain == key {
			return reqID, true
		}
	}
	for reqID, r := range s.optChain.chainReqs {
		if r.chain == key {
			return reqID, true
		}
	}
	return 0, false
}

// addChainWaiterLocked registers selectorID as needing the result of the
// pending chain lookup reqID. Caller holds s.optChain.mu.
func (s *Session) addChainWaiterLocked(reqID int64, selectorID int) {
	if r, ok := s.optChain.conIDReqs[reqID]; ok && !slices.Contains(r.waiters, selectorID) {
		r.waiters = append(r.waiters, selectorID)
	}
	if r, ok := s.optChain.chainReqs[reqID]; ok && !slices.Contains(r.waiters, selectorID) {
		r.waiters = append(r.waiters, selectorID)
	}
}

// selectorByIDLocked returns the current rotation entry for id. Caller holds
// s.optChain.mu.
func (s *Session) selectorByIDLocked(id int) (selector, bool) {
	for _, sel := range s.optChain.rotation {
		if sel.id == id {
			return sel, true
		}
	}
	return selector{}, false
}

// rotateOptionStrikes re-resolves the next selector in the rotation and
// advances the cursor. Driven by a steady ticker so strike renewal is a
// permanent slow rotation rather than a synchronized burst.
func (s *Session) rotateOptionStrikes() {
	s.reapStuckChainRequests()
	s.reapDeadOptionLegs()
	s.optChain.mu.Lock()
	sel, ok := s.pickRotationSelectorLocked()
	s.optChain.mu.Unlock()
	if !ok {
		return
	}
	s.resolveSelector(sel)
}

// chainResolutionMaxAge bounds how long a conId lookup or chain-params
// request may stay pending before reapStuckChainRequests gives up on it and
// frees its waiting selectors for another attempt. A healthy round trip
// completes in low single-digit seconds; this is a generous multiple of that
// so it only fires when IB never responded at all — most likely a pacing
// rejection from firing every configured selector's initial resolution nearly
// simultaneously at startup (requestOptionChains), which produces neither a
// success nor an error callback, so nothing else ever clears the entry.
// Without this, those selectors' "resolving" flag never clears, permanently
// excluding them from pickRotationSelectorLocked's oldest-first pick and
// leaving them stuck at zero background lines for the rest of the session —
// while the handful that did resolve become the only eligible candidates and
// get endlessly re-picked instead.
const chainResolutionMaxAge = 20 * time.Second

// reapStuckChainRequests frees any conId-lookup or chain-params request
// older than chainResolutionMaxAge whose IB response never arrived, so the
// next rotateOptionStrikes tick can retry instead of leaving its selectors
// stuck "resolving" forever. Logs each one loudly — this indicates a
// request IB silently dropped, not routine behavior.
func (s *Session) reapStuckChainRequests() {
	now := time.Now()
	s.optChain.mu.Lock()
	var stuck []string
	for reqID, req := range s.optChain.conIDReqs {
		if now.Sub(req.requestedAt) < chainResolutionMaxAge {
			continue
		}
		stuck = append(stuck, fmt.Sprintf("%s (conId lookup, reqID=%d, sels=%v)", req.chain.symbol, reqID, req.waiters))
		delete(s.optChain.conIDReqs, reqID)
	}
	for reqID, req := range s.optChain.chainReqs {
		if now.Sub(req.requestedAt) < chainResolutionMaxAge {
			continue
		}
		stuck = append(stuck, fmt.Sprintf("%s (chain params, reqID=%d, sels=%v)", req.chain.symbol, reqID, req.waiters))
		delete(s.optChain.chainReqs, reqID)
	}
	s.optChain.mu.Unlock()
	for _, desc := range stuck {
		s.optionLog.Printf("Option: WARNING reaped stuck resolution for %s — IB never responded within %s, freeing its selectors for retry", desc, chainResolutionMaxAge)
	}
}

// pickRotationSelectorLocked chooses the next selector to refresh, oldest
// data first. Must be called with s.optChain.mu held.
func (s *Session) pickRotationSelectorLocked() (selector, bool) {
	n := len(s.optChain.rotation)
	if n == 0 {
		return selector{}, false
	}
	// Scanning starts at rotateCursor (wrapping) rather than always at index
	// 0. selectorLastServicedLocked returns the zero time.Time for a selector
	// that has never been resolved, so a large batch of them — the common
	// startup case — all tie at the same score. score.Before(bestScore) is
	// strict, so a tie never replaces the current best; starting the scan at
	// index 0 every time therefore let entry 0 win every single tie forever,
	// starving every other selector's very first resolution attempt for the
	// rest of the session (the 2026-07-28 stuck-first-quote investigation: one
	// group got re-picked every 3s tick while dozens of others never got a
	// turn). Starting from the cursor and always advancing it past whichever
	// entry is returned makes ties resolve in fair round-robin order while a
	// genuinely staler selector (a strictly earlier real timestamp, found
	// anywhere in the scan) still wins over one that's merely next in line.
	var best selector
	var bestIdx int
	var bestScore time.Time
	found := false
	for i := range n {
		idx := (s.optChain.rotateCursor + i) % n
		sel := s.optChain.rotation[idx]
		if s.selectorResolvingLocked(sel.id) {
			continue
		}
		score := s.selectorLastServicedLocked(sel)
		if !found || score.Before(bestScore) {
			found, best, bestIdx, bestScore = true, sel, idx, score
		}
	}
	if !found {
		return selector{}, false
	}
	s.optChain.rotateCursor = (bestIdx + 1) % n
	return best, true
}

// selectorLastServicedLocked returns when the rotation last actually resolved
// this selector — the zero time.Time if it never has, so a brand-new selector
// is served first. Must be called with s.optChain.mu held.
//
// This is deliberately NOT scored on quote freshness. It used to be, and that
// was backwards: the rotation exists to re-estimate a strike as the underlying
// drifts, a need driven by how long it has been since it was last serviced,
// not by whether its leg happens to be quoting. Scoring on quote time made
// "has fresh data" — evidence of health — count as evidence of being up to
// date, so any continuously-quoting entry scored ~now and lost to every entry
// serviced even a fraction of a second earlier, permanently. On 2026-08-03
// that gave SPY, QQQ and IWM (the three most liquid underlyings, 6-15 delta
// misses each) zero rotation picks in two hours while NVDA (532 misses, i.e.
// the one least able to obtain a quote) consumed 794 log lines re-estimating
// futilely. The correlation was exact and inverted: the better the data, the
// less often it was refreshed.
//
// Keying on lastAttempt, which is recorded whether or not any leg results,
// also makes a legless selector age like any other — a state reachable when
// handleOptionMktError drops a leg on error 200 with no retry record left.
func (s *Session) selectorLastServicedLocked(sel selector) time.Time {
	return s.optChain.lastAttempt[sel.id]
}

// selectorResolvingLocked reports whether a resolution for selectorID is
// still in flight. Caller must hold s.optChain.mu.
func (s *Session) selectorResolvingLocked(selectorID int) bool {
	for _, r := range s.optChain.conIDReqs {
		if slices.Contains(r.waiters, selectorID) {
			return true
		}
	}
	for _, r := range s.optChain.chainReqs {
		if slices.Contains(r.waiters, selectorID) {
			return true
		}
	}
	_, probing := s.optChain.deltaRes[selectorID]
	return probing
}

// handleConIDContractDetails stores the first conId received for an option
// conId-lookup request. Returns true if reqID belongs to this phase.
func (s *Session) handleConIDContractDetails(reqID int64, contractDetails *ibapi.ContractDetails) bool {
	s.optChain.mu.Lock()
	req, ok := s.optChain.conIDReqs[reqID]
	if !ok {
		s.optChain.mu.Unlock()
		return false
	}
	if req.conID == 0 && contractDetails != nil {
		req.conID = contractDetails.Contract.ConID
		s.optionLog.Printf("Option: conId for %s = %d", req.chain.symbol, req.conID)
	}
	s.optChain.mu.Unlock()
	return true
}

// handleConIDContractDetailsEnd fires ReqSecDefOptParams with the resolved
// conId. Returns true if handled.
func (s *Session) handleConIDContractDetailsEnd(reqID int64) bool {
	s.optChain.mu.Lock()
	req, ok := s.optChain.conIDReqs[reqID]
	if !ok {
		s.optChain.mu.Unlock()
		return false
	}
	delete(s.optChain.conIDReqs, reqID)
	symbol := req.chain.symbol
	conID := req.conID

	if conID == 0 {
		s.optChain.mu.Unlock()
		s.optionLog.Printf("Option: could not resolve conId for %s, skipping chain lookup", symbol)
		return true
	}

	chainReqID := s.optChain.nextChainID
	s.optChain.nextChainID++
	s.optChain.chainReqs[chainReqID] = &optChainReq{
		chain: req.chain, waiters: req.waiters, requestedAt: time.Now(),
	}
	s.optChain.mu.Unlock()

	s.optionLog.Printf("Option: requesting chain params for %s conId=%d (reqID=%d)", symbol, conID, chainReqID)
	s.client.ReqSecDefOptParams(chainReqID, symbol, "", "STK", conID)
	return true
}

// SecurityDefinitionOptionParameter accumulates exchange callbacks for one
// reqSecDefOptParams call. Only the SMART exchange response is kept — it
// contains exactly the strikes routable via SMART for market data and orders —
// and each callback is filed under its own trading class, never merged across
// classes (see optChainReq).
func (s *Session) SecurityDefinitionOptionParameter(reqID int64, exchange string, underlyingConID int64, tradingClass string, multiplier string, expirations []string, strikes []float64) {
	if s.handleOptionQuerySecDefOptParams(reqID, exchange, tradingClass, multiplier, expirations, strikes) {
		return
	}
	if exchange != "SMART" {
		return
	}

	s.optChain.mu.Lock()
	req, ok := s.optChain.chainReqs[reqID]
	if !ok {
		s.optChain.mu.Unlock()
		return
	}
	if req.classes == nil {
		req.classes = make(map[string]*chainClass)
	}
	cls, ok := req.classes[tradingClass]
	if !ok {
		cls = &chainClass{tradingClass: tradingClass, multiplier: multiplier}
		req.classes[tradingClass] = cls
	}
	cls.mergeChainParams(expirations, strikes)
	s.optChain.mu.Unlock()
}

// SecurityDefinitionOptionParameterEnd fires when all SMART exchange
// callbacks for one reqSecDefOptParams call have been delivered. Picks the
// nearest expiry and ATM/target-delta strike, stores a sorted retry list in
// case error 200 forces a fallback, then subscribes market data.
func (s *Session) SecurityDefinitionOptionParameterEnd(reqID int64) {
	if s.handleOptionQuerySecDefOptParamsEnd(reqID) {
		return
	}
	s.optChain.mu.Lock()
	req, ok := s.optChain.chainReqs[reqID]
	if !ok {
		s.optChain.mu.Unlock()
		return
	}
	delete(s.optChain.chainReqs, reqID)
	chain := req.chain
	waiters := append([]int(nil), req.waiters...)
	symbol := chain.symbol
	chosen, ignored := pickChainClass(symbol, req.classes)
	s.optChain.mu.Unlock()

	if chosen == nil {
		s.optionLog.Printf("Option chain end: %s delay=%d (sels=%v) — no usable SMART trading class (saw: %s)",
			symbol, chain.optionDelay, waiters, describeChainClasses(ignored))
		s.logger.Printf("Option chain: %s — no SMART strikes/expirations found, skipping", symbol)
		return
	}

	expirations := append([]string(nil), chosen.expirations...)
	strikes := append([]float64(nil), chosen.strikes...)

	s.optionLog.Printf("Option chain end: %s delay=%d (sels=%v) — class=%s mult=%s: %d expirations, %d strikes (ignored: %s)",
		symbol, chain.optionDelay, waiters, chosen.tradingClass, chosen.multiplier,
		len(expirations), len(strikes), describeChainClasses(ignored))

	if chosen.multiplier != standardMultiplier {
		s.logger.Printf("Option chain: %s — no standard (multiplier %s) SMART trading class; selecting strikes on %s mult=%s instead",
			symbol, standardMultiplier, chosen.tradingClass, chosen.multiplier)
	}

	if filtered := wholeDollarStrikes(strikes); len(filtered) < len(strikes) {
		s.optionLog.Printf("Option chain: %s — filtered %d fractional strikes, %d whole-dollar strikes remain",
			symbol, len(strikes)-len(filtered), len(filtered))
		strikes = filtered
	}

	expiry := nearestExpiry(expirations, chain.optionDelay)
	if expiry == "" {
		s.optionLog.Printf("Option chain: %s — no current or future expirations found", symbol)
		return
	}

	undPrice := s.getUnderlyingPrice(symbol)
	if undPrice <= 0 {
		s.optionLog.Printf("Option chain: %s — underlying price not yet known, using median strike", symbol)
		sort.Float64s(strikes)
		undPrice = strikes[len(strikes)/2]
	}

	sort.Slice(strikes, func(i, j int) bool {
		return math.Abs(strikes[i]-undPrice) < math.Abs(strikes[j]-undPrice)
	})

	snap := chainSnapshot{expiry: expiry, strikes: append([]float64(nil), strikes...), at: time.Now()}

	s.optChain.mu.Lock()
	s.optChain.lastChainInfo[chain] = snap
	sels := make([]selector, 0, len(waiters))
	for _, id := range waiters {
		if sel, ok := s.selectorByIDLocked(id); ok {
			sels = append(sels, sel)
		}
	}
	s.optChain.mu.Unlock()

	// Every selector that was waiting on this chain gets served from it — the
	// round trip they shared is exactly why they queued together.
	for _, sel := range sels {
		s.selectStrike(sel, snap)
	}
}

// isATMDelta reports whether a target delta sits in the band where the library
// refuses delta-based selection and simply takes the nearest strike.
func isATMDelta(td float64) bool { return td >= 0.48 && td <= 0.52 }

// selectStrike picks the contract one selector should track from a resolved
// chain and subscribes it. Pure computation plus one possible subscription —
// no IB chain round trip — which is what lets the rotation re-select a strike
// from a cached chain.
func (s *Session) selectStrike(sel selector, snap chainSnapshot) {
	if len(snap.strikes) == 0 {
		return
	}
	undPrice := s.getUnderlyingPrice(sel.symbol)
	if undPrice <= 0 {
		undPrice = snap.strikes[len(snap.strikes)/2]
	}

	setRetry := func(expiry string) {
		s.optChain.mu.Lock()
		s.optChain.retries[sel.id] = &optStrikeRetry{
			selectorID: sel.id, symbol: sel.symbol, right: sel.right, expiry: expiry,
			strikes: snap.strikes, nextIdx: 1, undPrice: undPrice, busIdxs: sel.busIdxs,
		}
		s.optChain.mu.Unlock()
	}

	if isATMDelta(sel.targetDelta) {
		strike := snap.strikes[0]
		s.optionLog.Printf("Option chain resolved: %s %s (sel=%d) expiry=%s strike=%.2f (underlying≈%.2f, ATM, subs=%v)",
			sel.symbol, sel.right, sel.id, snap.expiry, strike, undPrice, sel.busIdxs)
		s.pointSelectorAt(sel, legKey{sel.symbol, sel.right, strike, snap.expiry}, false)
		setRetry(snap.expiry)
		return
	}

	s.optChain.mu.Lock()
	skip, skipReason := s.shouldSkipReEstimateLocked(sel, time.Now())
	s.optChain.mu.Unlock()
	if skip {
		s.optionLog.Printf("Option: skipping strike re-estimate %s %s (sel=%d) — %s", sel.symbol, sel.right, sel.id, skipReason)
		return
	}
	if skipReason != "" {
		s.optionLog.Printf("Option: WARNING forcing re-estimate %s %s (sel=%d) — %s", sel.symbol, sel.right, sel.id, skipReason)
	}

	s.optChain.mu.Lock()
	iv := s.optChain.lastIV[sel.symbol]
	s.optChain.mu.Unlock()
	if iv <= 0 {
		iv = defaultFallbackIV
	}
	strike := approximateStrikeForDelta(snap.strikes, undPrice, iv, sel.targetDelta, snap.expiry, sel.right)
	s.optionLog.Printf("Option chain: %s %s (sel=%d) — estimating strike=%.2f via target delta %.2f (iv=%.4f)",
		sel.symbol, sel.right, sel.id, strike, sel.targetDelta, iv)
	s.logDeltaMiss(sel.symbol, sel.right, sel.targetDelta, undPrice, snap.strikes, "estimate_default", strike, iv)
	if strike > 0 {
		s.pointSelectorAt(sel, legKey{sel.symbol, sel.right, strike, snap.expiry}, true)
		setRetry(snap.expiry)
	}
}

// selectITMCandidates picks up to n strikes on the ITM side of the
// underlying. For calls, ITM = strikes below undPrice. For puts, ITM =
// strikes above undPrice. Returns strikes sorted from nearest-ATM to
// deepest-ITM.
func selectITMCandidates(sortedStrikes []float64, undPrice float64, right string, n int) []float64 {
	sorted := make([]float64, len(sortedStrikes))
	copy(sorted, sortedStrikes)
	sort.Float64s(sorted)

	var candidates []float64
	if right == "call" {
		for i := len(sorted) - 1; i >= 0; i-- {
			if sorted[i] < undPrice {
				candidates = append(candidates, sorted[i])
				if len(candidates) >= n {
					break
				}
			}
		}
	} else {
		for _, st := range sorted {
			if st > undPrice {
				candidates = append(candidates, st)
				if len(candidates) >= n {
					break
				}
			}
		}
	}
	return candidates
}

// ── Leg registry ──────────────────────────────────────────────────────────
//
// One leg per contract, refcounted across holders. attach/detach are the only
// ways in and out; nothing else may delete a leg, because "is anyone else
// still using this?" is the question the old (symbol, right) ownership model
// could not ask.

// attachSelectorLocked records that a selector holds a contract. Caller holds
// s.optChain.mu.
func (s *Session) attachSelectorLocked(key legKey, selectorID int) {
	leg, ok := s.optChain.legs[key]
	if !ok {
		return
	}
	if leg.selectors == nil {
		leg.selectors = make(map[int]struct{})
	}
	leg.selectors[selectorID] = struct{}{}
	s.mdLines.Reclassify(leg.reqID, legCategory(leg))
}

// detachSelectorLocked drops a selector's hold on a contract and returns the
// reqID the caller must cancel at IB, or 0 when other holders remain. Caller
// holds s.optChain.mu.
func (s *Session) detachSelectorLocked(key legKey, selectorID int) int64 {
	leg, ok := s.optChain.legs[key]
	if !ok {
		return 0
	}
	delete(leg.selectors, selectorID)
	return s.releaseLegIfUnheldLocked(leg)
}

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
	s.mdLines.Release(leg.reqID)
	return leg.reqID
}

// legCategory is a leg's market-data priority: the highest any of its holders
// warrants. An open position makes the line guaranteed — a held contract must
// never lose its feed to line pressure, whether or not a watchlist row happens
// to point at the same strike.
func legCategory(leg *optLeg) mdlines.Category {
	if leg.pins > 0 {
		return mdlines.CategoryPosition
	}
	return mdlines.CategoryDiscretionaryNew
}

// legDisplayBusesLocked returns the subscriber buses that must receive this
// contract's option data: those of every selector currently DISPLAYING it. A
// selector merely warming into it (pendingSwap) is excluded — publishing to it
// early is exactly what the pending state exists to prevent, since the row
// would jump to a contract that has no quote yet. Caller holds s.optChain.mu.
func (s *Session) legDisplayBusesLocked(key legKey) []int {
	leg, ok := s.optChain.legs[key]
	if !ok {
		return nil
	}
	var out []int
	for selID := range leg.selectors {
		if s.optChain.selCurrent[selID] != key {
			continue
		}
		sel, ok := s.selectorByIDLocked(selID)
		if !ok {
			continue
		}
		for _, b := range sel.busIdxs {
			if !slices.Contains(out, b) {
				out = append(out, b)
			}
		}
	}
	sort.Ints(out) // map iteration is random; publish order must not be
	return out
}

// selectorLegLocked returns the leg a selector currently displays. Caller
// holds s.optChain.mu.
func (s *Session) selectorLegLocked(selectorID int) (*optLeg, bool) {
	key, ok := s.optChain.selCurrent[selectorID]
	if !ok {
		return nil, false
	}
	leg, ok := s.optChain.legs[key]
	return leg, ok
}

// nearestLegForLocked returns an existing leg for want's (symbol, right) to
// fall back on when a line cannot be granted — preferring one that actually
// quotes, then the closest strike to what was wanted. Deterministic despite
// random map order, so a fallback is reproducible. Caller holds s.optChain.mu.
func (s *Session) nearestLegForLocked(want legKey) (*optLeg, bool) {
	var best *optLeg
	for _, leg := range s.optChain.legs {
		if leg.symbol != want.symbol || leg.right != want.right {
			continue
		}
		if best == nil {
			best = leg
			continue
		}
		if best.quoteComplete() != leg.quoteComplete() {
			if leg.quoteComplete() {
				best = leg
			}
			continue
		}
		bd, ld := math.Abs(best.strike-want.strike), math.Abs(leg.strike-want.strike)
		if ld < bd || (ld == bd && leg.strike < best.strike) {
			best = leg
		}
	}
	return best, best != nil
}

// promotePendingLocked promotes a selector's warming leg to displayed once it
// carries a complete two-sided quote — or, after pendingPromoteGrace, on any
// usable price. Returns the reqID of the leg it replaced, when that leg lost
// its last holder and must be cancelled. Caller holds s.optChain.mu.
func (s *Session) promotePendingLocked(selectorID int, now time.Time) (cancel int64, promoted bool) {
	sw, waiting := s.optChain.selPending[selectorID]
	if !waiting {
		return 0, false
	}
	leg, ok := s.optChain.legs[sw.to]
	if !ok {
		delete(s.optChain.selPending, selectorID) // target vanished; stay put
		return 0, false
	}
	usable := leg.price > 0 || leg.bid > 0 || leg.ask > 0
	if !leg.quoteComplete() && !(usable && now.Sub(sw.since) > pendingPromoteGrace) {
		return 0, false
	}
	delete(s.optChain.selPending, selectorID)
	old, hadOld := s.optChain.selCurrent[selectorID]
	s.optChain.selCurrent[selectorID] = sw.to
	if hadOld && old != sw.to {
		return s.detachSelectorLocked(old, selectorID), true
	}
	return 0, true
}

// promoteWaitersLocked promotes every selector warming into key that is now
// ready, returning the reqIDs to cancel. Caller holds s.optChain.mu.
func (s *Session) promoteWaitersLocked(key legKey, now time.Time) []int64 {
	leg, ok := s.optChain.legs[key]
	if !ok {
		return nil
	}
	var cancel []int64
	for selID := range leg.selectors {
		if sw, waiting := s.optChain.selPending[selID]; !waiting || sw.to != key {
			continue
		}
		if id, ok := s.promotePendingLocked(selID, now); ok && id != 0 {
			cancel = append(cancel, id)
		}
	}
	return cancel
}

// shouldSkipReEstimateLocked reports whether the leg this selector already
// displays is close enough to its target delta that re-estimating the strike
// would be wasted work. reason is a human-readable explanation for the
// caller's log line — non-empty on a forced re-estimate too, so the operator
// can see WHY a leg that looks fine on paper is being refreshed anyway.
// Must be called with s.optChain.mu held.
//
// It reads THIS SELECTOR's own leg, never whatever leg happens to exist for
// the (symbol, right). Under the old ownership model a selector could skip
// because a sibling's leg looked healthy — while its own buses were attached
// to nothing — so the skip actively guaranteed the row would stay blank. That
// is one of the three mechanisms that blanked QQQ on 2026-08-13.
//
// The freshness test is the other half. leg.delta is a CACHED field, written
// only when IB delivers a greeks tick, so it freezes at its last value the
// instant a leg goes dead — and a frozen delta near the target is
// indistinguishable from a live one. Before this check the frozen value was
// what justified skipping the refresh, so a dead leg permanently talked the
// rotation out of repairing it: the 2026-08-03 incident, where QQQ's put froze
// at δ -0.5672 against a 0.60 target (drift 0.033, inside the 0.05 tolerance)
// and skipped re-estimation for the rest of the session while its quote aged
// past two hours. The old log line called it "live delta", which is precisely
// the false claim that hid the bug.
func (s *Session) shouldSkipReEstimateLocked(sel selector, now time.Time) (skip bool, reason string) {
	// A move onto a different contract is already under way; leave it alone
	// rather than starting a second one on top of it.
	if _, waiting := s.optChain.selPending[sel.id]; waiting {
		return true, "a strike change for this selector is already in flight"
	}
	leg, has := s.selectorLegLocked(sel.id)
	if !has {
		return false, ""
	}
	if leg.deltaSource != "matched" {
		return false, ""
	}
	if math.Abs(math.Abs(leg.delta)-sel.targetDelta) > deltaDriftTolerance {
		return false, ""
	}

	health := legHealthAt(leg.subscribedAt, leg.lastTickAt, s.optChain.lastAnyOptionTick, now)
	if health == legHealthy || health == legWarming {
		return true, fmt.Sprintf("live delta %.4f (last tick %s ago) still within %.2f of target %.2f",
			leg.delta, legAgeString(leg.lastTickAt, now), deltaDriftTolerance, sel.targetDelta)
	}
	return false, fmt.Sprintf("cached delta %.4f is STALE (last tick %s ago, health=%s) — refusing to trust it",
		leg.delta, legAgeString(leg.lastTickAt, now), health)
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
		deltaSource: deltaSource, selectors: make(map[int]struct{}), subscribedAt: now,
	}
	s.optChain.legs[key] = leg
	s.optChain.legByReqID[reqID] = key
	return leg
}

// pointSelectorAt moves one selector onto the contract it should be tracking.
//
// The four outcomes, in the order they are tried:
//
//  1. Already displaying it — nothing to do.
//  2. A leg for that exact contract already exists (another selector's, or an
//     open position's pin) — ATTACH to it. This costs no market-data line at
//     all, which is the whole point of keying legs by contract: two robots
//     that want the same strike want the same IB feed.
//  3. No leg yet — subscribe one, warming into it while the selector keeps
//     displaying whatever it had, so a strike roll never blanks the row.
//  4. The line budget refused (3) — attach to the nearest existing leg for
//     this (symbol, right) rather than returning empty-handed. A slightly-off
//     shared strike is strictly better than a blank row, and this is the
//     branch whose absence blanked QQQ for 41 minutes on 2026-08-13: the
//     refusal path was a bare `return` with no log line and no fallback.
//
// fallback marks a strike chosen without a genuine live delta match.
//
// Repairing a leg IB has silently stopped serving is NOT one of these
// outcomes: the contract is already right and only its subscription is dead,
// so that path is forceResubscribeLeg, which keeps every holder attached.
func (s *Session) pointSelectorAt(sel selector, want legKey, fallback bool) {
	deltaSource := "matched"
	if fallback {
		deltaSource = "atm_fallback"
	}
	now := time.Now()

	s.optChain.mu.Lock()

	// (1) already there.
	if cur, ok := s.optChain.selCurrent[sel.id]; ok && cur == want {
		delete(s.optChain.selPending, sel.id) // abandon any move away from it
		s.optChain.mu.Unlock()
		s.optionLog.Printf("Option: skipping background refresh %s %s (sel=%d) — estimate strike unchanged at %.2f",
			sel.symbol, sel.right, sel.id, want.strike)
		return
	}

	// (2) somebody already holds this contract — share their line.
	if _, exists := s.optChain.legs[want]; exists {
		od, cancel := s.adoptLegLocked(sel, want, now)
		s.optChain.mu.Unlock()
		s.cancelLines(cancel)
		s.optionLog.Printf("Option: %s %s (sel=%d) joining existing subscription at strike=%.2f expiry=%s (no new line)",
			sel.symbol, sel.right, sel.id, want.strike, want.expiry)
		s.publishOptionData(od)
		return
	}

	reqID := s.optChain.nextMktID
	s.optChain.nextMktID++
	cur, hadLeg := s.optChain.selCurrent[sel.id]
	// "Has a leg" and "has a quote" are different questions, and the tier below
	// turns on the second one.
	quoting := false
	if hadLeg {
		if leg, ok := s.optChain.legs[cur]; ok {
			quoting = leg.quoteComplete()
		}
	}
	s.optChain.mu.Unlock()

	// (3) a fresh contract needs a line. Replacing a leg this selector already
	// displays AND that is genuinely quoting is churn (the lowest tier — losing
	// the refresh costs nothing real). Anything else is a first-quote request:
	// a selector with no leg, and equally a selector whose leg has never
	// carried a two-sided quote, which is precisely what CategoryDiscretionaryNew
	// is defined to mean.
	//
	// Grading the second case as churn is what made 2026-08-17's MSFT permanent.
	// Its retry walk left it on a δ≈1.00 strike with no bid and no ask; because
	// that counted as "has a leg", every attempt to move back to the δ 0.55
	// strike asked for the churn tier, which is refused from 75/100 lines
	// upward. The account sat at 79. A row that has never had a usable quote
	// must not be starved by the tier meant to protect rows that already have one.
	var granted bool
	if quoting {
		granted = s.mdLines.GrantDiscretionaryChurn(reqID)
	} else {
		granted = s.mdLines.GrantDiscretionaryNew(reqID)
	}

	if !granted {
		// (4) refused. Never return blank: share whatever leg exists.
		used, max, _, _, _, _ := s.mdLines.StatusAll()
		s.optChain.mu.Lock()
		alt, haveAlt := s.nearestLegForLocked(want)
		if !haveAlt {
			s.optChain.mu.Unlock()
			s.optionLog.Printf("Option: WARNING %s %s (sel=%d) could not subscribe strike=%.2f — market-data lines %d/%d and no existing %s leg to share; this row has NO option data",
				sel.symbol, sel.right, sel.id, want.strike, used, max, sel.right)
			return
		}
		altKey := alt.key()
		od, cancel := s.adoptLegLocked(sel, altKey, now)
		s.optChain.mu.Unlock()
		s.cancelLines(cancel)
		s.optionLog.Printf("Option: WARNING %s %s (sel=%d) could not subscribe strike=%.2f — market-data lines %d/%d; sharing the existing strike=%.2f leg instead",
			sel.symbol, sel.right, sel.id, want.strike, used, max, altKey.strike)
		s.publishOptionData(od)
		return
	}

	s.optChain.mu.Lock()
	// The lock was released across the grant, so another goroutine — an entry
	// probe promoting this very contract, a sibling selector resolving onto it
	// — may have subscribed it meanwhile. Overwriting it here would orphan its
	// reqID: a live IB subscription with nothing left pointing at it, and a
	// ledger line nothing will ever release.
	if _, raced := s.optChain.legs[want]; raced {
		od, cancel := s.adoptLegLocked(sel, want, now)
		s.mdLines.Release(reqID)
		s.optChain.mu.Unlock()
		s.cancelLines(cancel)
		s.optionLog.Printf("Option: %s %s (sel=%d) joining strike=%.2f subscribed concurrently — giving back the line just granted",
			sel.symbol, sel.right, sel.id, want.strike)
		s.publishOptionData(od)
		return
	}

	leg := s.openLegLocked(want, reqID, deltaSource, now)
	leg.selectors[sel.id] = struct{}{}
	if hadLeg {
		// Keep showing the old contract until this one quotes.
		s.optChain.selPending[sel.id] = pendingSwap{to: want, since: now}
	} else {
		s.optChain.selCurrent[sel.id] = want
	}
	warming := hadLeg
	// Built under the lock: a tick may mutate this leg the instant it is
	// released.
	od := optionDataFor(leg, sel.busIdxs)
	s.optChain.mu.Unlock()

	s.optionLog.Printf("Option: subscribing market data %s %s (sel=%d) strike=%.2f expiry=%s (reqID=%d, warming=%v)",
		sel.symbol, sel.right, sel.id, want.strike, want.expiry, reqID, warming)
	ibRight := "C"
	if want.right == "put" {
		ibRight = "P"
	}
	s.client.ReqMktData(reqID, makeOptionContract(want.symbol, ibRight, want.strike, want.expiry), "", false, false, nil)

	if !warming {
		s.publishOptionData(od)
	}
}

// adoptLegLocked attaches a selector to an existing leg. When that leg already
// quotes the selector switches to it immediately (there is nothing to wait
// for); otherwise it warms into it, keeping its previous contract on screen.
// Returns the option data to publish (empty when nothing is displayable yet)
// and any reqID left unheld. Caller holds s.optChain.mu.
func (s *Session) adoptLegLocked(sel selector, key legKey, now time.Time) (optionDataPublish, []int64) {
	leg, ok := s.optChain.legs[key]
	if !ok {
		return optionDataPublish{}, nil
	}
	s.attachSelectorLocked(key, sel.id)

	cur, hadLeg := s.optChain.selCurrent[sel.id]
	if hadLeg && cur == key {
		delete(s.optChain.selPending, sel.id)
		return optionDataFor(leg, sel.busIdxs), nil
	}
	if !hadLeg || leg.quoteComplete() {
		delete(s.optChain.selPending, sel.id)
		s.optChain.selCurrent[sel.id] = key
		var cancel []int64
		if hadLeg {
			if id := s.detachSelectorLocked(cur, sel.id); id != 0 {
				cancel = append(cancel, id)
			}
		}
		return optionDataFor(leg, sel.busIdxs), cancel
	}
	s.optChain.selPending[sel.id] = pendingSwap{to: key, since: now}
	return optionDataPublish{}, nil
}

// optionDataPublish is one KindOptionData event plus its destination buses,
// built under the lock and published outside it.
type optionDataPublish struct {
	buses []int
	data  eventbus.OptionData
}

func optionDataFor(leg *optLeg, buses []int) optionDataPublish {
	if leg == nil || len(buses) == 0 {
		return optionDataPublish{}
	}
	return optionDataPublish{buses: buses, data: eventbus.OptionData{
		Symbol: leg.symbol, Right: leg.right, Strike: leg.strike, Expiry: leg.expiry,
		Price: leg.price, Bid: leg.bid, Ask: leg.ask, Delta: leg.delta, DeltaSource: leg.deltaSource,
	}}
}

func (s *Session) publishOptionData(p optionDataPublish) {
	if len(p.buses) == 0 {
		return
	}
	s.publishTo(p.buses, eventbus.Event{Kind: eventbus.KindOptionData, Payload: p.data})
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
// The line is granted as a first-quote request, not churn, even though a leg
// exists: there is no WORKING line for this contract, and churn is refused
// first under pressure (mdlines.ReserveChurnPct vs ReserveNewPct) — exactly
// when a dead leg squatting on a line costs the most. pointSelectorAt reasons
// the same way about an unquoted leg. If even that is refused, the
// dead leg's own line is released to make room, since a brief gap where a buy
// fails loudly beats filling one against a quote that stopped hours ago.
func (s *Session) forceResubscribeLeg(key legKey, deltaSource string) {
	s.optChain.mu.Lock()
	old, ok := s.optChain.legs[key]
	if !ok {
		s.optChain.mu.Unlock()
		return
	}
	oldReqID := old.reqID
	reqID := s.optChain.nextMktID
	s.optChain.nextMktID++
	s.optChain.mu.Unlock()

	granted := s.mdLines.GrantDiscretionaryNew(reqID)
	surrendered := false
	if !granted {
		s.optionLog.Printf("Option: WARNING forced re-subscribe %s %s strike=%.2f could not get a line — releasing the dead leg (reqID=%d) to make room",
			key.symbol, key.right, key.strike, oldReqID)
		s.mdLines.Release(oldReqID)
		surrendered = true
		granted = s.mdLines.GrantDiscretionaryNew(reqID)
	}
	if !granted {
		if surrendered {
			// Put the dead leg's line back on the books. It is still subscribed
			// at IB and still the leg's reqID, so leaving it unaccounted would
			// make the ledger under-count for the rest of the session.
			s.mdLines.GrantGuaranteed(oldReqID, mdlines.CategoryDiscretionaryNew)
		}
		s.optionLog.Printf("Option: WARNING forced re-subscribe %s %s strike=%.2f refused a market-data line — leaving the dead leg in place",
			key.symbol, key.right, key.strike)
		return
	}

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
	if deltaSource != "" {
		cur.deltaSource = deltaSource
	}
	delete(s.optChain.legByReqID, oldReqID)
	s.optChain.legByReqID[reqID] = key
	s.mdLines.Reclassify(reqID, legCategory(cur))
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

// resolveDeltaCandidates picks the candidate with delta closest to the
// target, promotes it to a live leg, and cancels the rest. Only called from
// ResolveEntryStrike.
func (s *Session) resolveDeltaCandidates(sel selector) (OptionQuote, EntryStrikeResult) {
	symbol, right := sel.symbol, sel.right
	// Read before the lock: getUnderlyingPrice takes the quote mutexes, which
	// sit below optChain.mu, and nothing here needs it to be consistent with
	// the chain state.
	undPrice := s.getUnderlyingPrice(symbol)

	s.optChain.mu.Lock()
	res, ok := s.optChain.deltaRes[sel.id]
	if !ok {
		s.optChain.mu.Unlock()
		return OptionQuote{}, EntryStrikeResult{Reason: entryFailQuoteTimeout,
			Detail: fmt.Sprintf("delta resolution for %s %s vanished before it could be read", symbol, right)}
	}
	delete(s.optChain.deltaRes, sel.id)

	var best *deltaCandidate
	bestDist := math.MaxFloat64
	for _, c := range res.candidates {
		if !c.ready {
			continue
		}
		dist := math.Abs(math.Abs(c.delta) - res.targetDelta)
		if dist < bestDist {
			bestDist = dist
			best = c
		}
	}

	if best == nil {
		// Classify BEFORE the candidates are released — an IB error stamped on
		// a candidate by noteCandidateError is the only thing that separates
		// "this account is not entitled to option data" from "IB never
		// answered", and both look identical from here otherwise.
		failure := classifyCandidateErrors(res.candidates)
		for _, c := range res.candidates {
			delete(s.optChain.deltaCands, c.reqID)
			s.mdLines.Release(c.reqID)
			s.client.CancelMktData(c.reqID)
		}
		iv := s.optChain.lastIV[symbol]
		allStrikes := res.allStrikes
		targetDelta := res.targetDelta
		expiry := res.expiry
		s.optChain.mu.Unlock()

		if iv <= 0 {
			iv = defaultFallbackIV
		}
		estStrike := approximateStrikeForDelta(allStrikes, undPrice, iv, targetDelta, expiry, right)
		s.optionLog.Printf("Option delta resolve: %s %s (sel=%d) — %s (%s), estimating strike=%.2f via target delta %.2f (iv=%.4f)",
			symbol, right, sel.id, failure.Reason, failure.Detail, estStrike, targetDelta, iv)
		s.logDeltaMiss(symbol, right, targetDelta, undPrice, allStrikes, failure.Reason, estStrike, iv)
		if estStrike > 0 {
			s.pointSelectorAt(sel, legKey{symbol, right, estStrike, expiry}, true)
		}
		return OptionQuote{}, failure
	}

	// The winner is already subscribed (as a probe) and already ticking — that
	// is how it won on delta — so it graduates into a leg in place, reusing its
	// reqID and its line rather than paying for a second subscription to the
	// same contract.
	key := legKey{best.symbol, best.right, best.strike, best.expiry}
	now := time.Now()
	delete(s.optChain.deltaCands, best.reqID)

	if existing, ok := s.optChain.legs[key]; ok {
		// A sibling selector already holds this exact contract. Keep the older
		// subscription and give the probe's line back.
		existing.delta, existing.deltaSource = best.delta, "matched"
		if best.bid > 0 {
			existing.bid = best.bid
		}
		if best.ask > 0 {
			existing.ask = best.ask
		}
		s.mdLines.Release(best.reqID)
		s.client.CancelMktData(best.reqID)
	} else {
		leg := s.openLegLocked(key, best.reqID, "matched", now)
		leg.delta, leg.bid, leg.ask = best.delta, best.bid, best.ask
		leg.lastTickAt = now
		s.mdLines.Reclassify(best.reqID, mdlines.CategoryDiscretionaryNew)
	}
	od, cancel := s.adoptLegLocked(sel, key, now)
	displayed := len(od.buses) > 0

	for _, c := range res.candidates {
		if c.reqID == best.reqID {
			continue
		}
		delete(s.optChain.deltaCands, c.reqID)
		s.mdLines.Release(c.reqID)
		cancel = append(cancel, c.reqID)
	}

	s.optChain.retries[sel.id] = &optStrikeRetry{
		selectorID: sel.id, symbol: symbol, right: right, expiry: res.expiry,
		strikes: res.allStrikes, nextIdx: 1, undPrice: undPrice, busIdxs: res.busIdxs,
	}
	s.optChain.mu.Unlock()

	s.cancelLines(cancel)
	s.optionLog.Printf("Option delta resolved: %s %s (sel=%d) target=%.2f → strike=%.2f (actual delta=%.4f, displayed=%v)",
		symbol, right, sel.id, res.targetDelta, best.strike, best.delta, displayed)
	s.publishOptionData(od)

	// best.bid/best.ask arrived during this synchronous, bounded probe (within
	// entryDeltaProbeTimeout), so `now` is an accurate freshness stamp for them —
	// there is no separate per-tick timestamp cached on deltaCandidate to read back.
	q := OptionQuote{Strike: best.strike, Expiry: best.expiry, Bid: best.bid, Ask: best.ask, Delta: best.delta, BidTime: now, AskTime: now}
	if !q.Valid() {
		// A winner on delta with no two-sided price. Callers already refused to
		// trade this (Valid() is the entry precondition); reporting it as its
		// own reason stops it hiding inside the generic "no quote" bucket, and
		// it is a genuinely different situation — the contract IS quoting
		// Greeks, so entitlement is fine and the price is merely late.
		return q, EntryStrikeResult{Reason: entryFailDeltaNoPrice,
			Detail: fmt.Sprintf("%s %s strike %.2f matched on delta %.4f but IB sent no two-sided price (bid=%.2f ask=%.2f)",
				symbol, right, best.strike, best.delta, best.bid, best.ask)}
	}
	return q, EntryStrikeResult{OK: true}
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
	for _, sel := range s.optChain.rotation {
		if sel.symbol != symbol || sel.right != right {
			continue
		}
		if busIdx < 0 || slices.Contains(sel.busIdxs, busIdx) {
			return sel, true
		}
	}
	return selector{}, false
}

// deltaCandidatesSettled is ResolveEntryStrike's poll-loop exit condition:
// true once EITHER every candidate has reported OR one already-ready
// candidate is within deltaGoodEnoughTolerance of targetDelta. Caller must
// hold s.optChain.mu.
func deltaCandidatesSettled(candidates []*deltaCandidate, targetDelta float64) bool {
	const deltaGoodEnoughTolerance = 0.02
	allReady := true
	for _, c := range candidates {
		if !c.ready {
			allReady = false
			continue
		}
		if math.Abs(math.Abs(c.delta)-targetDelta) <= deltaGoodEnoughTolerance {
			return true
		}
	}
	return allReady
}

// ResolveEntryStrike runs a synchronous, bounded, real IB delta probe for
// symbol+right — intended to be called just before placing an option
// order, the one moment a real (not estimated) strike is worth the round
// trip. Reuses the most recently cached chain strikes/expiry rather than
// repeating the conId + chain-params lookup. Blocks the calling goroutine
// for up to timeout waiting on IB's TickOptionComputation.
//
// sub identifies the calling subscriber so the caller's OWN selector is used
// when more than one tracks the same symbol+right at a different target_delta
// (e.g. two robots on the same underlying). Selectors are sorted by
// target_delta ascending when assigned (buildSelectors), so a lookup that
// ignored the caller would always hand back the smallest-target_delta one. That
// is what happened on 2026-08-04: VWmacdOptionRobot (target_delta 0.60) entered
// IWM call priced against VWmacdOptionDataRobot's 0.55 config instead of its
// own, landing near 0.60 only because a single candidate happened to be the
// only one to report a delta in time — not because anything validated the
// target.
func (s *Session) ResolveEntryStrike(sub Subscriber, symbol, right string, timeout time.Duration) (OptionQuote, EntryStrikeResult) {
	busIdx := s.busIndex(sub.Bus())
	s.optChain.mu.Lock()
	sel, hasSel := s.selectorForLocked(symbol, right, busIdx)
	info, hasInfo := s.optChain.lastChainInfo[sel.chainKey()]
	_, inFlight := s.optChain.deltaRes[sel.id]
	s.optChain.mu.Unlock()
	if !hasSel {
		return OptionQuote{}, EntryStrikeResult{Reason: entryFailNoChain,
			Detail: fmt.Sprintf("no option selector tracks %s %s for this robot", symbol, right)}
	}
	if !hasInfo || len(info.strikes) == 0 {
		return OptionQuote{}, EntryStrikeResult{Reason: entryFailNoChain,
			Detail: fmt.Sprintf("no cached option chain for %s yet (conId/chain-params lookup has not returned)", symbol)}
	}

	if q, ok := s.sharedResolvedEntry(sel.id, symbol, right); ok {
		return q, EntryStrikeResult{OK: true}
	}

	// A sibling subscriber configured identically (same symbol, right,
	// option_delay, target_delta — the selector key) is already probing this
	// exact contract. Don't duplicate the ReqMktData candidate probes and race
	// it for scarce mdlines probe-tier slots; wait for its result and share it
	// instead. This is what makes two robots that get the same crossover on the
	// same underlying converge on the identical strike (or identical failure)
	// rather than one silently losing the race and skipping the entry — see the
	// 2026-07-27 SPY put incident where VWmacdOptionDataRobot's larger symbol
	// universe made it more likely to lose exactly this race.
	if inFlight {
		return s.waitForEntryResolution(sel, timeout)
	}

	// No sibling to share with or join — becoming the owner means launching a
	// fresh batch of ReqMktData candidate probes, a real IB round trip. Throttle
	// that specifically (not the free reads/joins above) so a symbol sitting in
	// a zone with no obtainable quote can't spin the caller's event loop on
	// back-to-back probes, and so two robots sharing a selector can't each
	// independently re-launch within the other's cooldown window.
	s.optChain.mu.Lock()
	sinceLaunch := time.Since(s.optChain.lastProbeLaunch[sel.id])
	lastFail, hadFail := s.optChain.lastEntryFailure[sel.id]
	s.optChain.mu.Unlock()
	if sinceLaunch < entryProbeLaunchCooldown {
		// The previous launch's cause is the real explanation for this call
		// too — the cooldown is only why we are not re-asking IB right now.
		// Reporting the cooldown itself would bury an entitlement failure
		// behind an implementation detail for 15 of every 16 seconds.
		if hadFail {
			return OptionQuote{}, lastFail
		}
		return OptionQuote{}, EntryStrikeResult{Reason: entryFailProbeCooldown,
			Detail: fmt.Sprintf("last delta probe for %s %s was %.0fs ago; cooldown is %s", symbol, right, sinceLaunch.Seconds(), entryProbeLaunchCooldown)}
	}

	targetDelta := sel.targetDelta
	if isATMDelta(targetDelta) {
		return OptionQuote{}, EntryStrikeResult{Reason: entryFailDeltaTargetATM,
			Detail: fmt.Sprintf("target_delta %.2f for %s %s is inside the refused ATM band [0.48, 0.52]", targetDelta, symbol, right)}
	}

	undPrice := s.getUnderlyingPrice(symbol)
	candidates := selectITMCandidates(info.strikes, undPrice, right, 5)
	if len(candidates) == 0 {
		return OptionQuote{}, EntryStrikeResult{Reason: entryFailNoCandidates,
			Detail: fmt.Sprintf("no ITM %s strike for %s near underlying %.2f in a %d-strike chain", right, symbol, undPrice, len(info.strikes))}
	}

	s.optChain.mu.Lock()
	res := &deltaResolution{
		selectorID: sel.id, symbol: symbol, right: right, targetDelta: targetDelta,
		expiry: info.expiry, allStrikes: info.strikes, busIdxs: sel.busIdxs,
	}
	var evictedLines []int64
	for _, cs := range candidates {
		reqID := s.optChain.nextMktID
		s.optChain.nextMktID++
		evicted, ok := s.mdLines.GrantProbe(reqID)
		if !ok {
			continue
		}
		if evicted != 0 {
			// The ledger preempted a background line. Drop the leg it belonged
			// to — with every holder — since its IB subscription is going away.
			if leg, found := s.legByReqIDLocked(evicted); found {
				s.forgetLegLocked(leg)
			}
			evictedLines = append(evictedLines, evicted)
		}
		cand := &deltaCandidate{selectorID: sel.id, symbol: symbol, right: right, strike: cs, expiry: info.expiry, reqID: reqID, busIdxs: sel.busIdxs}
		s.optChain.deltaCands[reqID] = cand
		res.candidates = append(res.candidates, cand)

		ibRight := "C"
		if right == "put" {
			ibRight = "P"
		}
		contract := makeOptionContract(symbol, ibRight, cs, info.expiry)
		s.optionLog.Printf("Option: entry delta candidate %s %s strike=%.2f expiry=%s (reqID=%d)", symbol, right, cs, info.expiry, reqID)
		s.client.ReqMktData(reqID, contract, "", false, false, nil)
	}
	if len(res.candidates) == 0 {
		s.optChain.mu.Unlock()
		s.cancelLines(evictedLines)
		return OptionQuote{}, EntryStrikeResult{Reason: entryFailNoMDLines,
			Detail: fmt.Sprintf("market-data line budget refused all %d probe candidates for %s %s", len(candidates), symbol, right)}
	}
	s.optChain.deltaRes[sel.id] = res
	s.optChain.lastProbeLaunch[sel.id] = time.Now()
	s.optChain.mu.Unlock()
	s.cancelLines(evictedLines)

	const pollInterval = 100 * time.Millisecond
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s.optChain.mu.Lock()
		settled := deltaCandidatesSettled(res.candidates, targetDelta)
		s.optChain.mu.Unlock()
		if settled {
			break
		}
		time.Sleep(pollInterval)
	}
	q, res2 := s.resolveDeltaCandidates(sel)
	s.optChain.mu.Lock()
	if res2.OK {
		if s.optChain.resolvedEntry == nil {
			s.optChain.resolvedEntry = make(map[int]resolvedEntryLeg)
		}
		s.optChain.resolvedEntry[sel.id] = resolvedEntryLeg{strike: q.Strike, expiry: q.Expiry, delta: q.Delta, at: time.Now()}
		delete(s.optChain.lastEntryFailure, sel.id)
	} else {
		// Publish the cause for siblings still polling waitForEntryResolution,
		// and for our own next call inside the launch cooldown.
		if s.optChain.lastEntryFailure == nil {
			s.optChain.lastEntryFailure = make(map[int]EntryStrikeResult)
		}
		s.optChain.lastEntryFailure[sel.id] = res2
	}
	s.optChain.mu.Unlock()
	if !res2.OK {
		s.optionLog.Printf("Option entry probe FAILED: %s %s (sel=%d) — %s: %s",
			symbol, right, sel.id, res2.Reason, res2.Detail)
	}
	return q, res2
}

// forgetLegLocked drops a leg and every selector's reference to it without
// touching the ledger — for a line the ledger has ALREADY taken back (a probe
// preemption). Selectors left with no contract are simply blank until their
// next rotation turn, which is the honest state. Caller holds s.optChain.mu.
func (s *Session) forgetLegLocked(leg *optLeg) {
	key := leg.key()
	for selID := range leg.selectors {
		if s.optChain.selCurrent[selID] == key {
			delete(s.optChain.selCurrent, selID)
		}
		if sw, ok := s.optChain.selPending[selID]; ok && sw.to == key {
			delete(s.optChain.selPending, selID)
		}
	}
	delete(s.optChain.legs, key)
	delete(s.optChain.legByReqID, leg.reqID)
	delete(s.optChain.forcedResub, key)
}

// sharedResolvedEntry returns a fresh, Book-priced quote for the contract
// another subscriber recently resolved for this selector — the
// shared-selection fast path for ResolveEntryStrike.
func (s *Session) sharedResolvedEntry(selectorID int, symbol, right string) (OptionQuote, bool) {
	s.optChain.mu.Lock()
	leg, ok := s.optChain.resolvedEntry[selectorID]
	s.optChain.mu.Unlock()
	if !ok || time.Since(leg.at) > resolvedEntryTTL || s.book == nil {
		return OptionQuote{}, false
	}
	bq, ok := s.book.Option(quotes.ContractKey{Symbol: symbol, Right: right, Strike: leg.strike, Expiry: leg.expiry})
	if !ok {
		return OptionQuote{}, false
	}
	q := OptionQuote{Strike: leg.strike, Expiry: leg.expiry, Bid: bq.Bid, Ask: bq.Ask, Last: bq.Last, Delta: leg.delta, BidTime: bq.BidTime, AskTime: bq.AskTime}
	if !q.Valid() {
		return OptionQuote{}, false
	}
	return q, true
}

// waitForEntryResolution is ResolveEntryStrike's path for a caller that found
// this selector's deltaRes already owned by a concurrent sibling call. Rather
// than launching a duplicate set of candidate probes, it polls (same cadence
// the owning call's own loop uses) for that entry to clear — which happens
// the instant resolveDeltaCandidates finishes processing it, success or not —
// then reads the result via the same sharedResolvedEntry fast path the owner
// populates on success. If the owner's probe failed, resolvedEntry stays
// empty and this returns the same (OptionQuote{}, false) the owner got,
// rather than spending a second probe chasing the same answer.
func (s *Session) waitForEntryResolution(sel selector, timeout time.Duration) (OptionQuote, EntryStrikeResult) {
	const pollInterval = 100 * time.Millisecond
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s.optChain.mu.Lock()
		_, stillOwned := s.optChain.deltaRes[sel.id]
		s.optChain.mu.Unlock()
		if !stillOwned {
			break
		}
		time.Sleep(pollInterval)
	}
	if q, ok := s.sharedResolvedEntry(sel.id, sel.symbol, sel.right); ok {
		return q, EntryStrikeResult{OK: true}
	}
	// Report the owner's actual cause rather than "the other one failed too" —
	// whichever robot arrives second must not get a worse diagnosis than the
	// one that happened to own the probe.
	s.optChain.mu.Lock()
	fail, ok := s.optChain.lastEntryFailure[sel.id]
	s.optChain.mu.Unlock()
	if ok {
		return OptionQuote{}, fail
	}
	return OptionQuote{}, EntryStrikeResult{Reason: entryFailSiblingFailed,
		Detail: fmt.Sprintf("another robot's delta probe for %s %s finished without a usable quote", sel.symbol, sel.right)}
}

// handleOptionMktError handles error 200 for an option market data
// subscription. When both legs (call and put) of a strike fail, it
// automatically retries with the next nearest SMART strike. Returns true if
// reqID was an option market data request, false otherwise.
func (s *Session) handleOptionMktError(reqID int64, errStr string) bool {
	s.optChain.mu.Lock()

	// An explicit IB rejection (e.g. "no security definition found") of the
	// conId lookup or chain-params request — clean it up immediately rather
	// than waiting for reapStuckChainRequests' timeout, which exists for the
	// silent-drop case (no callback at all), not this one.
	if req, ok := s.optChain.conIDReqs[reqID]; ok {
		delete(s.optChain.conIDReqs, reqID)
		s.optChain.mu.Unlock()
		s.optionLog.Printf("Option: conId lookup FAILED for %s (sels=%v, reqID=%d) — %s", req.chain.symbol, req.waiters, reqID, errStr)
		return true
	}
	if req, ok := s.optChain.chainReqs[reqID]; ok {
		delete(s.optChain.chainReqs, reqID)
		s.optChain.mu.Unlock()
		s.optionLog.Printf("Option: chain params FAILED for %s (sels=%v, reqID=%d) — %s", req.chain.symbol, req.waiters, reqID, errStr)
		return true
	}

	if cand, ok := s.optChain.deltaCands[reqID]; ok {
		// Stamp the cause on the candidate BEFORE dropping it from the map:
		// res.candidates still holds this pointer, so classifyCandidateErrors
		// can read it even though the candidate is no longer discoverable by
		// reqID.
		cand.errCode = 200
		cand.errMsg = errStr
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

	// The contract itself is bad, so every holder must let go of it — this is
	// the one case where dropping a leg out from under other holders is
	// correct, because the subscription behind it does not exist.
	holders := make([]int, 0, len(leg.selectors))
	for selID := range leg.selectors {
		holders = append(holders, selID)
	}
	s.forgetLegLocked(leg)
	s.mdLines.Release(reqID)

	s.optionLog.Printf("Option market data FAILED: %s %s strike=%.2f expiry=%s (reqID=%d, sels=%v, pinned=%v) — contract not found, skipping",
		symbol, right, strike, expiry, reqID, holders, pinned)
	s.logger.Printf("Option market data FAILED: %s %s strike=%.2f expiry=%s — %s", symbol, right, strike, expiry, errStr)

	// Each holding selector retries onto its own next-nearest strike. They are
	// independent: one selector exhausting its candidate list says nothing
	// about another's. The walk is bounded — see optStrikeRetry.nextCandidate.
	type retryPlan struct {
		sel    selector
		strike float64
		expiry string
	}
	type retryGaveUp struct {
		selID  int
		expiry string
		reason string
		tried  []float64
	}
	var plans []retryPlan
	var gaveUp []retryGaveUp
	for _, selID := range holders {
		retry, hasRetry := s.optChain.retries[selID]
		if !hasRetry {
			continue
		}
		nextStrike, reason, ok := retry.nextCandidate()
		if !ok {
			delete(s.optChain.retries, selID)
			gaveUp = append(gaveUp, retryGaveUp{selID, retry.expiry, reason, retry.tried})
			continue
		}
		if sel, ok := s.selectorByIDLocked(selID); ok {
			plans = append(plans, retryPlan{sel, nextStrike, retry.expiry})
		}
	}
	s.optChain.mu.Unlock()

	for _, p := range plans {
		s.optionLog.Printf("Option: %s %s (sel=%d) — retrying with next nearest strike=%.2f expiry=%s", symbol, right, p.sel.id, p.strike, p.expiry)
		s.pointSelectorAt(p.sel, legKey{symbol, right, p.strike, p.expiry}, true)
	}
	// Giving up leaves the selector with NO leg (forgetLegLocked above already
	// cleared selCurrent), which is the point: its row goes blank rather than
	// showing a contract nobody asked for, and its next rotation attempt is a
	// first-quote grant instead of the churn tier a stuck leg would have forced.
	for _, g := range gaveUp {
		s.optionLog.Printf("Option: WARNING %s %s (sel=%d) — giving up on expiry=%s: %s (tried %v); this row has NO option data until the next chain refresh",
			symbol, right, g.selID, g.expiry, g.reason, g.tried)
		s.logger.Printf("Option: %s %s (sel=%d) — no listed strike for expiry=%s: %s (tried %v)",
			symbol, right, g.selID, g.expiry, g.reason, g.tried)
	}
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
			leg.bid = price
		case ibapi.ASK, ibapi.DELAYED_ASK:
			leg.ask = price
		case ibapi.LAST, ibapi.DELAYED_LAST, ibapi.CLOSE, ibapi.DELAYED_CLOSE:
			leg.price = price
		default:
			s.optChain.mu.Unlock()
			return true
		}
		key := leg.key()
		now := time.Now()
		// A quote arriving here may be the one a selector has been warming for.
		cancel := s.promoteWaitersLocked(key, now)
		od := eventbus.OptionData{
			Symbol: leg.symbol, Right: leg.right, Strike: leg.strike, Expiry: leg.expiry,
			Price: leg.price, Bid: leg.bid, Ask: leg.ask, Delta: leg.delta, DeltaSource: leg.deltaSource,
		}
		// One leg can be both a watchlist row's display contract and an open
		// position's pinned contract; they are different questions and each
		// consumer subscribes to its own event kind, so both are published.
		buses := s.legDisplayBusesLocked(key)
		pinned := leg.pins > 0
		s.optChain.mu.Unlock()

		s.cancelLines(cancel)
		s.bookOption(od)
		if len(buses) > 0 {
			s.publishTo(buses, eventbus.Event{Kind: eventbus.KindOptionData, Payload: od})
		}
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

// resolvedOption is the currently-subscribed ATM option leg for one symbol+right.
type resolvedOption struct {
	contract *ibapi.Contract
	strike   float64
	expiry   string
	bid      float64
	ask      float64
	mid      float64
}

// currentOptionContract returns the live option leg subscribed for symbol +
// right ("call"/"put") for the requesting subscriber (busIdx), or false if
// the chain has not resolved a contract yet.
func (s *Session) currentOptionContract(symbol, right string, busIdx int) (resolvedOption, bool) {
	s.optChain.mu.Lock()
	defer s.optChain.mu.Unlock()

	sel, ok := s.selectorForLocked(symbol, right, busIdx)
	if !ok {
		return resolvedOption{}, false
	}
	if leg, ok := s.selectorLegLocked(sel.id); ok {
		return resolveOptionLeg(leg), true
	}
	// Nothing displayed yet, but a contract this selector is warming into is
	// still the contract it means — better than reporting none at all.
	if sw, waiting := s.optChain.selPending[sel.id]; waiting {
		if leg, ok := s.optChain.legs[sw.to]; ok {
			return resolveOptionLeg(leg), true
		}
	}
	return resolvedOption{}, false
}

// resolveOptionLeg builds a resolvedOption from a leg.
func resolveOptionLeg(leg *optLeg) resolvedOption {
	ibRight := "C"
	if leg.right == "put" {
		ibRight = "P"
	}
	mid := leg.price
	switch {
	case leg.bid > 0 && leg.ask > 0:
		mid = (leg.bid + leg.ask) / 2
	case leg.ask > 0:
		mid = leg.ask
	case leg.bid > 0:
		mid = leg.bid
	}
	return resolvedOption{
		contract: makeOptionContract(leg.symbol, ibRight, leg.strike, leg.expiry),
		strike:   leg.strike, expiry: leg.expiry, bid: leg.bid, ask: leg.ask, mid: mid,
	}
}

// ── Helper functions ──────────────────────────────────────────────────────

// defaultFallbackIV is used by approximateStrikeForDelta only when no live
// IV sample exists at all for a symbol — a rare edge case limited to the
// very first resolution of a session before any option tick has arrived.
const defaultFallbackIV = 0.25

// deltaDriftTolerance bounds how far a delta-target leg's live,
// IB-confirmed delta may wander from its target_delta before the
// background rotation bothers re-estimating and resubscribing a closer strike.
const deltaDriftTolerance = 0.05

// approximateStrikeForDelta estimates the whole-dollar strike (from the
// candidates actually available in the chain) whose Black-Scholes delta is
// closest to targetDelta, for use when a live IB delta probe returns no
// usable quotes.
//
// Derivation: for a call, delta = N(d1); for a put, delta = N(d1) - 1 (so a
// put magnitude of targetDelta corresponds to N(d1) = 1 - targetDelta).
// Solving the standard d1 = (ln(S/K) + 0.5·σ²·T) / (σ√T) for K given a
// target d1 yields K = S / exp(d1·σ√T − 0.5·σ²·T).
func approximateStrikeForDelta(strikes []float64, undPrice, iv, targetDelta float64, expiry string, right string) float64 {
	if len(strikes) == 0 {
		return 0
	}
	if iv <= 0 || undPrice <= 0 || targetDelta <= 0 || targetDelta >= 1 {
		return strikes[0]
	}
	t := yearsToExpiry(expiry)
	if t <= 0 {
		return strikes[0]
	}

	var d1 float64
	if right == "call" {
		d1 = invNormCDF(targetDelta)
	} else {
		d1 = invNormCDF(1 - targetDelta)
	}

	sqrtT := math.Sqrt(t)
	exponent := d1*iv*sqrtT - 0.5*iv*iv*t
	target := undPrice / math.Exp(exponent)

	best := strikes[0]
	bestDist := math.Abs(strikes[0] - target)
	for _, st := range strikes {
		if d := math.Abs(st - target); d < bestDist {
			bestDist = d
			best = st
		}
	}
	return best
}

// yearsToExpiry converts an option's "YYYYMMDD" expiry into a year fraction
// from now, floored at 30 minutes so a same-day (0DTE) expiry never divides
// by (near-)zero. Treats expiry as 16:00 local time (US market close).
func yearsToExpiry(expiry string) float64 {
	exp, err := time.ParseInLocation("20060102", expiry, time.Local)
	if err != nil {
		return 0
	}
	closeTime := time.Date(exp.Year(), exp.Month(), exp.Day(), 16, 0, 0, 0, time.Local)
	d := time.Until(closeTime)
	const minDuration = 30 * time.Minute
	if d < minDuration {
		d = minDuration
	}
	const hoursPerYear = 24 * 365.25
	return d.Hours() / hoursPerYear
}

// invNormCDF returns the inverse of the standard normal CDF (the probit
// function) via Peter Acklam's rational approximation — accurate to about
// 1.15e-9 relative error. p must be in (0, 1).
func invNormCDF(p float64) float64 {
	if p <= 0 {
		p = 1e-10
	} else if p >= 1 {
		p = 1 - 1e-10
	}

	const (
		a1 = -3.969683028665376e+01
		a2 = 2.209460984245205e+02
		a3 = -2.759285104469687e+02
		a4 = 1.383577518672690e+02
		a5 = -3.066479806614716e+01
		a6 = 2.506628277459239e+00

		b1 = -5.447609879822406e+01
		b2 = 1.615858368580409e+02
		b3 = -1.556989798598866e+02
		b4 = 6.680131188771972e+01
		b5 = -1.328068155288572e+01

		c1 = -7.784894002430293e-03
		c2 = -3.223964580411365e-01
		c3 = -2.400758277161838e+00
		c4 = -2.549732539343734e+00
		c5 = 4.374664141464968e+00
		c6 = 2.938163982698783e+00

		d1 = 7.784695709041462e-03
		d2 = 3.224671290700398e-01
		d3 = 2.445134137142996e+00
		d4 = 3.754408661907416e+00

		pLow  = 0.02425
		pHigh = 1 - pLow
	)

	switch {
	case p < pLow:
		q := math.Sqrt(-2 * math.Log(p))
		return (((((c1*q+c2)*q+c3)*q+c4)*q+c5)*q + c6) /
			((((d1*q+d2)*q+d3)*q+d4)*q + 1)
	case p <= pHigh:
		q := p - 0.5
		r := q * q
		return (((((a1*r+a2)*r+a3)*r+a4)*r+a5)*r + a6) * q /
			(((((b1*r+b2)*r+b3)*r+b4)*r+b5)*r + 1)
	default:
		q := math.Sqrt(-2 * math.Log(1-p))
		return -(((((c1*q+c2)*q+c3)*q+c4)*q+c5)*q + c6) /
			((((d1*q+d2)*q+d3)*q+d4)*q + 1)
	}
}

// ── Delta-miss logging ────────────────────────────────────────────────────

var deltaMissMu sync.Mutex
var deltaMissFile *os.File
var deltaMissWriter *csv.Writer
var deltaMissDate string

var deltaMissHeader = []string{
	"timestamp", "symbol", "right", "target_delta", "underlying_price",
	"candidates_probed", "reason", "fallback_strike", "estimated_via_formula", "iv_used",
}

// logDeltaMiss appends one row for a delta-probe fallback event to a
// day-rotated CSV under "log/" (created if needed) so the frequency of
// delta-probe fallbacks is a simple CSV load away, rather than a manual
// option-log scrape.
func (s *Session) logDeltaMiss(symbol, right string, targetDelta, undPrice float64, candidates []float64, reason string, estimatedStrike, ivUsed float64) {
	deltaMissMu.Lock()
	defer deltaMissMu.Unlock()

	today := time.Now().Format("2006-01-02")
	if deltaMissWriter == nil || deltaMissDate != today {
		if deltaMissFile != nil {
			deltaMissWriter.Flush()
			deltaMissFile.Close()
		}
		dir := "log"
		if err := os.MkdirAll(dir, 0o755); err != nil {
			s.logger.Printf("delta-misses: cannot create log directory: %v", err)
			return
		}
		path := filepath.Join(dir, today+"-delta-misses.csv")
		writeHeader := true
		if info, err := os.Stat(path); err == nil && info.Size() > 0 {
			writeHeader = false
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			s.logger.Printf("delta-misses: cannot open %s: %v", path, err)
			return
		}
		deltaMissFile = f
		deltaMissWriter = csv.NewWriter(f)
		deltaMissDate = today
		if writeHeader {
			_ = deltaMissWriter.Write(deltaMissHeader)
		}
	}

	strikeStrs := make([]string, len(candidates))
	for i, st := range candidates {
		strikeStrs[i] = strconv.FormatFloat(st, 'f', 2, 64)
	}
	estimatedViaFormula := "true"
	if ivUsed <= 0 {
		estimatedViaFormula = "false"
	}
	row := []string{
		time.Now().Format("2006-01-02 15:04:05"), symbol, right,
		strconv.FormatFloat(targetDelta, 'f', 4, 64), strconv.FormatFloat(undPrice, 'f', 2, 64),
		strings.Join(strikeStrs, "|"), reason, strconv.FormatFloat(estimatedStrike, 'f', 2, 64),
		estimatedViaFormula, strconv.FormatFloat(ivUsed, 'f', 4, 64),
	}
	if err := deltaMissWriter.Write(row); err != nil {
		s.logger.Printf("delta-misses: write failed: %v", err)
		return
	}
	deltaMissWriter.Flush()
}

// wholeDollarStrikes returns only the strikes with no fractional dollar
// part (75, 76 — never 75.50). If the underlying lists no whole-dollar
// strikes at all, the original slice is returned unchanged.
func wholeDollarStrikes(strikes []float64) []float64 {
	whole := make([]float64, 0, len(strikes))
	for _, st := range strikes {
		if st == math.Trunc(st) {
			whole = append(whole, st)
		}
	}
	if len(whole) == 0 {
		return strikes
	}
	return whole
}

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
// option strike pinned to an open position. These subscriptions are
// independent of the ATM strike rotation, so a held position keeps its own
// feed even as the ATM leg re-resolves to a different strike. No-op if
// already subscribed for this symbol+right+strike combination.
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
	reqID := s.optChain.nextPosID
	s.optChain.nextPosID++
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

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

	// pins counts open-position holders, and is now the ONLY holder kind: a
	// leg exists because some position needs its contract priced.
	//
	// The refcount stays essential even so. It is what stopped one robot's exit
	// cancelling a contract a sibling's still-open IWM 298 PUT was pricing its
	// stops against (2026-08-04), and two robots holding the same contract —
	// including a live position and its own simulated mirror — is routine.
	//
	// A `selectors map[int]struct{}` sat beside this, counting watchlist rows
	// displaying the contract. Those rows no longer hold subscriptions.
	pins int

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
func (l *optLeg) held() bool { return l.pins > 0 }

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

	// dupTicker records that IB refused this candidate's request as a
	// duplicate ticker id, i.e. the id is another live request's. It gates the
	// cleanup in resolveDeltaCandidates: cancelling or releasing an id we were
	// refused tears down whoever does own it. See dupticker.go.
	dupTicker bool
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
	// entryFailNoCandidates — selectStrikeCandidates found no strike to probe.
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
	nextSelID   int
	conIDReqs   map[int64]*optConIDReq
	chainReqs   map[int64]*optChainReq

	// legs is the one registry of subscribed option contracts, background and
	// position-pinned alike, keyed by contract and refcounted across holders.
	// legByReqID is its reverse index for the IB callbacks, which only ever
	// carry a reqID.
	legs       map[legKey]*optLeg
	legByReqID map[int64]legKey

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
	// selector — i.e. refreshChainFor got past the selectorResolvingLocked
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

// resolvedEntryLeg is one selector's most recently resolved entry contract —
// strike, expiry, the delta that won it, and the two-sided quote it carried —
// with the wall-clock time it was resolved.
//
// The quote is stored here rather than re-read from the quotes.Book, because
// a probe's ticks never reach the Book (they land on a *deltaCandidate, which
// is dropped when the probe resolves) and the winning candidate's market-data
// line is cancelled the moment the probe finishes. There is no live feed on
// this contract to consult afterwards — the whole point of the change — so the
// resolution has to carry its own answer. resolvedEntryTTL (5s) is what keeps
// that answer honest.
type resolvedEntryLeg struct {
	strike   float64
	expiry   string
	delta    float64
	iv       float64
	bid, ask float64
	bidTime  time.Time
	askTime  time.Time
	at       time.Time
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
		s.refreshChainFor(sel)
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

// refreshChainFor re-selects the contract one selector should track. It uses
// the cached chain when there is a fresh one for this (symbol, optionDelay) —
// selecting a strike is pure computation over the strike list and a live
// underlying price — and otherwise fires the conId lookup that leads to one.
// Skips when a prior resolution for the same selector is still in flight.
func (s *Session) refreshChainFor(sel selector) {
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
		// A fresh chain IS the whole job. Nothing is selected or subscribed as
		// a result — ResolveEntryStrike reads this snapshot when an entry
		// actually needs a contract.
		s.optChain.mu.Unlock()
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

	reqID := s.nextReqID()
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

// refreshOptionChains keeps the option-chain snapshot cache warm. One selector
// per tick, oldest-serviced first, so chain lookups stay a slow steady trickle
// rather than a synchronized burst — and a selector whose chain is still inside
// chainSnapshotTTL costs nothing but a map read.
//
// This is what ResolveEntryStrike depends on. It reads lastChainInfo and never
// fetches a chain itself, so an entry arriving on a cold or expired snapshot
// pays for the round trip inline. Keeping the cache warm is the difference
// between a ~1.3s entry probe and one that also waits on a conId lookup.
//
// It used to do considerably more: pick a selector, re-estimate its strike from
// the cached chain, and subscribe/roll a market-data line onto the result. That
// was the background-leg rotation, and it is gone — along with the ATM
// starvation, dead legs, and cross-robot eviction that came with it. What
// remains costs no market-data lines at all.
func (s *Session) refreshOptionChains() {
	s.reapStuckChainRequests()
	s.reapDeadOptionLegs()
	s.optChain.mu.Lock()
	sel, ok := s.pickChainRefreshSelectorLocked()
	s.optChain.mu.Unlock()
	if !ok {
		return
	}
	s.refreshChainFor(sel)
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
// excluding them from pickChainRefreshSelectorLocked's oldest-first pick and
// leaving them stuck at zero background lines for the rest of the session —
// while the handful that did resolve become the only eligible candidates and
// get endlessly re-picked instead.
const chainResolutionMaxAge = 20 * time.Second

// reapStuckChainRequests frees any conId-lookup or chain-params request
// older than chainResolutionMaxAge whose IB response never arrived, so the
// next refreshOptionChains tick can retry instead of leaving its selectors
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

// pickChainRefreshSelectorLocked chooses the next selector to refresh, oldest
// data first. Must be called with s.optChain.mu held.
func (s *Session) pickChainRefreshSelectorLocked() (selector, bool) {
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

	chainReqID := s.nextReqID()
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

	// Caching the snapshot IS the result. Every selector that queued on this
	// round trip reads it from here on demand — at entry time, via
	// ResolveEntryStrike. Nothing is subscribed as a consequence of a chain
	// arriving any more: a chain tells us which contracts EXIST, which is a
	// different question from which one we want a live price for, and only an
	// imminent entry or an open position makes that second question worth a
	// market-data line.
	s.optChain.mu.Lock()
	s.optChain.lastChainInfo[chain] = snap
	s.optChain.mu.Unlock()
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

// legCategory is a leg's market-data priority. Every leg is held by at least
// one open position — that is the only thing that opens one — so this is
// always guaranteed. It survives as a function because openLegLocked and the
// resubscribe path both classify through it, and a future non-position holder
// would need exactly one place to change.
func legCategory(*optLeg) mdlines.Category { return mdlines.CategoryPosition }

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

// resolveDeltaCandidates picks the candidate with delta closest to the
// target, reads its quote, and cancels every candidate including the winner.
// Only called from ResolveEntryStrike.
//
// Nothing survives this call as a subscription. The winner used to graduate
// into a persistent background leg so a dashboard row could keep displaying
// it; now the quote is returned by value, cached in resolvedEntry for the
// sibling fast path, and the line goes straight back to the pool. If the
// entry that triggered this probe actually fills, SubscribePositionStrike
// opens a guaranteed CategoryPosition line for the same contract — which is
// the only reason to hold an option feed at all.
func (s *Session) resolveDeltaCandidates(sel selector) (OptionQuote, EntryStrikeResult) {
	symbol, right := sel.symbol, sel.right

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
			if c.dupTicker {
				continue // the id is another live request's — see dupticker.go
			}
			s.mdLines.Release(c.reqID)
			s.client.CancelMktData(c.reqID)
		}
		s.optChain.mu.Unlock()

		s.optionLog.Printf("Option delta resolve: %s %s (sel=%d) — %s (%s)",
			symbol, right, sel.id, failure.Reason, failure.Detail)
		return OptionQuote{}, failure
	}

	now := time.Now()
	var cancel []int64
	for _, c := range res.candidates {
		delete(s.optChain.deltaCands, c.reqID)
		if c.dupTicker {
			continue // the id is another live request's — see dupticker.go
		}
		s.mdLines.Release(c.reqID)
		cancel = append(cancel, c.reqID)
	}
	s.optChain.mu.Unlock()

	s.cancelLines(cancel)
	s.optionLog.Printf("Option delta resolved: %s %s (sel=%d) target=%.2f → strike=%.2f (actual delta=%.4f)",
		symbol, right, sel.id, res.targetDelta, best.strike, best.delta)

	// A miss this large means the ladder carried no strike near the target --
	// a 0DTE chain near the close, where delta steps from ~0.2 to ~0.8 across
	// one rung, or a coarse ladder on a low-priced underlying. The entry is
	// still taken (closest match), so this is the only signal that it happened.
	// It goes to s.logger, not optionLog: the resolution line above is one of
	// millions in the chain log, which is why "many transactions at 0.77" was
	// visible in a CSV weeks later and nowhere at the time.
	if miss := math.Abs(math.Abs(best.delta) - res.targetDelta); miss > deltaMissWarnThreshold {
		s.logger.Printf("Option: WARNING %s %s (sel=%d) target=%.2f resolved to strike=%.2f at delta=%.4f (miss %.2f) — no nearer strike on this ladder; entry taken",
			symbol, right, sel.id, res.targetDelta, best.strike, best.delta, miss)
	}

	// best.bid/best.ask arrived during this synchronous, bounded probe (within
	// entryDeltaProbeTimeout), so `now` is an accurate freshness stamp for them —
	// there is no separate per-tick timestamp cached on deltaCandidate to read back.
	q := OptionQuote{Strike: best.strike, Expiry: best.expiry, Bid: best.bid, Ask: best.ask, Delta: best.delta, IV: best.iv, BidTime: now, AskTime: now}
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

	if q, ok := s.sharedResolvedEntry(sel.id); ok {
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
	candidates := selectStrikeCandidates(info.strikes, undPrice, right, deltaProbeITMCandidates, deltaProbeOTMCandidates)
	if len(candidates) == 0 {
		return OptionQuote{}, EntryStrikeResult{Reason: entryFailNoCandidates,
			Detail: fmt.Sprintf("no %s strike for %s near underlying %.2f in a %d-strike chain", right, symbol, undPrice, len(info.strikes))}
	}

	s.optChain.mu.Lock()
	res := &deltaResolution{
		selectorID: sel.id, symbol: symbol, right: right, targetDelta: targetDelta,
		expiry: info.expiry, allStrikes: info.strikes, busIdxs: sel.busIdxs,
	}
	for _, cs := range candidates {
		reqID := s.nextReqID()
		if !s.mdLines.GrantProbe(reqID) {
			continue
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
		return OptionQuote{}, EntryStrikeResult{Reason: entryFailNoMDLines,
			Detail: fmt.Sprintf("market-data line budget refused all %d probe candidates for %s %s", len(candidates), symbol, right)}
	}
	s.optChain.deltaRes[sel.id] = res
	s.optChain.lastProbeLaunch[sel.id] = time.Now()
	s.optChain.mu.Unlock()

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
		s.optChain.resolvedEntry[sel.id] = resolvedEntryLeg{strike: q.Strike, expiry: q.Expiry, delta: q.Delta, iv: q.IV, bid: q.Bid, ask: q.Ask, bidTime: q.BidTime, askTime: q.AskTime, at: time.Now()}
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

// sharedResolvedEntry returns a fresh, Book-priced quote for the contract
// another subscriber recently resolved for this selector — the
// shared-selection fast path for ResolveEntryStrike.
func (s *Session) sharedResolvedEntry(selectorID int) (OptionQuote, bool) {
	s.optChain.mu.Lock()
	leg, ok := s.optChain.resolvedEntry[selectorID]
	s.optChain.mu.Unlock()
	if !ok || time.Since(leg.at) > resolvedEntryTTL {
		return OptionQuote{}, false
	}
	q := OptionQuote{
		Strike: leg.strike, Expiry: leg.expiry,
		Bid: leg.bid, Ask: leg.ask, Delta: leg.delta, IV: leg.iv,
		BidTime: leg.bidTime, AskTime: leg.askTime,
	}
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
	if q, ok := s.sharedResolvedEntry(sel.id); ok {
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
	//   an entry probe already subscribes five candidates at once and picks on
	//   delta, so a dead strike among them costs nothing and needs no walk.
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
			leg.bid = price
		case ibapi.ASK, ibapi.DELAYED_ASK:
			leg.ask = price
		case ibapi.LAST, ibapi.DELAYED_LAST, ibapi.CLOSE, ibapi.DELAYED_CLOSE:
			leg.price = price
		default:
			s.optChain.mu.Unlock()
			return true
		}
		od := eventbus.OptionData{
			Symbol: leg.symbol, Right: leg.right, Strike: leg.strike, Expiry: leg.expiry,
			Price: leg.price, Bid: leg.bid, Ask: leg.ask, Delta: leg.delta, DeltaSource: leg.deltaSource,
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

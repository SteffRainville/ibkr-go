package ibkr

import (
	"fmt"
	"time"

	"github.com/SteffRainville/ibkr-go/mdlines"
)

// Detection of option market-data legs that IB has silently stopped serving.
//
// A subscription can go dead without any error callback: only IB error 200 is
// routed to handleOptionMktError, while the codes that actually kill a stream
// while leaving it registered (354 not-subscribed, 10197 competing live
// session, 10167/10168 delayed substitution, pacing rejections) fall through
// to a log line and never touch mktReqs. Nothing else in the library observes
// tick ARRIVAL, so such a leg keeps its last good price forever, keeps its
// market-data line allocated forever, and — because its cached delta is also
// frozen at a plausible value — keeps talking the rotation out of refreshing
// it. On 2026-08-03 that left QQQ/SPY/IWM showing prices ~35% away from the
// real market for two hours.
//
// The signal these helpers work from is deliberately NOT the quotes.Book's
// BidTime/AskTime. Those advance only when a price CHANGES, by design, so
// they measure how current a VALUE is; they cannot separate a quiet-but-live
// leg from a dead one. optMktReq.lastTickAt is stamped on every IB message of
// any kind, which is what makes that distinction possible.

const (
	// legWarmupGrace is how long after subscribing a leg is exempt from any
	// liveness judgement. IB routinely takes several seconds to deliver the
	// first tick on a fresh option subscription, and a replacement leg is
	// created before its data exists by construction.
	legWarmupGrace = 20 * time.Second

	// legSilenceRTH / legSilenceOffRTH are how long a leg may go without any
	// IB message before it is considered dead. Generous relative to the tick
	// rate of a liquid option (sub-second) because the cost of a false
	// positive is a wasted re-subscribe, and much wider outside RTH where
	// even liquid contracts legitimately go minutes between messages.
	legSilenceRTH    = 90 * time.Second
	legSilenceOffRTH = 15 * time.Minute

	// feedAliveWindow is how recently SOME option leg must have received a
	// message before any leg may be judged dead. This is the load-bearing
	// guard against false positives: it makes deadness peer-relative rather
	// than absolute, so a halt, a weekend, an off-hours lull or an IB feed
	// outage — every case where the whole feed is legitimately quiet —
	// condemns nothing. Without it, the first market-wide silence would mark
	// every leg dead at once and fire a re-subscribe storm at a broker that
	// is not answering anyway.
	feedAliveWindow = 10 * time.Second
)

// legHealth classifies one option leg's liveness.
type legHealth int

const (
	// legHealthy — received an IB message within the silence threshold.
	legHealthy legHealth = iota
	// legWarming — subscribed too recently to judge.
	legWarming
	// legSilent — past warmup and has NEVER received a message. The strike
	// itself may not be quotable, so re-subscribing the same one is likely
	// to fail the same way; the right response is to re-estimate.
	legSilent
	// legStale — was ticking, then stopped, while the rest of the feed kept
	// flowing. This is a genuinely dead subscription for a contract known to
	// be quotable, so re-subscribing the SAME strike is the correct repair.
	legStale
	// legFeedQuiet — silent, but so is every other leg, so nothing can be
	// concluded about this one. Never act on it.
	legFeedQuiet
)

func (h legHealth) String() string {
	switch h {
	case legHealthy:
		return "healthy"
	case legWarming:
		return "warming"
	case legSilent:
		return "silent"
	case legStale:
		return "stale"
	case legFeedQuiet:
		return "feed_quiet"
	}
	return "unknown"
}

// legSilenceLimit returns the silence threshold applying at t.
func legSilenceLimit(t time.Time) time.Duration {
	if isRTHAt(t) {
		return legSilenceRTH
	}
	return legSilenceOffRTH
}

// legHealthAt classifies a leg from its subscribe time, its last-message
// time (zero = never), and the session-wide last-any-option-message time.
// Pure and now-explicit so the whole matrix is table-testable, mirroring
// isRTHAt and barStallTimeout.
func legHealthAt(subscribedAt, lastTickAt, lastAnyTick, now time.Time) legHealth {
	// A leg with no subscribe time is not a real subscription (zero-valued
	// struct in a test, or a map entry mid-construction); never condemn it.
	if subscribedAt.IsZero() {
		return legWarming
	}
	if now.Sub(subscribedAt) < legWarmupGrace {
		return legWarming
	}
	if !lastTickAt.IsZero() && now.Sub(lastTickAt) < legSilenceLimit(now) {
		return legHealthy
	}
	// Past warmup and quiet. Only a demonstrably live peer feed makes that
	// mean anything — see feedAliveWindow.
	if lastAnyTick.IsZero() || now.Sub(lastAnyTick) > feedAliveWindow {
		return legFeedQuiet
	}
	if lastTickAt.IsZero() {
		return legSilent
	}
	return legStale
}

// legTrustedAt reports whether a leg's cached values (notably delta) may be
// believed. Anything not currently ticking must not be trusted, because the
// cached value is exactly as old as the silence — this is the predicate that
// breaks the self-sustaining skip loop in shouldSkipReEstimateLocked.
func legTrustedAt(subscribedAt, lastTickAt, lastAnyTick, now time.Time) bool {
	switch legHealthAt(subscribedAt, lastTickAt, lastAnyTick, now) {
	case legHealthy, legWarming:
		return true
	default:
		return false
	}
}

const (
	// forcedResubBaseCooldown is the first backoff step after a forced
	// re-subscribe; it doubles per consecutive attempt up to
	// forcedResubMaxCooldown. A contract IB will never quote (delisted,
	// wrong expiry, no entitlement) would otherwise burn a market-data line
	// every tick for the rest of the session.
	forcedResubBaseCooldown = 60 * time.Second
	forcedResubMaxCooldown  = 15 * time.Minute

	// maxForcedResubsPerTick bounds how many legs one reaper pass may
	// re-subscribe. The feed-alive gate should already prevent any mass
	// event, so this is the backstop that keeps a wrong assumption from
	// turning into an IB pacing violation.
	maxForcedResubsPerTick = 2
)

// resubCooldown returns the backoff after n consecutive forced attempts.
func resubCooldown(attempts int) time.Duration {
	d := forcedResubBaseCooldown
	for i := 1; i < attempts; i++ {
		d *= 2
		if d >= forcedResubMaxCooldown {
			return forcedResubMaxCooldown
		}
	}
	return d
}

// deadLegAction is one repair the reaper decided to perform.
type deadLegAction struct {
	reqID   int64
	groupID int
	busIdxs []int
	symbol  string
	right   string
	strike  float64
	expiry  string
	health  legHealth
	age     string
	pinned  bool
}

// reapDeadOptionLegs finds option legs IB has silently stopped serving and
// repairs them. It runs on the rotation ticker but is deliberately NOT part
// of chain resolution: a dead leg already knows its own symbol/right/strike/
// expiry, so the repair needs no conId or chain-params round trip. That makes
// recovery independent of both the re-estimate guards and the rotation
// scoring — neither of which could ever repair a dead leg, since the first
// trusts the leg's own frozen delta and the second never picks a group whose
// (frozen) quotes look fresh.
//
// Only legStale legs are re-subscribed at the same strike. A legSilent leg
// has never ticked at all, so the strike itself may be unquotable and
// re-requesting it would fail identically; those are logged and left to the
// rotation to re-estimate onto a different strike.
func (s *Session) reapDeadOptionLegs() {
	now := time.Now()

	s.optChain.mu.Lock()
	actions, silent := s.planDeadLegRepairsLocked(now)
	s.optChain.mu.Unlock()

	for _, a := range silent {
		s.optionLog.Printf("Option: WARNING %s %s strike=%.2f (group=%d, reqID=%d) has NEVER ticked (health=%s) — leaving it for the rotation to re-estimate onto another strike",
			a.symbol, a.right, a.strike, a.groupID, a.reqID, a.health)
	}

	for _, a := range actions {
		if a.pinned {
			s.recoverPinnedLeg(a, now)
			continue
		}
		s.optionLog.Printf("Option: WARNING %s %s strike=%.2f (group=%d, reqID=%d) went SILENT (last tick %s ago) while the option feed is live — forcing re-subscribe of the same strike",
			a.symbol, a.right, a.strike, a.groupID, a.reqID, a.age)
		s.subscribeOptionLeg(a.groupID, a.busIdxs, a.symbol, a.right, a.strike, a.expiry, false, true)
	}

	s.sweepStuckPendingLegs(now)
}

// planDeadLegRepairsLocked decides which legs need repair without performing
// any of it: repair holds the legStale legs cleared by the cooldown and
// per-tick cap (and records the attempt against their backoff), silent holds
// the never-ticked legs, which are reported but never re-subscribed at the
// same strike. Split from the execution so the whole decision matrix —
// health classification, feed-alive gate, backoff, cap — is testable without
// a broker client. Must be called with s.optChain.mu held.
func (s *Session) planDeadLegRepairsLocked(now time.Time) (repair, silent []deadLegAction) {
	lastAny := s.optChain.lastAnyOptionTick

	consider := func(a deadLegAction, subscribedAt, lastTickAt time.Time, key string) {
		health := legHealthAt(subscribedAt, lastTickAt, lastAny, now)
		if health != legStale && health != legSilent {
			return
		}
		a.health = health
		a.age = legAgeString(lastTickAt, now)
		if health == legSilent {
			silent = append(silent, a)
			return
		}
		st := s.optChain.forcedResub[key]
		if !st.last.IsZero() && now.Sub(st.last) < resubCooldown(st.attempts) {
			return
		}
		if len(repair) >= maxForcedResubsPerTick {
			return
		}
		st.last, st.attempts = now, st.attempts+1
		s.optChain.forcedResub[key] = st
		repair = append(repair, a)
	}

	for reqID, req := range s.optChain.mktReqs {
		if req.pending {
			continue // handled by the promote-or-abandon sweep instead
		}
		consider(deadLegAction{
			reqID: reqID, groupID: req.groupID, busIdxs: req.busIdxs,
			symbol: req.symbol, right: req.right, strike: req.strike, expiry: req.expiry,
		}, req.subscribedAt, req.lastTickAt, retryKeyLeg(req.groupID, req.right))
	}
	for reqID, sub := range s.optChain.posSubs {
		consider(deadLegAction{
			reqID: reqID, groupID: -1,
			symbol: sub.symbol, right: sub.right, strike: sub.strike, expiry: sub.expiry,
			pinned: true,
		}, sub.subscribedAt, sub.lastTickAt, posSubKey(sub.symbol, sub.right, sub.strike))
	}
	return repair, silent
}

// sweepStuckPendingLegs promotes or abandons replacement legs stuck pending.
//
// promoteIfReadyLocked is only ever reached from a price tick, so a
// replacement that receives greeks but no price — or nothing at all — stays
// pending forever while the leg it was meant to replace stays active. That
// was harmless when replacements only ever appeared on a strike change; the
// forced-re-subscribe path makes it reachable for a leg that is already known
// to be dead, which would leave the dead leg in place indefinitely.
func (s *Session) sweepStuckPendingLegs(now time.Time) {
	s.optChain.mu.Lock()
	lastAny := s.optChain.lastAnyOptionTick
	type stuck struct {
		reqID  int64
		symbol string
		right  string
		strike float64
		age    string
	}
	var abandoned []stuck
	for reqID, req := range s.optChain.mktReqs {
		if !req.pending || req.pendingSince.IsZero() {
			continue
		}
		if now.Sub(req.pendingSince) <= pendingPromoteGrace {
			continue
		}
		// Try the normal promotion first — it succeeds if any usable price
		// arrived while nothing called it.
		if s.promoteIfReadyLocked(req, reqID) {
			continue
		}
		switch legHealthAt(req.subscribedAt, req.lastTickAt, lastAny, now) {
		case legStale, legSilent:
			// fall through to abandon
		default:
			continue
		}
		abandoned = append(abandoned, stuck{reqID, req.symbol, req.right, req.strike, legAgeString(req.lastTickAt, now)})
		s.dropLegLocked(reqID)
	}
	s.optChain.mu.Unlock()

	for _, a := range abandoned {
		s.optionLog.Printf("Option: WARNING abandoning stuck pending leg %s %s strike=%.2f (reqID=%d) — never produced a usable quote (last tick %s ago)",
			a.symbol, a.right, a.strike, a.reqID, a.age)
	}
}

// posSubKey is the posSubs lookup key. Note it carries no expiry, which is
// what lets a pinned leg be re-subscribed under a new reqID without the key
// changing.
func posSubKey(symbol, right string, strike float64) string {
	return fmt.Sprintf("%s|%s|%.0f", symbol, right, strike)
}

// recoverPinnedLeg alerts on, then re-subscribes, a position-pinned option
// leg that has gone silent.
//
// This is the highest-stakes case on the page. A pinned leg is what stop-loss
// and trailing-stop evaluation price against, so a frozen one does not merely
// look wrong — it silently disarms the exit logic, which then either never
// trips or trips against a stale extreme. The alert fires first and
// independently of the repair succeeding, because a human being told "the
// stop-loss quote for this position is frozen" is worth more than any
// automatic action: it reaches the console banner, the dashboard banner and
// ntfy through the existing outage plumbing.
//
// The re-subscribe allocates a NEW reqID rather than reusing the dead one
// (IB may still consider the old id live) and grants CategoryPosition, which
// is guaranteed and never refused — a held position must never lose its feed
// to line pressure. posSubKeys is re-pointed at the new id; its key format
// carries no expiry, so it survives the swap unchanged.
func (s *Session) recoverPinnedLeg(a deadLegAction, now time.Time) {
	if s.opts.OnError != nil {
		s.opts.OnError(ErrorEvent{
			Type: "subscription",
			Message: fmt.Sprintf("Stop-loss quote frozen: %s %s %.0f has not ticked in %s — re-subscribing.",
				a.symbol, a.right, a.strike, a.age),
		})
	}
	s.optionLog.Printf("Option: WARNING POSITION-PINNED %s %s strike=%.2f (reqID=%d) went SILENT (last tick %s ago) — stop-loss was pricing against a frozen quote; re-subscribing",
		a.symbol, a.right, a.strike, a.reqID, a.age)

	key := posSubKey(a.symbol, a.right, a.strike)

	s.optChain.mu.Lock()
	old, ok := s.optChain.posSubs[a.reqID]
	if !ok || s.optChain.posSubKeys[key] != a.reqID {
		s.optChain.mu.Unlock()
		return // unsubscribed or already recovered since the scan
	}
	newReqID := s.optChain.nextPosID
	s.optChain.nextPosID++
	// Carry the last known values forward so the row does not blank while the
	// replacement warms up, but reset the liveness clocks so the new leg is
	// judged on its own behaviour.
	s.optChain.posSubs[newReqID] = &posStrikeSub{
		symbol: old.symbol, right: old.right, strike: old.strike, expiry: old.expiry,
		reqID: newReqID, price: old.price, bid: old.bid, ask: old.ask,
		delta: old.delta, deltaSource: old.deltaSource, subscribedAt: now,
		refCount: old.refCount,
	}
	s.optChain.posSubKeys[key] = newReqID
	delete(s.optChain.posSubs, a.reqID)
	s.optChain.mu.Unlock()

	s.mdLines.GrantGuaranteed(newReqID, mdlines.CategoryPosition)

	ibRight := "C"
	if a.right == "put" {
		ibRight = "P"
	}
	s.client.ReqMktData(newReqID, makeOptionContract(a.symbol, ibRight, a.strike, a.expiry), "", false, false, nil)

	s.mdLines.Release(a.reqID)
	s.client.CancelMktData(a.reqID)
	s.optionLog.Printf("Option: POSITION-PINNED %s %s strike=%.2f re-subscribed (reqID %d → %d)",
		a.symbol, a.right, a.strike, a.reqID, newReqID)
}

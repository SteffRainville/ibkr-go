// The entry delta probe: at the moment a robot wants to buy, subscribe a few
// strikes around the money, read IB's deltas, keep the one nearest the target.
//
// Robots that share a selector (same symbol, right, option_delay, target_delta)
// and react to the same bar share ONE probe. That is a plain single-flight:
// the first caller owns a probeCall and runs the probe; every other caller
// waits on its done channel and receives the identical answer. The answer is
// written before done is closed, so a waiter can never observe "finished" and
// "no answer yet" at the same time.
//
// It replaces hand-built coordination — an ownership map polled every 100ms, a
// separate share cache for successes, another for failures, a rule that both
// be published in the same critical section as the ownership release, and a
// grace period for waiters whose clocks started before the owner's. Each of
// those was added after an incident caused by the one before it (2026-07-27
// SPY, 09-02 ORCL, 09-22 AMD, 09-28 AGQ); a channel has none of those windows.
package ibkr

import (
	"fmt"
	"math"
	"time"
)

// probeCall is one selector's current or most recent entry probe.
type probeCall struct {
	done     chan struct{} // closed once q/res/finishedAt are final
	deadline time.Time     // when the owner stops waiting for deltas

	q          OptionQuote
	res        EntryStrikeResult
	finishedAt time.Time
}

const (
	// resolvedEntryTTL is how long a successful probe's contract and quote are
	// handed to later callers on the same selector instead of probing again.
	resolvedEntryTTL = 5 * time.Second

	// entryProbeFailCooldown is how long a failed probe's cause is returned to
	// later callers instead of re-asking IB, so a symbol with no obtainable
	// quote cannot spin the caller's event loop on back-to-back probes.
	entryProbeFailCooldown = 15 * time.Second

	// probeJoinSafety bounds a waiter beyond the owner's deadline. The owner
	// always closes done within milliseconds of its deadline; this exists only
	// so a defect cannot park a robot forever.
	probeJoinSafety = 2 * time.Second
)

// ResolveEntryStrike returns the contract nearest the caller's target delta,
// with a live two-sided quote, by probing IB — or the reason it could not.
// Blocks for up to about timeout.
//
// sub identifies the caller so its OWN selector is used when several track
// the same symbol+right at different target deltas (2026-08-04: an IWM call
// priced against a sibling robot's 0.55 target instead of its own 0.60).
func (s *Session) ResolveEntryStrike(sub Subscriber, symbol, right string, timeout time.Duration) (OptionQuote, EntryStrikeResult) {
	busIdx := s.busIndex(sub.Bus())
	now := time.Now()

	s.optChain.mu.Lock()
	sel, ok := s.selectorForLocked(symbol, right, busIdx)
	if !ok {
		s.optChain.mu.Unlock()
		return OptionQuote{}, EntryStrikeResult{Reason: entryFailNoChain,
			Detail: fmt.Sprintf("no option selector tracks %s %s for this robot", symbol, right)}
	}

	if call := s.optChain.probes[sel.id]; call != nil {
		select {
		case <-call.done:
			if call.res.OK && now.Sub(call.finishedAt) <= resolvedEntryTTL {
				s.optChain.mu.Unlock()
				return call.q, call.res
			}
			if !call.res.OK && now.Sub(call.finishedAt) < entryProbeFailCooldown {
				s.optChain.mu.Unlock()
				return OptionQuote{}, call.res
			}
		default:
			s.optChain.mu.Unlock()
			return joinProbe(call, sel)
		}
	}

	if isATMDelta(sel.targetDelta) {
		s.optChain.mu.Unlock()
		return OptionQuote{}, EntryStrikeResult{Reason: entryFailDeltaTargetATM,
			Detail: fmt.Sprintf("target_delta %.2f for %s %s is inside the refused ATM band [0.48, 0.52]", sel.targetDelta, symbol, right)}
	}
	expiry, strikes, ready := s.chainStrikesLocked(sel)
	if !ready {
		s.optChain.mu.Unlock()
		return OptionQuote{}, EntryStrikeResult{Reason: entryFailNoChain,
			Detail: fmt.Sprintf("option chain for %s (delay %d) is not loaded for today yet", symbol, sel.optionDelay)}
	}

	call := &probeCall{done: make(chan struct{}), deadline: now.Add(timeout)}
	s.optChain.probes[sel.id] = call
	s.optChain.mu.Unlock()

	q, res := s.runProbe(sel, expiry, strikes, call.deadline)

	s.optChain.mu.Lock()
	call.q, call.res, call.finishedAt = q, res, time.Now()
	close(call.done)
	s.optChain.mu.Unlock()

	if !res.OK {
		s.optionLog.Printf("Option entry probe FAILED: %s %s (sel=%d) — %s: %s", symbol, right, sel.id, res.Reason, res.Detail)
	}
	return q, res
}

// joinProbe waits for a sibling's probe on the same selector and returns its
// answer.
func joinProbe(call *probeCall, sel selector) (OptionQuote, EntryStrikeResult) {
	wait := time.Until(call.deadline) + probeJoinSafety
	select {
	case <-call.done:
		return call.q, call.res
	case <-time.After(wait):
		return OptionQuote{}, EntryStrikeResult{Reason: entryFailSiblingFailed,
			Detail: fmt.Sprintf("the shared delta probe for %s %s did not finish within %s of its deadline (internal defect)", sel.symbol, sel.right, probeJoinSafety)}
	}
}

// runProbe subscribes the candidate strikes, waits until their deltas settle
// or deadline passes, and resolves the winner. Every line it opens is released
// before it returns.
func (s *Session) runProbe(sel selector, expiry string, strikes []float64, deadline time.Time) (OptionQuote, EntryStrikeResult) {
	symbol, right := sel.symbol, sel.right
	undPrice := s.getUnderlyingPrice(symbol)
	picks := selectStrikeCandidates(strikes, undPrice, right, deltaProbeITMCandidates, deltaProbeOTMCandidates)
	if len(picks) == 0 {
		return OptionQuote{}, EntryStrikeResult{Reason: entryFailNoCandidates,
			Detail: fmt.Sprintf("no %s strike for %s near underlying %.2f in a %d-strike chain", right, symbol, undPrice, len(strikes))}
	}

	ibRight := "C"
	if right == "put" {
		ibRight = "P"
	}
	var cands []*deltaCandidate
	s.optChain.mu.Lock()
	for _, st := range picks {
		reqID := s.nextReqID()
		if !s.mdLines.GrantProbe(reqID) {
			continue
		}
		cand := &deltaCandidate{selectorID: sel.id, symbol: symbol, right: right, strike: st, expiry: expiry, reqID: reqID}
		s.optChain.deltaCands[reqID] = cand
		cands = append(cands, cand)
		s.optionLog.Printf("Option: entry delta candidate %s %s strike=%.2f expiry=%s (reqID=%d)", symbol, right, st, expiry, reqID)
		s.client.ReqMktData(reqID, makeOptionContract(symbol, ibRight, st, expiry), "", false, false, nil)
	}
	s.optChain.mu.Unlock()
	if len(cands) == 0 {
		return OptionQuote{}, EntryStrikeResult{Reason: entryFailNoMDLines,
			Detail: fmt.Sprintf("market-data line budget refused all %d probe candidates for %s %s", len(picks), symbol, right)}
	}

	const pollInterval = 100 * time.Millisecond
	for time.Now().Before(deadline) {
		s.optChain.mu.Lock()
		settled := deltaCandidatesSettled(cands, sel.targetDelta)
		s.optChain.mu.Unlock()
		if settled {
			break
		}
		time.Sleep(pollInterval)
	}
	return s.resolveDeltaCandidates(sel, cands)
}

// resolveDeltaCandidates picks the candidate with delta closest to the target,
// reads its quote, and cancels every candidate including the winner — nothing
// survives as a subscription. If the entry fills, SubscribePositionStrike
// opens the position's own line for the contract.
func (s *Session) resolveDeltaCandidates(sel selector, cands []*deltaCandidate) (OptionQuote, EntryStrikeResult) {
	symbol, right := sel.symbol, sel.right

	s.optChain.mu.Lock()
	var best *deltaCandidate
	bestDist := math.MaxFloat64
	// A priced candidate beats a closer-on-delta one with no price: the entry
	// needs a real quote, and the unpriced one can only fail. Delta decides
	// within each class; an unpriced winner is still returned when nothing is
	// priced, so the failure keeps its option_delta_no_price classification.
	for _, c := range cands {
		if !c.ready {
			continue
		}
		dist := math.Abs(math.Abs(c.delta) - sel.targetDelta)
		if best == nil || (c.quoted() && !best.quoted()) || (c.quoted() == best.quoted() && dist < bestDist) {
			bestDist, best = dist, c
		}
	}

	if best == nil {
		// Classify BEFORE the candidates are released — an IB error stamped on
		// a candidate (noteCandidateError) is the only thing that separates "not
		// entitled to option data" from "IB never answered".
		failure := classifyCandidateErrors(cands)
		cancel := s.releaseCandidatesLocked(cands)
		s.optChain.mu.Unlock()
		s.cancelLines(cancel)
		s.optionLog.Printf("Option delta resolve: %s %s (sel=%d) — %s (%s)", symbol, right, sel.id, failure.Reason, failure.Detail)
		return OptionQuote{}, failure
	}

	// The bid/ask arrived during this bounded probe, so now is an accurate
	// freshness stamp for them.
	now := time.Now()
	q := OptionQuote{Strike: best.strike, Expiry: best.expiry, Bid: best.bid, Ask: best.ask, Delta: best.delta, IV: best.iv, BidTime: now, AskTime: now}
	outcome := EntryStrikeResult{OK: true}
	if !q.Valid() {
		// A winner on delta with no two-sided price: the contract IS quoting
		// Greeks, so entitlement is fine and the price is merely late.
		outcome = EntryStrikeResult{Reason: entryFailDeltaNoPrice,
			Detail: fmt.Sprintf("%s %s strike %.2f matched on delta %.4f but IB sent no two-sided price (bid=%.2f ask=%.2f)",
				symbol, right, best.strike, best.delta, best.bid, best.ask)}
	}
	cancel := s.releaseCandidatesLocked(cands)
	s.optChain.mu.Unlock()

	s.cancelLines(cancel)
	s.optionLog.Printf("Option delta resolved: %s %s (sel=%d) target=%.2f → strike=%.2f (actual delta=%.4f)",
		symbol, right, sel.id, sel.targetDelta, best.strike, best.delta)

	// A miss this large means the ladder carried no strike near the target (a
	// 0DTE chain near the close, or a coarse ladder). The entry is still taken,
	// so this line in error.log is the only signal that it happened.
	if miss := math.Abs(math.Abs(best.delta) - sel.targetDelta); miss > deltaMissWarnThreshold {
		s.logger.Printf("Option: WARNING %s %s (sel=%d) target=%.2f resolved to strike=%.2f at delta=%.4f (miss %.2f) — no nearer strike on this ladder; entry taken",
			symbol, right, sel.id, sel.targetDelta, best.strike, best.delta, miss)
	}
	return q, outcome
}

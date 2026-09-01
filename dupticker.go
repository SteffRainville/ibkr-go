package ibkr

import (
	"fmt"
	"time"

	"github.com/SteffRainville/ibkr-go/mdlines"
)

// errCodeDuplicateTickerID is IB's refusal of a market-data request whose
// ticker id is already live on this connection ("Duplicate ticker id").
const errCodeDuplicateTickerID = 322

// maxDupTickerRepairs bounds how many times one contract may be re-requested
// after a duplicate-id refusal before the leg is dropped and the operator is
// alerted instead. With a single request-id sequence (reqids.go) a 322 cannot
// happen at all, so this is a backstop against an unknown second source of
// collisions turning an error path into a request loop — not a retry budget
// anybody is expected to spend.
const maxDupTickerRepairs = 2

// handleDuplicateTickerID repairs the local state left behind when IB refuses
// a market-data request as a duplicate ticker id. Returns true if reqID was an
// option request this package owns.
//
// The refusal itself is survivable; what is not is that the request was
// rejected while our own bookkeeping had already been updated as though it
// succeeded. Both option paths recorded the id before IB answered:
//
//   - a re-subscribed leg (forceResubscribeLeg) repoints legByReqID at the new
//     id, so every tick arriving under it is decoded as this contract — and if
//     the id was refused *because another contract is streaming on it*, those
//     ticks are that contract's. On 2026-09-01 HOOD's held $2.91 call was
//     repointed onto MU's live 925 call and marked at $21.80, which ran the
//     whole take-profit ladder and booked +639% on a position whose underlying
//     had not moved.
//
//   - a delta-probe candidate holds the id in deltaCands, and its cleanup
//     cancels it. Cancelling an id we do not own tears down the real owner's
//     feed: that is how HOOD's original subscription died 90 seconds before
//     the mis-pricing, which is what invited the re-subscribe in the first
//     place.
//
// So the repair is the same idea on both sides — stop claiming an id that
// belongs to someone else, and never cancel or release it — plus, for a leg, a
// fresh request for the contract that still needs one.
func (s *Session) handleDuplicateTickerID(reqID int64) bool {
	s.optChain.mu.Lock()

	// A probe candidate: leave it in deltaCands so classifyCandidateErrors can
	// still read the cause off it, but flag the id as not ours so
	// resolveDeltaCandidates neither cancels nor releases it.
	if cand, ok := s.optChain.deltaCands[reqID]; ok {
		cand.dupTicker = true
		s.optChain.mu.Unlock()
		s.optionLog.Printf("Option: DUPLICATE TICKER ID on delta candidate %s %s strike=%.2f expiry=%s (reqID=%d) — id belongs to another live request; leaving it alone",
			cand.symbol, cand.right, cand.strike, cand.expiry, reqID)
		s.logger.Printf("Option: DUPLICATE TICKER ID (reqID=%d) refused a delta probe for %s %s strike=%.2f — request-id collision",
			reqID, cand.symbol, cand.right, cand.strike)
		return true
	}

	leg, ok := s.legByReqIDLocked(reqID)
	if !ok || leg.reqID != reqID {
		s.optChain.mu.Unlock()
		return false
	}
	key := leg.key()

	// Detach the reverse index FIRST and unconditionally. Until this line runs,
	// whatever is genuinely streaming under reqID is being decoded as this
	// contract — repricing a held position against a stranger's quote. Nothing
	// below may return early past it.
	delete(s.optChain.legByReqID, reqID)

	// No mdLines.Release(reqID) anywhere in this function: the ledger is keyed
	// by request id, so the entry under this one is the *other* holder's line.
	// Releasing it would free a line IB is still serving and let the pool
	// oversubscribe.

	s.optChain.dupRepairs[key]++
	attempts := s.optChain.dupRepairs[key]
	if attempts > maxDupTickerRepairs {
		s.forgetLegLocked(leg)
		delete(s.optChain.dupRepairs, key)
		s.optChain.mu.Unlock()

		s.optionLog.Printf("Option: DUPLICATE TICKER ID on %s %s strike=%.2f expiry=%s (reqID=%d) — %d repairs exhausted, dropping the leg",
			key.symbol, key.right, key.strike, key.expiry, reqID, maxDupTickerRepairs)
		s.logger.Printf("Option: DUPLICATE TICKER ID exhausted repairs for %s %s strike=%.2f expiry=%s — this contract now has NO market data",
			key.symbol, key.right, key.strike, key.expiry)
		if s.opts.OnError != nil {
			s.opts.OnError(ErrorEvent{
				Type: "subscription",
				Message: fmt.Sprintf("Request-id collision: %s %s %.0f could not be re-subscribed and has no quote — check the position manually.",
					key.symbol, key.right, key.strike),
			})
		}
		return true
	}

	// Re-request the same contract under a fresh id. The leg keeps its holders
	// and its cached values; only the subscription behind it is replaced.
	newReqID := s.nextReqID()
	leg.reqID = newReqID
	leg.subscribedAt = time.Now()
	leg.lastTickAt = time.Time{}
	s.optChain.legByReqID[newReqID] = key
	s.optChain.mu.Unlock()

	s.mdLines.GrantGuaranteed(newReqID, mdlines.CategoryPosition)

	ibRight := "C"
	if key.right == "put" {
		ibRight = "P"
	}
	s.client.ReqMktData(newReqID, makeOptionContract(key.symbol, ibRight, key.strike, key.expiry), "", false, false, nil)

	// Deliberately no CancelMktData(reqID) — see above; that id is another
	// request's, and cancelling it is the half of this bug that kills feeds.
	s.optionLog.Printf("Option: DUPLICATE TICKER ID on %s %s strike=%.2f expiry=%s — detached from reqID=%d (another request owns it) and re-subscribed as reqID=%d (repair %d/%d)",
		key.symbol, key.right, key.strike, key.expiry, reqID, newReqID, attempts, maxDupTickerRepairs)
	s.logger.Printf("Option: DUPLICATE TICKER ID (reqID=%d) — request-id collision on %s %s strike=%.2f; re-subscribed as %d",
		reqID, key.symbol, key.right, key.strike, newReqID)
	return true
}

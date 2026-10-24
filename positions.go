package ibkr

import (
	"fmt"
	"strings"
	"time"

	"github.com/scmhub/ibapi"
)

// AccountSummary is called once per tag per account in response to ReqAccountSummary.
func (s *Session) AccountSummary(reqID int64, account string, tag string, value string, currency string) {
	s.acctLog.Printf("account=%s tag=%s value=%s currency=%s", account, tag, value, currency)

	s.acct.mu.Lock()
	as := s.acct.accounts[account]
	as.Account = account
	as.Currency = currency
	switch tag {
	case "AccountType":
		as.AccountType = value
	case "TradingType":
		as.TradingType = value
	case "AccountAlias":
		as.AccountAlias = value
	case "TotalCashValue":
		as.TotalCashValue = parseFloat(value)
	case "AvailableFunds":
		as.AvailableFunds = parseFloat(value)
	case "BuyingPower":
		as.BuyingPower = parseFloat(value)
	case "LookAheadAvailableFunds":
		as.LookAheadAvailableFunds = parseFloat(value)
	case "InitMarginReq":
		as.InitMarginReq = parseFloat(value)
	case "NetLiquidation":
		as.NetLiquidation = parseFloat(value)
	}
	s.acct.accounts[account] = as
	s.acct.mu.Unlock()

	if s.opts.OnAccountValue != nil {
		s.opts.OnAccountValue(as, tag, value)
	}
}

func parseFloat(s string) float64 {
	var f float64
	_, _ = fmt.Sscanf(s, "%g", &f)
	return f
}

// AccountSnapshots returns the current per-account summary snapshot.
func (s *Session) AccountSnapshots() []AccountSummary {
	s.acct.mu.RLock()
	defer s.acct.mu.RUnlock()
	out := make([]AccountSummary, 0, len(s.acct.accounts))
	for _, a := range s.acct.accounts {
		out = append(out, a)
	}
	return out
}

// AccountSummaryEnd is called when all AccountSummary data has been
// received. Validates that the configured trading account exists among the
// IB accounts — a hard stop, since trading against the wrong account (e.g.
// live vs paper) must never happen silently.
func (s *Session) AccountSummaryEnd(reqID int64) {
	s.logger.Println("AccountSummaryEnd")

	if s.tradingAccount == "" {
		return
	}
	snapshots := s.AccountSnapshots()
	for _, snap := range snapshots {
		if snap.Account == s.tradingAccount {
			return
		}
	}

	known := make([]string, 0, len(snapshots))
	for _, snap := range snapshots {
		known = append(known, snap.Account)
	}
	banner := strings.Repeat("*", 60)
	fmt.Println(banner)
	fmt.Printf("* WARNING: ACCOUNT %s NOT FOUND IN YOUR IBKR ACCOUNTS  *\n", s.tradingAccount)
	fmt.Printf("* Known accounts: %s\n", strings.Join(known, ", "))
	fmt.Println(banner)
	s.logger.Fatalf("FATAL: trading account %q not found in IBKR accounts: %v", s.tradingAccount, known)
}

// Position is called by IB for each position in the account.
//
// Before the first PositionEnd() (initial snapshot): accumulate in posMap.
// After PositionEnd() (real-time subscription updates): apply immediately —
// including to posMap itself, since posMap is the naked-short safety gate's
// (ensureOptionSellable/heldOptionQtyShares in orders.go) sole source of
// truth. A fill that only fired OnPositionUpdate without updating posMap
// left a freshly opened position invisible to that gate until the next
// periodic resync — up to position_refresh_interval_minutes later — so a
// same-session close saw "0 held" and refused to sell a position IB had
// already confirmed (the 2026-08-10 IWM incident).
func (s *Session) Position(account string, contract *ibapi.Contract, position ibapi.Decimal, avgCost float64) {
	posQty := float64(position.Int())
	s.logger.Printf("Position: account=%s symbol=%s pos=%.0f avgCost=%.2f", account, contract.Symbol, posQty, avgCost)

	info := PositionInfo{
		Account:  account,
		Symbol:   contract.Symbol,
		SecType:  contract.SecType,
		Exchange: contract.Exchange,
		Currency: contract.Currency,
		Position: posQty,
		AvgCost:  avgCost,
	}
	if contract.SecType == "OPT" {
		info.Right = contract.Right
		info.Strike = contract.Strike
		info.Expiry = contract.LastTradeDateOrContractMonth
	}
	// Keyed by full leg identity (not just account+Symbol+SecType) so two
	// different option legs of the same underlying — e.g. two robots
	// holding different IWM strikes/rights at once — occupy distinct
	// posMap slots instead of clobbering each other.
	key := account + "|" + legIdentity(info)

	s.pos.posMapMu.Lock()
	done := s.pos.initialDone
	s.pos.posMapMu.Unlock()

	if !done {
		if posQty == 0 {
			// A held leg closing mid-replay: the replay may already have listed
			// it, so drop it from the staged snapshot and let PositionEnd confirm
			// the close against what IB reports (otherwise the swap keeps it).
			s.pos.posMapMu.Lock()
			prev, held := s.pos.posMap[key]
			if s.pos.accum != nil {
				if p, ok := s.pos.accum[key]; ok {
					prev, held = p, held || p.Position != 0
					delete(s.pos.accum, key)
				}
			}
			if held && prev.Position != 0 {
				s.pos.pendingClose[legIdentity(info)] = info
			}
			s.pos.posMapMu.Unlock()
			return
		}
		// A refresh replay is staged in accum and swapped into posMap at
		// PositionEnd, so Positions() — the naked-short gate's only truth — is
		// never a half-rebuilt map. A non-zero push still lands in posMap at
		// once: a fill during the replay window must be sellable immediately.
		s.pos.posMapMu.Lock()
		if s.pos.accum != nil {
			s.pos.accum[key] = info
		}
		s.pos.posMap[key] = info
		s.pos.posMapMu.Unlock()
		return
	}

	if posQty == 0 {
		// IB keeps echoing every contract flat today as pos=0. A zero for a leg
		// this session does not hold is that echo — nothing to confirm. Acting
		// on it re-requested positions, whose replay carried the same echo: a
		// loop that ran twice a second from 09:50 to the close on 2026-10-23.
		s.pos.posMapMu.Lock()
		prev, held := s.pos.posMap[key]
		s.pos.posMapMu.Unlock()
		if !held || prev.Position == 0 {
			return
		}
		// A real-time qty=0 for a held leg is NOT acted on immediately: IB can
		// send a transient or erroneous zero for a position that is still
		// genuinely held. Record the leg and confirm it against a fresh
		// COMPLETE snapshot in PositionEnd.
		legKey := legIdentity(info)
		s.pos.posMapMu.Lock()
		s.pos.pendingClose[legKey] = info
		s.pos.posMapMu.Unlock()
		s.logger.Printf("Position: qty=0 for %s — deferring close pending snapshot confirmation", legKey)
		s.refreshPositions()
		return
	}

	s.pos.posMapMu.Lock()
	s.pos.posMap[key] = info
	s.pos.posMapMu.Unlock()

	if s.opts.OnPositionUpdate != nil {
		s.opts.OnPositionUpdate(info)
	}
}

// legIdentity is the key used to match a deferred close against a fresh
// position snapshot.
func legIdentity(p PositionInfo) string {
	if p.SecType == "OPT" {
		return fmt.Sprintf("%s|OPT|%s|%.4f|%s", p.Symbol, p.Right, p.Strike, p.Expiry)
	}
	return p.Symbol + "|" + p.SecType
}

// partitionPendingCloses splits deferred-close legs into those CONFIRMED
// closed (absent from the fresh complete snapshot) and those still held
// (present — the qty=0 was transient and must be ignored). Pure so it can
// be unit-tested without an IB connection.
func partitionPendingCloses(pending map[string]PositionInfo, snapshot []PositionInfo) (confirmed, stillHeld []PositionInfo) {
	present := make(map[string]bool, len(snapshot))
	for _, p := range snapshot {
		present[legIdentity(p)] = true
	}
	for key, info := range pending {
		if present[key] {
			stillHeld = append(stillHeld, info)
		} else {
			confirmed = append(confirmed, info)
		}
	}
	return confirmed, stillHeld
}

// commitPositionSnapshot ends a replay: the staged snapshot (if a refresh
// staged one) becomes posMap, and the result is returned as the complete
// snapshot.
func (s *Session) commitPositionSnapshot() map[string]PositionInfo {
	s.pos.posMapMu.Lock()
	defer s.pos.posMapMu.Unlock()
	if s.pos.accum != nil {
		s.pos.posMap = s.pos.accum
		s.pos.accum = nil
	}
	snapshot := make(map[string]PositionInfo, len(s.pos.posMap))
	for k, v := range s.pos.posMap {
		snapshot[k] = v
	}
	s.pos.initialDone = true
	s.pos.refreshing = false
	return snapshot
}

// PositionEnd is called when the initial batch of Position() callbacks is complete.
func (s *Session) PositionEnd() {
	snapshot := s.commitPositionSnapshot()

	s.logger.Printf("PositionEnd: %d positions received", len(snapshot))
	s.client.ReqMarketDataType(int64(ibapi.DELAYED_FROZEN))

	positions := make([]PositionInfo, 0, len(snapshot))
	seen := make(map[string]bool)
	for _, p := range snapshot {
		positions = append(positions, p)

		if p.SecType != "STK" {
			s.logger.Printf("Skipping snapshot for %s (SecType=%q)", p.Symbol, p.SecType)
			continue
		}
		if !seen[p.Symbol] && s.stockSnapshotDue(p.Symbol) {
			seen[p.Symbol] = true
			reqID := s.nextReqID()
			s.mktData.snapReqSymbol[reqID] = p.Symbol
			currency := p.Currency
			if currency == "" {
				currency = "USD"
			}
			primaryExchange := p.Exchange
			if primaryExchange == "NASDAQ" {
				primaryExchange = "ISLAND"
			}
			contract := &ibapi.Contract{
				Symbol:          p.Symbol,
				SecType:         "STK",
				Exchange:        "SMART",
				PrimaryExchange: primaryExchange,
				Currency:        currency,
			}
			s.logger.Printf("Snapshot request: %s primaryExchange=%s (reqID=%d)", p.Symbol, p.Exchange, reqID)
			s.mdLines.TrackSnapshot(reqID)
			s.client.ReqMktData(reqID, contract, "", true, false, nil)
		}
	}
	s.client.ReqMarketDataType(int64(ibapi.REALTIME))

	if s.opts.OnPositionSnapshot != nil {
		s.opts.OnPositionSnapshot(positions)
	}

	s.pos.posMapMu.Lock()
	pending := s.pos.pendingClose
	s.pos.pendingClose = make(map[string]PositionInfo)
	s.pos.posMapMu.Unlock()

	if len(pending) > 0 {
		confirmed, stillHeld := partitionPendingCloses(pending, positions)
		for _, info := range stillHeld {
			s.logger.Printf("Position close for %s NOT confirmed — still held in fresh snapshot; ignoring transient qty=0", legIdentity(info))
		}
		for _, info := range confirmed {
			s.logger.Printf("Position close for %s CONFIRMED by fresh snapshot — finalizing", legIdentity(info))
			if s.opts.OnPositionClosed != nil {
				s.opts.OnPositionClosed(info)
			}
		}
	}

	s.pos.posMapMu.Lock()
	again := s.pos.refreshAgain
	s.pos.refreshAgain = false
	s.pos.posMapMu.Unlock()
	if again {
		s.refreshPositions()
	}
}

// posRefreshStaleAfter is how long a refresh may wait for its PositionEnd
// before another is allowed; it only guards against a lost PositionEnd.
const posRefreshStaleAfter = 10 * time.Second

// stockSnapshotMinInterval spaces the price snapshot a PositionEnd requests for
// each held stock. Every snapshot occupies a market-data line until it ends.
const stockSnapshotMinInterval = time.Minute

// stockSnapshotDue reports whether symbol may be snapshotted now, and records
// the request if so.
func (s *Session) stockSnapshotDue(symbol string) bool {
	s.pos.posMapMu.Lock()
	defer s.pos.posMapMu.Unlock()
	if s.pos.snapAt == nil {
		s.pos.snapAt = make(map[string]time.Time)
	}
	if at, ok := s.pos.snapAt[symbol]; ok && time.Since(at) < stockSnapshotMinInterval {
		return false
	}
	s.pos.snapAt[symbol] = time.Now()
	return true
}

// refreshPositions re-requests IB's positions. The replay is staged
// (posMap keeps serving the naked-short gate until PositionEnd swaps the
// complete snapshot in) and single-flight: a refresh already waiting for its
// PositionEnd is not stacked with another. The previous subscription is
// cancelled first so replays never multiply. Used as a safety-net periodic
// sync, to confirm a deferred close and on demand.
func (s *Session) refreshPositions() {
	s.pos.posMapMu.Lock()
	if s.pos.refreshing && time.Since(s.pos.refreshStart) < posRefreshStaleAfter {
		// The replay in flight may predate what triggered this call; run once
		// more when it ends so a deferred close is still confirmed.
		s.pos.refreshAgain = true
		s.pos.posMapMu.Unlock()
		return
	}
	s.pos.refreshing = true
	s.pos.refreshStart = time.Now()
	s.pos.accum = make(map[string]PositionInfo)
	s.pos.initialDone = false
	s.pos.posMapMu.Unlock()

	s.logger.Println("Refreshing positions from IB")
	if s.client == nil {
		return
	}
	s.client.CancelPositions()
	s.client.ReqPositions()
}

// requestPositionsAfterConnect starts the position subscription on a fresh
// connection, discarding any replay staged on the previous one.
func (s *Session) requestPositionsAfterConnect() {
	s.pos.posMapMu.Lock()
	s.pos.refreshing = false
	s.pos.refreshAgain = false
	s.pos.accum = nil
	s.pos.posMapMu.Unlock()
	s.refreshPositions()
}

// RefreshPositions triggers an on-demand IB position re-sync (e.g. from a
// caller's manual "refresh" UI action).
func (s *Session) RefreshPositions() { s.refreshPositions() }

// Positions returns IB's own confirmed portfolio snapshot from the most
// recent PositionEnd, filtered to positions actually held (nonzero).
func (s *Session) Positions() []PositionInfo {
	s.pos.posMapMu.Lock()
	defer s.pos.posMapMu.Unlock()
	out := make([]PositionInfo, 0, len(s.pos.posMap))
	for _, p := range s.pos.posMap {
		if p.Position != 0 {
			out = append(out, p)
		}
	}
	return out
}

package ibkr

// Coverage retired with the background option legs.
//
// Three test files stood here, and every case in them described machinery that
// no longer exists:
//
//   subscribeoptionleg_test.go — pointSelectorAt's four outcomes (already
//     displaying / adopt an existing leg / grant a new line / share the nearest
//     leg when refused), plus the sharing invariants between selectors and pins.
//     No selector holds a market-data line now, so there is nothing to point,
//     adopt, or share.
//
//   pendingleg_test.go — the warming path: a selector kept displaying its old
//     contract until a replacement quoted (pendingSwap / promotePendingLocked),
//     and currentOptionContract preferring the displayed leg over the pending
//     one. A row displays no contract at all, so nothing warms into one.
//
//   strikeretry_test.go — optStrikeRetry's bounded walk to the next-nearest
//     strike after IB error 200. It existed to keep a watchlist row populated
//     when the estimated strike turned out not to be listed; see
//     handleOptionMktError for why neither remaining leg kind wants "any
//     contract that resolves" in place of the one it asked for.
//
// What those tests were ultimately protecting — that one holder cannot cancel a
// contract another still needs — is unchanged and still covered, by
// possub_refcount_test.go, against the only holder kind left: open positions.

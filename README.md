# ibkr-go

A standalone Interactive Brokers (TWS / IB Gateway) connectivity layer for Go: connection lifecycle, bar/tick streaming, order placement, position and account sync, option chain resolution with delta-based strike selection and position-pinning, a market scanner, and a market-data-line-cap governor.

It knows nothing about any particular trading strategy, dashboard, or bot framework. You bring a `Subscriber` (a symbol list, an event bus, an order-priority function, and a "connected" hook); the library handles the IB wire protocol.

First version was hand coded, improvements and unit tests were done by Claude.

## Status

Core is feature-complete and tested (`go test ./...` — 100+ tests, including regression locks for several real production incidents: oversell, drifted-ATM-strike closes, double-fills, market-data-line starvation, snapshot leaks). Not yet run against a live paper account from a fresh checkout — see [Verification](#verification) before trusting it with real money.

No external dependencies beyond [`github.com/scmhub/ibapi`](https://github.com/scmhub/ibapi), the IB API client this library wraps.

## Install

```bash
go get github.com/SteffRainville/ibkr-go
```

## Quick start

```go
sess := ibkr.NewSession(ibkr.Options{
    Port:           7497, // TWS paper
    ClientID:       100,
    TradingAccount: "DU1234567",
}, nil, nil) // nil, nil = let the session create its own quote book / candle store

ctx := context.Background()
if err := sess.Connect(ctx); err != nil {
    log.Fatal(err)
}

sub := myApp.NewSubscriber(...) // implements ibkr.Subscriber
done, err := sess.Run([]ibkr.Subscriber{sub}, time.Now().Add(8*time.Hour))
```

See [`examples/minimal`](examples/minimal) for a runnable version that connects, subscribes to a couple of symbols, and prints bars/quotes to stdout.

## The `Subscriber` interface

One `Subscriber` per independent strategy sharing the session's single IB connection:

```go
type Subscriber interface {
    Symbols() []SymbolSpec               // what to subscribe to
    Bus() *eventbus.Bus                  // where bars/ticks/fills/rejections get published
    OrderPriority(category string) string // IB Adaptive Algo urgency per transaction category
    OnConnected(ctx context.Context)     // called once the session is connected and subscribed
}
```

The library publishes generic events on each subscriber's bus — `KindCandle`, `KindLiveTick`, `KindOrderFilled`, `KindCommissionReport`, `KindOrderRejected`, `KindOrderQtyMismatch`, `KindOptionData` — and exposes `Session.OpenPosition` / `ClosePosition` for order placement, `Session.ResolveEntryStrike` / `CurrentLeg` / `SubscribePositionStrike` for option chain work, and `Session.RunScanner` / `FetchHistorical` for the rest.

## What this library deliberately does not do

Position/dashboard state (a "pending" row, a trailing-stop watermark, which account owns a fill) is your application's job, not this library's. The library owns _what IB says_ — positions, accounts, fills — and pushes it to you via `Options` callbacks (`OnPositionSnapshot`, `OnPositionUpdate`, `OnAccountValue`, ...) and eventbus events; what you _do_ with that (mutate a database row, update a UI) is outside its scope. This is why the library has no dependency on any dashboard or bot framework, and why it's the same ~15 files whether you're running one symbol or five hundred.

## Verification

Before trusting this against a live account:

1. Run `examples/minimal` against **paper trading** (never live) and confirm bars/quotes stream correctly.
2. Exercise the reconnect path: kill TWS mid-session, confirm your app's reconnect loop picks back up.
3. If you use the naked-short gate (`ClosePosition`'s option path) or position-pinning, verify against a paper account with real (paper) positions open.

The unit test suite proves logic; it cannot prove the live IB wire-protocol handshake, which only a real connection exercises.

## License

Not yet decided — this is currently a private extraction, not a public release.

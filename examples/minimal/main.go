// Command minimal connects to a paper-trading TWS/IB Gateway instance,
// subscribes to a couple of symbols, and prints bars and quotes to stdout
// until interrupted.
//
// Run TWS or IB Gateway locally with the paper-trading API port open
// (TWS paper = 7497, Gateway paper = 4002 by default), then:
//
//	go run . -port 7497
//
// Ctrl-C to exit; the session shuts down cleanly.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/scmhub/ibapi"

	"github.com/SteffRainville/ibkr-go"
	"github.com/SteffRainville/ibkr-go/eventbus"
)

func main() {
	port := flag.Int("port", 7497, "TWS/IB Gateway API port (7497 = TWS paper, 4002 = Gateway paper)")
	clientID := flag.Int64("client-id", 100, "IB API client ID")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	bus := eventbus.New()
	sub := &printSubscriber{
		bus: bus,
		symbols: []ibkr.SymbolSpec{
			stockSpec("AAPL"),
			stockSpec("SPY"),
		},
	}

	sess := ibkr.NewSession(ibkr.Options{
		Port:     *port,
		ClientID: *clientID,
		Logger:   log.Default(),
	}, nil, nil)

	fmt.Printf("Connecting to 127.0.0.1:%d (client ID %d)...\n", *port, *clientID)
	if err := sess.Connect(ctx); err != nil {
		log.Fatalf("connect failed: %v", err)
	}
	fmt.Println("Connected. Streaming bars and quotes — Ctrl-C to stop.")

	// Print every candle and live tick as it arrives.
	go func() {
		ch := bus.Subscribe(eventbus.KindCandle, eventbus.KindLiveTick)
		defer bus.Unsubscribe(ch)
		for evt := range ch {
			switch evt.Kind {
			case eventbus.KindCandle:
				c := evt.Payload.(eventbus.Candle)
				fmt.Printf("[bar] %-6s %s  O:%.2f H:%.2f L:%.2f C:%.2f  live=%v\n",
					c.Symbol, c.Date, c.Open, c.High, c.Low, c.Close, c.IsLive)
			case eventbus.KindLiveTick:
				t := evt.Payload.(eventbus.LiveTick)
				fmt.Printf("[tick] %-6s %s  price=%.2f\n", t.Symbol, t.Date, t.Price)
			}
		}
	}()

	stop := time.Now().Add(8 * time.Hour) // Run() needs a stop time; Ctrl-C exits sooner via ctx
	done, err := sess.Run([]ibkr.Subscriber{sub}, stop)
	if err != nil {
		log.Fatalf("session ended with error: %v", err)
	}
	if done {
		fmt.Println("Stop time reached — clean shutdown.")
	} else {
		fmt.Println("Disconnected.")
	}
}

// stockSpec builds a SymbolSpec for a simple SMART-routed US stock.
func stockSpec(symbol string) ibkr.SymbolSpec {
	return ibkr.SymbolSpec{
		Symbol:   symbol,
		Tag:      "long",
		Name:     symbol,
		Exchange: "SMART",
		Currency: "USD",
		Contract: &ibapi.Contract{
			Symbol:   symbol,
			SecType:  "STK",
			Exchange: "SMART",
			Currency: "USD",
		},
	}
}

// printSubscriber is the simplest possible ibkr.Subscriber: it has no
// dashboard, no bot, nothing to prioritize — just a symbol list and a bus.
type printSubscriber struct {
	bus     *eventbus.Bus
	symbols []ibkr.SymbolSpec
}

func (s *printSubscriber) Symbols() []ibkr.SymbolSpec           { return s.symbols }
func (s *printSubscriber) Bus() *eventbus.Bus                   { return s.bus }
func (s *printSubscriber) OrderPriority(category string) string { return "" }
func (s *printSubscriber) OnConnected(ctx context.Context)      {}

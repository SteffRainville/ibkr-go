// Command minimal is a placeholder example for the ibkr-go module.
//
// It will grow into a small program that connects to a paper-trading TWS/IB
// Gateway instance, subscribes to a couple of symbols, and prints bars and
// quotes to stdout — once the Session/Subscriber API (Phase 4 of the
// extraction) lands. For now it just proves the module's leaf packages
// (quotes, candlestore, eventbus) compose correctly.
package main

import (
	"fmt"

	"github.com/SteffRainville/ibkr-go/candlestore"
	"github.com/SteffRainville/ibkr-go/eventbus"
	"github.com/SteffRainville/ibkr-go/quotes"
)

func main() {
	book := quotes.NewBook()
	store := candlestore.New()
	bus := eventbus.New()

	ch := bus.Subscribe(eventbus.KindCandle)
	defer bus.Unsubscribe(ch)

	fmt.Println("ibkr-go scaffold OK — quotes.Book, candlestore.Store, eventbus.Bus constructed")
	_ = book
	_ = store
}

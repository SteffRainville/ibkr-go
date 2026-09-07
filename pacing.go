package ibkr

import "time"

// reqPacingBatchSize and reqPacingDelay bound a burst of outbound IB request
// messages to roughly reqPacingBatchSize/reqPacingDelay msgs/sec. IB's
// documented general message limit is 50 outgoing API messages per second,
// counting every request type together — historical-data and market-data
// requests included. Connect (Session.Run) and a symbol reload
// (ResyncSymbols) both fire a ReqHistoricalData and a ReqMktData call per
// subscribed symbol back-to-back with nothing to space them out; at ~30
// symbols that is ~60 messages dispatched in a handful of milliseconds —
// already within reach of the ceiling, and it only grows as symbols/robots
// are added. 10 messages per 250ms keeps sustained throughput at 40/sec,
// comfortable margin under the limit, without meaningfully slowing down a
// connect that today completes in well under a second either way.
const (
	reqPacingBatchSize = 10
	reqPacingDelay     = 250 * time.Millisecond
)

// errCodeHistoricalDataPacing is IB's "Historical Market Data Service error
// message" code. It is a grab-bag (also covers a cancelled scanner
// subscription, "no data for the range", ...) — see the errCode ==
// errCodeHistoricalDataPacing branch in Error() (session.go) for why a bare
// code check is not enough to identify an actual pacing rejection.
const errCodeHistoricalDataPacing = 162

// reqPacer paces a sequence of outbound request messages. Call pace() once
// per message, after sending it; every reqPacingBatchSize calls it sleeps
// reqPacingDelay. A single pacer shared across multiple loops (e.g. the
// historical-data loop and the market-data loop in Session.Run) paces their
// combined message count, not each loop independently — IB counts every
// outbound message toward one shared per-second budget regardless of type.
type reqPacer struct {
	sent int
}

func (p *reqPacer) pace() {
	p.sent++
	if p.sent%reqPacingBatchSize == 0 {
		time.Sleep(reqPacingDelay)
	}
}

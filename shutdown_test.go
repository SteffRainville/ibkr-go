package ibkr

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/scmhub/ibapi"
	"github.com/SteffRainville/ibkr-go/mdlines"
)

// fakeTWS is just enough of TWS for a teardown test: it completes the API
// handshake at a pre-protobuf server version, records the message ID of every
// request it receives until the client hangs up, and answers ReqCurrentTime.
type fakeTWS struct {
	ln       net.Listener
	received chan []int // every request's message ID, sent once the socket closes
}

func startFakeTWS(t *testing.T) *fakeTWS {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeTWS{ln: ln, received: make(chan []int, 1)}
	go f.serve()
	t.Cleanup(func() { ln.Close() })
	return f
}

func (f *fakeTWS) port() int { return f.ln.Addr().(*net.TCPAddr).Port }

func (f *fakeTWS) serve() {
	conn, err := f.ln.Accept()
	if err != nil {
		return
	}
	defer conn.Close()
	r := bufio.NewReader(conn)

	// Handshake: "API\0", then the length-prefixed client version range.
	if _, err := io.ReadFull(r, make([]byte, 4)); err != nil {
		return
	}
	if _, err := readFrame(r); err != nil {
		return
	}
	writeFrame(conn, "176", time.Now().Format("20060102 15:04:05 EST")) // < 201: text encoding

	var ids []int
	defer func() { f.received <- ids }()
	for {
		frame, err := readFrame(r)
		if err != nil {
			return // the client disconnected
		}
		id, _ := strconv.Atoi(string(bytes.SplitN(frame, []byte{0}, 2)[0]))
		ids = append(ids, id)
		if id == int(ibapi.REQ_CURRENT_TIME) {
			writeFrame(conn, strconv.Itoa(int(ibapi.CURRENT_TIME)), "1", strconv.FormatInt(time.Now().Unix(), 10))
		}
	}
}

func readFrame(r *bufio.Reader) ([]byte, error) {
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return nil, err
	}
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}

func writeFrame(w io.Writer, fields ...string) {
	var payload bytes.Buffer
	for _, f := range fields {
		payload.WriteString(f)
		payload.WriteByte(0)
	}
	binary.Write(w, binary.BigEndian, uint32(payload.Len()))
	w.Write(payload.Bytes())
}

// TestCloseDeliversEveryCancelBeforeDisconnecting pins the Ctrl-C leak: a
// session that disconnected straight after issuing its cancels lost them in
// ibapi's request queue, and TWS kept the historical streams alive until it
// was restarted. Every cancel must reach the socket before it closes.
func TestCloseDeliversEveryCancelBeforeDisconnecting(t *testing.T) {
	tws := startFakeTWS(t)

	s := NewSession(Options{Logger: log.New(io.Discard, "", 0)}, nil, nil)
	s.client = ibapi.NewEClient(s)
	if err := s.client.Connect("127.0.0.1", tws.port(), 1); err != nil {
		t.Fatalf("connect to fake TWS: %v", err)
	}

	// More standing requests than ibapi's 10-slot request queue holds, so a
	// disconnect that does not wait for the writer is bound to drop some.
	const hist, lines = 30, 30
	for i := range hist {
		s.mdLines.GrantHist(int64(1000 + i))
	}
	for i := range lines {
		s.mdLines.GrantGuaranteed(int64(2000+i), mdlines.CategoryStock)
	}
	s.acctSummaryID = 7

	start := time.Now()
	s.Close(2 * time.Second)
	if time.Since(start) >= 2*time.Second {
		t.Error("Close waited out its timeout: the CurrentTime barrier was never answered")
	}

	var ids []int
	select {
	case ids = <-tws.received:
	case <-time.After(3 * time.Second):
		t.Fatal("fake TWS never saw the client disconnect")
	}
	count := map[int]int{}
	for _, id := range ids {
		count[id]++
	}
	for _, want := range []struct {
		name string
		id   ibapi.OUT
		n    int
	}{
		{"CancelHistoricalData", ibapi.CANCEL_HISTORICAL_DATA, hist},
		{"CancelMktData", ibapi.CANCEL_MKT_DATA, lines},
		{"CancelAccountSummary", ibapi.CANCEL_ACCOUNT_SUMMARY, 1},
		{"CancelPositions", ibapi.CANCEL_POSITIONS, 1},
		{"ReqCurrentTime", ibapi.REQ_CURRENT_TIME, 1},
	} {
		if got := count[int(want.id)]; got != want.n {
			t.Errorf("%s reached TWS %d time(s) before the disconnect, want %d", want.name, got, want.n)
		}
	}
}

// TestCloseWithoutConnectionIsANoOp: Ctrl-C can arrive before the first
// connection or between reconnects.
func TestCloseWithoutConnectionIsANoOp(t *testing.T) {
	s := NewSession(Options{Logger: log.New(io.Discard, "", 0)}, nil, nil)
	s.Close(time.Second) // no client yet
	s.client = ibapi.NewEClient(s)
	s.Close(time.Second) // client never connected
}

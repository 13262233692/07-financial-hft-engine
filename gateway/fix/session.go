package fix

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hft-engine/model"
)

type OrderCallback func(*model.Order)
type CancelCallback func(symbol string, orderID uint64)

type Session struct {
	senderCompID  string
	targetCompID  string
	seqIn         uint64
	seqOut        uint64
	heartBtInt    int
	lastRecvTime  atomic.Int64
	lastSendTime  atomic.Int64
	authenticated bool
	onOrder       OrderCallback
	onCancel      CancelCallback
	outCh         chan<- []byte
	mu            sync.Mutex
	loggedIn      bool
}

func NewSession(senderCompID, targetCompID string, heartBtInt int, outCh chan<- []byte, onOrder OrderCallback, onCancel CancelCallback) *Session {
	now := time.Now().UnixNano()
	s := &Session{
		senderCompID: senderCompID,
		targetCompID: targetCompID,
		heartBtInt:   heartBtInt,
		onOrder:      onOrder,
		onCancel:     onCancel,
		outCh:        outCh,
	}
	s.lastRecvTime.Store(now)
	s.lastSendTime.Store(now)
	return s
}

func (s *Session) HandleMessage(msg *FIXMessage) {
	s.lastRecvTime.Store(time.Now().UnixNano())

	msgType := msg.MsgType()

	switch msgType {
	case MsgTypeLogon:
		s.handleLogon(msg)
	case MsgTypeLogout:
		s.handleLogout(msg)
	case MsgTypeHeartbeat:
		// no-op, lastRecvTime already updated
	case MsgTypeTestRequest:
		s.handleTestRequest(msg)
	case MsgTypeNewOrderSingle:
		s.handleNewOrderSingle(msg)
	case MsgTypeOrderCancel:
		s.handleOrderCancel(msg)
	default:
		log.Printf("[FIX] unhandled msg type: %s from %s", msgType, s.senderCompID)
	}
}

func (s *Session) handleLogon(msg *FIXMessage) {
	heartBtStr, ok := msg.Get(108)
	if ok {
		_, err := ParseSide(heartBtStr)
		if err != nil {
			hb, err2 := msg.GetInt(108)
			if err2 == nil {
				s.heartBtInt = hb
			}
		}
	}

	s.mu.Lock()
	s.authenticated = true
	s.loggedIn = true
	s.mu.Unlock()

	seq := atomic.AddUint64(&s.seqOut, 1)
	resp := MakeLogonResponse(s.targetCompID, s.senderCompID, int(seq), s.heartBtInt)
	s.send(resp)
	log.Printf("[FIX] Logon accepted from %s", s.senderCompID)
}

func (s *Session) handleLogout(msg *FIXMessage) {
	s.mu.Lock()
	s.loggedIn = false
	s.authenticated = false
	s.mu.Unlock()

	seq := atomic.AddUint64(&s.seqOut, 1)
	resp := MakeLogout(s.targetCompID, s.senderCompID, "Logout acknowledged", int(seq))
	s.send(resp)
	log.Printf("[FIX] Logout from %s", s.senderCompID)
}

func (s *Session) handleTestRequest(msg *FIXMessage) {
	testReqID, _ := msg.Get(112)
	seq := atomic.AddUint64(&s.seqOut, 1)
	resp := MakeHeartbeat(s.targetCompID, s.senderCompID, int(seq), testReqID)
	s.send(resp)
}

func (s *Session) handleNewOrderSingle(msg *FIXMessage) {
	if !s.isAuthenticated() {
		log.Printf("[FIX] received NewOrderSingle from unauthenticated session %s", s.senderCompID)
		return
	}

	clOrdID, _ := msg.Get(11)
	symbol, _ := msg.Get(55)
	sideStr, _ := msg.Get(54)
	ordTypeStr, _ := msg.Get(40)
	priceStr, _ := msg.Get(44)
	qtyStr, _ := msg.Get(38)

	if clOrdID == "" || symbol == "" || sideStr == "" || qtyStr == "" {
		s.sendReject(clOrdID, "missing required fields")
		return
	}

	sideInt, err := ParseSide(sideStr)
	if err != nil {
		s.sendReject(clOrdID, "invalid side")
		return
	}

	qty, err := strconvParseInt(qtyStr)
	if err != nil || qty <= 0 {
		s.sendReject(clOrdID, "invalid quantity")
		return
	}

	var side model.Side
	if sideInt == 1 {
		side = model.SideBuy
	} else {
		side = model.SideSell
	}

	var otype model.OrderType
	var price int64
	if ordTypeStr == "2" {
		otype = model.OrderTypeMarket
		price = 0
	} else {
		otype = model.OrderTypeLimit
		price, err = strconvParseInt(priceStr)
		if err != nil || price <= 0 {
			s.sendReject(clOrdID, "invalid price for limit order")
			return
		}
	}

	order := model.NewOrder(0, clOrdID, symbol, side, otype, price, qty, s.senderCompID)

	if s.onOrder != nil {
		s.onOrder(order)
	}
}

func (s *Session) handleOrderCancel(msg *FIXMessage) {
	if !s.isAuthenticated() {
		return
	}

	origClOrdID, _ := msg.Get(41)
	symbol, _ := msg.Get(55)
	clOrdID, _ := msg.Get(11)

	_ = origClOrdID
	_ = clOrdID

	if symbol == "" {
		seq := atomic.AddUint64(&s.seqOut, 1)
		rej := MakeOrderCancelReject(s.targetCompID, s.senderCompID, int(seq), clOrdID, 0, "missing symbol")
		s.send(rej)
		return
	}

	log.Printf("[FIX] Cancel request from %s for symbol %s", s.senderCompID, symbol)
}

func (s *Session) send(data []byte) {
	s.lastSendTime.Store(time.Now().UnixNano())
	select {
	case s.outCh <- data:
	default:
		log.Printf("[FIX] output channel full, dropping message to %s", s.senderCompID)
	}
}

func (s *Session) sendReject(clOrdID, text string) {
	seq := atomic.AddUint64(&s.seqOut, 1)
	rej := MakeExecutionReport(s.targetCompID, s.senderCompID, int(seq),
		clOrdID, "", "", "", 0, 0, 0, 0, 0, "8", text)
	s.send(rej)
}

func (s *Session) SendExecutionReport(clOrdID, symbol, side, ordType string,
	price, qty, cumQty, avgPx int64, orderID uint64, ordStatus, text string) {

	s.mu.Lock()
	defer s.mu.Unlock()

	seq := atomic.AddUint64(&s.seqOut, 1)
	report := MakeExecutionReport(s.targetCompID, s.senderCompID, int(seq),
		clOrdID, symbol, side, ordType, price, qty, cumQty, avgPx, orderID, ordStatus, text)
	s.send(report)
}

func (s *Session) isAuthenticated() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.authenticated
}

func (s *Session) IsLoggedIn() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loggedIn
}

func (s *Session) CheckHeartbeat(timeout time.Duration) bool {
	lastRecv := time.Unix(0, s.lastRecvTime.Load())
	return time.Since(lastRecv) < timeout
}

func (s *Session) SendHeartbeat() {
	seq := atomic.AddUint64(&s.seqOut, 1)
	hb := MakeHeartbeat(s.targetCompID, s.senderCompID, int(seq), "")
	s.send(hb)
}

func strconvParseInt(s string) (int64, error) {
	var result int64
	var negative bool
	i := 0
	if len(s) > 0 && s[0] == '-' {
		negative = true
		i = 1
	}
	for ; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, errInvalidNumber
		}
		result = result*10 + int64(s[i]-'0')
	}
	if negative {
		result = -result
	}
	return result, nil
}

var errInvalidNumber = &parseError{"invalid number"}

type parseError struct {
	msg string
}

func (e *parseError) Error() string { return e.msg }

package fix

import (
	"bufio"
	"bytes"
	"context"
	"log"
	"net"
	"sync"
	"time"

	"github.com/hft-engine/engine"
	"github.com/hft-engine/model"
)

type Gateway struct {
	listener     net.Listener
	addr         string
	me           *engine.MatchingEngine
	sessions     map[string]*Session
	sessionMu    sync.RWMutex
	heartbeatCh  chan struct{}
	targetCompID string
	wg           sync.WaitGroup
	cancel       context.CancelFunc
}

func NewGateway(addr, targetCompID string, me *engine.MatchingEngine) *Gateway {
	return &Gateway{
		addr:         addr,
		me:           me,
		sessions:     make(map[string]*Session),
		heartbeatCh:  make(chan struct{}, 1),
		targetCompID: targetCompID,
	}
}

func (g *Gateway) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	g.cancel = cancel

	listener, err := net.Listen("tcp", g.addr)
	if err != nil {
		return err
	}
	g.listener = listener

	log.Printf("[FIX] Gateway listening on %s", g.addr)

	g.wg.Add(1)
	go g.heartbeatMonitor(ctx)

	g.wg.Add(1)
	go g.acceptLoop(ctx)

	return nil
}

func (g *Gateway) Stop() {
	if g.cancel != nil {
		g.cancel()
	}
	if g.listener != nil {
		g.listener.Close()
	}
	g.wg.Wait()
	log.Println("[FIX] Gateway stopped")
}

func (g *Gateway) acceptLoop(ctx context.Context) {
	defer g.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn, err := g.listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				log.Printf("[FIX] Accept error: %v", err)
				continue
			}
		}

		g.wg.Add(1)
		go g.handleConnection(ctx, conn)
	}
}

func (g *Gateway) handleConnection(ctx context.Context, conn net.Conn) {
	defer g.wg.Done()
	defer conn.Close()

	outCh := make(chan []byte, 4096)
	var senderCompID string
	var session *Session

	defer func() {
		if senderCompID != "" {
			g.sessionMu.Lock()
			delete(g.sessions, senderCompID)
			g.sessionMu.Unlock()
		}
		close(outCh)
	}()

	go func() {
		for data := range outCh {
			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if _, err := conn.Write(data); err != nil {
				log.Printf("[FIX] Write error to %s: %v", senderCompID, err)
				return
			}
		}
	}()

	reader := bufio.NewReaderSize(conn, 65536)
	var buf bytes.Buffer

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(time.Duration(30) * time.Second))
		line, err := reader.ReadBytes(SOH)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				if session != nil && session.IsLoggedIn() {
					session.SendHeartbeat()
				}
				buf.Write(line)
				continue
			}
			if err.Error() != "EOF" {
				log.Printf("[FIX] Read error from %s: %v", senderCompID, err)
			}
			return
		}

		buf.Write(line)

		for {
			msg, consumed, err := ReadFIXMessage(&buf)
			if err != nil {
				log.Printf("[FIX] Parse error: %v", err)
				buf.Reset()
				break
			}
			if msg == nil {
				break
			}

			buf.Next(consumed)

			scid := msg.SenderCompID()
			if scid == "" {
				continue
			}

			if session == nil || senderCompID != scid {
				senderCompID = scid
				session = NewSession(senderCompID, g.targetCompID, 30, outCh,
					func(order *model.Order) {
						result := g.me.SubmitOrder(order)
						if result != nil && result.Order != nil && result.Order.ID > 0 {
							session.TrackOrder(order.ClOrdID, result.Order.ID)
						}
						g.sendResult(session, result)
					},
					func(symbol string, orderID uint64) *engine.CancelEngineResult {
						return g.me.CancelOrder(symbol, orderID)
					},
				)

				g.sessionMu.Lock()
				g.sessions[senderCompID] = session
				g.sessionMu.Unlock()
			}

			session.HandleMessage(msg)
		}
	}
}

func (g *Gateway) sendResult(session *Session, result *engine.OrderResult) {
	if session == nil || result == nil {
		return
	}

	order := result.Order
	if order == nil {
		return
	}

	sideStr := "1"
	if order.Side == model.SideSell {
		sideStr = "2"
	}

	ordTypeStr := "2"
	if order.Type == model.OrderTypeLimit {
		ordTypeStr = "1"
	}

	status := order.GetStatus()
	var ordStatus string
	switch status {
	case model.OrderStatusNew:
		ordStatus = "0"
	case model.OrderStatusPartially:
		ordStatus = "1"
	case model.OrderStatusFilled:
		ordStatus = "2"
	case model.OrderStatusCancelled:
		ordStatus = "4"
	case model.OrderStatusRejected:
		ordStatus = "8"
	default:
		ordStatus = "0"
	}

	filledQty := order.GetFilledQty()
	avgPx := int64(0)
	if filledQty > 0 && len(result.Trades) > 0 {
		var totalValue, totalQty int64
		for _, t := range result.Trades {
			totalValue += t.Price * t.Quantity
			totalQty += t.Quantity
		}
		if totalQty > 0 {
			avgPx = totalValue / totalQty
		}
	}

	session.SendExecutionReport(
		order.ClOrdID,
		order.Symbol,
		sideStr,
		ordTypeStr,
		order.Price,
		order.Quantity,
		filledQty,
		avgPx,
		order.ID,
		ordStatus,
		"",
	)
}

func (g *Gateway) heartbeatMonitor(ctx context.Context) {
	defer g.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.sessionMu.RLock()
			for id, session := range g.sessions {
				if !session.CheckHeartbeat(time.Duration(session.heartBtInt*2) * time.Second) {
					log.Printf("[FIX] Session %s heartbeat timeout, disconnecting", id)
					session.SendHeartbeat()
				}
			}
			g.sessionMu.RUnlock()
		}
	}
}
